package backend

import (
	"context"
	"errors"
	"hash/fnv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

// affinityOwnerGrace is how long a workflow item waits for its warm owner
// stream before any free stream may take it. Sends are pipelined, so the
// owner is only slow when its whole outbox is full; a short grace keeps the
// turn a delta send instead of a full-history resend on a cold stream.
const affinityOwnerGrace = 2 * time.Millisecond

// maxWarmInstancesPerStream bounds the number of warm (stateful-history) instance
// entries tracked per work-item stream. Evicting an entry only costs a single
// full-history send the next time that instance runs, so this is a soft cap.
const maxWarmInstancesPerStream = 1_000_000

// streamState holds the per-connection state for a single GetWorkItems stream.
// It is created when a worker connects and discarded when the stream closes.
type streamState struct {
	// id is the stream's unique identifier, also used as the node key for
	// rendezvous (HRW) affinity hashing.
	id string

	// ch delivers work items routed to this specific stream by instance
	// affinity. Small buffer (see newStreamState): a routed item is taken
	// when the stream's dispatch loop is ready or queued briefly while it
	// is mid-send; the producer falls back to the shared queue (any
	// stream) when the buffer is full.
	ch chan *protos.WorkItem

	// statefulHistory is true if the worker advertised
	// WORKER_CAPABILITY_STATEFUL_HISTORY, meaning it retains an instance's
	// committed history between turns so the service can send only deltas.
	statefulHistory bool

	// healthPing is true if the worker advertised
	// WORKER_CAPABILITY_HEALTH_PING, so it may be sent periodic HealthPing
	// work items to keep the stream from looking idle to proxies.
	healthPing bool

	// warm maps an instance ID to the number of committed (past) history events
	// this stream is believed to already hold for it, i.e. the length of the
	// pastEvents prefix that may be omitted on the next turn. Only accessed from
	// the owning stream's GetWorkItems dispatch loop, so it needs no locking.
	warm map[api.InstanceID]int

	// maxWarm is the soft cap on warm entries; defaults to maxWarmInstancesPerStream.
	maxWarm int

	// sendMu serializes producer sends into ch against teardown: producers
	// hold it shared around the closed check and the send, teardown takes it
	// exclusively to flip closed before draining. That guarantees no producer
	// is between its closed check and its send when the buffer is drained, so
	// nothing can land in the buffer afterwards.
	sendMu sync.RWMutex

	// closed is set (under sendMu) when the stream begins tearing down.
	// Producers must not route new items into ch once set: the buffer is
	// drained and re-offered to the shared queue, and anything routed in
	// afterwards would be orphaned with the dead stream. Read lock-free by
	// affinityStreamOwner as a cheap skim; trySend re-checks under sendMu.
	closed atomic.Bool
}

func newStreamState(id string, req *protos.GetWorkItemsRequest) *streamState {
	s := &streamState{
		id: id,
		// Small buffer: the affinity fast path can queue a few turns for
		// the warm owner while it is mid-send, keeping the delta
		// optimization instead of spilling to the shared queue (full
		// history resend) whenever the owner is briefly busy. The
		// overflow fallback to the shared queue is unchanged, so a slow
		// owner still never stalls an instance. Undelivered items in a
		// dying stream's buffer recover via the turn-timeout retry path,
		// the same class as a mid-send disconnect.
		ch:      make(chan *protos.WorkItem, 8),
		warm:    make(map[api.InstanceID]int),
		maxWarm: maxWarmInstancesPerStream,
	}
	for _, c := range req.GetCapabilities() {
		switch c {
		case protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY:
			s.statefulHistory = true
		case protos.WorkerCapability_WORKER_CAPABILITY_HEALTH_PING:
			s.healthPing = true
		}
	}
	return s
}

// dispatchWorkflowWorkItem delivers a workflow work item to the stream that owns the
// instance under affinity, falling back to the shared queue (any stream) when the owner
// is not connected or not ready. Affinity keeps an instance's turns on the stream that
// already holds its cached history, so subsequent turns are sent as deltas (cachedHistory)
// rather than full histories. It is purely an optimization: a fallback send is always
// safe (the receiving stream just sends the full history), so the producer never blocks
// solely on the owner.
func (g *grpcExecutor) dispatchWorkflowWorkItem(ctx context.Context, iid api.InstanceID, wi *protos.WorkItem) error {
	owner := g.affinityStreamOwner(iid)
	if owner != nil {
		// Fast path: the owner is parked and ready, hand it over directly.
		if owner.trySend(wi) {
			return nil
		}
		// Owner busy: give it a short grace to drain before falling to the
		// shared queue, so a momentarily busy owner keeps the delta send.
		// After the grace any free stream may take over, preserving the
		// guarantee that the producer never blocks solely on the owner. Both
		// sends hold the stream's send lock shared, so they serialize with
		// teardown: an item either lands before the teardown drain (and is
		// re-offered to the shared queue) or the closed check diverts it
		// here; it can never land in the buffer after the drain.
		ok, err := owner.trySendGrace(ctx, wi, affinityOwnerGrace)
		if err != nil {
			return err
		}
		if ok {
			return nil
		}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case g.workItemQueue <- wi:
		return nil
	}
}

// drainStreamBuffer marks the stream closed and re-offers work items still
// sitting in its affinity buffer to the shared queue so a surviving stream
// delivers them. Taking sendMu exclusively before flipping closed waits out
// any producer already between its closed check and its send, so once the
// drain starts nothing can land in the buffer: a single drain is complete.
func (g *grpcExecutor) drainStreamBuffer(ss *streamState) {
	ss.sendMu.Lock()
	ss.closed.Store(true)
	ss.sendMu.Unlock()

	for {
		select {
		case wi := <-ss.ch:
			g.requeueWorkItem(wi)
		default:
			return
		}
	}
}

// requeueWorkItem settles an undelivered work item pulled from a closed
// stream's buffer: back onto the shared queue when it has capacity (the item
// was never sent to any worker, so redelivery is safe), otherwise by
// cancelling the task so the backend's retry path re-derives it. Both are
// gated on the item still being the instance's tracked dispatch (completion
// token match): a superseded or already-settled attempt is dropped so it
// cannot disturb a newer registration. A live matching item must not be
// given up on: a transient cancel error is retried with capped backoff,
// alternating with the requeue attempt, until it is settled; an unknown
// registration means it settled concurrently.
func (g *grpcExecutor) requeueWorkItem(wi *protos.WorkItem) {
	x, ok := wi.GetRequest().(*protos.WorkItem_WorkflowRequest)
	if !ok {
		// Only workflow items are affinity-routed, so only they can be
		// drained here.
		return
	}
	iid := api.InstanceID(x.WorkflowRequest.GetInstanceId())

	backoff := 10 * time.Millisecond
	for {
		value, tracked := g.pendingWorkflows.Load(iid)
		p, pok := value.(*pendingWorkflow)
		if !tracked || !pok || p.completionToken != wi.GetCompletionToken() {
			g.logger.Debugf("dropping drained work item for %s: its dispatch was superseded or already settled", iid)
			return
		}

		g.queueLock.RLock()
		requeued := false
		if !g.queueClosed {
			select {
			case g.workItemQueue <- wi:
				requeued = true
			default:
			}
		}
		g.queueLock.RUnlock()
		if requeued {
			return
		}

		if value, ok := g.pendingWorkflows.Load(iid); !ok || value != any(p) {
			g.logger.Debugf("dropping drained work item for %s: its dispatch settled during the drain", iid)
			return
		}

		err := g.backend.CancelWorkflowTask(context.Background(), iid)
		if err == nil {
			g.logger.Warnf("cannot requeue work item while draining a closed stream; cancelled workflow task for %s so it is redelivered", iid)
			return
		}
		if api.IsUnknownInstanceIDError(err) || errors.Is(err, api.ErrInstanceNotFound) {
			// The registration is gone: the attempt settled concurrently.
			return
		}
		g.logger.Warnf("failed to cancel workflow task for %s while draining a closed stream; retrying in %s: %v", iid, backoff, err)
		time.Sleep(backoff)
		if backoff < time.Second {
			backoff *= 2
		}
	}
}

// affinityStreamOwner returns the stateful-history-capable stream that owns the instance
// under rendezvous (HRW) hashing, or nil if no capable stream is connected. HRW pins an
// instance to the same stream as workers come and go: a membership change only reassigns
// the instances whose top-scoring stream actually changed (~1/N of them), so most caches
// survive a connect or disconnect, unlike modulo-over-index which reshuffles nearly all.
func (g *grpcExecutor) affinityStreamOwner(iid api.InstanceID) *streamState {
	var best *streamState
	var bestScore uint64
	g.streams.Range(func(_, value any) bool {
		ss, ok := value.(*streamState)
		if !ok || !ss.statefulHistory || ss.closed.Load() {
			return true
		}
		score := rendezvousScore(iid, ss.id)
		if best == nil || score > bestScore || (score == bestScore && ss.id < best.id) {
			best, bestScore = ss, score
		}
		return true
	})
	return best
}

// rendezvousScore is the HRW weight of pairing an instance with a stream. fnv-1a is fast,
// allocation-light, and deterministic within the process, which is all affinity needs.
func rendezvousScore(iid api.InstanceID, streamID string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(iid))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(streamID))
	return h.Sum64()
}

// applyStatefulHistory rewrites a workflow work item, in place, into a delta send
// when this stream is warm for the instance: the committed history prefix the
// worker already holds is stripped from PastEvents and the worker is told (via the
// CachedHistory message) to reconstruct it from its own cache. It then records the
// instance as warm up to the full committed-history length, so the next turn can
// be sent as a delta too. NewEvents is always left intact.
//
// Only ever called from the owning stream's GetWorkItems dispatch loop, so the
// warm map needs no locking. Stripping mutates a work item with a single
// consumer (this stream), so it is safe.
func (s *streamState) applyStatefulHistory(req *protos.WorkflowRequest) {
	if s == nil || !s.statefulHistory {
		return
	}

	iid := api.InstanceID(req.GetInstanceId())
	pastLen := len(req.GetPastEvents())

	// n = committed events the worker is believed to already hold. Send a delta
	// whenever that prefix is non-empty and not longer than the current history
	// (0 < n <= pastLen); anything else (no warm entry, or a shrunk history after
	// continue-as-new) falls back to a full send. n == pastLen is allowed and means
	// the worker already holds the whole committed history, so the delta is empty
	// and only NewEvents are sent.
	if n, ok := s.warm[iid]; ok && n > 0 && n <= pastLen {
		req.PastEvents = req.GetPastEvents()[n:]
		req.CachedHistory = &protos.CachedHistory{EventCount: int32(n)}
	}

	// After this send the worker holds the full committed history (it either had
	// the prefix and is receiving the delta, or is receiving everything). Record
	// that length for the next turn's delta computation.
	s.warm[iid] = pastLen

	// Completed instances are never re-dispatched, so their warm entries are
	// never naturally evicted. Bound the map: dropping an entry only forces a
	// (safe) full send next turn, so evicting arbitrary entries is harmless.
	if len(s.warm) > s.maxWarm {
		for k := range s.warm {
			if k == iid {
				continue
			}
			delete(s.warm, k)
			if len(s.warm) <= s.maxWarm {
				break
			}
		}
	}
}

// trySend offers wi to this stream's affinity buffer without blocking.
// Returns false when the buffer is full or the stream is tearing down.
func (s *streamState) trySend(wi *protos.WorkItem) bool {
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()
	if s.closed.Load() {
		return false
	}
	select {
	case s.ch <- wi:
		return true
	default:
		return false
	}
}

// trySendGrace offers wi to this stream's affinity buffer, waiting up to
// grace for buffer space. Returns false when the grace expires or the stream
// is tearing down, and an error only when ctx is done.
func (s *streamState) trySendGrace(ctx context.Context, wi *protos.WorkItem, grace time.Duration) (bool, error) {
	s.sendMu.RLock()
	defer s.sendMu.RUnlock()
	if s.closed.Load() {
		return false, nil
	}
	t := time.NewTimer(grace)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false, ctx.Err()
	case s.ch <- wi:
		return true, nil
	case <-t.C:
		return false, nil
	}
}
