package backend

import (
	"context"
	"hash/fnv"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

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

	// ch delivers work items routed to this specific stream by instance affinity.
	// Unbuffered: a routed item is taken only when this stream's dispatch loop is
	// ready, otherwise the producer falls back to the shared queue (any stream).
	ch chan *protos.WorkItem

	// statefulHistory is true if the worker advertised
	// WORKER_CAPABILITY_STATEFUL_HISTORY, meaning it retains an instance's
	// committed history between turns so the service can send only deltas.
	statefulHistory bool

	// warm maps an instance ID to the number of committed (past) history events
	// this stream is believed to already hold for it, i.e. the length of the
	// pastEvents prefix that may be omitted on the next turn. Only accessed from
	// the owning stream's GetWorkItems dispatch loop, so it needs no locking.
	warm map[api.InstanceID]int

	// maxWarm is the soft cap on warm entries; defaults to maxWarmInstancesPerStream.
	maxWarm int
}

func newStreamState(id string, req *protos.GetWorkItemsRequest) *streamState {
	s := &streamState{
		id:      id,
		ch:      make(chan *protos.WorkItem),
		warm:    make(map[api.InstanceID]int),
		maxWarm: maxWarmInstancesPerStream,
	}
	for _, c := range req.GetCapabilities() {
		if c == protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY {
			s.statefulHistory = true
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
		select {
		case owner.ch <- wi:
			return nil
		default:
		}
		// Owner busy: give it first crack but let any free stream take over so a
		// slow or overloaded owner never stalls the instance.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case owner.ch <- wi:
			return nil
		case g.workItemQueue <- wi:
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
		if !ok || !ss.statefulHistory {
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

	// n = committed events the worker is believed to already hold. Only send a
	// delta when that prefix is a strict, non-empty prefix of the current history
	// (n within (0, pastLen]); anything else falls back to a full send.
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
