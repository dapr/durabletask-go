package backend

import (
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

func newStreamState(req *protos.GetWorkItemsRequest) *streamState {
	s := &streamState{warm: make(map[api.InstanceID]int), maxWarm: maxWarmInstancesPerStream}
	for _, c := range req.GetCapabilities() {
		if c == protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY {
			s.statefulHistory = true
		}
	}
	return s
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
