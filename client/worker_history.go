package client

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

const (
	// maxCachedWorkflowHistories bounds the number of per-instance histories
	// retained on a single work-item stream. Evicting an entry only costs one
	// GetInstanceHistory fetch the next time that instance runs, so this is a
	// soft cap.
	maxCachedWorkflowHistories = 100_000

	// cachedWorkflowHistoryTTL is how long an instance's cached history is kept
	// after its last turn. It is a sliding window: every turn refreshes it. This
	// reclaims memory from instances that stop getting turns without completing
	// (parked indefinitely, abandoned, or purged out-of-band) on top of the
	// eviction that already happens on completion, continue-as-new, and
	// reconnect. Eviction is always safe: the next delta becomes a cache miss
	// recovered via GetInstanceHistory, which for a long-idle instance is
	// negligible.
	cachedWorkflowHistoryTTL = time.Hour

	// cachedWorkflowHistorySweepInterval is how often the janitor reclaims
	// expired entries.
	cachedWorkflowHistorySweepInterval = time.Minute
)

// workflowHistoryCacheConfig tunes the per-stream history cache. Each field is a
// pointer so that nil signals "not configured" and takes the package default,
// distinct from an explicit value. ttl, sweepInterval, and maxInstances fall back
// to their defaults; maxBytes defaults to unlimited (bounded by maxInstances and
// the TTL). A configured-but-non-positive value is also treated as the default.
type workflowHistoryCacheConfig struct {
	ttl           *time.Duration
	sweepInterval *time.Duration
	maxInstances  *int
	maxBytes      *int64
}

// resolveWorkflowHistory returns the full committed history to replay for a
// workflow work item. For a full send it is simply PastEvents; for a delta send
// (cachedHistory) it is the cached prefix plus the delta, falling back to a
// GetInstanceHistory fetch on any cache miss.
func (c *TaskHubGrpcClient) resolveWorkflowHistory(
	ctx context.Context,
	historyCache *workflowHistoryCache,
	workItem *protos.WorkflowRequest,
) ([]*protos.HistoryEvent, error) {
	cachedHistory := workItem.GetCachedHistory()
	if cachedHistory == nil {
		return workItem.GetPastEvents(), nil
	}

	iid := api.InstanceID(workItem.InstanceId)
	delta := workItem.GetPastEvents()

	if cached, ok := historyCache.get(iid); ok && len(cached) == int(cachedHistory.GetEventCount()) {
		full := make([]*protos.HistoryEvent, 0, len(cached)+len(delta))
		full = append(full, cached...)
		full = append(full, delta...)
		return full, nil
	}

	// Cache miss: recover the full committed history from the sidecar. NewEvents
	// is applied on top of this by the caller, so only the committed past is needed.
	resp, err := c.GetInstanceHistory(ctx, iid)
	if err != nil {
		return nil, err
	}
	return resp.GetEvents(), nil
}

// workflowHistoryReset reports whether a workflow turn ended the current
// execution (completion, failure, termination, or continue-as-new), after which
// the cached history no longer extends the instance's history and must be dropped.
func workflowHistoryReset(resp *protos.WorkflowResponse) bool {
	for _, a := range resp.GetActions() {
		if a.GetCompleteWorkflow() != nil {
			return true
		}
	}
	return false
}

type cachedWorkflowHistory struct {
	events     []*protos.HistoryEvent
	lastAccess time.Time
	bytes      int
}

// historyBytes returns the serialized size of a history, used as a proxy for its
// memory footprint when enforcing the cache's byte budget.
func historyBytes(events []*protos.HistoryEvent) int {
	total := 0
	for _, e := range events {
		total += proto.Size(e)
	}
	return total
}

// workflowHistoryCache holds each instance's committed history for the lifetime of
// a single work-item stream, enabling delta (cachedHistory) work items. Entries are
// reclaimed by a TTL janitor (see runJanitor), on completion, and by least-recently-
// used eviction once either the instance-count cap or the byte budget is exceeded.
type workflowHistoryCache struct {
	mu            sync.Mutex
	entries       map[api.InstanceID]*cachedWorkflowHistory
	totalBytes    int64
	ttl           time.Duration
	sweepInterval time.Duration
	maxInstances  int
	maxBytes      int64
	now           func() time.Time
}

func newWorkflowHistoryCache(cfg workflowHistoryCacheConfig) *workflowHistoryCache {
	ttl := cachedWorkflowHistoryTTL
	if cfg.ttl != nil && *cfg.ttl > 0 {
		ttl = *cfg.ttl
	}
	sweepInterval := cachedWorkflowHistorySweepInterval
	if cfg.sweepInterval != nil && *cfg.sweepInterval > 0 {
		sweepInterval = *cfg.sweepInterval
	}
	maxInstances := maxCachedWorkflowHistories
	if cfg.maxInstances != nil && *cfg.maxInstances > 0 {
		maxInstances = *cfg.maxInstances
	}
	var maxBytes int64 // unset/non-positive means unlimited
	if cfg.maxBytes != nil && *cfg.maxBytes > 0 {
		maxBytes = *cfg.maxBytes
	}
	return &workflowHistoryCache{
		entries:       make(map[api.InstanceID]*cachedWorkflowHistory),
		ttl:           ttl,
		sweepInterval: sweepInterval,
		maxInstances:  maxInstances,
		maxBytes:      maxBytes,
		now:           time.Now,
	}
}

func (h *workflowHistoryCache) get(iid api.InstanceID) ([]*protos.HistoryEvent, bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	e, ok := h.entries[iid]
	if !ok {
		return nil, false
	}
	e.lastAccess = h.now()
	return e.events, true
}

func (h *workflowHistoryCache) put(iid api.InstanceID, events []*protos.HistoryEvent) {
	h.mu.Lock()
	defer h.mu.Unlock()
	if old, ok := h.entries[iid]; ok {
		h.totalBytes -= int64(old.bytes)
	}
	size := historyBytes(events)
	h.entries[iid] = &cachedWorkflowHistory{events: events, lastAccess: h.now(), bytes: size}
	h.totalBytes += int64(size)
	h.evictToFit(iid)
}

// evictToFit evicts least-recently-used entries until the cache is within both the
// instance-count cap and the byte budget, always keeping the just-touched entry so
// the active working set is never evicted out from under the current turn. A single
// entry larger than the byte budget is kept (a soft overage) rather than thrashing.
func (h *workflowHistoryCache) evictToFit(keep api.InstanceID) {
	for len(h.entries) > 1 {
		overCount := len(h.entries) > h.maxInstances
		overBytes := h.maxBytes > 0 && h.totalBytes > h.maxBytes
		if !overCount && !overBytes {
			return
		}
		victim, ok := h.lruExcept(keep)
		if !ok {
			return
		}
		h.removeEntry(victim)
	}
}

func (h *workflowHistoryCache) lruExcept(keep api.InstanceID) (api.InstanceID, bool) {
	var oldest api.InstanceID
	var oldestAccess time.Time
	found := false
	for id, e := range h.entries {
		if id == keep {
			continue
		}
		if !found || e.lastAccess.Before(oldestAccess) {
			oldest, oldestAccess, found = id, e.lastAccess, true
		}
	}
	return oldest, found
}

func (h *workflowHistoryCache) removeEntry(iid api.InstanceID) {
	if e, ok := h.entries[iid]; ok {
		h.totalBytes -= int64(e.bytes)
		delete(h.entries, iid)
	}
}

func (h *workflowHistoryCache) delete(iid api.InstanceID) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.removeEntry(iid)
}

func (h *workflowHistoryCache) reset() {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.entries = make(map[api.InstanceID]*cachedWorkflowHistory)
	h.totalBytes = 0
}

// sweepExpired drops entries whose last turn was longer ago than the TTL.
func (h *workflowHistoryCache) sweepExpired() {
	h.mu.Lock()
	defer h.mu.Unlock()
	now := h.now()
	for iid, e := range h.entries {
		if now.Sub(e.lastAccess) > h.ttl {
			h.removeEntry(iid)
		}
	}
}

// runJanitor periodically reclaims expired cache entries until ctx is done. It is
// started once per work-item listener and exits when the listener stops.
func (h *workflowHistoryCache) runJanitor(ctx context.Context) {
	ticker := time.NewTicker(h.sweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			h.sweepExpired()
		}
	}
}
