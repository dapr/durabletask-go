/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/ptr"
)

func histEvents(n int) []*protos.HistoryEvent {
	e := make([]*protos.HistoryEvent, n)
	for i := range e {
		e[i] = &protos.HistoryEvent{EventId: int32(i)}
	}
	return e
}

func TestResolveWorkflowHistory_FullSend(t *testing.T) {
	c := &TaskHubGrpcClient{}
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{})

	req := &protos.WorkflowRequest{
		InstanceId: "a",
		PastEvents: histEvents(4),
		NewEvents:  histEvents(1),
	}

	past, err := c.resolveWorkflowHistory(context.Background(), cache, req)
	require.NoError(t, err)
	assert.Len(t, past, 4, "a full send returns PastEvents verbatim")
}

func TestResolveWorkflowHistory_CacheHitReconstructs(t *testing.T) {
	c := &TaskHubGrpcClient{}
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{})
	cache.put("a", histEvents(5))

	// Delta send: worker is told it already holds 5 events; here is the 3-event delta.
	req := &protos.WorkflowRequest{
		InstanceId:    "a",
		CachedHistory: &protos.CachedHistory{EventCount: 5},
		PastEvents:    histEvents(3),
		NewEvents:     histEvents(1),
	}

	past, err := c.resolveWorkflowHistory(context.Background(), cache, req)
	require.NoError(t, err)
	assert.Len(t, past, 8, "cached prefix (5) + delta (3) = full committed history (8)")
}

// fakeSidecarClient is a partial protos.TaskHubSidecarServiceClient that implements
// only GetInstanceHistory (the resolve path calls nothing else on the client), so the
// cache-miss fallback can be exercised deterministically.
type fakeSidecarClient struct {
	protos.TaskHubSidecarServiceClient
	events []*protos.HistoryEvent
	calls  int
}

func (f *fakeSidecarClient) GetInstanceHistory(_ context.Context, _ *protos.GetInstanceHistoryRequest, _ ...grpc.CallOption) (*protos.GetInstanceHistoryResponse, error) {
	f.calls++
	return &protos.GetInstanceHistoryResponse{Events: f.events}, nil
}

func TestResolveWorkflowHistory_CacheMissFetchesFromServer(t *testing.T) {
	// No cached entry for the instance, but the work item is a delta: the miss path
	// must fetch the full history via GetInstanceHistory rather than reconstruct.
	stub := &fakeSidecarClient{events: histEvents(9)}
	c := &TaskHubGrpcClient{client: stub}
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{})

	req := &protos.WorkflowRequest{
		InstanceId:    "a",
		CachedHistory: &protos.CachedHistory{EventCount: 5},
		PastEvents:    histEvents(3),
	}

	got, err := c.resolveWorkflowHistory(context.Background(), cache, req)
	require.NoError(t, err)
	assert.Equal(t, 1, stub.calls, "the miss path must route to GetInstanceHistory")
	assert.Len(t, got, 9, "resolved history must be the fetched full history, not a reconstruction")
}

func TestResolveWorkflowHistory_LengthMismatchIsMiss(t *testing.T) {
	// The worker holds 4 events but the service expects it to hold 5: a desync. The
	// worker must not reconstruct from its stale cache (which would yield 4+3=7
	// events); it must fall back to the fetched full history (9 events here).
	stub := &fakeSidecarClient{events: histEvents(9)}
	c := &TaskHubGrpcClient{client: stub}
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{})
	cache.put("a", histEvents(4))

	req := &protos.WorkflowRequest{
		InstanceId:    "a",
		CachedHistory: &protos.CachedHistory{EventCount: 5},
		PastEvents:    histEvents(3),
	}

	got, err := c.resolveWorkflowHistory(context.Background(), cache, req)
	require.NoError(t, err)
	assert.Equal(t, 1, stub.calls, "a count mismatch must route to GetInstanceHistory")
	assert.Len(t, got, 9, "must use the fetched history, not reconstruct 4+3 from the stale cache")
}

func TestWorkflowHistoryReset(t *testing.T) {
	running := &protos.WorkflowResponse{
		Actions: []*protos.WorkflowAction{
			{WorkflowActionType: &protos.WorkflowAction_ScheduleTask{}},
		},
	}
	assert.False(t, workflowHistoryReset(running))

	completing := &protos.WorkflowResponse{
		Actions: []*protos.WorkflowAction{
			{WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{
				CompleteWorkflow: &protos.CompleteWorkflowAction{},
			}},
		},
	}
	assert.True(t, workflowHistoryReset(completing))
}

func TestWorkflowHistoryCache(t *testing.T) {
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{})

	_, ok := cache.get("a")
	assert.False(t, ok)

	cache.put("a", histEvents(3))
	got, ok := cache.get("a")
	assert.True(t, ok)
	assert.Len(t, got, 3)

	cache.delete("a")
	_, ok = cache.get("a")
	assert.False(t, ok)

	cache.put("b", histEvents(1))
	cache.reset()
	_, ok = cache.get("b")
	assert.False(t, ok, "reset drops all entries (used on stream reconnect)")
}

func TestWorkflowHistoryCache_Bounded(t *testing.T) {
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{})
	for i := 0; i < maxCachedWorkflowHistories+5; i++ {
		cache.put(api.InstanceID("i"+string(rune(i))), histEvents(1))
	}
	assert.LessOrEqual(t, len(cache.entries), maxCachedWorkflowHistories)
}

func TestWorkflowHistoryCache_ConfigurableMaxInstances(t *testing.T) {
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{maxInstances: ptr.Of(2)})
	cache.put("a", histEvents(1))
	cache.put("b", histEvents(1))
	cache.put("c", histEvents(1))
	assert.LessOrEqual(t, len(cache.entries), 2, "the configured max must bound the cache")
}

func TestWorkflowHistoryCache_ConfigDefaults(t *testing.T) {
	// An unset (all-nil) config falls back to the package defaults.
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{})
	assert.Equal(t, cachedWorkflowHistoryTTL, cache.ttl)
	assert.Equal(t, cachedWorkflowHistorySweepInterval, cache.sweepInterval)
	assert.Equal(t, maxCachedWorkflowHistories, cache.maxInstances)
	assert.Zero(t, cache.maxBytes, "no byte limit by default")
}

// sizedEvents builds events with non-zero serialized size (EventId 0 is the proto
// default and would encode to zero bytes).
func sizedEvents(n int) []*protos.HistoryEvent {
	e := make([]*protos.HistoryEvent, n)
	for i := range e {
		e[i] = &protos.HistoryEvent{EventId: int32(i + 1)}
	}
	return e
}

func TestWorkflowHistoryCache_MaxBytesEvictsLRU(t *testing.T) {
	entrySize := historyBytes(sizedEvents(4))
	require.Positive(t, entrySize, "test events must have non-zero size")

	now := time.Unix(0, 0)
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{maxBytes: ptr.Of(int64(entrySize) + 1)})
	cache.now = func() time.Time { return now }

	cache.put("a", sizedEvents(4))
	now = now.Add(time.Second)
	cache.put("b", sizedEvents(4)) // two entries now exceed the byte budget

	_, okA := cache.get("a")
	_, okB := cache.get("b")
	assert.False(t, okA, "the least-recently-used entry is evicted to fit the byte budget")
	assert.True(t, okB, "the most recent entry is kept")
	assert.LessOrEqual(t, cache.totalBytes, int64(entrySize)+1, "total bytes stays within budget")
}

func TestWorkflowHistoryCache_SingleOversizedEntryKept(t *testing.T) {
	// A lone entry bigger than the whole budget is kept (a soft overage) rather
	// than thrashing on every turn.
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{maxBytes: ptr.Of[int64](1)})
	cache.put("big", sizedEvents(5))
	_, ok := cache.get("big")
	assert.True(t, ok)
}

func TestWorkflowHistoryCache_ByteAccounting(t *testing.T) {
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{})

	cache.put("a", sizedEvents(3))
	cache.put("b", sizedEvents(2))
	assert.Equal(t, int64(historyBytes(sizedEvents(3))+historyBytes(sizedEvents(2))), cache.totalBytes)

	// Replacing an entry adjusts the running total to the new size.
	cache.put("a", sizedEvents(6))
	assert.Equal(t, int64(historyBytes(sizedEvents(6))+historyBytes(sizedEvents(2))), cache.totalBytes)

	cache.delete("a")
	assert.Equal(t, int64(historyBytes(sizedEvents(2))), cache.totalBytes)

	cache.reset()
	assert.Zero(t, cache.totalBytes)
}

func TestWorkflowHistoryCache_TTLSweepUpdatesBytes(t *testing.T) {
	now := time.Unix(0, 0)
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{ttl: ptr.Of(time.Minute)})
	cache.now = func() time.Time { return now }

	cache.put("a", sizedEvents(3))
	require.Positive(t, cache.totalBytes)

	now = now.Add(2 * time.Minute)
	cache.sweepExpired()
	assert.Zero(t, cache.totalBytes, "byte total is reclaimed when the TTL sweep evicts entries")
}

func TestWorkflowHistoryCache_TTLSweep(t *testing.T) {
	now := time.Unix(0, 0)
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{})
	cache.ttl = time.Minute
	cache.now = func() time.Time { return now }

	cache.put("idle", histEvents(2))
	cache.put("active", histEvents(2))

	// Advance past the TTL, but refresh "active" with a turn just before sweeping.
	now = now.Add(2 * time.Minute)
	_, ok := cache.get("active") // sliding window: refreshes lastAccess to now
	require.True(t, ok)

	cache.sweepExpired()

	_, ok = cache.get("idle")
	assert.False(t, ok, "an instance idle longer than the TTL must be evicted")
	_, ok = cache.get("active")
	assert.True(t, ok, "an instance with a recent turn must survive the sweep")
}

func TestWorkflowHistoryCache_TTLSlidingOnPut(t *testing.T) {
	now := time.Unix(0, 0)
	cache := newWorkflowHistoryCache(workflowHistoryCacheConfig{})
	cache.ttl = time.Minute
	cache.now = func() time.Time { return now }

	cache.put("a", histEvents(1))
	now = now.Add(90 * time.Second) // would expire...
	cache.put("a", histEvents(3))   // ...but a new turn refreshes the entry

	cache.sweepExpired()
	got, ok := cache.get("a")
	assert.True(t, ok)
	assert.Len(t, got, 3, "the refreshed entry survives and holds the latest history")
}
