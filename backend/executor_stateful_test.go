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

package backend

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

func events(n int) []*protos.HistoryEvent {
	e := make([]*protos.HistoryEvent, n)
	for i := range e {
		e[i] = &protos.HistoryEvent{EventId: int32(i)}
	}
	return e
}

func workflowReq(iid string, past, new int) *protos.WorkflowRequest {
	return &protos.WorkflowRequest{
		InstanceId: iid,
		PastEvents: events(past),
		NewEvents:  events(new),
	}
}

func TestNewStreamState_Capabilities(t *testing.T) {
	t.Run("no capabilities", func(t *testing.T) {
		ss := newStreamState("s1", &protos.GetWorkItemsRequest{})
		assert.False(t, ss.statefulHistory)
	})

	t.Run("stateful history advertised alongside an unknown capability", func(t *testing.T) {
		ss := newStreamState("s1", &protos.GetWorkItemsRequest{
			Capabilities: []protos.WorkerCapability{
				protos.WorkerCapability(99), // an unknown/future capability is ignored
				protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY,
			},
		})
		assert.True(t, ss.statefulHistory)
	})
}

func TestApplyStatefulHistory_NonCapableStreamUnchanged(t *testing.T) {
	ss := newStreamState("s1", &protos.GetWorkItemsRequest{})
	req := workflowReq("a", 5, 2)

	ss.applyStatefulHistory(req)

	assert.Len(t, req.PastEvents, 5, "full history must be retained for non-capable workers")
	assert.Nil(t, req.CachedHistory)
	assert.Empty(t, ss.warm)
}

func TestApplyStatefulHistory_FirstTurnSendsFullThenWarms(t *testing.T) {
	ss := newStreamState("s1", &protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	})
	req := workflowReq("a", 5, 2)

	ss.applyStatefulHistory(req)

	// First turn: no warm entry yet, so the full history is sent...
	assert.Len(t, req.PastEvents, 5)
	assert.Nil(t, req.CachedHistory)
	// ...but the instance is now warm up to the committed-history length.
	assert.Equal(t, 5, ss.warm["a"])
}

func TestApplyStatefulHistory_SubsequentTurnSendsDelta(t *testing.T) {
	ss := newStreamState("s1", &protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	})

	// Turn 1: 5 committed events, full send, warm -> 5.
	ss.applyStatefulHistory(workflowReq("a", 5, 2))
	assert.Equal(t, 5, ss.warm["a"])

	// Turn 2: history has grown to 8 committed events. The worker already holds
	// the first 5, so only the 3-event delta should be sent.
	req2 := workflowReq("a", 8, 1)
	ss.applyStatefulHistory(req2)

	require.NotNil(t, req2.CachedHistory)
	assert.Equal(t, int32(5), req2.CachedHistory.GetEventCount())
	assert.Len(t, req2.PastEvents, 3, "only events 5..8 should be sent as the delta")
	assert.Len(t, req2.NewEvents, 1, "new events are always sent in full")
	assert.Equal(t, 8, ss.warm["a"])
}

func TestApplyStatefulHistory_PerInstanceIsolation(t *testing.T) {
	ss := newStreamState("s1", &protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	})

	ss.applyStatefulHistory(workflowReq("a", 3, 0))
	// "b" has never been seen on this stream: it must get a full send.
	reqB := workflowReq("b", 4, 0)
	ss.applyStatefulHistory(reqB)

	assert.Nil(t, reqB.CachedHistory)
	assert.Len(t, reqB.PastEvents, 4)
	assert.Equal(t, 3, ss.warm["a"])
	assert.Equal(t, 4, ss.warm["b"])
}

func TestApplyStatefulHistory_ShrinkingHistoryFallsBackToFull(t *testing.T) {
	// A continue-as-new resets the committed history to a shorter list. The warm
	// count is now larger than the current history, so we must not send a delta.
	ss := newStreamState("s1", &protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	})

	ss.applyStatefulHistory(workflowReq("a", 10, 0))
	assert.Equal(t, 10, ss.warm["a"])

	req2 := workflowReq("a", 2, 1)
	ss.applyStatefulHistory(req2)

	assert.Nil(t, req2.CachedHistory, "must not send a delta when history shrank")
	assert.Len(t, req2.PastEvents, 2)
	assert.Equal(t, 2, ss.warm["a"], "warm count is re-based to the new history length")
}

func TestApplyStatefulHistory_BoundsWarmMap(t *testing.T) {
	ss := newStreamState("s1", &protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	})
	ss.maxWarm = 16

	for i := 0; i < ss.maxWarm+10; i++ {
		ss.applyStatefulHistory(workflowReq("inst-"+strconv.Itoa(i), 1, 0))
	}

	assert.LessOrEqual(t, len(ss.warm), ss.maxWarm+1,
		"warm map must stay bounded as new instances are dispatched")
}

// streamsWith builds a grpcExecutor whose stream registry holds the given streams,
// for exercising affinity owner selection.
func streamsWith(ss ...*streamState) *grpcExecutor {
	g := &grpcExecutor{streams: &sync.Map{}}
	for _, s := range ss {
		g.streams.Store(s.id, s)
	}
	return g
}

func capableStream(id string) *streamState {
	return newStreamState(id, &protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	})
}

func TestAffinityStreamOwner_Deterministic(t *testing.T) {
	g := streamsWith(capableStream("s1"), capableStream("s2"), capableStream("s3"))

	owner := g.affinityStreamOwner("inst-1")
	require.NotNil(t, owner)
	// Stable across repeated lookups regardless of map iteration order.
	for i := 0; i < 50; i++ {
		assert.Same(t, owner, g.affinityStreamOwner("inst-1"))
	}
}

func TestAffinityStreamOwner_DistributesAcrossStreams(t *testing.T) {
	g := streamsWith(capableStream("s1"), capableStream("s2"), capableStream("s3"))

	owners := map[string]struct{}{}
	for i := 0; i < 200; i++ {
		owner := g.affinityStreamOwner(api.InstanceID("inst-" + strconv.Itoa(i)))
		require.NotNil(t, owner)
		owners[owner.id] = struct{}{}
	}
	assert.Len(t, owners, 3, "every stream should own some share of the instances")
}

func TestAffinityStreamOwner_SkipsNonCapableStreams(t *testing.T) {
	capable := capableStream("capable")
	g := streamsWith(capable, newStreamState("plain", &protos.GetWorkItemsRequest{}))

	for i := 0; i < 50; i++ {
		owner := g.affinityStreamOwner(api.InstanceID("inst-" + strconv.Itoa(i)))
		require.NotNil(t, owner)
		assert.Equal(t, "capable", owner.id, "only stateful-history-capable streams may own an instance")
	}
}

func TestAffinityStreamOwner_NilWhenNoCapableStream(t *testing.T) {
	g := streamsWith(newStreamState("plain", &protos.GetWorkItemsRequest{}))
	assert.Nil(t, g.affinityStreamOwner("inst-1"))

	empty := &grpcExecutor{streams: &sync.Map{}}
	assert.Nil(t, empty.affinityStreamOwner("inst-1"))
}

// TestAffinityStreamOwner_MinimalRemapOnMembershipChange is the property that motivates
// rendezvous hashing over modulo-over-index: removing one stream must leave the owner of
// instances that did not belong to it unchanged (only its instances remap).
func TestAffinityStreamOwner_MinimalRemapOnMembershipChange(t *testing.T) {
	s1, s2, s3 := capableStream("s1"), capableStream("s2"), capableStream("s3")
	before := streamsWith(s1, s2, s3)

	ownerBefore := map[string]string{}
	for i := 0; i < 300; i++ {
		iid := api.InstanceID("inst-" + strconv.Itoa(i))
		ownerBefore[string(iid)] = before.affinityStreamOwner(iid).id
	}

	// Drop s3; only instances previously owned by s3 may move.
	after := streamsWith(s1, s2)
	for i := 0; i < 300; i++ {
		iid := api.InstanceID("inst-" + strconv.Itoa(i))
		got := after.affinityStreamOwner(iid).id
		if prev := ownerBefore[string(iid)]; prev != "s3" {
			assert.Equal(t, prev, got, "instance %s must not remap when its owner stayed connected", iid)
		}
	}
}

func TestAffinityStreamOwner_SkipsClosedStreams(t *testing.T) {
	closing := capableStream("closing")
	survivor := capableStream("survivor")
	g := streamsWith(closing, survivor)

	closing.closed.Store(true)
	for i := 0; i < 50; i++ {
		owner := g.affinityStreamOwner(api.InstanceID("inst-" + strconv.Itoa(i)))
		require.NotNil(t, owner)
		assert.Equal(t, "survivor", owner.id,
			"a stream that has begun tearing down must not be chosen as owner")
	}
}

func wfWorkItem(iid string) *protos.WorkItem {
	return &protos.WorkItem{Request: &protos.WorkItem_WorkflowRequest{
		WorkflowRequest: workflowReq(iid, 0, 1),
	}}
}

// trackWorkflow tracks a new work item for iid as its latest pending
// execution, so the drain treats the item as the live dispatch.
func trackWorkflow(g *grpcExecutor, iid string) *protos.WorkItem {
	wi := wfWorkItem(iid)
	g.pendingWorkflows.add(iid, wi, api.InstanceID(iid), 0, func() {})
	return wi
}

// A dying stream's affinity buffer must be re-offered to the shared queue so
// a surviving worker delivers the items, instead of orphaning them with the
// stream (which stalled the instance until an external recovery kicked in).
func TestDrainStreamBuffer_RequeuesToSharedQueue(t *testing.T) {
	g := &grpcExecutor{
		workItemQueue:    make(chan *protos.WorkItem, 8),
		pendingWorkflows: newPendingTasks(),
		streams:          &sync.Map{},
		logger:           DefaultLogger(),
	}
	wiWfA := trackWorkflow(g, "wf-a")
	wiWfB := trackWorkflow(g, "wf-b")
	ss := capableStream("dying")
	ss.closed.Store(true)
	ss.ch <- wiWfA
	ss.ch <- wiWfB

	g.drainStreamBuffer(ss)

	require.Len(t, g.workItemQueue, 2)
	got := map[string]struct{}{}
	for i := 0; i < 2; i++ {
		wi := <-g.workItemQueue
		got[wi.GetWorkflowRequest().GetInstanceId()] = struct{}{}
	}
	assert.Contains(t, got, "wf-a")
	assert.Contains(t, got, "wf-b")
	assert.Empty(t, ss.ch, "the dying stream's buffer must be fully drained")
}

type fakeCancelBackend struct {
	Backend
	mu        sync.Mutex
	cancelled []api.InstanceID
}

func (f *fakeCancelBackend) CancelWorkflowTask(_ context.Context, iid api.InstanceID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancelled = append(f.cancelled, iid)
	return nil
}

// When the shared queue has no capacity to absorb a drained item, the turn is
// aborted so the backend's retry path re-derives it: degraded, but never lost.
func TestDrainStreamBuffer_CancelsWhenQueueFull(t *testing.T) {
	fb := &fakeCancelBackend{}
	g := &grpcExecutor{
		workItemQueue:    make(chan *protos.WorkItem),
		pendingWorkflows: newPendingTasks(),
		streams:          &sync.Map{},
		backend:          fb,
		logger:           DefaultLogger(),
	}
	wiWfOrphan := trackWorkflow(g, "wf-orphan")
	ss := capableStream("dying")
	ss.closed.Store(true)
	ss.ch <- wiWfOrphan

	g.drainStreamBuffer(ss)

	fb.mu.Lock()
	defer fb.mu.Unlock()
	require.Len(t, fb.cancelled, 1)
	assert.Equal(t, api.InstanceID("wf-orphan"), fb.cancelled[0])
}

// A producer must not route into a closed owner's buffer: with the owner
// closed the item takes the shared queue, where any surviving stream can
// deliver it.
func TestDispatchWorkflowWorkItem_ClosedOwnerFallsToSharedQueue(t *testing.T) {
	owner := capableStream("owner")
	g := streamsWith(owner)
	g.workItemQueue = make(chan *protos.WorkItem, 1)
	g.logger = DefaultLogger()

	// Fill the owner's buffer so the fast path cannot accept, then close it:
	// the post-grace re-check must divert to the shared queue rather than
	// blocking on the dead owner.
	for i := 0; i < cap(owner.ch); i++ {
		owner.ch <- wfWorkItem("filler")
	}
	owner.closed.Store(true)

	wi := wfWorkItem("wf-live")
	require.NoError(t, g.dispatchWorkflowWorkItem(context.Background(), "wf-live", wi))
	require.Len(t, g.workItemQueue, 1)
	assert.Equal(t, "wf-live", (<-g.workItemQueue).GetWorkflowRequest().GetInstanceId())
}

// After Shutdown closed the shared queue, a stream teardown drain must not
// panic on the send; it cancels the task so the backend's retry redelivers.
func TestDrainStreamBuffer_ShutdownCancelsInsteadOfPanicking(t *testing.T) {
	fb := &fakeCancelBackend{}
	g := &grpcExecutor{
		workItemQueue:    make(chan *protos.WorkItem, 8),
		pendingWorkflows: newPendingTasks(),
		streams:          &sync.Map{},
		backend:          fb,
		logger:           DefaultLogger(),
	}
	g.queueLock.Lock()
	g.queueClosed = true
	close(g.workItemQueue)
	g.queueLock.Unlock()

	wiWfShutdown := trackWorkflow(g, "wf-shutdown")
	ss := capableStream("dying")
	ss.ch <- wiWfShutdown

	require.NotPanics(t, func() { g.drainStreamBuffer(ss) })

	fb.mu.Lock()
	defer fb.mu.Unlock()
	require.Len(t, fb.cancelled, 1)
	assert.Equal(t, api.InstanceID("wf-shutdown"), fb.cancelled[0])
}

// A producer that selected an owner just before teardown must serialize with
// the drain: its item either lands before the drain (re-offered) or diverts
// to the shared queue, never into the dead buffer afterwards.
func TestTrySend_ClosedStreamRefuses(t *testing.T) {
	ss := capableStream("s")
	g := &grpcExecutor{workItemQueue: make(chan *protos.WorkItem, 8), pendingWorkflows: newPendingTasks(), streams: &sync.Map{}, logger: DefaultLogger()}
	require.True(t, ss.trySend(trackWorkflow(g, "a")))
	g.drainStreamBuffer(ss)
	assert.False(t, ss.trySend(wfWorkItem("b")), "a drained stream must refuse new sends")
	ok, err := ss.trySendGrace(context.Background(), wfWorkItem("c"), time.Millisecond)
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Empty(t, ss.ch, "nothing may land in the buffer after the drain")
}

type flakyCancelBackend struct {
	Backend
	mu        sync.Mutex
	failures  int
	cancelled []api.InstanceID
}

func (f *flakyCancelBackend) CancelWorkflowTask(_ context.Context, iid api.InstanceID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failures > 0 {
		f.failures--
		return errors.New("transient backend error")
	}
	f.cancelled = append(f.cancelled, iid)
	return nil
}

// A transient cancel error during the drain fallback must not discard the
// item: the completion waiter is live, so the drain retries until the item
// is settled one way or the other.
func TestRequeueWorkItem_RetriesTransientCancelFailure(t *testing.T) {
	fb := &flakyCancelBackend{failures: 3}
	g := &grpcExecutor{
		workItemQueue:    make(chan *protos.WorkItem),
		pendingWorkflows: newPendingTasks(),
		streams:          &sync.Map{},
		backend:          fb,
		logger:           DefaultLogger(),
	}
	wiWfFlaky := trackWorkflow(g, "wf-flaky")
	ss := capableStream("dying")
	ss.ch <- wiWfFlaky

	g.drainStreamBuffer(ss)

	fb.mu.Lock()
	defer fb.mu.Unlock()
	require.Len(t, fb.cancelled, 1, "the item must be settled once the transient error clears")
	assert.Equal(t, api.InstanceID("wf-flaky"), fb.cancelled[0])
	assert.Zero(t, fb.failures)
}

// A drained item whose dispatch was superseded (completion token no longer
// matches the tracked entry) or already settled (no entry) must be dropped,
// never requeued or cancelled against the live registration.
func TestRequeueWorkItem_DropsSupersededOrSettled(t *testing.T) {
	fb := &fakeCancelBackend{}
	g := &grpcExecutor{
		workItemQueue:    make(chan *protos.WorkItem, 8),
		pendingWorkflows: newPendingTasks(),
		streams:          &sync.Map{},
		backend:          fb,
		logger:           DefaultLogger(),
	}

	// Superseded: a newer dispatch of the same instance was tracked after it.
	older := trackWorkflow(g, "wf-old")
	trackWorkflow(g, "wf-old")
	g.requeueWorkItem(older)

	// Settled: no entry at all.
	g.requeueWorkItem(wfWorkItem("wf-gone"))

	assert.Empty(t, g.workItemQueue, "stale items must not be requeued")
	fb.mu.Lock()
	defer fb.mu.Unlock()
	assert.Empty(t, fb.cancelled, "stale items must not cancel the live registration")
}

type unknownInstanceBackend struct {
	Backend
	mu    sync.Mutex
	calls int
}

func (f *unknownInstanceBackend) CancelWorkflowTask(_ context.Context, iid api.InstanceID) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return &api.UnknownInstanceIDError{InstanceID: string(iid)}
}

// An unknown-registration error from the cancel fallback is permanent (the
// attempt settled concurrently): the drain must treat it as settled, not
// retry forever.
func TestRequeueWorkItem_UnknownInstanceIsSettled(t *testing.T) {
	fb := &unknownInstanceBackend{}
	g := &grpcExecutor{
		workItemQueue:    make(chan *protos.WorkItem),
		pendingWorkflows: newPendingTasks(),
		streams:          &sync.Map{},
		backend:          fb,
		logger:           DefaultLogger(),
	}
	wiWfUnknown := trackWorkflow(g, "wf-unknown")

	done := make(chan struct{})
	go func() {
		g.requeueWorkItem(wiWfUnknown)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("requeueWorkItem must return once the registration is unknown, not retry forever")
	}
	fb.mu.Lock()
	defer fb.mu.Unlock()
	assert.Equal(t, 1, fb.calls)
}
