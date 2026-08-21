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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

// fakeExecBackend implements just enough of Backend to drive the executor's
// GetWorkItems streams: completion waiters keyed by instance ID and a record
// of cancelled workflow tasks (the stream-teardown recovery path).
type fakeExecBackend struct {
	Backend
	mu       sync.Mutex
	waiters  map[string]chan *protos.WorkflowResponse
	canceled []api.InstanceID
}

func newFakeExecBackend() *fakeExecBackend {
	return &fakeExecBackend{waiters: make(map[string]chan *protos.WorkflowResponse)}
}

func (f *fakeExecBackend) OnWorkflowTaskCompletion(req *protos.WorkflowRequest, cb func(*protos.WorkflowResponse, error)) func() {
	ch := make(chan *protos.WorkflowResponse, 1)
	f.mu.Lock()
	f.waiters[req.GetInstanceId()] = ch
	f.mu.Unlock()
	done := make(chan struct{})
	go func() {
		select {
		case <-done:
		case resp := <-ch:
			if resp == nil {
				cb(nil, api.ErrTaskCancelled)
				return
			}
			cb(resp, nil)
		}
	}()
	return func() { close(done) }
}

func (f *fakeExecBackend) complete(iid string) {
	f.mu.Lock()
	ch := f.waiters[iid]
	f.mu.Unlock()
	if ch != nil {
		select {
		case ch <- &protos.WorkflowResponse{InstanceId: iid}:
		default:
		}
	}
}

func (f *fakeExecBackend) CancelWorkflowTask(_ context.Context, iid api.InstanceID) error {
	f.mu.Lock()
	f.canceled = append(f.canceled, iid)
	ch := f.waiters[string(iid)]
	f.mu.Unlock()
	if ch != nil {
		select {
		case ch <- nil:
		default:
		}
	}
	return nil
}

func (f *fakeExecBackend) CancelActivityTask(context.Context, api.InstanceID, int32) error {
	return nil
}

func (f *fakeExecBackend) canceledInstances() []api.InstanceID {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]api.InstanceID(nil), f.canceled...)
}

// fakeWorkItemsStream is a controllable GetWorkItems server stream: Send is
// delegated to the test.
type fakeWorkItemsStream struct {
	grpc.ServerStream
	ctx  context.Context
	send func(*protos.WorkItem) error
}

func (f *fakeWorkItemsStream) Context() context.Context       { return f.ctx }
func (f *fakeWorkItemsStream) Send(wi *protos.WorkItem) error { return f.send(wi) }

func statefulWorkItemsRequest() *protos.GetWorkItemsRequest {
	return &protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	}
}

// TestGetWorkItems_PerInstanceOrderAcrossStreams runs N instances x M turns
// through a few streams, with a recording handler completing each turn, and
// asserts every instance observed its turns in order. Turn k+1 is only
// dispatched after turn k completes, mirroring the actor turn lock upstream.
func TestGetWorkItems_PerInstanceOrderAcrossStreams(t *testing.T) {
	fb := newFakeExecBackend()
	exec, _ := NewGrpcExecutor(fb, defaultLogger)
	g, ok := exec.(*grpcExecutor)
	require.True(t, ok)

	var recMu sync.Mutex
	received := map[string][]int32{}

	streamCtx, streamCancel := context.WithCancel(t.Context())
	defer streamCancel()

	var wg sync.WaitGroup
	const numStreams = 3
	for range numStreams {
		stream := &fakeWorkItemsStream{
			ctx: streamCtx,
			send: func(wi *protos.WorkItem) error {
				req := wi.GetWorkflowRequest()
				iid := req.GetInstanceId()
				recMu.Lock()
				received[iid] = append(received[iid], req.GetNewEvents()[0].GetEventId())
				recMu.Unlock()
				fb.complete(iid)
				return nil
			},
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, g.GetWorkItems(statefulWorkItemsRequest(), stream))
		}()
	}

	const numInstances, numTurns = 8, 10
	var iwg sync.WaitGroup
	for i := range numInstances {
		iwg.Add(1)
		go func() {
			defer iwg.Done()
			iid := api.InstanceID(fmt.Sprintf("instance-%d", i))
			for turn := range int32(numTurns) {
				_, err := g.ExecuteWorkflow(t.Context(), iid, nil,
					[]*protos.HistoryEvent{{EventId: turn}}, ExecuteOptions{})
				assert.NoError(t, err)
			}
		}()
	}
	iwg.Wait()

	recMu.Lock()
	defer recMu.Unlock()
	require.Len(t, received, numInstances)
	for iid, turns := range received {
		require.Len(t, turns, numTurns, "instance %s", iid)
		for turn := range int32(numTurns) {
			assert.Equal(t, turn, turns[turn], "instance %s out of order", iid)
		}
	}

	streamCancel()
	wg.Wait()
}

// TestGetWorkItems_SendFailureRecoversWorkItem asserts a failed Send still
// tears the stream down and cancels the affected pending work item through
// the backend (the abandon/retry path), instead of dropping it.
func TestGetWorkItems_SendFailureRecoversWorkItem(t *testing.T) {
	fb := newFakeExecBackend()
	exec, _ := NewGrpcExecutor(fb, defaultLogger)
	g, ok := exec.(*grpcExecutor)
	require.True(t, ok)

	sendErr := errors.New("send exploded mid-stream")
	stream := &fakeWorkItemsStream{
		ctx:  t.Context(),
		send: func(*protos.WorkItem) error { return sendErr },
	}

	streamDone := make(chan error, 1)
	go func() {
		streamDone <- g.GetWorkItems(statefulWorkItemsRequest(), stream)
	}()

	execDone := make(chan error, 1)
	go func() {
		_, err := g.ExecuteWorkflow(t.Context(), "doomed", nil,
			[]*protos.HistoryEvent{{EventId: 0}}, ExecuteOptions{})
		execDone <- err
	}()

	select {
	case err := <-streamDone:
		require.ErrorIs(t, err, sendErr)
	case <-time.After(5 * time.Second):
		t.Fatal("GetWorkItems did not return after send failure")
	}

	select {
	case err := <-execDone:
		require.EqualError(t, err, "operation aborted")
	case <-time.After(5 * time.Second):
		t.Fatal("ExecuteWorkflow did not return after send failure")
	}

	assert.Equal(t, []api.InstanceID{"doomed"}, fb.canceledInstances())
}

// TestGetWorkItems_DispatchNotBlockedDuringSlowSend asserts the dispatch loop
// keeps draining the shared queue while a Send is in flight: many small items
// enqueued behind one slow send are all accepted before that send completes,
// i.e. there is no per-item rendezvous.
func TestGetWorkItems_DispatchNotBlockedDuringSlowSend(t *testing.T) {
	fb := newFakeExecBackend()
	exec, _ := NewGrpcExecutor(fb, defaultLogger)
	g, ok := exec.(*grpcExecutor)
	require.True(t, ok)

	gate := make(chan struct{})
	var sendMu sync.Mutex
	var sent int
	stream := &fakeWorkItemsStream{
		ctx: t.Context(),
		send: func(*protos.WorkItem) error {
			sendMu.Lock()
			sent++
			first := sent == 1
			sendMu.Unlock()
			if first {
				<-gate
			}
			return nil
		},
	}

	streamDone := make(chan error, 1)
	go func() {
		streamDone <- g.GetWorkItems(&protos.GetWorkItemsRequest{}, stream)
	}()

	const numItems = 32
	for i := range numItems {
		g.workItemQueue <- &protos.WorkItem{
			Request: &protos.WorkItem_WorkflowRequest{
				WorkflowRequest: &protos.WorkflowRequest{InstanceId: fmt.Sprintf("wi-%d", i)},
			},
		}
	}

	// The first send is parked on the gate; with pipelining the dispatch loop
	// still drains everything into the stream's outbox.
	require.Eventually(t, func() bool {
		return len(g.workItemQueue) == 0
	}, 5*time.Second, 5*time.Millisecond, "dispatch loop rendezvoused with the in-flight send")

	sendMu.Lock()
	assert.Equal(t, 1, sent, "only the gated send should have started")
	sendMu.Unlock()

	close(gate)
	require.Eventually(t, func() bool {
		sendMu.Lock()
		defer sendMu.Unlock()
		return sent == numItems
	}, 5*time.Second, 5*time.Millisecond)
}

// TestDispatchWorkflowWorkItem_OwnerBusyFallsBackAfterGrace asserts the
// warm-owner preference never stalls dispatch: with the owner's affinity
// buffer full and no stream draining it, the item falls back to the shared
// queue after the grace.
func TestDispatchWorkflowWorkItem_OwnerBusyFallsBackAfterGrace(t *testing.T) {
	fb := newFakeExecBackend()
	exec, _ := NewGrpcExecutor(fb, defaultLogger)
	g, ok := exec.(*grpcExecutor)
	require.True(t, ok)

	ss := newStreamState("s1", statefulWorkItemsRequest())
	for range cap(ss.ch) {
		ss.ch <- &protos.WorkItem{}
	}
	g.streams.Store(ss.id, ss)

	wi := &protos.WorkItem{
		Request: &protos.WorkItem_WorkflowRequest{
			WorkflowRequest: &protos.WorkflowRequest{InstanceId: "stuck-owner"},
		},
	}
	require.NoError(t, g.dispatchWorkflowWorkItem(t.Context(), "stuck-owner", wi))
	assert.Len(t, g.workItemQueue, 1)
}

// TestGetWorkItems_SendTimeoutCancelsPending exercises the
// WithStreamSendTimeout path: a Send that never returns must fail the stream
// with the timeout error, and the disconnect cleanup must cancel BOTH the
// item stuck in the in-flight send and the items still buffered in the
// outbox behind it.
func TestGetWorkItems_SendTimeoutCancelsPending(t *testing.T) {
	fb := newFakeExecBackend()
	exec, _ := NewGrpcExecutor(fb, defaultLogger, WithStreamSendTimeout(3*time.Second))
	g, ok := exec.(*grpcExecutor)
	require.True(t, ok)

	sendEntered := make(chan struct{})
	unblockSend := make(chan struct{})
	var sendOnce sync.Once
	stream := &fakeWorkItemsStream{
		ctx: t.Context(),
		send: func(*protos.WorkItem) error {
			sendOnce.Do(func() { close(sendEntered) })
			<-unblockSend
			return errors.New("stream torn down")
		},
	}
	defer close(unblockSend)

	streamDone := make(chan error, 1)
	go func() {
		streamDone <- g.GetWorkItems(statefulWorkItemsRequest(), stream)
	}()

	execDone := make(chan error, 3)
	launch := func(iid string) {
		go func() {
			_, err := g.ExecuteWorkflow(t.Context(), api.InstanceID(iid), nil,
				[]*protos.HistoryEvent{{EventId: 0}}, ExecuteOptions{})
			execDone <- err
		}()
	}

	// First item occupies the writer goroutine inside the blocked Send; the
	// send timeout is now armed and counting.
	launch("stuck-in-send")
	select {
	case <-sendEntered:
	case <-time.After(30 * time.Second):
		t.Fatal("first work item never reached Send")
	}

	// Two more items must be dispatched (stamped with this stream and queued
	// in its outbox) before the armed timeout fires, so the cleanup below is
	// provably covering buffered items and not just the in-flight one. An
	// empty shared queue means the dispatch loop has picked both up; the
	// bound is deliberately inside the 3s timeout so exceeding it fails here
	// with a clear message instead of a confusing cancellation miss later.
	launch("buffered-1")
	launch("buffered-2")
	require.Eventually(t, func() bool {
		return len(g.workItemQueue) == 0
	}, 2*time.Second, time.Millisecond, "buffered items were not dispatched before the send timeout")

	select {
	case err := <-streamDone:
		require.ErrorContains(t, err, "timed out while sending work item")
	case <-time.After(30 * time.Second):
		t.Fatal("GetWorkItems did not return after the send timeout")
	}

	for range 3 {
		select {
		case err := <-execDone:
			require.EqualError(t, err, "operation aborted")
		case <-time.After(30 * time.Second):
			t.Fatal("ExecuteWorkflow did not return after the send timeout")
		}
	}

	assert.ElementsMatch(t,
		[]api.InstanceID{"stuck-in-send", "buffered-1", "buffered-2"},
		fb.canceledInstances())
}
