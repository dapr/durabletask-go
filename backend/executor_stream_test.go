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
// GetWorkItems streams: completion waiters keyed by instance ID.
type fakeExecBackend struct {
	Backend
	mu      sync.Mutex
	waiters map[string]chan *protos.WorkflowResponse
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
// with the timeout error, and the disconnect cleanup must abort BOTH the
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

}

// TestGetWorkItems_DisconnectRedeliversBufferedItem exercises the teardown
// path through a real GetWorkItems disconnect: items parked in the dying
// stream's affinity buffer (never pulled by its dispatch loop) must be
// re-offered to the shared queue and delivered by a surviving stream. This
// covers the defer ordering (registry delete before drain, drain before the
// stream returns) that the direct drainStreamBuffer tests cannot.
func TestGetWorkItems_DisconnectRedeliversBufferedItem(t *testing.T) {
	fb := newFakeExecBackend()
	exec, _ := NewGrpcExecutor(fb, defaultLogger)
	g, ok := exec.(*grpcExecutor)
	require.True(t, ok)

	// Survivor stream: plain (non-stateful) so it never becomes the affinity
	// owner; it only consumes the shared queue.
	var recMu sync.Mutex
	survivorGot := map[string]bool{}
	survivorCtx, survivorCancel := context.WithCancel(t.Context())
	defer survivorCancel()
	survivor := &fakeWorkItemsStream{
		ctx: survivorCtx,
		send: func(wi *protos.WorkItem) error {
			recMu.Lock()
			survivorGot[wi.GetWorkflowRequest().GetInstanceId()] = true
			recMu.Unlock()
			return nil
		},
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		assert.NoError(t, g.GetWorkItems(&protos.GetWorkItemsRequest{}, survivor))
	}()

	// Dying stream: stateful-history capable, its Send blocks forever so the
	// dispatch loop backs up and items park in the affinity buffer.
	dyingCtx, dyingCancel := context.WithCancel(t.Context())
	defer dyingCancel()
	blockSend := make(chan struct{})
	defer close(blockSend)
	dying := &fakeWorkItemsStream{
		ctx: dyingCtx,
		send: func(wi *protos.WorkItem) error {
			select {
			case <-blockSend:
			case <-dyingCtx.Done():
			}
			return dyingCtx.Err()
		},
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		// The dying stream may return the failed send's error; teardown
		// behavior is what this test asserts, not the return value.
		_ = g.GetWorkItems(statefulWorkItemsRequest(), dying)
	}()

	// Wait for the dying stream to register, then locate its streamState.
	var ss *streamState
	require.Eventually(t, func() bool {
		g.streams.Range(func(_, value any) bool {
			if s, sok := value.(*streamState); sok && s.statefulHistory {
				ss = s
			}
			return true
		})
		return ss != nil
	}, time.Second*5, time.Millisecond)

	// Route items into the affinity buffer until at least one is parked
	// there (the dispatch loop drains into its outbox until the blocked
	// writer backs it up; over-filling by the outbox size guarantees a
	// residue in ss.ch).
	i := 0
	require.Eventually(t, func() bool {
		if len(ss.ch) > 0 && i > streamOutboxSize {
			return true
		}
		i++
		iid := fmt.Sprintf("parked-%d", i)
		wi := &protos.WorkItem{Request: &protos.WorkItem_WorkflowRequest{
			WorkflowRequest: &protos.WorkflowRequest{InstanceId: iid},
		}}
		g.pendingWorkflows.add(iid, wi, api.InstanceID(iid), 0, func() {})
		ok := ss.trySend(wi)
		_ = ok
		return false
	}, time.Second*5, time.Millisecond)

	// Disconnect the dying stream: the parked residue must reach the
	// survivor via the shared queue.
	dyingCancel()

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		recMu.Lock()
		defer recMu.Unlock()
		assert.NotEmpty(c, survivorGot, "a surviving stream must receive the items parked in the dead stream's buffer")
	}, time.Second*10, time.Millisecond*5)

	assert.Empty(t, ss.ch, "the dead stream's buffer must be fully drained")
	survivorCancel()
	wg.Wait()
}

// TestGetWorkItems_HealthPing asserts HealthPings are sent only to streams
// whose worker advertised WORKER_CAPABILITY_HEALTH_PING, since older workers
// mishandle work item types they do not know.
func TestGetWorkItems_HealthPing(t *testing.T) {
	for name, tc := range map[string]struct {
		caps     []protos.WorkerCapability
		wantPing bool
	}{
		"advertised": {
			caps:     []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_HEALTH_PING},
			wantPing: true,
		},
		"not advertised": {
			caps: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
		},
	} {
		t.Run(name, func(t *testing.T) {
			exec, _ := NewGrpcExecutor(newFakeExecBackend(), defaultLogger, WithHealthPingInterval(10*time.Millisecond))
			g, ok := exec.(*grpcExecutor)
			require.True(t, ok)

			ctx, cancel := context.WithCancel(t.Context())
			pings := make(chan struct{}, 1)
			stream := &fakeWorkItemsStream{
				ctx: ctx,
				send: func(wi *protos.WorkItem) error {
					if wi.GetHealthPing() != nil {
						select {
						case pings <- struct{}{}:
						default:
						}
					}
					return nil
				},
			}

			done := make(chan error, 1)
			go func() {
				done <- g.GetWorkItems(&protos.GetWorkItemsRequest{Capabilities: tc.caps}, stream)
			}()

			if tc.wantPing {
				select {
				case <-pings:
				case <-time.After(5 * time.Second):
					t.Fatal("no health ping received")
				}
			} else {
				select {
				case <-pings:
					t.Fatal("health ping sent to a worker that did not advertise it")
				case <-time.After(100 * time.Millisecond):
				}
			}

			cancel()
			require.NoError(t, <-done)
		})
	}
}
