package local_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/local"
)

type tasksOnlyBackend struct {
	backend.Backend
	tasks *local.TasksBackend
}

func (b *tasksOnlyBackend) CompleteActivityTask(ctx context.Context, res *protos.ActivityResponse) error {
	return b.tasks.CompleteActivityTask(ctx, res)
}

func (b *tasksOnlyBackend) CancelActivityTask(ctx context.Context, iid api.InstanceID, taskID int32) error {
	return b.tasks.CancelActivityTask(ctx, iid, taskID)
}

func (b *tasksOnlyBackend) WaitForActivityCompletion(req *protos.ActivityRequest) func(context.Context) (*protos.ActivityResponse, error) {
	return b.tasks.WaitForActivityCompletion(req)
}

type workItemsStream struct {
	grpc.ServerStream
	ctx     context.Context
	items   chan *protos.WorkItem
	sending chan *protos.WorkItem
}

func (s *workItemsStream) Context() context.Context { return s.ctx }

func (s *workItemsStream) Send(wi *protos.WorkItem) error {
	if s.sending != nil {
		s.sending <- wi
	}
	select {
	case s.items <- wi:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

// Two executions of the same activity task pending on one host, as when an
// instance is recreated while its previous run's activity is still running.
// The response cannot be attributed to either, so both are aborted and none is
// left waiting.
func Test_concurrentExecutionsOfSameActivityAreAborted(t *testing.T) {
	be := &tasksOnlyBackend{tasks: local.NewTasksBackend()}
	exec, _ := backend.NewGrpcExecutor(be, backend.DefaultLogger())
	server := exec.(protos.TaskHubSidecarServiceServer)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &workItemsStream{ctx: ctx, items: make(chan *protos.WorkItem, 2)}
	go func() { _ = server.GetWorkItems(&protos.GetWorkItemsRequest{}, stream) }()

	errs := make(chan error, 2)
	for range 2 {
		go func() {
			_, err := exec.ExecuteActivity(ctx, api.InstanceID("wf1"), &protos.HistoryEvent{
				EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{Name: "act"}},
			}, backend.ExecuteOptions{})
			errs <- err
		}()
	}

	for range 2 {
		select {
		case <-stream.items:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "work item was not dispatched")
		}
	}

	require.ErrorIs(t, be.CompleteActivityTask(ctx, &protos.ActivityResponse{InstanceId: "wf1", TaskId: 0, Result: wrapperspb.String("x")}), local.ErrAmbiguousCompletion)
	for range 2 {
		select {
		case err := <-errs:
			require.EqualError(t, err, "operation aborted")
		case <-time.After(5 * time.Second):
			require.FailNow(t, "execution was stranded")
		}
	}

	require.Error(t, be.CompleteActivityTask(ctx, &protos.ActivityResponse{InstanceId: "wf1", TaskId: 0}))
}

// When the newer of two executions of the same task ends first, the older must
// still be cancelled when its stream disconnects or the executor shuts down.
func Test_olderExecutionCancelledAfterNewerEnds(t *testing.T) {
	for name, release := range map[string]func(backend.Executor, context.CancelFunc){
		"stream disconnect": func(_ backend.Executor, closeStream context.CancelFunc) { closeStream() },
		"shutdown": func(exec backend.Executor, closeStream context.CancelFunc) {
			require.NoError(t, exec.Shutdown(t.Context()))
			closeStream()
		},
	} {
		t.Run(name, func(t *testing.T) {
			be := &tasksOnlyBackend{tasks: local.NewTasksBackend()}
			exec, _ := backend.NewGrpcExecutor(be, backend.DefaultLogger())
			server := exec.(protos.TaskHubSidecarServiceServer)

			streamCtx, closeStream := context.WithCancel(t.Context())
			defer closeStream()
			stream := &workItemsStream{ctx: streamCtx, items: make(chan *protos.WorkItem, 2)}
			go func() { _ = server.GetWorkItems(&protos.GetWorkItemsRequest{}, stream) }()

			execute := func(ctx context.Context, errs chan<- error) {
				_, err := exec.ExecuteActivity(ctx, api.InstanceID("wf1"), &protos.HistoryEvent{
					EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{Name: "act"}},
				}, backend.ExecuteOptions{})
				errs <- err
			}
			dispatched := func() {
				select {
				case <-stream.items:
				case <-time.After(5 * time.Second):
					require.FailNow(t, "work item was not dispatched")
				}
			}

			olderErr := make(chan error, 1)
			go execute(t.Context(), olderErr)
			dispatched()

			newerCtx, cancelNewer := context.WithCancel(t.Context())
			newerErr := make(chan error, 1)
			go execute(newerCtx, newerErr)
			dispatched()
			cancelNewer()
			select {
			case <-newerErr:
			case <-time.After(5 * time.Second):
				require.FailNow(t, "newer execution did not end")
			}

			release(exec, closeStream)
			select {
			case err := <-olderErr:
				require.EqualError(t, err, "operation aborted")
			case <-time.After(5 * time.Second):
				require.FailNow(t, "older execution was stranded")
			}
		})
	}
}

// Closing a stream aborts only the executions sent on it: a newer execution
// of the same task on another stream keeps running, whether the older one is
// still live or has already ended.
func Test_streamCloseAbortsOnlyItsExecutions(t *testing.T) {
	for name, olderEnds := range map[string]bool{"older live": false, "older ended": true} {
		t.Run(name, func(t *testing.T) {
			testStreamCloseAbortsOnlyItsExecutions(t, olderEnds)
		})
	}
}

func testStreamCloseAbortsOnlyItsExecutions(t *testing.T, olderEnds bool) {
	be := &tasksOnlyBackend{tasks: local.NewTasksBackend()}
	exec, _ := backend.NewGrpcExecutor(be, backend.DefaultLogger())
	server := exec.(protos.TaskHubSidecarServiceServer)

	execute := func(ctx context.Context, taskID int32, errs chan<- error) {
		_, err := exec.ExecuteActivity(ctx, api.InstanceID("wf1"), &protos.HistoryEvent{
			EventId:   taskID,
			EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{Name: "act"}},
		}, backend.ExecuteOptions{})
		errs <- err
	}
	receive := func(ch <-chan *protos.WorkItem) {
		select {
		case <-ch:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "work item was not dispatched")
		}
	}

	s1Ctx, closeS1 := context.WithCancel(t.Context())
	defer closeS1()
	s1 := &workItemsStream{ctx: s1Ctx, items: make(chan *protos.WorkItem), sending: make(chan *protos.WorkItem, 2)}
	go func() { _ = server.GetWorkItems(&protos.GetWorkItemsRequest{}, s1) }()

	olderCtx, cancelOlder := context.WithCancel(t.Context())
	defer cancelOlder()
	olderErr := make(chan error, 1)
	go execute(olderCtx, 0, olderErr)
	receive(s1.items)

	// Keep s1 busy so the next execution goes to s2.
	go execute(t.Context(), 1, make(chan error, 1))
	receive(s1.sending)
	receive(s1.sending)

	s2Ctx, closeS2 := context.WithCancel(t.Context())
	defer closeS2()
	s2 := &workItemsStream{ctx: s2Ctx, items: make(chan *protos.WorkItem, 1)}
	go func() { _ = server.GetWorkItems(&protos.GetWorkItemsRequest{}, s2) }()

	newerErr := make(chan error, 1)
	go execute(t.Context(), 0, newerErr)
	receive(s2.items)

	if olderEnds {
		cancelOlder()
		select {
		case <-olderErr:
		case <-time.After(5 * time.Second):
			require.FailNow(t, "older execution did not end")
		}
	}

	closeS1()
	if !olderEnds {
		select {
		case err := <-olderErr:
			require.EqualError(t, err, "operation aborted")
		case <-time.After(5 * time.Second):
			require.FailNow(t, "older execution was not aborted")
		}
	}
	select {
	case err := <-newerErr:
		require.FailNow(t, "newer execution was cancelled by another stream", "%v", err)
	case <-time.After(500 * time.Millisecond):
	}

	require.NoError(t, be.CompleteActivityTask(t.Context(), &protos.ActivityResponse{InstanceId: "wf1", TaskId: 0, Result: wrapperspb.String("x")}))
	select {
	case err := <-newerErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "newer execution did not complete")
	}
}
