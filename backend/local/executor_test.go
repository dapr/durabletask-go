package local_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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

func (b *tasksOnlyBackend) OnActivityCompletion(req *protos.ActivityRequest, cb func(*protos.ActivityResponse, error)) func() {
	return b.tasks.OnActivityCompletion(req, cb)
}

type workItemsStream struct {
	grpc.ServerStream
	ctx   context.Context
	items chan *protos.WorkItem
}

func (s *workItemsStream) Context() context.Context { return s.ctx }

func (s *workItemsStream) Send(wi *protos.WorkItem) error {
	select {
	case s.items <- wi:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func taskScheduled(taskID int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   taskID,
		EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{Name: "act"}},
	}
}

// Two executions of the same activity task pending on one host, as when an
// instance is recreated while its previous run's activity is still running:
// each must settle on its own response and none may be left waiting.
func Test_concurrentExecutionsOfSameActivityAllSettle(t *testing.T) {
	be := &tasksOnlyBackend{tasks: local.NewTasksBackend()}
	exec, _ := backend.NewGrpcExecutor(be, backend.DefaultLogger())
	server := exec.(protos.TaskHubSidecarServiceServer)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	stream := &workItemsStream{ctx: ctx, items: make(chan *protos.WorkItem, 2)}
	go func() { _ = server.GetWorkItems(&protos.GetWorkItemsRequest{}, stream) }()

	results := make(chan string, 2)
	for range 2 {
		go func() {
			ev, err := exec.ExecuteActivity(ctx, api.InstanceID("wf1"), taskScheduled(0), backend.ExecuteOptions{})
			if assert.NoError(t, err) {
				results <- ev.GetTaskCompleted().GetResult().GetValue()
			}
		}()
	}

	var tokens []string
	for range 2 {
		select {
		case wi := <-stream.items:
			tokens = append(tokens, wi.GetCompletionToken())
		case <-time.After(5 * time.Second):
			require.FailNow(t, "work item was not dispatched")
		}
	}
	require.NotEqual(t, tokens[0], tokens[1])

	for _, token := range tokens {
		require.NoError(t, be.CompleteActivityTask(ctx, &protos.ActivityResponse{
			InstanceId:      "wf1",
			TaskId:          0,
			Result:          wrapperspb.String(token),
			CompletionToken: token,
		}))
		select {
		case got := <-results:
			assert.Equal(t, token, got)
		case <-time.After(5 * time.Second):
			require.FailNow(t, "execution did not settle on its own response", "token %s", token)
		}
	}

	require.Error(t, be.CompleteActivityTask(ctx, &protos.ActivityResponse{InstanceId: "wf1", TaskId: 0}))
}

// A worker that does not echo completion tokens cannot say which of several
// pending executions a response belongs to, so none may adopt it.
func Test_tokenlessResponseCancelsConcurrentExecutions(t *testing.T) {
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
			_, err := exec.ExecuteActivity(ctx, api.InstanceID("wf1"), taskScheduled(0), backend.ExecuteOptions{})
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

	require.ErrorIs(t, be.CompleteActivityTask(ctx, &protos.ActivityResponse{
		InstanceId: "wf1",
		TaskId:     0,
		Result:     wrapperspb.String("ambiguous"),
	}), local.ErrAmbiguousCompletion)
	for range 2 {
		select {
		case err := <-errs:
			require.EqualError(t, err, "operation aborted")
		case <-time.After(5 * time.Second):
			require.FailNow(t, "execution did not settle")
		}
	}

	require.Error(t, be.CompleteActivityTask(ctx, &protos.ActivityResponse{InstanceId: "wf1", TaskId: 0}))
}
