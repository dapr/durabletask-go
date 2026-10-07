package local

import (
	"context"
	"slices"
	"sync"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
)

type TasksBackend struct {
	workflows  registry[*protos.WorkflowResponse]
	activities registry[*protos.ActivityResponse]
}

func NewTasksBackend() *TasksBackend {
	return &TasksBackend{
		workflows:  newRegistry[*protos.WorkflowResponse](),
		activities: newRegistry[*protos.ActivityResponse](),
	}
}

func (be *TasksBackend) CompleteActivityTask(ctx context.Context, response *protos.ActivityResponse) error {
	if be.activities.deliver(backend.GetActivityExecutionKey(response.GetInstanceId(), response.GetTaskId()), response) {
		return nil
	}
	return api.NewUnknownTaskIDError(response.GetInstanceId(), response.GetTaskId())
}

func (be *TasksBackend) CancelActivityTask(ctx context.Context, instanceID api.InstanceID, taskID int32) error {
	if be.activities.deliver(backend.GetActivityExecutionKey(string(instanceID), taskID), nil) {
		return nil
	}
	return api.NewUnknownTaskIDError(instanceID.String(), taskID)
}

func (be *TasksBackend) WaitForActivityCompletion(request *protos.ActivityRequest) func(context.Context) (*protos.ActivityResponse, error) {
	return be.activities.wait(backend.GetActivityExecutionKey(request.GetWorkflowInstance().GetInstanceId(), request.GetTaskId()))
}

func (be *TasksBackend) CompleteWorkflowTask(ctx context.Context, response *protos.WorkflowResponse) error {
	if be.workflows.deliver(response.GetInstanceId(), response) {
		return nil
	}
	return api.NewUnknownInstanceIDError(response.GetInstanceId())
}

func (be *TasksBackend) CancelWorkflowTask(ctx context.Context, instanceID api.InstanceID) error {
	if be.workflows.deliver(string(instanceID), nil) {
		return nil
	}
	return api.NewUnknownInstanceIDError(instanceID.String())
}

func (be *TasksBackend) WaitForWorkflowTaskCompletion(request *protos.WorkflowRequest) func(context.Context) (*protos.WorkflowResponse, error) {
	return be.workflows.wait(request.GetInstanceId())
}

type waiter[R any] struct {
	response R
	err      error
	complete chan struct{}
}

// registry holds every pending waiter per task key. Several executions of the
// same task can be pending at once, for example when an instance is recreated
// while its previous run's activity is still running. Keeping only the latest
// would strand the others until their context ends.
type registry[R comparable] struct {
	lock  sync.Mutex
	byKey map[string][]*waiter[R]
}

func newRegistry[R comparable]() registry[R] {
	return registry[R]{byKey: make(map[string][]*waiter[R])}
}

func (r *registry[R]) wait(key string) func(context.Context) (R, error) {
	w := &waiter[R]{complete: make(chan struct{})}

	r.lock.Lock()
	r.byKey[key] = append(r.byKey[key], w)
	r.lock.Unlock()

	return func(ctx context.Context) (R, error) {
		select {
		case <-ctx.Done():
			r.remove(key, w)
			var zero R
			return zero, ctx.Err()
		case <-w.complete:
			return w.response, w.err
		}
	}
}

func (r *registry[R]) remove(key string, w *waiter[R]) {
	r.lock.Lock()
	defer r.lock.Unlock()
	waiters := slices.DeleteFunc(r.byKey[key], func(c *waiter[R]) bool { return c == w })
	if len(waiters) == 0 {
		delete(r.byKey, key)
	} else {
		r.byKey[key] = waiters
	}
}

// deliver completes every waiter for key. A nil response is a cancellation.
// Responses carry no completion token, so a response cannot be matched to one
// of several pending executions and they are all cancelled instead.
func (r *registry[R]) deliver(key string, res R) bool {
	r.lock.Lock()
	waiters := r.byKey[key]
	delete(r.byKey, key)
	r.lock.Unlock()

	var zero R
	var err error
	if res == zero || len(waiters) > 1 {
		res, err = zero, api.ErrTaskCancelled
	}
	for _, w := range waiters {
		w.response, w.err = res, err
		close(w.complete)
	}
	return len(waiters) > 0
}
