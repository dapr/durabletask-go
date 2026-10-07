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
	if be.activities.deliver(backend.GetActivityExecutionKey(response.GetInstanceId(), response.GetTaskId()), response, nil) {
		return nil
	}
	return api.NewUnknownTaskIDError(response.GetInstanceId(), response.GetTaskId())
}

func (be *TasksBackend) CancelActivityTask(ctx context.Context, instanceID api.InstanceID, taskID int32) error {
	if be.activities.deliver(backend.GetActivityExecutionKey(string(instanceID), taskID), nil, api.ErrTaskCancelled) {
		return nil
	}
	return api.NewUnknownTaskIDError(instanceID.String(), taskID)
}

func (be *TasksBackend) OnActivityCompletion(request *protos.ActivityRequest, onResult func(*protos.ActivityResponse, error)) func() {
	return be.activities.add(backend.GetActivityExecutionKey(request.GetWorkflowInstance().GetInstanceId(), request.GetTaskId()), onResult)
}

func (be *TasksBackend) CompleteWorkflowTask(ctx context.Context, response *protos.WorkflowResponse) error {
	if be.workflows.deliver(response.GetInstanceId(), response, nil) {
		return nil
	}
	return api.NewUnknownInstanceIDError(response.GetInstanceId())
}

func (be *TasksBackend) CancelWorkflowTask(ctx context.Context, instanceID api.InstanceID) error {
	if be.workflows.deliver(string(instanceID), nil, api.ErrTaskCancelled) {
		return nil
	}
	return api.NewUnknownInstanceIDError(instanceID.String())
}

func (be *TasksBackend) OnWorkflowTaskCompletion(request *protos.WorkflowRequest, onResult func(*protos.WorkflowResponse, error)) func() {
	return be.workflows.add(request.GetInstanceId(), onResult)
}

type completion interface {
	GetCompletionToken() string
}

type registration[R completion] struct {
	onResult func(R, error)
}

// registry holds every live registration per task key. Several executions of
// the same task can be pending at once (a recreated instance or a superseded
// dispatch), so a delivery reaches all of them and each execution's arbiter
// settles on its own response. Keeping only the latest would strand the
// others until their context ends.
type registry[R completion] struct {
	lock  sync.Mutex
	byKey map[string][]*registration[R]
}

func newRegistry[R completion]() registry[R] {
	return registry[R]{byKey: make(map[string][]*registration[R])}
}

func (r *registry[R]) add(key string, onResult func(R, error)) func() {
	reg := &registration[R]{onResult: onResult}

	r.lock.Lock()
	r.byKey[key] = append(r.byKey[key], reg)
	r.lock.Unlock()

	return func() {
		r.lock.Lock()
		defer r.lock.Unlock()
		regs := slices.DeleteFunc(r.byKey[key], func(c *registration[R]) bool { return c == reg })
		if len(regs) == 0 {
			delete(r.byKey, key)
		} else {
			r.byKey[key] = regs
		}
	}
}

// deliver runs every registered callback for key outside the lock, since a
// callback settling its execution calls back into the deregister closure. A
// response without a completion token cannot be matched to one of several
// pending executions, so they are all cancelled instead.
func (r *registry[R]) deliver(key string, res R, err error) bool {
	r.lock.Lock()
	regs := slices.Clone(r.byKey[key])
	r.lock.Unlock()

	if err == nil && len(regs) > 1 && res.GetCompletionToken() == "" {
		var zero R
		res, err = zero, api.ErrTaskCancelled
	}

	for _, reg := range regs {
		reg.onResult(res, err)
	}
	return len(regs) > 0
}
