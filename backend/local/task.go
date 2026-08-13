package local

import (
	"context"
	"sync"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
)

type pendingWorkflow struct {
	response *protos.WorkflowResponse
	complete chan struct{}
	cb       func(*protos.WorkflowResponse, error)
}

type pendingActivity struct {
	response *protos.ActivityResponse
	complete chan struct{}
	cb       func(*protos.ActivityResponse, error)
}

type TasksBackend struct {
	pendingWorkflows  *sync.Map
	pendingActivities *sync.Map
}

func NewTasksBackend() *TasksBackend {
	return &TasksBackend{
		pendingWorkflows:  &sync.Map{},
		pendingActivities: &sync.Map{},
	}
}

func (be *TasksBackend) CompleteActivityTask(ctx context.Context, response *protos.ActivityResponse) error {
	if be.deletePendingActivityTask(response.GetInstanceId(), response.GetTaskId(), response) {
		return nil
	}

	return api.NewUnknownTaskIDError(response.GetInstanceId(), response.GetTaskId())
}

func (be *TasksBackend) CancelActivityTask(ctx context.Context, instanceID api.InstanceID, taskID int32) error {
	if be.deletePendingActivityTask(string(instanceID), taskID, nil) {
		return nil
	}
	return api.NewUnknownTaskIDError(instanceID.String(), taskID)
}

func (be *TasksBackend) WaitForActivityCompletion(request *protos.ActivityRequest) func(context.Context) (*protos.ActivityResponse, error) {
	key := backend.GetActivityExecutionKey(request.GetWorkflowInstance().GetInstanceId(), request.GetTaskId())
	pending := &pendingActivity{
		response: nil,
		complete: make(chan struct{}, 1),
	}
	be.pendingActivities.Store(key, pending)

	return func(ctx context.Context) (*protos.ActivityResponse, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pending.complete:
			if pending.response == nil {
				return nil, api.ErrTaskCancelled
			}
			return pending.response, nil
		}
	}
}

// OnActivityCompletion implements backend.CompletionCallbackBackend.
func (be *TasksBackend) OnActivityCompletion(request *protos.ActivityRequest, cb func(*protos.ActivityResponse, error)) func() {
	key := backend.GetActivityExecutionKey(request.GetWorkflowInstance().GetInstanceId(), request.GetTaskId())
	pending := &pendingActivity{cb: cb}
	be.pendingActivities.Store(key, pending)

	return func() {
		be.pendingActivities.CompareAndDelete(key, pending)
	}
}

func (be *TasksBackend) CompleteWorkflowTask(ctx context.Context, response *protos.WorkflowResponse) error {
	if be.deletePendingWorkflow(response.GetInstanceId(), response) {
		return nil
	}
	return api.NewUnknownInstanceIDError(response.GetInstanceId())
}

func (be *TasksBackend) CancelWorkflowTask(ctx context.Context, instanceID api.InstanceID) error {
	if be.deletePendingWorkflow(string(instanceID), nil) {
		return nil
	}
	return api.NewUnknownInstanceIDError(instanceID.String())
}

func (be *TasksBackend) WaitForWorkflowTaskCompletion(request *protos.WorkflowRequest) func(context.Context) (*protos.WorkflowResponse, error) {
	pending := &pendingWorkflow{
		response: nil,
		complete: make(chan struct{}, 1),
	}
	be.pendingWorkflows.Store(request.GetInstanceId(), pending)

	return func(ctx context.Context) (*protos.WorkflowResponse, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-pending.complete:
			if pending.response == nil {
				return nil, api.ErrTaskCancelled
			}
			return pending.response, nil
		}
	}
}

// OnWorkflowTaskCompletion implements backend.CompletionCallbackBackend.
func (be *TasksBackend) OnWorkflowTaskCompletion(request *protos.WorkflowRequest, cb func(*protos.WorkflowResponse, error)) func() {
	key := request.GetInstanceId()
	pending := &pendingWorkflow{cb: cb}
	be.pendingWorkflows.Store(key, pending)

	return func() {
		be.pendingWorkflows.CompareAndDelete(key, pending)
	}
}

func (be *TasksBackend) deletePendingActivityTask(iid string, taskID int32, res *protos.ActivityResponse) bool {
	key := backend.GetActivityExecutionKey(iid, taskID)
	p, ok := be.pendingActivities.LoadAndDelete(key)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*pendingActivity)
	if pending.cb != nil {
		if res == nil {
			pending.cb(nil, api.ErrTaskCancelled)
		} else {
			pending.cb(res, nil)
		}
		return true
	}
	pending.response = res
	close(pending.complete)
	return true
}

func (be *TasksBackend) deletePendingWorkflow(instanceID string, res *protos.WorkflowResponse) bool {
	p, ok := be.pendingWorkflows.LoadAndDelete(instanceID)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*pendingWorkflow)
	if pending.cb != nil {
		if res == nil {
			pending.cb(nil, api.ErrTaskCancelled)
		} else {
			pending.cb(res, nil)
		}
		return true
	}
	pending.response = res
	close(pending.complete)
	return true
}
