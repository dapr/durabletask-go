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
}

type pendingActivity struct {
	response *protos.ActivityResponse
	complete chan struct{}
}

type TasksBackend struct {
	pendingWorkflows *sync.Map
	pendingActivities    *sync.Map
}

func NewTasksBackend() *TasksBackend {
	return &TasksBackend{
		pendingWorkflows: &sync.Map{},
		pendingActivities:    &sync.Map{},
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

func (be *TasksBackend) deletePendingActivityTask(iid string, taskID int32, res *protos.ActivityResponse) bool {
	key := backend.GetActivityExecutionKey(iid, taskID)
	p, ok := be.pendingActivities.LoadAndDelete(key)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*pendingActivity)
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
	pending.response = res
	close(pending.complete)
	return true
}
