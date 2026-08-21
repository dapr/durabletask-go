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
	if be.deliverPendingActivityTask(response.GetInstanceId(), response.GetTaskId(), response) {
		return nil
	}

	return api.NewUnknownTaskIDError(response.GetInstanceId(), response.GetTaskId())
}

func (be *TasksBackend) CancelActivityTask(ctx context.Context, instanceID api.InstanceID, taskID int32) error {
	if be.deliverPendingActivityTask(string(instanceID), taskID, nil) {
		return nil
	}
	return api.NewUnknownTaskIDError(instanceID.String(), taskID)
}
func (be *TasksBackend) OnActivityCompletion(request *protos.ActivityRequest, cb func(*protos.ActivityResponse, error)) func() {
	key := backend.GetActivityExecutionKey(request.GetWorkflowInstance().GetInstanceId(), request.GetTaskId())
	pending := &pendingActivity{cb: cb}
	be.pendingActivities.Store(key, pending)

	return func() {
		be.pendingActivities.CompareAndDelete(key, pending)
	}
}

func (be *TasksBackend) CompleteWorkflowTask(ctx context.Context, response *protos.WorkflowResponse) error {
	if be.deliverPendingWorkflow(response.GetInstanceId(), response) {
		return nil
	}
	return api.NewUnknownInstanceIDError(response.GetInstanceId())
}

func (be *TasksBackend) CancelWorkflowTask(ctx context.Context, instanceID api.InstanceID) error {
	if be.deliverPendingWorkflow(string(instanceID), nil) {
		return nil
	}
	return api.NewUnknownInstanceIDError(instanceID.String())
}
func (be *TasksBackend) OnWorkflowTaskCompletion(request *protos.WorkflowRequest, cb func(*protos.WorkflowResponse, error)) func() {
	key := request.GetInstanceId()
	pending := &pendingWorkflow{cb: cb}
	be.pendingWorkflows.Store(key, pending)

	return func() {
		be.pendingWorkflows.CompareAndDelete(key, pending)
	}
}

func (be *TasksBackend) deliverPendingActivityTask(iid string, taskID int32, res *protos.ActivityResponse) bool {
	key := backend.GetActivityExecutionKey(iid, taskID)
	p, ok := be.pendingActivities.Load(key)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*pendingActivity)
	if pending.cb != nil {
		// Callback registrations stay in the map until the executor's arbiter
		// accepts a delivery and runs the deregister closure. Deleting here
		// would open a window where a stale-token delivery consumes the only
		// routing entry while the genuine response races in and is dropped as
		// unknown, stranding the re-armed callback forever.
		if res == nil {
			pending.cb(nil, api.ErrTaskCancelled)
		} else {
			pending.cb(res, nil)
		}
		return true
	}
	// Channel path: single delivery, the first responder to win the entry
	// parks the payload; a racing duplicate reports unknown as before.
	if !be.pendingActivities.CompareAndDelete(key, p) {
		return false
	}
	pending.response = res
	close(pending.complete)
	return true
}

func (be *TasksBackend) deliverPendingWorkflow(instanceID string, res *protos.WorkflowResponse) bool {
	p, ok := be.pendingWorkflows.Load(instanceID)
	if !ok {
		return false
	}

	// Note that res can be nil in case of certain failures
	pending := p.(*pendingWorkflow)
	if pending.cb != nil {
		// See deliverPendingActivityTask: the registration outlives stale
		// deliveries; only the deregister closure removes it.
		if res == nil {
			pending.cb(nil, api.ErrTaskCancelled)
		} else {
			pending.cb(res, nil)
		}
		return true
	}
	if !be.pendingWorkflows.CompareAndDelete(instanceID, p) {
		return false
	}
	pending.response = res
	close(pending.complete)
	return true
}
