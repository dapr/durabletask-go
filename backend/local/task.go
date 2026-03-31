package local

import (
	"context"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/local/loops"
	looptask "github.com/dapr/durabletask-go/backend/local/loops/task"
	"github.com/dapr/kit/events/loop"
)

type TasksBackend struct {
	loop loop.Interface[loops.EventTask]
}

func NewTasksBackend() *TasksBackend {
	return &TasksBackend{
		loop: looptask.New(),
	}
}

func (be *TasksBackend) Run(ctx context.Context) error {
	return be.loop.Run(ctx)
}

func (be *TasksBackend) Close() {
	be.loop.Close(new(loops.Shutdown))
}

func (be *TasksBackend) CompleteActivityTask(ctx context.Context, response *protos.ActivityResponse) error {
	errCh := make(chan error, 1)
	be.loop.Enqueue(&loops.CompleteActivity{
		InstanceID: response.GetInstanceId(),
		TaskID:     response.GetTaskId(),
		Response:   response,
		ErrCh:      errCh,
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (be *TasksBackend) CancelActivityTask(ctx context.Context, instanceID api.InstanceID, taskID int32) error {
	errCh := make(chan error, 1)
	be.loop.Enqueue(&loops.CancelActivity{
		InstanceID: instanceID,
		TaskID:     taskID,
		ErrCh:      errCh,
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (be *TasksBackend) WaitForActivityCompletion(request *protos.ActivityRequest) func(context.Context) (*protos.ActivityResponse, error) {
	key := backend.GetActivityExecutionKey(request.GetOrchestrationInstance().GetInstanceId(), request.GetTaskId())
	responseCh := make(chan *protos.ActivityResponse, 1)

	be.loop.Enqueue(&loops.RegisterPendingActivity{
		Key:      key,
		Response: responseCh,
	})

	return func(ctx context.Context) (*protos.ActivityResponse, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp, ok := <-responseCh:
			if !ok || resp == nil {
				return nil, api.ErrTaskCancelled
			}
			return resp, nil
		}
	}
}

func (be *TasksBackend) CompleteOrchestratorTask(ctx context.Context, response *protos.OrchestratorResponse) error {
	errCh := make(chan error, 1)
	be.loop.Enqueue(&loops.CompleteOrchestrator{
		InstanceID: response.GetInstanceId(),
		Response:   response,
		ErrCh:      errCh,
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (be *TasksBackend) CancelOrchestratorTask(ctx context.Context, instanceID api.InstanceID) error {
	errCh := make(chan error, 1)
	be.loop.Enqueue(&loops.CancelOrchestrator{
		InstanceID: instanceID,
		ErrCh:      errCh,
	})

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}

func (be *TasksBackend) WaitForOrchestratorCompletion(request *protos.OrchestratorRequest) func(context.Context) (*protos.OrchestratorResponse, error) {
	responseCh := make(chan *protos.OrchestratorResponse, 1)

	be.loop.Enqueue(&loops.RegisterPendingOrchestrator{
		InstanceID: request.GetInstanceId(),
		Response:   responseCh,
	})

	return func(ctx context.Context) (*protos.OrchestratorResponse, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case resp, ok := <-responseCh:
			if !ok || resp == nil {
				return nil, api.ErrTaskCancelled
			}
			return resp, nil
		}
	}
}
