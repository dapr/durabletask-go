package backend

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

const (
	priorityLow      = protos.PriorityLevel_PRIORITY_LEVEL_LOW
	priorityHigh     = protos.PriorityLevel_PRIORITY_LEVEL_HIGH
	priorityCritical = protos.PriorityLevel_PRIORITY_LEVEL_CRITICAL
)

// captureBackend keeps the create request it was given.
type captureBackend struct {
	Backend

	created *CreateWorkflowInstanceRequest
}

func (b *captureBackend) CreateWorkflowInstance(_ context.Context, req *CreateWorkflowInstanceRequest) error {
	b.created = req
	return nil
}

func (b *captureBackend) WatchWorkflowRuntimeStatus(_ context.Context, id api.InstanceID, _ *protos.TaskRouter, condition func(*WorkflowMetadata) bool) error {
	condition(&protos.WorkflowMetadata{
		InstanceId:    string(id),
		RuntimeStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING,
	})
	return nil
}

func TestGrpcExecutor_StartInstancePriority(t *testing.T) {
	ctx := t.Context()

	for name, priority := range map[string]*protos.Priority{
		"unset":       nil,
		"level only":  {PriorityLevel: priorityCritical.Enum()},
		"weight only": {FairnessWeight: new(uint32(300))},
		"both":        {PriorityLevel: priorityLow.Enum(), FairnessWeight: new(uint32(5))},
		"weight 0":    {PriorityLevel: priorityLow.Enum(), FairnessWeight: new(uint32(0))},
	} {
		t.Run(name, func(t *testing.T) {
			be := new(captureBackend)
			exec, _ := NewGrpcExecutor(be, DefaultLogger())
			_, err := exec.(*grpcExecutor).StartInstance(ctx, &protos.CreateInstanceRequest{InstanceId: "iid", Name: "wf", Priority: priority})
			require.NoError(t, err)
			require.NotNil(t, be.created)
			started := be.created.StartEvent.GetExecutionStarted()
			require.NotNil(t, started)
			assert.Equal(t, priority.GetPriorityLevel(), started.GetPriority().GetPriorityLevel())
			assert.Equal(t, priority.GetFairnessWeight(), started.GetPriority().GetFairnessWeight())
			assert.Equal(t, priority == nil, started.GetPriority() == nil)
			if priority != nil {
				assert.NotSame(t, priority, started.GetPriority(), "a copy, not the caller's message")
			}
		})
	}
}

func TestBackendClient_ScheduleNewWorkflowPriority(t *testing.T) {
	for name, priority := range map[string]*protos.Priority{
		"unset": nil,
		"both":  {PriorityLevel: priorityHigh.Enum(), FairnessWeight: new(uint32(7))},
	} {
		t.Run(name, func(t *testing.T) {
			be := new(captureBackend)
			_, err := NewTaskHubClient(be).ScheduleNewWorkflow(t.Context(), "wf", func(req *protos.CreateInstanceRequest) error {
				req.Priority = priority
				return nil
			})
			require.NoError(t, err)
			require.NotNil(t, be.created)
			started := be.created.StartEvent.GetExecutionStarted()
			require.NotNil(t, started)
			assert.Equal(t, priority.GetPriorityLevel(), started.GetPriority().GetPriorityLevel())
			assert.Equal(t, priority.GetFairnessWeight(), started.GetPriority().GetFairnessWeight())
			assert.Equal(t, priority == nil, started.GetPriority() == nil)
			if priority != nil {
				assert.NotSame(t, priority, started.GetPriority(), "a copy, not the caller's message")
			}
		})
	}
}

func TestBackendClient_WithPriority(t *testing.T) {
	be := new(captureBackend)
	_, err := NewTaskHubClient(be).ScheduleNewWorkflow(t.Context(), "wf", api.WithPriority(priorityHigh, 7))
	require.NoError(t, err)
	started := be.created.StartEvent.GetExecutionStarted()
	assert.Equal(t, priorityHigh, started.GetPriority().GetPriorityLevel())
	assert.Equal(t, uint32(7), started.GetPriority().GetFairnessWeight())

	_, err = NewTaskHubClient(be).ScheduleNewWorkflow(t.Context(), "wf", api.WithPriority(protos.PriorityLevel_PRIORITY_LEVEL_UNSPECIFIED, 0))
	require.NoError(t, err)
	assert.Nil(t, be.created.StartEvent.GetExecutionStarted().GetPriority())

	be.created = nil
	_, err = NewTaskHubClient(be).ScheduleNewWorkflow(t.Context(), "wf", api.WithPriority(priorityLow, 1001))
	require.ErrorContains(t, err, "between 1 and 1000")
	assert.Nil(t, be.created, "nothing is stored")
}

func TestGrpcExecutor_StartInstanceRejectsBadPriority(t *testing.T) {
	for name, priority := range map[string]*protos.Priority{
		"weight 1001":   {FairnessWeight: new(uint32(1001))},
		"unknown level": {PriorityLevel: protos.PriorityLevel(42).Enum()},
	} {
		t.Run(name, func(t *testing.T) {
			be := new(captureBackend)
			exec, _ := NewGrpcExecutor(be, DefaultLogger())
			_, err := exec.(*grpcExecutor).StartInstance(t.Context(), &protos.CreateInstanceRequest{InstanceId: "iid", Name: "wf", Priority: priority})
			require.Equal(t, codes.InvalidArgument, status.Code(err))
			assert.Nil(t, be.created, "nothing is stored")

			_, err = NewTaskHubClient(be).ScheduleNewWorkflow(t.Context(), "wf", func(req *protos.CreateInstanceRequest) error {
				req.Priority = priority
				return nil
			})
			require.Error(t, err)
			assert.Nil(t, be.created)
		})
	}
}
