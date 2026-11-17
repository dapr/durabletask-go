package backend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
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
		"level only":  {PriorityLevel: new("critical")},
		"weight only": {FairnessWeight: new(uint32(300))},
		"both":        {PriorityLevel: new("low"), FairnessWeight: new(uint32(5))},
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
		})
	}
}

func TestBackendClient_ScheduleNewWorkflowPriority(t *testing.T) {
	for name, priority := range map[string]*protos.Priority{
		"unset": nil,
		"both":  {PriorityLevel: new("high"), FairnessWeight: new(uint32(7))},
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
		})
	}
}
