package tests

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/runtimestate"
)

func startedWithPriority(iid string, priority *protos.Priority) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name: "Parent",
				WorkflowInstance: &protos.WorkflowInstance{
					InstanceId:  iid,
					ExecutionId: wrapperspb.String(uuid.New().String()),
				},
				Priority: priority,
			},
		},
	}
}

func Test_RuntimeState_ContinueAsNew_KeepsPriority(t *testing.T) {
	for name, priority := range map[string]*protos.Priority{
		"unset":      nil,
		"level only": {PriorityLevel: new("critical")},
		"both":       {PriorityLevel: new("low"), FairnessWeight: new(uint32(300))},
	} {
		t.Run(name, func(t *testing.T) {
			state := runtimestate.NewWorkflowRuntimeState("abc", nil, []*protos.HistoryEvent{startedWithPriority("abc", priority)})
			actions := []*protos.WorkflowAction{{
				Id: 1,
				WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{
					CompleteWorkflow: &protos.CompleteWorkflowAction{
						WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
						Result:         wrapperspb.String("next"),
					},
				},
			}}

			result, err := runtimestate.NewApplier("example", "").Actions(state, nil, actions, nil, nil)
			require.NoError(t, err)
			require.True(t, result.ContinuedAsNew)
			require.Len(t, state.NewEvents, 2)
			started := state.NewEvents[1].GetExecutionStarted()
			require.NotNil(t, started)
			assert.Equal(t, priority.GetPriorityLevel(), started.GetPriority().GetPriorityLevel())
			assert.Equal(t, priority.GetFairnessWeight(), started.GetPriority().GetFairnessWeight())
			assert.Equal(t, priority == nil, started.GetPriority() == nil)
		})
	}
}

func Test_RuntimeState_ChildWorkflow_StartsWithParentPriority(t *testing.T) {
	for name, priority := range map[string]*protos.Priority{
		"unset": nil,
		"both":  {PriorityLevel: new("high"), FairnessWeight: new(uint32(7))},
	} {
		t.Run(name, func(t *testing.T) {
			state := runtimestate.NewWorkflowRuntimeState("parent", nil, []*protos.HistoryEvent{startedWithPriority("parent", priority)})
			actions := []*protos.WorkflowAction{{
				Id: 1,
				WorkflowActionType: &protos.WorkflowAction_CreateChildWorkflow{
					CreateChildWorkflow: &protos.CreateChildWorkflowAction{Name: "Child", InstanceId: "child"},
				},
			}}

			_, err := runtimestate.NewApplier("example", "").Actions(state, nil, actions, nil, nil)
			require.NoError(t, err)
			require.Len(t, state.PendingMessages, 1)
			started := state.PendingMessages[0].GetHistoryEvent().GetExecutionStarted()
			require.NotNil(t, started)
			assert.Equal(t, "Child", started.GetName())
			assert.Equal(t, priority.GetPriorityLevel(), started.GetPriority().GetPriorityLevel())
			assert.Equal(t, priority.GetFairnessWeight(), started.GetPriority().GetFairnessWeight())
			assert.Equal(t, priority == nil, started.GetPriority() == nil)
		})
	}
}
