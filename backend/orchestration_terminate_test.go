package backend

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/runtimestate"
)

// The async turn path (ProcessWorkItemAsync -> workflowTurn.applyResponse) is
// what a callback-capable executor drives in production; these tests pin that
// it enforces a mid-batch terminate exactly like the synchronous
// ProcessWorkItem path.

func newTerminatedTurn(t *testing.T, done func(error)) (*workflowTurn, *WorkflowWorkItem) {
	t.Helper()

	const workflowID = "wf-terminate-async"
	state := runtimestate.NewWorkflowRuntimeState(workflowID, nil, nil)
	terminate := &protos.ExecutionTerminatedEvent{Input: wrapperspb.String(`"reason"`)}
	events := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyOrch",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  workflowID,
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionTerminated{ExecutionTerminated: terminate},
		},
	}
	for _, e := range events {
		require.NoError(t, runtimestate.AddEvent(state, e))
	}

	wi := &WorkflowWorkItem{
		InstanceID: workflowID,
		NewEvents:  events,
		State:      state,
	}
	return &workflowTurn{
		processor: &workflowProcessor{
			logger:  DefaultLogger(),
			applier: runtimestate.NewApplier("testapp", ""),
		},
		wi:             wi,
		ctx:            context.Background(),
		span:           trace.SpanFromContext(context.Background()),
		terminateEvent: terminate,
		done:           done,
	}, wi
}

func Test_workflowTurn_applyResponse_forcesTerminateWhenExecutorIgnoresIt(t *testing.T) {
	var doneErr error
	turn, wi := newTerminatedTurn(t, func(err error) { doneErr = err })

	turn.applyResponse(&protos.WorkflowResponse{
		Actions: []*protos.WorkflowAction{{
			Id: 0,
			WorkflowActionType: &protos.WorkflowAction_CreateTimer{
				CreateTimer: &protos.CreateTimerAction{
					FireAt: timestamppb.New(time.Now().Add(time.Second)),
				},
			},
		}},
	}, nil, ExecuteOptions{})

	require.NoError(t, doneErr)
	require.True(t, runtimestate.IsCompleted(wi.State))
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, runtimestate.RuntimeStatus(wi.State))
	output, err := runtimestate.Output(wi.State)
	require.NoError(t, err)
	require.Equal(t, `"reason"`, output.GetValue())
	require.Empty(t, wi.State.PendingTasks)
	require.Empty(t, wi.State.PendingTimers)
	require.Empty(t, wi.State.PendingMessages)
}

func Test_workflowTurn_applyResponse_terminateBeatsContinueAsNew(t *testing.T) {
	var doneErr error
	turn, wi := newTerminatedTurn(t, func(err error) { doneErr = err })

	turn.applyResponse(&protos.WorkflowResponse{
		Actions: []*protos.WorkflowAction{{
			Id: 0,
			WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{
				CompleteWorkflow: &protos.CompleteWorkflowAction{
					WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
					Result:         wrapperspb.String(`"restart"`),
				},
			},
		}},
	}, nil, ExecuteOptions{})

	require.NoError(t, doneErr)
	require.True(t, runtimestate.IsCompleted(wi.State),
		"the CAN must be stripped, not applied: no new generation may start")
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, runtimestate.RuntimeStatus(wi.State))
	output, err := runtimestate.Output(wi.State)
	require.NoError(t, err)
	require.Equal(t, `"reason"`, output.GetValue())
}
