package backend

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/runtimestate"
)

// responsesExecutor answers each ExecuteWorkflow call with the next response.
type responsesExecutor struct {
	Executor
	responses []*protos.WorkflowResponse
}

func (e *responsesExecutor) ExecuteWorkflow(context.Context, api.InstanceID, []*protos.HistoryEvent, []*protos.HistoryEvent, ExecuteOptions) (*protos.WorkflowResponse, error) {
	if len(e.responses) == 0 {
		return nil, errors.New("no response left")
	}
	resp := e.responses[0]
	e.responses = e.responses[1:]
	return resp, nil
}

func newTokenTurn(t *testing.T, executor Executor, done func(error)) (*workflowTurn, *WorkflowWorkItem) {
	t.Helper()

	const workflowID = "wf-token"
	state := runtimestate.NewWorkflowRuntimeState(workflowID, nil, nil)
	started := &protos.HistoryEvent{
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
	}
	require.NoError(t, runtimestate.AddEvent(state, started))

	wi := &WorkflowWorkItem{
		InstanceID: workflowID,
		NewEvents:  []*protos.HistoryEvent{started},
		State:      state,
	}
	return &workflowTurn{
		processor: &workflowProcessor{
			executor: executor,
			logger:   DefaultLogger(),
			applier:  runtimestate.NewApplier("testapp", ""),
		},
		wi:   wi,
		ctx:  context.Background(),
		span: trace.SpanFromContext(context.Background()),
		done: done,
	}, wi
}

func completeAction(status protos.OrchestrationStatus) []*protos.WorkflowAction {
	return []*protos.WorkflowAction{{
		Id: 0,
		WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{
			CompleteWorkflow: &protos.CompleteWorkflowAction{WorkflowStatus: status},
		},
	}}
}

// The work item carries the completion token of the response it finishes
// with, so the backend can tell which response it completes.
func Test_workflowTurn_recordsCompletionToken(t *testing.T) {
	t.Run("completed", func(t *testing.T) {
		var doneErr error
		turn, wi := newTokenTurn(t, nil, func(err error) { doneErr = err })

		turn.applyResponse(&protos.WorkflowResponse{
			CompletionToken: "t1",
			Actions:         completeAction(protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED),
		}, nil, ExecuteOptions{})

		require.NoError(t, doneErr)
		assert.Equal(t, "t1", wi.CompletionToken)
	})

	t.Run("continue-as-new ends on the last response", func(t *testing.T) {
		var doneErr error
		executor := &responsesExecutor{responses: []*protos.WorkflowResponse{{
			CompletionToken: "t2",
			Actions:         completeAction(protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED),
		}}}
		turn, wi := newTokenTurn(t, executor, func(err error) { doneErr = err })

		turn.applyResponse(&protos.WorkflowResponse{
			CompletionToken: "t1",
			Actions:         completeAction(protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW),
		}, nil, ExecuteOptions{})

		require.NoError(t, doneErr)
		assert.Empty(t, executor.responses)
		assert.Equal(t, "t2", wi.CompletionToken)
	})

	t.Run("no response", func(t *testing.T) {
		var doneErr error
		turn, wi := newTokenTurn(t, nil, func(err error) { doneErr = err })
		wi.CompletionToken = "stale"

		turn.applyResponse(nil, errors.New("boom"), ExecuteOptions{})

		require.Error(t, doneErr)
		assert.Empty(t, wi.CompletionToken)
	})
}

func Test_activityProcessor_recordsCompletionToken(t *testing.T) {
	be := &fakeCallbackBackend{}
	g := &grpcExecutor{
		workItemQueue:     make(chan *protos.WorkItem, 1),
		pendingWorkflows:  newPendingTasks(),
		pendingActivities: newPendingTasks(),
		streams:           &sync.Map{},
		backend:           be,
		logger:            DefaultLogger(),
	}
	p := newActivityProcessor(nil, g, nil, "")

	awi := &ActivityWorkItem{
		InstanceID: "wf1",
		NewEvent: &protos.HistoryEvent{
			EventId:   1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_TaskScheduled{
				TaskScheduled: &protos.TaskScheduledEvent{Name: "act"},
			},
		},
	}

	ended := make(chan error, 1)
	p.ProcessWorkItemAsync(context.Background(), awi, func(err error) { ended <- err })

	var wi *protos.WorkItem
	select {
	case wi = <-g.workItemQueue:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "work item was not dispatched")
	}

	be.deliverActivity(&protos.ActivityResponse{InstanceId: "wf1", TaskId: 1, CompletionToken: wi.GetCompletionToken()})

	select {
	case err := <-ended:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		require.FailNow(t, "activity did not end")
	}
	assert.Equal(t, wi.GetCompletionToken(), awi.CompletionToken)
	assert.NotNil(t, awi.Result.GetTaskCompleted())
}
