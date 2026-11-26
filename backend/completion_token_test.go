package backend

import (
	"context"
	"errors"
	"fmt"
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

// dispatchExecutor stands in for the workflow executor. Each dispatch gets
// a new completion token and is answered with the next of responses, or with
// a continue-as-new once they run out. With async set it delivers by
// callback, like the gRPC executor; otherwise through ExecuteWorkflow.
type dispatchExecutor struct {
	Executor
	async      bool
	responses  []*protos.WorkflowResponse
	err        error
	dispatches int
}

func (e *dispatchExecutor) next() (*protos.WorkflowResponse, string, error) {
	e.dispatches++
	token := fmt.Sprintf("d%d", e.dispatches)
	if e.err != nil {
		return nil, token, e.err
	}
	if len(e.responses) == 0 {
		return &protos.WorkflowResponse{Actions: completeAction(protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW)}, token, nil
	}
	resp := e.responses[0]
	e.responses = e.responses[1:]
	return resp, token, nil
}

func (e *dispatchExecutor) canExecuteAsync() bool { return e.async }

func (e *dispatchExecutor) executeWorkflowAsync(_ context.Context, _ api.InstanceID, _ []*protos.HistoryEvent, _ []*protos.HistoryEvent, _ ExecuteOptions, done func(*protos.WorkflowResponse, string, error)) {
	done(e.next())
}

func (e *dispatchExecutor) ExecuteWorkflow(context.Context, api.InstanceID, []*protos.HistoryEvent, []*protos.HistoryEvent, ExecuteOptions) (*protos.WorkflowResponse, error) {
	resp, _, err := e.next()
	return resp, err
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

// The work item carries the completion token of the dispatch it finishes
// with, so the backend can tell which worker response it completes.
func Test_workflowTurn_recordsCompletionToken(t *testing.T) {
	completed := &protos.WorkflowResponse{Actions: completeAction(protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED)}

	tests := map[string]struct {
		executor  *dispatchExecutor
		wantToken string
		wantErr   bool
	}{
		"completed": {
			executor:  &dispatchExecutor{async: true, responses: []*protos.WorkflowResponse{completed}},
			wantToken: "d1",
		},
		"continue-as-new keeps the last dispatch": {
			executor: &dispatchExecutor{async: true, responses: []*protos.WorkflowResponse{
				{Actions: completeAction(protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW)},
				completed,
			}},
			wantToken: "d2",
		},
		"a turn that fails after a response keeps its dispatch": {
			// Continues as new until the tight-loop limit fails the turn.
			executor:  &dispatchExecutor{async: true},
			wantToken: fmt.Sprintf("d%d", maxContinueAsNewCount+1),
			wantErr:   true,
		},
		"a dispatch that failed keeps its token": {
			executor:  &dispatchExecutor{async: true, err: errors.New("boom")},
			wantToken: "d1",
			wantErr:   true,
		},
		"an executor without callbacks records no token": {
			executor:  &dispatchExecutor{responses: []*protos.WorkflowResponse{completed}},
			wantToken: "",
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			var doneErr error
			turn, wi := newTokenTurn(t, tc.executor, func(err error) { doneErr = err })
			wi.CompletionToken = "stale"

			turn.execute()

			if tc.wantErr {
				require.Error(t, doneErr)
			} else {
				require.NoError(t, doneErr)
			}
			assert.Equal(t, tc.wantToken, wi.CompletionToken)
		})
	}
}

// The activity work item carries the token of its dispatch, whether or not
// the worker echoes it.
func Test_activityProcessor_recordsCompletionToken(t *testing.T) {
	for name, echo := range map[string]bool{"echoed": true, "not echoed": false} {
		t.Run(name, func(t *testing.T) {
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
			require.NotEmpty(t, wi.GetCompletionToken())

			resp := &protos.ActivityResponse{InstanceId: "wf1", TaskId: 1}
			if echo {
				resp.CompletionToken = wi.GetCompletionToken()
			}
			be.deliverActivity(resp)

			select {
			case err := <-ended:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				require.FailNow(t, "activity did not end")
			}
			assert.Equal(t, wi.GetCompletionToken(), awi.CompletionToken)
			assert.NotNil(t, awi.Result.GetTaskCompleted())
		})
	}
}
