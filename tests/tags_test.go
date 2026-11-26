package tests

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/runtimestate"
	"github.com/dapr/durabletask-go/tests/mocks"
)

// scheduled returns the ExecutionStarted event the in-process client hands
// the backend for a start with opts.
func scheduled(t *testing.T, opts ...api.NewWorkflowOptions) *protos.ExecutionStartedEvent {
	t.Helper()
	be := mocks.NewBackend(t)
	var started *protos.ExecutionStartedEvent
	be.EXPECT().CreateWorkflowInstance(mock.Anything, mock.Anything).
		RunAndReturn(func(_ context.Context, req *protos.CreateWorkflowInstanceRequest) error {
			started = req.GetStartEvent().GetExecutionStarted()
			return nil
		}).Once()

	_, err := backend.NewTaskHubClient(be).ScheduleNewWorkflow(context.Background(), "Tagged", opts...)
	require.NoError(t, err)
	require.NotNil(t, started)
	return started
}

func Test_TaskHubClient_StartKeepsTags(t *testing.T) {
	tags := map[string]string{"owner": "user-1", "team": "a"}
	started := scheduled(t, api.WithTags(tags))

	assert.Equal(t, tags, started.GetTags())

	tags["owner"] = "changed"
	assert.Equal(t, "user-1", started.GetTags()["owner"], "the event holds a copy of the caller's tags")
}

func Test_TaskHubClient_StartWithoutTags(t *testing.T) {
	started := scheduled(t)

	assert.Empty(t, started.GetTags())
}

func Test_RuntimeState_ContinueAsNew_KeepsTags(t *testing.T) {
	tags := map[string]string{"owner": "user-1"}
	start := startedWithPriority("abc", nil)
	start.GetExecutionStarted().Tags = tags
	state := runtimestate.NewWorkflowRuntimeState("abc", nil, []*protos.HistoryEvent{start})
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

	assert.Equal(t, tags, started.GetTags())
	assert.NotEqual(t, start.GetExecutionStarted().GetWorkflowInstance().GetExecutionId().GetValue(),
		started.GetWorkflowInstance().GetExecutionId().GetValue(), "the next execution has an ID of its own")

	tags["owner"] = "changed"
	assert.Equal(t, "user-1", started.GetTags()["owner"], "the next execution holds a copy")
}
