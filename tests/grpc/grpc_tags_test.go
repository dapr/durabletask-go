package tests_grpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/task"
)

// startedEvent returns the ExecutionStarted event in the history of id.
func startedEvent(t *testing.T, id api.InstanceID) *protos.ExecutionStartedEvent {
	t.Helper()
	res, err := grpcClient.GetInstanceHistory(ctx, id)
	require.NoError(t, err)
	for _, e := range res.GetEvents() {
		if s := e.GetExecutionStarted(); s != nil {
			return s
		}
	}
	require.FailNow(t, "no ExecutionStarted event in the history")
	return nil
}

func Test_Grpc_StartKeepsTags(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Tagged", func(*task.WorkflowContext) (any, error) {
		return nil, nil
	})
	cancelListener := startGrpcListener(t, r)
	defer cancelListener()

	run := func(t *testing.T, opts ...api.NewWorkflowOptions) *protos.ExecutionStartedEvent {
		t.Helper()
		id, err := grpcClient.ScheduleNewWorkflow(ctx, "Tagged", opts...)
		require.NoError(t, err)
		timeoutCtx, cancelTimeout := context.WithTimeout(ctx, 30*time.Second)
		defer cancelTimeout()
		_, err = grpcClient.WaitForWorkflowCompletion(timeoutCtx, id)
		require.NoError(t, err)
		started := startedEvent(t, id)
		require.NoError(t, grpcClient.PurgeWorkflowState(ctx, id))
		return started
	}

	t.Run("tagged", func(t *testing.T) {
		tags := map[string]string{"owner": "user-1", "team": "a"}
		started := run(t, api.WithTags(tags))
		assert.Equal(t, tags, started.GetTags())
	})

	t.Run("untagged", func(t *testing.T) {
		started := run(t)
		assert.Empty(t, started.GetTags())
	})
}
