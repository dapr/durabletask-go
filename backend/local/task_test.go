package local_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/local"
)

func activityRequest(iid string, taskID int32) *protos.ActivityRequest {
	return &protos.ActivityRequest{
		WorkflowInstance: &protos.WorkflowInstance{InstanceId: iid},
		TaskId:           taskID,
	}
}

func Test_OnActivityCompletion_Delivers(t *testing.T) {
	be := local.NewTasksBackend()

	var got *protos.ActivityResponse
	var gotErr error
	calls := 0
	dereg := be.OnActivityCompletion(activityRequest("abc", 1), func(resp *protos.ActivityResponse, err error) {
		calls++
		got, gotErr = resp, err
	})

	resp := &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}
	require.NoError(t, be.CompleteActivityTask(context.Background(), resp))
	require.Equal(t, 1, calls)
	require.Same(t, resp, got)
	require.NoError(t, gotErr)

	// Delivery does NOT consume the registration: the executor's arbiter
	// discards stale-token deliveries and keeps waiting on this registration,
	// so a genuine response arriving after a discarded stale one must still
	// route (pre-fix, the stale delivery consumed the entry and the genuine
	// response was dropped as unknown, stranding the waiter forever).
	require.NoError(t, be.CompleteActivityTask(context.Background(), resp))
	require.Equal(t, 2, calls)

	// Only the deregister closure removes the registration.
	dereg()
	require.Error(t, be.CompleteActivityTask(context.Background(), resp))
	require.Equal(t, 2, calls)
}

func Test_OnActivityCompletion_Cancelled(t *testing.T) {
	be := local.NewTasksBackend()

	var gotErr error
	calls := 0
	be.OnActivityCompletion(activityRequest("abc", 1), func(resp *protos.ActivityResponse, err error) {
		calls++
		gotErr = err
	})

	require.NoError(t, be.CancelActivityTask(context.Background(), api.InstanceID("abc"), 1))
	require.Equal(t, 1, calls)
	require.ErrorIs(t, gotErr, api.ErrTaskCancelled)
}

func Test_OnActivityCompletion_Deregister(t *testing.T) {
	be := local.NewTasksBackend()

	calls := 0
	dereg := be.OnActivityCompletion(activityRequest("abc", 1), func(*protos.ActivityResponse, error) {
		calls++
	})
	dereg()

	require.Error(t, be.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}))
	require.Zero(t, calls)
}

func Test_OnWorkflowTaskCompletion_Delivers(t *testing.T) {
	be := local.NewTasksBackend()

	var got *protos.WorkflowResponse
	var gotErr error
	calls := 0
	be.OnWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "abc"}, func(resp *protos.WorkflowResponse, err error) {
		calls++
		got, gotErr = resp, err
	})

	resp := &protos.WorkflowResponse{InstanceId: "abc"}
	require.NoError(t, be.CompleteWorkflowTask(context.Background(), resp))
	require.Equal(t, 1, calls)
	require.Same(t, resp, got)
	require.NoError(t, gotErr)

	// See the activity variant: delivery must not consume the registration.
	require.NoError(t, be.CompleteWorkflowTask(context.Background(), resp))
	require.Equal(t, 2, calls)
}

func Test_OnWorkflowTaskCompletion_Cancelled(t *testing.T) {
	be := local.NewTasksBackend()

	var gotErr error
	calls := 0
	be.OnWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "abc"}, func(resp *protos.WorkflowResponse, err error) {
		calls++
		gotErr = err
	})

	require.NoError(t, be.CancelWorkflowTask(context.Background(), api.InstanceID("abc")))
	require.Equal(t, 1, calls)
	require.ErrorIs(t, gotErr, api.ErrTaskCancelled)
}

func Test_OnWorkflowTaskCompletion_Deregister(t *testing.T) {
	be := local.NewTasksBackend()

	calls := 0
	dereg := be.OnWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "abc"}, func(*protos.WorkflowResponse, error) {
		calls++
	})
	dereg()

	require.Error(t, be.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "abc"}))
	require.Zero(t, calls)
}
