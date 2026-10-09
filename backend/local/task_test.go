package local_test

import (
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
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

func Test_OnActivityCompletion_ConcurrentRegistrations(t *testing.T) {
	be := local.NewTasksBackend()

	var firstCalls, secondCalls int
	var firstErr, secondErr error
	dereg1 := be.OnActivityCompletion(activityRequest("abc", 1), func(_ *protos.ActivityResponse, err error) {
		firstCalls++
		firstErr = err
	})
	dereg2 := be.OnActivityCompletion(activityRequest("abc", 1), func(_ *protos.ActivityResponse, err error) {
		secondCalls++
		secondErr = err
	})

	resp := &protos.ActivityResponse{InstanceId: "abc", TaskId: 1, CompletionToken: "t1"}
	require.NoError(t, be.CompleteActivityTask(context.Background(), resp))
	require.Equal(t, 1, firstCalls)
	require.Equal(t, 1, secondCalls)
	require.NoError(t, firstErr)
	require.NoError(t, secondErr)

	require.NoError(t, be.CancelActivityTask(context.Background(), api.InstanceID("abc"), 1))
	require.Equal(t, 2, firstCalls)
	require.Equal(t, 2, secondCalls)
	require.ErrorIs(t, firstErr, api.ErrTaskCancelled)
	require.ErrorIs(t, secondErr, api.ErrTaskCancelled)

	dereg2()
	require.NoError(t, be.CompleteActivityTask(context.Background(), resp))
	require.Equal(t, 3, firstCalls)
	require.Equal(t, 2, secondCalls)

	dereg1()
	require.Error(t, be.CompleteActivityTask(context.Background(), resp))
	require.Equal(t, 3, firstCalls)
}

func Test_OnWorkflowTaskCompletion_ConcurrentRegistrations(t *testing.T) {
	be := local.NewTasksBackend()

	var firstCalls, secondCalls int
	dereg1 := be.OnWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "abc"}, func(*protos.WorkflowResponse, error) {
		firstCalls++
	})
	dereg2 := be.OnWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "abc"}, func(*protos.WorkflowResponse, error) {
		secondCalls++
	})

	require.NoError(t, be.CancelWorkflowTask(context.Background(), api.InstanceID("abc")))
	require.Equal(t, 1, firstCalls)
	require.Equal(t, 1, secondCalls)

	dereg1()
	require.NoError(t, be.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "abc"}))
	require.Equal(t, 1, firstCalls)
	require.Equal(t, 2, secondCalls)

	dereg2()
	require.Error(t, be.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "abc"}))
}

func Test_OnActivityCompletion_DeregisterFromCallback(t *testing.T) {
	be := local.NewTasksBackend()

	var dereg1, dereg2 func()
	dereg1 = be.OnActivityCompletion(activityRequest("abc", 1), func(*protos.ActivityResponse, error) { dereg1() })
	dereg2 = be.OnActivityCompletion(activityRequest("abc", 1), func(*protos.ActivityResponse, error) { dereg2() })

	require.ErrorIs(t, be.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}), local.ErrAmbiguousCompletion)
	require.Error(t, be.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}))
}

func Test_OnActivityCompletion_ConcurrentUse(t *testing.T) {
	be := local.NewTasksBackend()

	var wg sync.WaitGroup
	for g := range 8 {
		wg.Go(func() {
			token := strconv.Itoa(g)
			for range 200 {
				var own atomic.Bool
				dereg := be.OnActivityCompletion(activityRequest("abc", 1), func(resp *protos.ActivityResponse, err error) {
					if err == nil && resp.GetCompletionToken() == token {
						own.Store(true)
					}
				})
				assert.NoError(t, be.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "abc", TaskId: 1, CompletionToken: token}))
				assert.True(t, own.Load())
				dereg()
			}
		})
	}
	wg.Wait()

	require.Error(t, be.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}))
}

func Test_OnActivityCompletion_TokenlessResponseWithConcurrentRegistrations(t *testing.T) {
	be := local.NewTasksBackend()

	var gotResps []*protos.ActivityResponse
	var gotErrs []error
	record := func(resp *protos.ActivityResponse, err error) {
		gotResps = append(gotResps, resp)
		gotErrs = append(gotErrs, err)
	}
	dereg1 := be.OnActivityCompletion(activityRequest("abc", 1), record)
	be.OnActivityCompletion(activityRequest("abc", 1), record)

	require.ErrorIs(t, be.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}), local.ErrAmbiguousCompletion)
	require.Len(t, gotErrs, 2)
	for i := range gotErrs {
		require.ErrorIs(t, gotErrs[i], api.ErrTaskCancelled)
		require.Nil(t, gotResps[i])
	}

	dereg1()
	resp := &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}
	require.NoError(t, be.CompleteActivityTask(context.Background(), resp))
	require.Len(t, gotErrs, 3)
	require.NoError(t, gotErrs[2])
	require.Same(t, resp, gotResps[2])
}

func Test_OnWorkflowTaskCompletion_TokenlessResponseWithConcurrentRegistrations(t *testing.T) {
	be := local.NewTasksBackend()

	var gotErrs []error
	record := func(resp *protos.WorkflowResponse, err error) {
		require.Nil(t, resp)
		gotErrs = append(gotErrs, err)
	}
	be.OnWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "abc"}, record)
	be.OnWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "abc"}, record)

	require.ErrorIs(t, be.CompleteWorkflowTask(context.Background(), &protos.WorkflowResponse{InstanceId: "abc"}), local.ErrAmbiguousCompletion)
	require.Len(t, gotErrs, 2)
	require.ErrorIs(t, gotErrs[0], api.ErrTaskCancelled)
	require.ErrorIs(t, gotErrs[1], api.ErrTaskCancelled)
}

func Test_OnActivityCompletion_NoCallbackAfterDeregister(t *testing.T) {
	be := local.NewTasksBackend()

	var dereg2 func()
	var calls2 int
	be.OnActivityCompletion(activityRequest("abc", 1), func(*protos.ActivityResponse, error) { dereg2() })
	dereg2 = be.OnActivityCompletion(activityRequest("abc", 1), func(*protos.ActivityResponse, error) { calls2++ })

	require.NoError(t, be.CompleteActivityTask(context.Background(), &protos.ActivityResponse{InstanceId: "abc", TaskId: 1, CompletionToken: "t"}))
	assert.Zero(t, calls2, "a callback deregistered during the delivery must not run")
}
