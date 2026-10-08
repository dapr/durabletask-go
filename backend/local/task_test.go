package local_test

import (
	"context"
	"sync"
	"testing"
	"time"

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

type result[R any] struct {
	resp R
	err  error
}

func waitAsync[R any](ctx context.Context, wait func(context.Context) (R, error)) <-chan result[R] {
	ch := make(chan result[R], 1)
	go func() {
		resp, err := wait(ctx)
		ch <- result[R]{resp, err}
	}()
	return ch
}

func requireResult[R any](t *testing.T, ch <-chan result[R]) result[R] {
	t.Helper()
	select {
	case r := <-ch:
		return r
	case <-time.After(5 * time.Second):
		require.FailNow(t, "wait did not return")
		return result[R]{}
	}
}

func Test_WaitForActivityCompletion_Completes(t *testing.T) {
	be := local.NewTasksBackend()
	ch := waitAsync(t.Context(), be.WaitForActivityCompletion(activityRequest("abc", 1)))

	resp := &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}
	require.NoError(t, be.CompleteActivityTask(t.Context(), resp))
	r := requireResult(t, ch)
	require.NoError(t, r.err)
	require.Same(t, resp, r.resp)

	require.Error(t, be.CompleteActivityTask(t.Context(), resp))
}

func Test_WaitForActivityCompletion_Cancelled(t *testing.T) {
	be := local.NewTasksBackend()
	ch := waitAsync(t.Context(), be.WaitForActivityCompletion(activityRequest("abc", 1)))

	require.NoError(t, be.CancelActivityTask(t.Context(), api.InstanceID("abc"), 1))
	require.ErrorIs(t, requireResult(t, ch).err, api.ErrTaskCancelled)
}

func Test_WaitForActivityCompletion_ConcurrentWaitsAreCancelled(t *testing.T) {
	be := local.NewTasksBackend()
	first := waitAsync(t.Context(), be.WaitForActivityCompletion(activityRequest("abc", 1)))
	second := waitAsync(t.Context(), be.WaitForActivityCompletion(activityRequest("abc", 1)))

	require.ErrorIs(t, be.CompleteActivityTask(t.Context(), &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}), local.ErrAmbiguousCompletion)
	for _, ch := range []<-chan result[*protos.ActivityResponse]{first, second} {
		r := requireResult(t, ch)
		require.ErrorIs(t, r.err, api.ErrTaskCancelled)
		require.Nil(t, r.resp)
	}

	require.Error(t, be.CompleteActivityTask(t.Context(), &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}))
}

func Test_WaitForWorkflowTaskCompletion_ConcurrentWaitsAreCancelled(t *testing.T) {
	be := local.NewTasksBackend()
	first := waitAsync(t.Context(), be.WaitForWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "abc"}))
	second := waitAsync(t.Context(), be.WaitForWorkflowTaskCompletion(&protos.WorkflowRequest{InstanceId: "abc"}))

	require.ErrorIs(t, be.CompleteWorkflowTask(t.Context(), &protos.WorkflowResponse{InstanceId: "abc"}), local.ErrAmbiguousCompletion)
	require.ErrorIs(t, requireResult(t, first).err, api.ErrTaskCancelled)
	require.ErrorIs(t, requireResult(t, second).err, api.ErrTaskCancelled)
}

func Test_WaitForActivityCompletion_ContextEndRemovesWaiter(t *testing.T) {
	be := local.NewTasksBackend()
	ctx, cancel := context.WithCancel(t.Context())
	stale := waitAsync(ctx, be.WaitForActivityCompletion(activityRequest("abc", 1)))
	cancel()
	require.ErrorIs(t, requireResult(t, stale).err, context.Canceled)

	live := waitAsync(t.Context(), be.WaitForActivityCompletion(activityRequest("abc", 1)))
	resp := &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}
	require.NoError(t, be.CompleteActivityTask(t.Context(), resp))
	r := requireResult(t, live)
	require.NoError(t, r.err)
	require.Same(t, resp, r.resp)
}

func Test_WaitForActivityCompletion_NoWaiterIsStranded(t *testing.T) {
	be := local.NewTasksBackend()

	var wg sync.WaitGroup
	for range 2 {
		wait := be.WaitForActivityCompletion(activityRequest("abc", 1))
		wg.Go(func() { _, _ = wait(context.Background()) })
	}
	require.ErrorIs(t, be.CompleteActivityTask(t.Context(), &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}), local.ErrAmbiguousCompletion)

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "a waiter was stranded")
	}
}

func Test_WaitForActivityCompletion_DeliveredBeforeContextEnd(t *testing.T) {
	be := local.NewTasksBackend()
	for range 200 {
		ctx, cancel := context.WithCancel(t.Context())
		wait := be.WaitForActivityCompletion(activityRequest("abc", 1))
		resp := &protos.ActivityResponse{InstanceId: "abc", TaskId: 1}
		require.NoError(t, be.CompleteActivityTask(t.Context(), resp))
		cancel()

		got, err := wait(ctx)
		require.NoError(t, err)
		require.Same(t, resp, got)
	}
}
