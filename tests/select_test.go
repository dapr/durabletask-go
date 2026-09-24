package tests

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/task"
)

// Test_Select_ExternalEventRace exercises the scenario from
// https://github.com/dapr/dapr/issues/10447: waiting on whichever of several
// named external events arrives first, without polling.
func Test_Select_ExternalEventRace(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectRaceWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		approve := ctx.WaitForSingleEvent("Approve", -1)
		reject := ctx.WaitForSingleEvent("Reject", -1)
		abort := ctx.WaitForSingleEvent("Abort", -1)

		winner, err := ctx.Select(approve, reject, abort)
		if err != nil {
			return nil, err
		}

		switch winner {
		case 0:
			var v string
			if err := approve.Await(&v); err != nil {
				return nil, err
			}
			return "Approve:" + v, nil
		case 1:
			var v string
			if err := reject.Await(&v); err != nil {
				return nil, err
			}
			return "Reject:" + v, nil
		default:
			var v string
			if err := abort.Await(&v); err != nil {
				return nil, err
			}
			return "Abort:" + v, nil
		}
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectRaceWorkflow")
	require.NoError(t, err)

	_, err = client.WaitForWorkflowStart(ctx, id)
	require.NoError(t, err)

	// Only raise the event that should win the race; the workflow must not
	// need the other two to ever be raised.
	require.NoError(t, client.RaiseEvent(ctx, id, "Reject", api.WithEventPayload("nope")))

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"Reject:nope"`, metadata.Output.Value)
}

// Test_Select_LoopOverRemaining models a workflow that must observe every one
// of several events, in whatever order they arrive, by repeatedly Selecting
// over the tasks that have not yet completed.
func Test_Select_LoopOverRemaining(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectLoopWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		pending := []task.Task{
			ctx.WaitForSingleEvent("First", -1),
			ctx.WaitForSingleEvent("Second", -1),
			ctx.WaitForSingleEvent("Third", -1),
		}
		var order []string
		for len(pending) > 0 {
			winner, err := ctx.Select(pending...)
			if err != nil {
				return nil, err
			}
			var v string
			if err := pending[winner].Await(&v); err != nil {
				return nil, err
			}
			order = append(order, v)
			pending = append(pending[:winner], pending[winner+1:]...)
		}
		return order, nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectLoopWorkflow")
	require.NoError(t, err)
	_, err = client.WaitForWorkflowStart(ctx, id)
	require.NoError(t, err)

	// Raise out of declaration order to prove Select isn't just returning
	// index 0 every time.
	require.NoError(t, client.RaiseEvent(ctx, id, "Third", api.WithEventPayload("c")))
	require.NoError(t, client.RaiseEvent(ctx, id, "First", api.WithEventPayload("a")))
	require.NoError(t, client.RaiseEvent(ctx, id, "Second", api.WithEventPayload("b")))

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `["c","a","b"]`, metadata.Output.Value)
}

// Test_Select_AlreadyCompletedWins verifies that a task which is already
// complete by the time Select is called (e.g. a timer created earlier in the
// same execution that has since fired) is picked immediately, without
// needing to process any further history.
func Test_Select_AlreadyCompletedWins(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectAlreadyCompletedWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		immediate := ctx.CreateTimer(0)
		if err := immediate.Await(nil); err != nil {
			return nil, err
		}

		neverFires := ctx.WaitForSingleEvent("NeverSent", -1)

		winner, err := ctx.Select(neverFires, immediate)
		if err != nil {
			return nil, err
		}
		return winner, nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectAlreadyCompletedWorkflow")
	require.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `1`, metadata.Output.Value)
}

// Test_Select_NoTasks verifies that calling Select with no tasks reports an
// error instead of panicking or blocking forever.
func Test_Select_NoTasks(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectNoTasksWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		_, err := ctx.Select()
		if err == nil {
			return nil, nil
		}
		return err.Error(), nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectNoTasksWorkflow")
	require.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"Select requires at least one task"`, metadata.Output.Value)
}

// Test_Select_RetryWrappedTaskWins verifies that a task returned by CallActivity with a retry
// policy is a real, selectable task: its retries are driven independently of Select or Await, so
// it can win a Select once its retries succeed, racing normally against a plain task.
func Test_Select_RetryWrappedTaskWins(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectRetryWrappedWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		retried := ctx.CallActivity("FlakyActivity", task.WithActivityRetryPolicy(&task.RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: 10 * time.Millisecond,
		}))
		neverFires := ctx.CreateTimer(1 * time.Hour)

		winner, err := ctx.Select(retried, neverFires)
		if err != nil {
			return nil, err
		}
		if winner != 0 {
			return nil, fmt.Errorf("expected the retried activity (index 0) to win, got index %d", winner)
		}

		var v string
		if err := retried.Await(&v); err != nil {
			return nil, err
		}
		return v, nil
	})
	var attempts int32
	r.AddActivityN("FlakyActivity", func(ctx task.ActivityContext) (any, error) {
		if atomic.AddInt32(&attempts, 1) < 2 {
			return nil, errors.New("not yet")
		}
		return "eventually succeeded", nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectRetryWrappedWorkflow")
	require.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"eventually succeeded"`, metadata.Output.Value)
	assert.Equal(t, int32(2), atomic.LoadInt32(&attempts))
}

// Test_Select_ActivityRacesTimer covers the most common WhenAny shape: a plain (non-retry)
// CallActivity racing a CreateTimer. The activity's 1s sleep is deliberately much longer than the
// timer's 50ms delay so the timer wins with a wide margin, comfortably covering the extra latency
// the sqlite backend's polling (with its own exponential backoff) adds on top of the timer's raw
// delay before the workflow actually picks the TimerFired event up.
func Test_Select_ActivityRacesTimer(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectActivityRacesTimerWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		slowActivity := ctx.CallActivity("SlowActivity")
		timer := ctx.CreateTimer(50 * time.Millisecond)

		winner, err := ctx.Select(slowActivity, timer)
		if err != nil {
			return nil, err
		}
		if winner != 1 {
			return nil, fmt.Errorf("expected the timer (index 1) to win, got index %d", winner)
		}
		if err := timer.Await(nil); err != nil {
			return nil, err
		}
		return "timer won", nil
	})
	r.AddActivityN("SlowActivity", func(ctx task.ActivityContext) (any, error) {
		time.Sleep(1 * time.Second)
		return "too slow", nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectActivityRacesTimerWorkflow")
	require.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"timer won"`, metadata.Output.Value)
}

// Test_Select_WaitForSingleEventTimeoutWins verifies that when a WaitForSingleEvent task times out
// before its event is ever raised, Select returns that task as the winner and Await on it surfaces
// ErrTaskCanceled, exactly as it would outside of Select.
func Test_Select_WaitForSingleEventTimeoutWins(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SelectEventTimeoutWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		neverRaised := ctx.WaitForSingleEvent("NeverRaised", 50*time.Millisecond)
		neverFires := ctx.CreateTimer(1 * time.Hour)

		winner, err := ctx.Select(neverRaised, neverFires)
		if err != nil {
			return nil, err
		}
		if winner != 0 {
			return nil, fmt.Errorf("expected the timed-out event wait (index 0) to win, got index %d", winner)
		}

		err = neverRaised.Await(nil)
		if !errors.Is(err, task.ErrTaskCanceled) {
			return nil, fmt.Errorf("expected ErrTaskCanceled, got %v", err)
		}
		return "timed out as expected", nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "SelectEventTimeoutWorkflow")
	require.NoError(t, err)

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"timed out as expected"`, metadata.Output.Value)
}
