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
// policy is a real, selectable task: Select's own polling drives its retries forward, so it can
// win a Select once its retries succeed, racing normally against a plain task.
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
// CallActivity racing a CreateTimer. The activity blocks on a channel the test only closes after
// the timer has had a chance to win, so the timer wins deterministically rather than by a wall-clock
// margin that could be eaten by the sqlite backend's own polling latency.
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
	// Block until the test is done instead of racing a wall-clock sleep against the timer: with a
	// fixed sleep, the sqlite backend's poll backoff could delay TimerFired past the activity's
	// own completion under load, making the activity win and the test fail.
	release := make(chan struct{})
	r.AddActivityN("SlowActivity", func(ctx task.ActivityContext) (any, error) {
		<-release
		return "too slow", nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)
	// Registered after worker.Shutdown's defer, so it runs first (LIFO) and unblocks the activity
	// before Shutdown's StopAndDrain waits on it.
	defer close(release)

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

// Test_Select_ApprovalGateLoop_CarriesLosingWaitForward demonstrates the approval-gate pattern from
// https://github.com/dapr/dapr/issues/10447 across multiple iterations of the same event names: on
// each iteration the workflow races WaitForSingleEvent("Approve", ...) against
// WaitForSingleEvent("Reject", ...) via Select, and must carry the losing task into the next
// iteration's Select call rather than discarding it and creating a fresh WaitForSingleEvent for the
// same name -- see WaitForSingleEvent's doc comment for why discarding it would hang.
//
// Iteration 1 is won by "Approve", leaving "Reject" as a genuine loser that must be carried
// forward; iteration 2 is then won by a fresh "Reject". This specifically exercises the hazard: if
// the carried-forward "Reject" from iteration 1 were discarded and a new one created instead, the
// stale task would still be the oldest queued waiter for that name and would steal iteration 2's
// "Reject" event out from under the new task, hanging the workflow. Racing two iterations both won
// by the same name (e.g. "Reject" then "Reject") would not catch this: nothing would have lost yet
// for that name to go stale.
func Test_Select_ApprovalGateLoop_CarriesLosingWaitForward(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("ApprovalGateLoopWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		approve := ctx.WaitForSingleEvent("Approve", -1)
		reject := ctx.WaitForSingleEvent("Reject", -1)

		var decisions []string
		for i := 0; i < 2; i++ {
			winner, err := ctx.Select(approve, reject)
			if err != nil {
				return nil, err
			}

			var v string
			switch winner {
			case 0:
				if err := approve.Await(&v); err != nil {
					return nil, err
				}
				decisions = append(decisions, "Approve:"+v)
				// Carry the losing "Reject" wait forward; a fresh WaitForSingleEvent("Reject", ...)
				// here would leave this one permanently queued and stealing the next "Reject" event.
				approve = ctx.WaitForSingleEvent("Approve", -1)
			case 1:
				if err := reject.Await(&v); err != nil {
					return nil, err
				}
				decisions = append(decisions, "Reject:"+v)
				reject = ctx.WaitForSingleEvent("Reject", -1)
			}
		}
		return decisions, nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "ApprovalGateLoopWorkflow")
	require.NoError(t, err)
	_, err = client.WaitForWorkflowStart(ctx, id)
	require.NoError(t, err)

	// Iteration 1 is won by "Approve", so the original "Reject" task loses and must be carried
	// forward. Iteration 2 is then resolved by a second "Reject" event: if the carry-forward were
	// wrong (losing task discarded, a fresh one created instead), the stale iteration-1 "Reject"
	// task would still be the oldest queued waiter for that name and would steal this event,
	// leaving iteration 2's Select -- and the test -- hanging until the timeout below.
	require.NoError(t, client.RaiseEvent(ctx, id, "Approve", api.WithEventPayload("first")))
	require.NoError(t, client.RaiseEvent(ctx, id, "Reject", api.WithEventPayload("second")))

	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `["Approve:first","Reject:second"]`, metadata.Output.Value)
}
