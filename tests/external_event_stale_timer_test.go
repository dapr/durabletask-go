package tests

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/task"
)

// Verifies that an external event delivered well within the WaitForSingleEvent
// timeout window is not lost when the (now unnecessary) event timeout timer
// later fires while the workflow is still running.
//
// Sequence exercised through the full stack (client -> sqlite backend ->
// workflow worker -> task executor):
//
//  1. The workflow starts waiting for event "A" with a 2s timeout and, without
//     awaiting it yet, blocks on an unrelated 4s durable timer.
//  2. The client raises event "A" immediately, well within the 2s window. The
//     event completes the pending external-event task with its payload.
//  3. At ~2s the durable timeout timer for the (already satisfied) event wait
//     fires anyway; no layer cancels it in this repo. The stale TimerFired is
//     delivered to the executor while the workflow is still blocked on the 4s
//     timer.
//  4. At ~4s the unrelated timer completes and the workflow finally awaits the
//     event task. The await must observe the delivered payload, not a
//     cancellation.
func Test_ExternalEvent_RaisedBeforeTimeout_SurvivesStaleTimerFired(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("EventThenStaleTimer", func(ctx *task.WorkflowContext) (any, error) {
		w := ctx.WaitForSingleEvent("A", 2*time.Second)

		// Keep the workflow alive past the event-timeout window so the stale
		// TimerFired for the satisfied wait is delivered while it still runs.
		if err := ctx.CreateTimer(4 * time.Second).Await(nil); err != nil {
			return nil, fmt.Errorf("unrelated timer failed: %w", err)
		}

		var value int
		if err := w.Await(&value); err != nil {
			return nil, fmt.Errorf("event await failed even though the event was raised in time: %w", err)
		}
		return value, nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "EventThenStaleTimer")
	require.NoError(t, err)

	// Raise the event immediately, well within the 2s timeout window.
	require.NoError(t, client.RaiseEvent(ctx, id, "A", api.WithEventPayload(42)))

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)

	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus,
		"workflow should complete successfully; failure details: %v", metadata.FailureDetails.GetErrorMessage())
	assert.Equal(t, `42`, metadata.Output.GetValue())
}

// Receive-loop variant: a workflow waits for the same event name twice in a
// row. The first wait is satisfied immediately; its timeout timer still fires
// later and must not disturb the second wait registered under the same event
// name. The second event is raised after the first wait's stale timer has
// fired but well within the second wait's timeout window.
func Test_ExternalEvent_ReceiveLoop_SecondEventAfterStaleTimerFired(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("EventReceiveLoop", func(ctx *task.WorkflowContext) (any, error) {
		var v1 int
		if err := ctx.WaitForSingleEvent("A", 2*time.Second).Await(&v1); err != nil {
			return nil, fmt.Errorf("first wait failed: %w", err)
		}
		var v2 int
		if err := ctx.WaitForSingleEvent("A", 10*time.Second).Await(&v2); err != nil {
			return nil, fmt.Errorf("second wait failed: %w", err)
		}
		return []int{v1, v2}, nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "EventReceiveLoop")
	require.NoError(t, err)

	// First event: raised immediately, well within the first wait's 2s window.
	require.NoError(t, client.RaiseEvent(ctx, id, "A", api.WithEventPayload(1)))

	// Wait until the first wait's timeout timer (due at ~2s) has fired and
	// been processed, so the second EventRaised lands after the stale
	// TimerFired in history. 5s is well past the 2s due time while still well
	// within the second wait's 10s window.
	time.Sleep(5 * time.Second)

	// Second event: must complete the second wait, which is still pending.
	require.NoError(t, client.RaiseEvent(ctx, id, "A", api.WithEventPayload(2)))

	timeoutCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)

	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus,
		"workflow should complete successfully; failure details: %v", metadata.FailureDetails.GetErrorMessage())
	assert.Equal(t, `[1,2]`, metadata.Output.GetValue())
}
