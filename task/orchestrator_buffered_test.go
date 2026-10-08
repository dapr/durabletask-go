/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package task

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
)

// captureLogger records formatted log lines per level for assertions.
type captureLogger struct {
	mu    sync.Mutex
	warns []string
}

func (c *captureLogger) log(dst *[]string, format string, v ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	*dst = append(*dst, fmt.Sprintf(format, v...))
}

func (c *captureLogger) Debug(v ...any)                 {}
func (c *captureLogger) Debugf(format string, v ...any) {}
func (c *captureLogger) Info(v ...any)                  {}
func (c *captureLogger) Infof(format string, v ...any)  {}
func (c *captureLogger) Warn(v ...any)                  {}
func (c *captureLogger) Warnf(format string, v ...any)  { c.log(&c.warns, format, v...) }
func (c *captureLogger) Error(v ...any)                 {}
func (c *captureLogger) Errorf(format string, v ...any) {}

func evExecutionStarted(name string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name:             name,
				WorkflowInstance: &protos.WorkflowInstance{InstanceId: "buffered-test"},
			},
		},
	}
}

func evTaskScheduled(id int32, name string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   id,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TaskScheduled{
			TaskScheduled: &protos.TaskScheduledEvent{Name: name},
		},
	}
}

func evTaskCompleted(id int32, result string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TaskCompleted{
			TaskCompleted: &protos.TaskCompletedEvent{
				TaskScheduledId: id,
				Result:          wrapperspb.String(result),
			},
		},
	}
}

func evTaskFailed(id int32, execID string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TaskFailed{
			TaskFailed: &protos.TaskFailedEvent{
				TaskScheduledId: id,
				TaskExecutionId: execID,
				FailureDetails:  &protos.TaskFailureDetails{ErrorType: "TestError", ErrorMessage: "injected failure"},
			},
		},
	}
}

func evEventRaised(name string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_EventRaised{
			EventRaised: &protos.EventRaisedEvent{Name: name},
		},
	}
}

func evSuspended() *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionSuspended{
			ExecutionSuspended: &protos.ExecutionSuspendedEvent{},
		},
	}
}

func evTimerFired(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TimerFired{
			TimerFired: &protos.TimerFiredEvent{TimerId: id, FireAt: timestamppb.Now()},
		},
	}
}

// evRetryTimerCreated is the TimerCreated event the backend records for a retry backoff timer
// action, as emitted by both the Await-driven retry design and the pollable one.
func evRetryTimerCreated(id int32, execID string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   id,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TimerCreated{
			TimerCreated: &protos.TimerCreatedEvent{
				FireAt: timestamppb.Now(),
				Origin: &protos.TimerCreatedEvent_ActivityRetry{
					ActivityRetry: &protos.TimerOriginActivityRetry{TaskExecutionId: execID},
				},
			},
		},
	}
}

func evResumed() *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionResumed{
			ExecutionResumed: &protos.ExecutionResumedEvent{},
		},
	}
}

func runBuffered(t *testing.T, registry *TaskRegistry, oldEvents, newEvents []*protos.HistoryEvent) ([]*protos.WorkflowAction, *captureLogger) {
	t.Helper()
	cl := &captureLogger{}
	ctx := NewWorkflowContext(registry, "buffered-test", oldEvents, newEvents)
	ctx.SetLogger(cl)
	return ctx.start(), cl
}

func completeAction(t *testing.T, actions []*protos.WorkflowAction) *protos.CompleteWorkflowAction {
	t.Helper()
	for _, a := range actions {
		if co := a.GetCompleteWorkflow(); co != nil {
			return co
		}
	}
	return nil
}

func countActions(actions []*protos.WorkflowAction, pred func(*protos.WorkflowAction) bool) int {
	n := 0
	for _, a := range actions {
		if pred(a) {
			n++
		}
	}
	return n
}

func isScheduleTask(a *protos.WorkflowAction) bool { return a.GetScheduleTask() != nil }

func Test_RetryPolicy_UnobservedAttemptNeverRetries(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		_ = ctx.CallActivity("A", WithActivityRetryPolicy(&RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: time.Second,
			BackoffCoefficient:   2,
		}))
		b := ctx.CallActivity("B")
		return nil, b.Await(nil)
	}))
	require.NoError(t, r.AddActivityN("A", func(ActivityContext) (any, error) { return nil, nil }))
	require.NoError(t, r.AddActivityN("B", func(ActivityContext) (any, error) { return nil, nil }))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskFailed(0, "exec-a"),
		evTaskCompleted(1, `null`),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co, "the workflow must complete once its only awaited task (B) resolves")
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Zero(t, countActions(actions, func(a *protos.WorkflowAction) bool { return a.GetCreateTimer() != nil }),
		"A's failure was never observed, so no retry backoff timer may be armed for it")
	assert.Empty(t, cl.warns)
}

// Test_RetryPolicy_TimerSequenceIdMatchesAwaitObservationPoint is a regression test for replay
// compatibility: the retry backoff timer's action id (sequence number) must match where the
// workflow function actually observes the failed attempt by calling Await, not where the attempt
// happens to fail in history. A workflow that schedules C between observing A's failure and
// awaiting A again must see C take the sequence id that would otherwise go to A's retry timer, and
// the retry timer take the next id after that -- exactly as if the retry decision were made inline
// in Await itself, which is what histories recorded before retries became pollable assume.
func Test_RetryPolicy_TimerSequenceIdMatchesAwaitObservationPoint(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		a := ctx.CallActivity("A", WithActivityRetryPolicy(&RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: time.Second,
			BackoffCoefficient:   2,
		})) // id 0
		b := ctx.CallActivity("B") // id 1
		if err := b.Await(nil); err != nil {
			return nil, err
		}
		c := ctx.CallActivity("C") // id 2: scheduled before A's failure is ever observed
		// A's retry timer is armed here, once Await actually polls A. The call then blocks by
		// panicking with ErrTaskBlocked (history has run out), so nothing after it runs.
		_ = a.Await(nil)
		return nil, c.Await(nil)
	}))
	require.NoError(t, r.AddActivityN("A", func(ActivityContext) (any, error) { return nil, nil }))
	require.NoError(t, r.AddActivityN("B", func(ActivityContext) (any, error) { return nil, nil }))
	require.NoError(t, r.AddActivityN("C", func(ActivityContext) (any, error) { return nil, nil }))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskFailed(0, "exec-a"),
		evTaskCompleted(1, `null`),
	})

	require.Equal(t, 3, countActions(actions, isScheduleTask), "A's first attempt, B, and C are each scheduled on this turn")
	require.True(t, countActions(actions, func(a *protos.WorkflowAction) bool {
		return a.GetScheduleTask().GetName() == "C" && a.Id == 2
	}) == 1, "C must take sequence id 2, the id it would get if A's retry were still decided inside Await rather than eagerly on TaskFailed")
	require.True(t, countActions(actions, func(a *protos.WorkflowAction) bool {
		return a.GetCreateTimer() != nil && a.Id == 3
	}) == 1, "A's retry backoff timer must take the next id after C, since Await only observes A's failure after C is scheduled")
	assert.Empty(t, cl.warns)
}

// Test_RetryPolicy_SecondAwaitDoesNotReRunChain is a regression test: once a retry-configured
// task's outer task has failed, polling it again (a second Await, or a Select that still includes
// it) must be a no-op -- it must not consult policy.Handle again, and it must not arm another
// backoff timer for a task the caller has already observed fail. MaxAttempts is left high enough
// that retries are nominally still available; policy.Handle itself declines the retry, which is the
// only way to reach the failure path without exhausting MaxAttempts, so a bug that re-runs this
// branch on every subsequent poll is caught by handleCalls incrementing again, not masked by
// retryCount already being at its limit.
func Test_RetryPolicy_SecondAwaitDoesNotReRunChain(t *testing.T) {
	r := NewTaskRegistry()
	var handleCalls int
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		policy := &RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: time.Second,
			BackoffCoefficient:   2,
			Handle: func(err error) bool {
				handleCalls++
				return false
			},
		}
		flaky := ctx.CallActivity("Flaky", WithActivityRetryPolicy(policy)) // id 0
		err1 := flaky.Await(nil)
		// The chain already failed (Handle declined the only attempt, resolved from buffered
		// history below): this second Await must be a pure no-op that returns the same error.
		err2 := flaky.Await(nil)
		if err1 == nil || err2 == nil || err1.Error() != err2.Error() {
			return nil, fmt.Errorf("expected both Awaits to return the same error, got %v and %v", err1, err2)
		}
		return nil, nil
	}))
	require.NoError(t, r.AddActivityN("Flaky", func(ActivityContext) (any, error) { return nil, nil }))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskFailed(0, "exec-1"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co, "the workflow must complete once the retry chain fails and is observed twice")
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Zero(t, countActions(actions, func(a *protos.WorkflowAction) bool { return a.GetCreateTimer() != nil }),
		"Handle declined the only attempt; no backoff timer may ever be armed, including on the second Await")
	assert.Equal(t, 1, handleCalls, "policy.Handle must run exactly once, for the one real failure, not again on the second Await")
	assert.Empty(t, cl.warns)
}

// Test_RetryPolicy_TaskExecutionIdReflectsFailedAttempt pins the TaskExecutionId contract of a
// retry-configured task: it is the id recorded on the most recently observed failed attempt, and
// empty while no attempt has failed. The Await-driven design delegated to the first attempt, which
// only ever learned its id from a TaskFailed event, so this is what callers saw before retries
// became pollable.
func Test_RetryPolicy_TaskExecutionIdReflectsFailedAttempt(t *testing.T) {
	policy := func(maxAttempts int) *RetryPolicy {
		return &RetryPolicy{MaxAttempts: maxAttempts, InitialRetryInterval: time.Second, BackoffCoefficient: 2}
	}
	awaitThenReport := func(maxAttempts int) func(ctx *WorkflowContext) (any, error) {
		return func(ctx *WorkflowContext) (any, error) {
			a := ctx.CallActivity("A", WithActivityRetryPolicy(policy(maxAttempts)))
			_ = a.Await(nil)
			return a.TaskExecutionId(), nil
		}
	}

	for name, tc := range map[string]struct {
		wf      func(ctx *WorkflowContext) (any, error)
		history []*protos.HistoryEvent
		want    string
	}{
		"retried then succeeded": {
			wf: awaitThenReport(3),
			history: []*protos.HistoryEvent{
				evTaskScheduled(0, "A"), evTaskFailed(0, "exec-1"), evRetryTimerCreated(1, "exec-1"),
				evTimerFired(1), evTaskScheduled(2, "A"), evTaskCompleted(2, `null`),
			},
			want: `"exec-1"`,
		},
		"retries exhausted": {
			wf: awaitThenReport(2),
			history: []*protos.HistoryEvent{
				evTaskScheduled(0, "A"), evTaskFailed(0, "exec-1"), evRetryTimerCreated(1, "exec-1"),
				evTimerFired(1), evTaskScheduled(2, "A"), evTaskFailed(2, "exec-1"),
			},
			want: `"exec-1"`,
		},
		"first attempt succeeded": {
			wf:      awaitThenReport(3),
			history: []*protos.HistoryEvent{evTaskScheduled(0, "A"), evTaskCompleted(0, `null`)},
			want:    `""`,
		},
		"observed through Select": {
			wf: func(ctx *WorkflowContext) (any, error) {
				a := ctx.CallActivity("A", WithActivityRetryPolicy(policy(3)))
				b := ctx.CallActivity("B")
				if i, err := ctx.Select(a, b); err != nil || i != 0 {
					return nil, fmt.Errorf("expected A (index 0) to win, got %d, %v", i, err)
				}
				return a.TaskExecutionId(), nil
			},
			history: []*protos.HistoryEvent{
				evTaskScheduled(0, "A"), evTaskScheduled(1, "B"), evTaskFailed(0, "exec-1"),
				evRetryTimerCreated(2, "exec-1"), evTimerFired(2), evTaskScheduled(3, "A"), evTaskCompleted(3, `null`),
			},
			want: `"exec-1"`,
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := NewTaskRegistry()
			require.NoError(t, r.AddWorkflowN("wf", tc.wf))
			require.NoError(t, r.AddActivityN("A", func(ActivityContext) (any, error) { return nil, nil }))
			require.NoError(t, r.AddActivityN("B", func(ActivityContext) (any, error) { return nil, nil }))

			actions, cl := runBuffered(t, r, nil, append([]*protos.HistoryEvent{evExecutionStarted("wf")}, tc.history...))

			co := completeAction(t, actions)
			require.NotNil(t, co)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus, co.GetFailureDetails().GetErrorMessage())
			assert.Equal(t, tc.want, co.GetResult().GetValue())
			assert.Empty(t, cl.warns)
		})
	}
}

// Test_RetryPolicy_CanceledAttemptPropagatesCancel guards a latent path: nothing cancels an
// activity or child task today, but a canceled attempt has nil failureDetails, so if the chain
// gave up with outer.fail(nil) the caller would see a nil-error success. The attempt is canceled
// directly on the pending task here, since no history event can do it.
func Test_RetryPolicy_CanceledAttemptPropagatesCancel(t *testing.T) {
	t.Run("give up surfaces ErrTaskCanceled", func(t *testing.T) {
		r := NewTaskRegistry()
		require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
			a := ctx.CallActivity("A", WithActivityRetryPolicy(&RetryPolicy{MaxAttempts: 1, InitialRetryInterval: time.Second}))
			ctx.pendingTasks[0].cancel()
			err := a.Await(nil)
			return errors.Is(err, ErrTaskCanceled), nil
		}))
		require.NoError(t, r.AddActivityN("A", func(ActivityContext) (any, error) { return nil, nil }))

		actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{evExecutionStarted("wf")})

		co := completeAction(t, actions)
		require.NotNil(t, co)
		assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
		assert.Equal(t, `true`, co.GetResult().GetValue(), "Await must return ErrTaskCanceled, not nil")
		assert.Empty(t, cl.warns)
	})

	t.Run("retries remaining consults Handle with ErrTaskCanceled", func(t *testing.T) {
		r := NewTaskRegistry()
		var handled error
		require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
			a := ctx.CallActivity("A", WithActivityRetryPolicy(&RetryPolicy{
				MaxAttempts:          3,
				InitialRetryInterval: time.Second,
				BackoffCoefficient:   2,
				Handle:               func(err error) bool { handled = err; return true },
			}))
			ctx.pendingTasks[0].cancel()
			return nil, a.Await(nil)
		}))
		require.NoError(t, r.AddActivityN("A", func(ActivityContext) (any, error) { return nil, nil }))

		actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{evExecutionStarted("wf")})

		assert.Nil(t, completeAction(t, actions), "the workflow must block on the backoff timer")
		assert.ErrorIs(t, handled, ErrTaskCanceled)
		var timerExecID string
		for _, act := range actions {
			if ct := act.GetCreateTimer(); ct != nil {
				timerExecID = ct.GetActivityRetry().GetTaskExecutionId()
			}
		}
		assert.Equal(t, 1, countActions(actions, func(a *protos.WorkflowAction) bool { return a.GetCreateTimer() != nil }))
		assert.NotEmpty(t, timerExecID,
			"a canceled attempt has no execution id of its own; the retry timer must still carry the chain's real id, not overwrite it with an empty one")
		assert.Empty(t, cl.warns)
	})
}

// Test_Select_PollsEveryCandidateRegardlessOfWinner is a regression test: Select must poll every
// candidate on every check, not stop at the first one found completed. A candidate Select never
// polls because an earlier one already won never gets its own advance run (see
// internalScheduleTaskWithRetries), so a retry candidate's backoff timer is left for whenever
// something later happens to poll it again -- here, scheduling D right after Select -- instead of
// being allocated during the Select call itself. That makes the timer's action sequence number land
// after D's rather than before it, breaking replay for any history recorded before this fix.
//
// The shape here is load-bearing and deliberate: c.Await(nil) is what lets A's failure and B's
// completion land from already-present history before Select is ever called, and D is what gives
// the fix something to prove its ordering against. A bare comparison of Select(a, b) vs Select(b, a)
// with no intervening task would NOT catch this bug -- awaitUntil's very first poll runs before any
// history event is processed, so on that first call neither candidate is resolved yet regardless of
// argument order, and the short-circuit never gets a chance to matter.
func Test_Select_PollsEveryCandidateRegardlessOfWinner(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		a := ctx.CallActivity("A", WithActivityRetryPolicy(&RetryPolicy{ // id 0
			MaxAttempts:          3,
			InitialRetryInterval: time.Second,
			BackoffCoefficient:   2,
		}))
		b := ctx.CallActivity("B") // id 1
		c := ctx.CallActivity("C") // id 2
		// A fails and B completes while waiting here; neither a nor b (the retry chain's outer
		// task and the plain task) has been polled yet.
		if err := c.Await(nil); err != nil {
			return nil, err
		}

		// B wins at once. If Select stopped polling once it found B's winning completion, A would
		// never be polled this call, so its backoff timer would not be allocated here.
		winner, err := ctx.Select(b, a)
		if err != nil {
			return nil, err
		}
		if winner != 0 {
			return nil, fmt.Errorf("expected B (index 0) to win Select(b, a), got index %d", winner)
		}

		if err := ctx.CallActivity("D").Await(nil); err != nil { // must get sequence id 4, after A's timer
			return nil, err
		}
		return nil, a.Await(nil)
	}))
	require.NoError(t, r.AddActivityN("A", func(ActivityContext) (any, error) { return nil, nil }))
	require.NoError(t, r.AddActivityN("B", func(ActivityContext) (any, error) { return nil, nil }))
	require.NoError(t, r.AddActivityN("C", func(ActivityContext) (any, error) { return nil, nil }))
	require.NoError(t, r.AddActivityN("D", func(ActivityContext) (any, error) { return nil, nil }))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskFailed(0, "exec-a"),
		evTaskCompleted(1, `null`),
		evTaskCompleted(2, `null`),
	})

	require.Equal(t, 1, countActions(actions, func(a *protos.WorkflowAction) bool {
		return a.GetCreateTimer() != nil && a.Id == 3
	}), "A's retry timer must be allocated during the Select call that observed its failure (id 3), before D")
	require.Equal(t, 1, countActions(actions, func(act *protos.WorkflowAction) bool {
		return act.GetScheduleTask().GetName() == "D" && act.Id == 4
	}), "D must come after A's retry timer, not before it")
	assert.Empty(t, cl.warns)
}

// Test_Select_RejectsNilTask verifies that Select reports a nil task explicitly, rather than
// misattributing it to ErrTaskNotSelectable, the error for an unrelated, unsupported Task
// implementation.
func Test_Select_RejectsNilTask(t *testing.T) {
	ctx := newTestContext(t)
	other := newTask(ctx)

	_, err := ctx.Select(other, nil)
	require.EqualError(t, err, "task at index 1 is nil")
}

// Test_Select_RejectsTaskFromDifferentContext is a regression test: a *completableTask obtained
// from a different WorkflowContext passes the type assertion in Select's validation loop, so
// without this check Select would poll a task that can never complete from this context's point of
// view (its workflow execution belongs to a different ctx, whose history this ctx never processes),
// blocking the workflow indefinitely with no diagnostic.
func Test_Select_RejectsTaskFromDifferentContext(t *testing.T) {
	ctx := newTestContext(t)
	other := newTestContext(t)

	own := newTask(ctx)
	foreign := newTask(other)

	_, err := ctx.Select(own, foreign)
	require.EqualError(t, err, "task at index 1 belongs to a different WorkflowContext")
}

// Test_Select_DoesNotRegisterOnLosingTasks confirms Select's polling implementation never touches
// a losing candidate's completedCallback: repeatedly Selecting a still-pending task against an
// already-completed one must leave the pending task's callback field untouched (nil), since Select
// only reads isCompleted -- it never calls onCompleted.
func Test_Select_DoesNotRegisterOnLosingTasks(t *testing.T) {
	ctx := newTestContext(t)

	done := newTask(ctx)
	done.complete([]byte(`null`))

	pending := newTask(ctx)

	const iterations = 5
	for i := 0; i < iterations; i++ {
		winner, err := ctx.Select(pending, done)
		require.NoError(t, err)
		require.Equal(t, 1, winner)
		require.Nil(t, pending.completedCallback,
			"Select must never register a callback on a task it only polls")
	}
}

// Test_Select_TieBreak_LowestIndexWinsAcrossSuspendedBatch is a regression test for Select's
// tie-break rule in the one scenario where it's actually observable: several candidates completing
// within a single processNextEvent call. onExecutionResumed replays a whole batch of events
// buffered during a suspension in one such call, so if the event for the higher-index candidate
// (EventB, index 1) appears in that batch before the event for the lower-index one (EventA, index
// 0), Select must still return index 0 -- history order must not win over argument order.
func Test_Select_TieBreak_LowestIndexWinsAcrossSuspendedBatch(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		taskA := ctx.WaitForSingleEvent("EventA", -1)
		taskB := ctx.WaitForSingleEvent("EventB", -1)

		winner, err := ctx.Select(taskA, taskB)
		if err != nil {
			return nil, err
		}
		return winner, nil
	}))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evSuspended(),
		// EventB (Select index 1) is raised first in the suspended batch; EventA (Select index 0)
		// second. If Select's tie-break preferred history order, it would return 1 here.
		evEventRaised("EventB"),
		evEventRaised("EventA"),
		evResumed(),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Equal(t, `0`, co.GetResult().GetValue(), "the lowest-index candidate (EventA) must win regardless of which event appeared first in the replayed batch")
	assert.Empty(t, cl.warns)
}

// Benchmark_ReplaySequentialActivities measures a full replay of a workflow
// with 50 sequential completed activities, the shape dominated by the
// per-event and per-schedule bookkeeping this file's feature adds to.
func Benchmark_ReplaySequentialActivities(b *testing.B) {
	const n = 50
	r := NewTaskRegistry()
	if err := r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		for range n {
			if err := ctx.CallActivity("act").Await(nil); err != nil {
				return nil, err
			}
		}
		return "done", nil
	}); err != nil {
		b.Fatal(err)
	}
	if err := r.AddActivityN("act", func(ActivityContext) (any, error) { return nil, nil }); err != nil {
		b.Fatal(err)
	}

	events := make([]*protos.HistoryEvent, 0, 2*n+1)
	events = append(events, evExecutionStarted("wf"))
	for i := range int32(n) {
		events = append(events, evTaskScheduled(i, "act"), evTaskCompleted(i, `null`))
	}

	b.ReportAllocs()
	for b.Loop() {
		ctx := NewWorkflowContext(r, "bench", events, nil)
		if actions := ctx.start(); len(actions) != 1 {
			b.Fatalf("expected 1 action, got %d", len(actions))
		}
	}
}

// Benchmark_SelectFanOut measures a full replay of a workflow that fans out 50 activities and then
// repeatedly Selects over whichever ones haven't completed yet until all 50 have -- the shape
// dominated by Select's own per-call cost, as opposed to Benchmark_ReplaySequentialActivities,
// which is dominated by Await/CallActivity's per-schedule bookkeeping.
func Benchmark_SelectFanOut(b *testing.B) {
	const n = 50
	r := NewTaskRegistry()
	if err := r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		pending := make([]Task, n)
		for i := range pending {
			pending[i] = ctx.CallActivity("act")
		}
		for len(pending) > 0 {
			winner, err := ctx.Select(pending...)
			if err != nil {
				return nil, err
			}
			if err := pending[winner].Await(nil); err != nil {
				return nil, err
			}
			pending = append(pending[:winner], pending[winner+1:]...)
		}
		return "done", nil
	}); err != nil {
		b.Fatal(err)
	}
	if err := r.AddActivityN("act", func(ActivityContext) (any, error) { return nil, nil }); err != nil {
		b.Fatal(err)
	}

	events := make([]*protos.HistoryEvent, 0, 2*n+1)
	events = append(events, evExecutionStarted("wf"))
	for i := range int32(n) {
		events = append(events, evTaskScheduled(i, "act"))
	}
	for i := range int32(n) {
		events = append(events, evTaskCompleted(i, `null`))
	}

	b.ReportAllocs()
	for b.Loop() {
		ctx := NewWorkflowContext(r, "bench", events, nil)
		if actions := ctx.start(); len(actions) != 1 {
			b.Fatalf("expected 1 action, got %d", len(actions))
		}
	}
}
