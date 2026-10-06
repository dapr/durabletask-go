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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/runtimestate"
)

// captureLogger records formatted log lines per level for assertions.
type captureLogger struct {
	mu    sync.Mutex
	warns []string
	debug []string
}

func (c *captureLogger) log(dst *[]string, format string, v ...any) {
	c.mu.Lock()
	defer c.mu.Unlock()
	*dst = append(*dst, fmt.Sprintf(format, v...))
}

func (c *captureLogger) Debug(v ...any)                 {}
func (c *captureLogger) Debugf(format string, v ...any) { c.log(&c.debug, format, v...) }
func (c *captureLogger) Info(v ...any)                  {}
func (c *captureLogger) Infof(format string, v ...any)  {}
func (c *captureLogger) Warn(v ...any)                  {}
func (c *captureLogger) Warnf(format string, v ...any)  { c.log(&c.warns, format, v...) }
func (c *captureLogger) Error(v ...any)                 {}
func (c *captureLogger) Errorf(format string, v ...any) {}

func (c *captureLogger) warnsContaining(sub string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, w := range c.warns {
		if strings.Contains(w, sub) {
			n++
		}
	}
	return n
}

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

func evTimerFired(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TimerFired{
			TimerFired: &protos.TimerFiredEvent{TimerId: id, FireAt: timestamppb.Now()},
		},
	}
}

func evChildCompleted(id int32, result string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ChildWorkflowInstanceCompleted{
			ChildWorkflowInstanceCompleted: &protos.ChildWorkflowInstanceCompletedEvent{
				TaskScheduledId: id,
				Result:          wrapperspb.String(result),
			},
		},
	}
}

func evChildFailed(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ChildWorkflowInstanceFailed{
			ChildWorkflowInstanceFailed: &protos.ChildWorkflowInstanceFailedEvent{
				TaskScheduledId: id,
				FailureDetails:  &protos.TaskFailureDetails{ErrorType: "TestError", ErrorMessage: "injected child failure"},
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

// A schedule whose resolution was delivered from the buffer is still emitted:
// the backend must record its scheduling event so the history stays
// replayable, and the applier withholds the dispatch because the resolution
// is already in the state (see Test_BufferedResolution_ExecutorReplayRoundTrip
// and the applier tests in backend/runtimestate).
const emittedNotDispatched = "the resolved schedule must still be emitted; the applier withholds its dispatch"
func isCreateChild(a *protos.WorkflowAction) bool  { return a.GetCreateChildWorkflow() != nil }

// waitThenActivityRegistry registers a workflow that waits for the "go" event
// and then calls the "act" activity, returning the activity output. Sequence
// numbers: the WaitForSingleEvent synthetic timer takes id 0, the activity
// takes id 1.
func waitThenActivityRegistry(t *testing.T) *TaskRegistry {
	t.Helper()
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
			return nil, err
		}
		var out string
		if err := ctx.CallActivity("act").Await(&out); err != nil {
			return nil, err
		}
		return out, nil
	}))
	require.NoError(t, r.AddActivityN("act", func(ActivityContext) (any, error) { return "ran", nil }))
	return r
}

func Test_BufferedResolution_EarlyTaskCompleted(t *testing.T) {
	actions, cl := runBuffered(t, waitThenActivityRegistry(t), nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(1, `"injected"`),
		evEventRaised("go"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co, "workflow must complete using the early completion")
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Equal(t, `"injected"`, co.GetResult().GetValue())
	assert.Equal(t, 1, countActions(actions, isScheduleTask), emittedNotDispatched)
	assert.Empty(t, cl.warns)
}

func Test_BufferedResolution_EarlyTaskFailed(t *testing.T) {
	var gotExecID string
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
			return nil, err
		}
		task := ctx.CallActivity("act")
		err := task.Await(nil)
		gotExecID = task.TaskExecutionId()
		return nil, err
	}))
	require.NoError(t, r.AddActivityN("act", func(ActivityContext) (any, error) { return nil, nil }))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskFailed(1, "exec-x"),
		evEventRaised("go"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, co.WorkflowStatus)
	assert.Contains(t, co.GetFailureDetails().GetErrorMessage(), "injected failure")
	assert.Equal(t, 1, countActions(actions, isScheduleTask), emittedNotDispatched)
	assert.Equal(t, "exec-x", gotExecID)
	assert.Empty(t, cl.warns)
}

func Test_BufferedResolution_EarlyTimerFired(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
			return nil, err
		}
		if err := ctx.CreateTimer(time.Hour).Await(nil); err != nil {
			return nil, err
		}
		return "done", nil
	}))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTimerFired(1),
		evEventRaised("go"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Equal(t, 1, countActions(actions, func(a *protos.WorkflowAction) bool {
		return a.GetCreateTimer() != nil && a.Id == 1
	}), emittedNotDispatched)
	assert.Empty(t, cl.warns)
}

func Test_BufferedResolution_EarlyChildCompleted(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
			return nil, err
		}
		var out string
		if err := ctx.CallChildWorkflow("child").Await(&out); err != nil {
			return nil, err
		}
		return out, nil
	}))
	require.NoError(t, r.AddWorkflowN("child", func(ctx *WorkflowContext) (any, error) { return "child-ran", nil }))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evChildCompleted(1, `"injected-child"`),
		evEventRaised("go"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Equal(t, `"injected-child"`, co.GetResult().GetValue())
	assert.Equal(t, 1, countActions(actions, isCreateChild), emittedNotDispatched)
	assert.Empty(t, cl.warns)
}

func Test_BufferedResolution_EarlyChildFailed(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
			return nil, err
		}
		return nil, ctx.CallChildWorkflow("child").Await(nil)
	}))
	require.NoError(t, r.AddWorkflowN("child", func(ctx *WorkflowContext) (any, error) { return nil, nil }))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evChildFailed(1),
		evEventRaised("go"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, co.WorkflowStatus)
	assert.Equal(t, 1, countActions(actions, isCreateChild), emittedNotDispatched)
	assert.Empty(t, cl.warns)
}

func Test_BufferedResolution_EarlyTimerFiredForExternalEventTimer(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("a", -1).Await(nil); err != nil {
			return nil, err
		}
		err := ctx.WaitForSingleEvent("b", time.Hour).Await(nil)
		if errors.Is(err, ErrTaskCanceled) {
			return "timedout", nil
		}
		return nil, err
	}))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTimerFired(1),
		evEventRaised("a"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co, "the buffered TimerFired must cancel the wait for event b immediately")
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Equal(t, `"timedout"`, co.GetResult().GetValue())
	assert.Empty(t, cl.warns)
}

func Test_BufferedResolution_UnconsumedOrphanWarns(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("go", -1).Await(nil)
	}))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(99, `"orphan"`),
		evEventRaised("go"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Equal(t, 1, cl.warnsContaining("TaskCompleted for id 99"))
}

func Test_BufferedResolution_UnconsumedOrphanWarnsOnBlockedTurn(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("go", -1).Await(nil)
	}))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(99, `"orphan"`),
	})

	assert.Nil(t, completeAction(t, actions), "workflow stays blocked on the external event")
	assert.Equal(t, 1, cl.warnsContaining("TaskCompleted for id 99"))
}

func Test_BufferedResolution_DuplicateAfterResolutionDropped(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		var out string
		if err := ctx.CallActivity("act").Await(&out); err != nil {
			return nil, err
		}
		return out, nil
	}))
	require.NoError(t, r.AddActivityN("act", func(ActivityContext) (any, error) { return nil, nil }))

	actions, cl := runBuffered(t, r,
		[]*protos.HistoryEvent{
			evExecutionStarted("wf"),
			evTaskScheduled(0, "act"),
		},
		[]*protos.HistoryEvent{
			evTaskCompleted(0, `"first"`),
			evTaskCompleted(0, `"second"`),
		})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Equal(t, `"first"`, co.GetResult().GetValue(), "the first resolution wins; the duplicate is dropped")
	assert.Empty(t, cl.warns)
}

func Test_BufferedResolution_LateTaskScheduledAfterDelivery(t *testing.T) {
	// Histories produced by the pre-fix stall can contain the orphan
	// completion followed by a late TaskScheduled for the same id. The
	// retained pending action must match it without a nondeterminism error.
	actions, cl := runBuffered(t, waitThenActivityRegistry(t), nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(1, `"injected"`),
		evEventRaised("go"),
		evTaskScheduled(1, "act"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Equal(t, `"injected"`, co.GetResult().GetValue())
	assert.Zero(t, countActions(actions, isScheduleTask))
	assert.Empty(t, cl.warns)
}

func Test_BufferedResolution_KindMismatchNotDelivered(t *testing.T) {
	// A TimerFired for id 1 must not resolve the activity task at id 1: the
	// activity is dispatched as before and the unmatched timer resolution
	// warns at the end of the turn.
	actions, cl := runBuffered(t, waitThenActivityRegistry(t), nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTimerFired(1),
		evEventRaised("go"),
	})

	assert.Nil(t, completeAction(t, actions))
	assert.Equal(t, 1, countActions(actions, isScheduleTask), "the activity dispatch is unaffected")
	assert.Equal(t, 1, cl.warnsContaining("TimerFired for id 1"))
}

func Test_BufferedResolution_SuspensionPrecedence(t *testing.T) {
	actions, cl := runBuffered(t, waitThenActivityRegistry(t), nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evSuspended(),
		evTaskCompleted(1, `"injected"`),
		evEventRaised("go"),
		evResumed(),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Equal(t, `"injected"`, co.GetResult().GetValue())
	assert.Equal(t, 1, countActions(actions, isScheduleTask), emittedNotDispatched)
	assert.Empty(t, cl.warns)
}

func Test_BufferedResolution_FreshContextPerExecution(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("go", -1).Await(nil)
	}))

	_, cl1 := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(99, `"orphan"`),
		evEventRaised("go"),
	})
	require.Equal(t, 1, cl1.warnsContaining("TaskCompleted for id 99"))

	_, cl2 := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evEventRaised("go"),
	})
	assert.Empty(t, cl2.warns, "a fresh execution must not inherit buffered resolutions")
}

func Test_BufferedResolution_EarlyFailureWithRetryPolicy(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
			return nil, err
		}
		return nil, ctx.CallActivity("act", WithActivityRetryPolicy(&RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: time.Second,
			BackoffCoefficient:   2,
		})).Await(nil)
	}))
	require.NoError(t, r.AddActivityN("act", func(ActivityContext) (any, error) { return nil, nil }))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskFailed(1, "exec-x"),
		evEventRaised("go"),
	})

	// The buffered failure resolves attempt one as soon as it's scheduled, so by the time Await
	// polls it, the retry chain is already in its failed-with-retries-remaining state and arms the
	// backoff timer right there: the workflow blocks on the retry timer instead of completing. The
	// failed attempt's ScheduleTask is still emitted (the applier withholds its dispatch, see
	// emittedNotDispatched), and the retry timer action is emitted alongside it.
	assert.Nil(t, completeAction(t, actions))
	assert.Equal(t, 1, countActions(actions, isScheduleTask), emittedNotDispatched)
	assert.Equal(t, 1, countActions(actions, func(a *protos.WorkflowAction) bool {
		return a.GetCreateTimer() != nil && a.Id == 2
	}), "the retry backoff timer must be armed")
	assert.Empty(t, cl.warns)
}

// Test_RetryPolicy_UnobservedAttemptNeverRetries is a regression test: a retry-configured task that
// nothing ever awaits or selects must not retry in the background. The retry chain only advances
// when its outer task is polled (see internalScheduleTaskWithRetries's advance hook), so a failed
// attempt nobody observes stays failed and emits no further actions, even though the workflow
// itself goes on to complete via a different, awaited task.
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
		if err := a.Await(nil); err != nil && !errors.Is(err, ErrTaskBlocked) {
			// A's retry timer is armed here, once Await actually polls A -- this call blocks
			// (processNextEvent runs out of history) rather than returning an error.
		}
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

func Test_BufferedResolution_FanOutTwoEarlyCompletions(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
			return nil, err
		}
		t1 := ctx.CallActivity("a")
		t2 := ctx.CallActivity("b")
		var out1, out2 string
		if err := t1.Await(&out1); err != nil {
			return nil, err
		}
		if err := t2.Await(&out2); err != nil {
			return nil, err
		}
		return out1 + out2, nil
	}))
	require.NoError(t, r.AddActivityN("a", func(ActivityContext) (any, error) { return nil, nil }))
	require.NoError(t, r.AddActivityN("b", func(ActivityContext) (any, error) { return nil, nil }))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(2, `"two"`),
		evTaskCompleted(1, `"one"`),
		evEventRaised("go"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Equal(t, `"onetwo"`, co.GetResult().GetValue())
	assert.Equal(t, 2, countActions(actions, isScheduleTask), emittedNotDispatched)
	assert.Empty(t, cl.warns)
}

func Test_BufferedResolution_TerminatedTurnEmitsOnlyCompletion(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		return nil, ctx.WaitForSingleEvent("go", -1).Await(nil)
	}))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(7, `"orphan"`),
		{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_ExecutionTerminated{
				ExecutionTerminated: &protos.ExecutionTerminatedEvent{Input: wrapperspb.String(`"stop"`)},
			},
		},
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, co.WorkflowStatus)
	for _, a := range actions {
		assert.NotNil(t, a.GetCompleteWorkflow(), "a terminated turn must emit only the completion action")
	}
	assert.Equal(t, 1, cl.warnsContaining("TaskCompleted for id 7"))
}

func Test_BufferedResolution_ContinueAsNewNoCarryover(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
			return nil, err
		}
		ctx.ContinueAsNew(nil, WithKeepUnprocessedEvents())
		return nil, nil
	}))

	actions, cl := runBuffered(t, r, nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(5, `"orphan"`),
		evEventRaised("go"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW, co.WorkflowStatus)
	for _, e := range co.GetCarryoverEvents() {
		assert.Nil(t, e.GetTaskCompleted(), "buffered resolutions must not be carried into the next generation")
	}
	assert.Equal(t, 1, cl.warnsContaining("TaskCompleted for id 5"))
}

func Test_BufferedResolution_ExecutorSurfacesWarning(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
			return nil, err
		}
		var out string
		if err := ctx.CallActivity("act").Await(&out); err != nil {
			return nil, err
		}
		return out, nil
	}))
	require.NoError(t, r.AddActivityN("act", func(ActivityContext) (any, error) { return nil, nil }))

	cl := &captureLogger{}
	ex := NewTaskExecutorWithLogger(r, cl)

	resp, err := ex.ExecuteWorkflow(t.Context(), "exec-test", nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(1, `"injected"`),
		evEventRaised("go"),
	}, backend.ExecuteOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, countActions(resp.GetActions(), isScheduleTask), emittedNotDispatched)
	assert.Empty(t, cl.warns)

	_, err = ex.ExecuteWorkflow(t.Context(), "exec-test", nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(42, `"orphan"`),
		evEventRaised("go"),
	}, backend.ExecuteOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, cl.warnsContaining("TaskCompleted for id 42"))
}


// Test_BufferedResolution_ExecutorReplayRoundTrip drives the dapr shape of
// the bug end to end: the completion of the first activity reaches the
// workflow before its TaskScheduled was committed, the turn re-emits that
// schedule and dispatches the second activity, and the next turn must replay
// the recorded history without a non-determinism error.
func Test_BufferedResolution_ExecutorReplayRoundTrip(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("wf", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("go", -1).Await(nil); err != nil {
			return nil, err
		}
		var out string
		if err := ctx.CallActivity("act").Await(&out); err != nil {
			return nil, err
		}
		if err := ctx.CallActivity("act2").Await(nil); err != nil {
			return nil, err
		}
		return out, nil
	}))
	require.NoError(t, r.AddActivityN("act", func(ActivityContext) (any, error) { return nil, nil }))
	require.NoError(t, r.AddActivityN("act2", func(ActivityContext) (any, error) { return nil, nil }))
	ex := NewTaskExecutor(r)
	applier := runtimestate.NewApplier("app", "ns")

	state := runtimestate.NewWorkflowRuntimeState("exec-test", nil, nil)
	for _, e := range []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(1, `"injected"`),
		evEventRaised("go"),
	} {
		require.NoError(t, runtimestate.AddEvent(state, e))
	}
	resp, err := ex.ExecuteWorkflow(t.Context(), "exec-test", state.GetOldEvents(), state.GetNewEvents(), backend.ExecuteOptions{})
	require.NoError(t, err)
	assert.Equal(t, 2, countActions(resp.GetActions(), isScheduleTask), emittedNotDispatched)
	_, err = applier.Actions(state, resp.GetCustomStatus(), resp.GetActions(), nil, nil)
	require.NoError(t, err)

	require.Len(t, state.GetPendingTasks(), 1, "only the unresolved activity is dispatched")
	assert.Equal(t, int32(2), state.GetPendingTasks()[0].GetEventId())
	var scheduled, completed bool
	for _, e := range state.GetNewEvents() {
		scheduled = scheduled || (e.GetTaskScheduled() != nil && e.GetEventId() == 1)
		completed = completed || (e.GetTaskCompleted() != nil && e.GetTaskCompleted().GetTaskScheduledId() == 1)
	}
	assert.True(t, scheduled, "TaskScheduled#1 must be recorded so the completion keeps its match")
	assert.True(t, completed, "TaskCompleted#1 must be retained")

	// The next turn replays the committed history with the second
	// activity's completion.
	history := append(state.GetOldEvents(), state.GetNewEvents()...)
	resp, err = ex.ExecuteWorkflow(t.Context(), "exec-test", history, []*protos.HistoryEvent{evTaskCompleted(2, `null`)}, backend.ExecuteOptions{})
	require.NoError(t, err)
	co := completeAction(t, resp.GetActions())
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus, co.GetFailureDetails().GetErrorMessage())
	assert.Equal(t, `"injected"`, co.GetResult().GetValue())
	assert.Zero(t, countActions(resp.GetActions(), isScheduleTask))
}

func Test_CompletableTask_OnCompletedAfterCompletion(t *testing.T) {
	task := newTask(newTestContext(t))
	task.complete([]byte("x"))
	fired := false
	task.onCompleted(func() { fired = true })
	assert.True(t, fired, "onCompleted on an already completed task must fire immediately")
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

func Test_BufferedResolution_KindGuardOnOccupiedID(t *testing.T) {
	// A TaskCompleted whose id is occupied by a pending TIMER (here the
	// synthetic external event timer at id 0) must buffer rather than
	// complete the timer, which would wrongly cancel the external event wait.
	actions, cl := runBuffered(t, waitThenActivityRegistry(t), nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(0, `"x"`),
		evEventRaised("go"),
	})

	assert.Nil(t, completeAction(t, actions),
		"the wait must complete via the event, not fail via a wrongly cancelled timer")
	assert.Equal(t, 1, countActions(actions, isScheduleTask),
		"the activity dispatch proceeds normally")
	assert.Equal(t, 1, cl.warnsContaining("TaskCompleted for id 0"))
}

func Test_BufferedResolution_PreSyntheticTimerMigration(t *testing.T) {
	// A history produced before WaitForSingleEvent emitted its synthetic
	// timer numbers the activity as id 0, which the current replay assigns
	// to the synthetic timer. The early completion for id 0 must buffer past
	// the timer (kind mismatch), and when the historical TaskScheduled(0)
	// drops the optional timer and shifts the activity onto id 0, the
	// buffered completion must be delivered to it.
	actions, cl := runBuffered(t, waitThenActivityRegistry(t), nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(0, `"migrated"`),
		evEventRaised("go"),
		evTaskScheduled(0, "act"),
	})

	co := completeAction(t, actions)
	require.NotNil(t, co)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, co.WorkflowStatus)
	assert.Equal(t, `"migrated"`, co.GetResult().GetValue())
	assert.Zero(t, countActions(actions, isScheduleTask))
	assert.Empty(t, cl.warns)
}
