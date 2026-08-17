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
	assert.Zero(t, countActions(actions, isScheduleTask), "the resolved activity must not be dispatched")
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
	assert.Zero(t, countActions(actions, isScheduleTask))
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
	assert.Zero(t, countActions(actions, func(a *protos.WorkflowAction) bool {
		return a.GetCreateTimer() != nil && a.Id == 1
	}), "the resolved timer must not be dispatched")
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
	assert.Zero(t, countActions(actions, isCreateChild))
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
	assert.Zero(t, countActions(actions, isCreateChild))
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
	assert.Zero(t, countActions(actions, isScheduleTask))
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

	// The buffered failure resolves attempt one at scheduling time and the
	// retry wrapper immediately arms the backoff timer: the workflow blocks
	// on the retry timer instead of completing, the failed attempt's
	// ScheduleTask is suppressed, and the retry timer action is emitted.
	assert.Nil(t, completeAction(t, actions))
	assert.Zero(t, countActions(actions, isScheduleTask))
	assert.Equal(t, 1, countActions(actions, func(a *protos.WorkflowAction) bool {
		return a.GetCreateTimer() != nil && a.Id == 2
	}), "the retry backoff timer must be armed")
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
	assert.Zero(t, countActions(actions, isScheduleTask))
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
	for _, a := range resp.GetActions() {
		assert.Nil(t, a.GetScheduleTask(), "the suppressed action must not reach the backend response")
	}
	assert.Empty(t, cl.warns)

	_, err = ex.ExecuteWorkflow(t.Context(), "exec-test", nil, []*protos.HistoryEvent{
		evExecutionStarted("wf"),
		evTaskCompleted(42, `"orphan"`),
		evEventRaised("go"),
	}, backend.ExecuteOptions{})
	require.NoError(t, err)
	assert.Equal(t, 1, cl.warnsContaining("TaskCompleted for id 42"))
}

func Test_BufferedResolution_DropShiftsSuppressedIDs(t *testing.T) {
	ctx := newTestContext(t)
	ctx.suppressedActionIDs = map[int32]struct{}{2: {}, 3: {}}
	ctx.pendingActions[1] = &protos.WorkflowAction{
		Id: 1,
		WorkflowActionType: &protos.WorkflowAction_CreateTimer{
			CreateTimer: &protos.CreateTimerAction{
				FireAt: timestamppb.New(externalEventIndefiniteFireAt),
				Origin: &protos.CreateTimerAction_ExternalEvent{
					ExternalEvent: &protos.TimerOriginExternalEvent{Name: "e"},
				},
			},
		},
	}
	ctx.pendingActions[2] = &protos.WorkflowAction{Id: 2}
	ctx.pendingActions[3] = &protos.WorkflowAction{Id: 3}
	ctx.sequenceNumber = 4

	require.True(t, ctx.dropOptionalExternalEventTimerAt(1))

	assert.Equal(t, map[int32]struct{}{1: {}, 2: {}}, ctx.suppressedActionIDs,
		"suppressed ids above the dropped id must shift down with their actions")
}

func Test_CompletableTask_OnCompletedAfterCompletion(t *testing.T) {
	task := newTask(newTestContext(t))
	task.complete([]byte("x"))
	fired := false
	task.onCompleted(func() { fired = true })
	assert.True(t, fired, "onCompleted on an already completed task must fire immediately")
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
