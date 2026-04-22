// This file contains tests for the task executor, which is used only for workflows authored in Go.
package tests

import (
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/task"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Verifies that the WaitForSingleEvent API implicitly creates a timer when the timeout is non-zero.
func Test_Executor_WaitForEventSchedulesTimer(t *testing.T) {
	timerDuration := 5 * time.Second
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Workflow", func(ctx *task.WorkflowContext) (any, error) {
		var value int
		ctx.WaitForSingleEvent("MyEvent", timerDuration).Await(&value)
		return value, nil
	})

	iid := api.InstanceID("abc123")
	startEvent := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_WorkflowStarted{
			WorkflowStarted: &protos.WorkflowStartedEvent{},
		},
	}
	oldEvents := []*protos.HistoryEvent{}
	newEvents := []*protos.HistoryEvent{
		startEvent,
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Workflow",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	}

	// Execute the workflow function and expect to get back a single timer action
	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteWorkflow(ctx, iid, oldEvents, newEvents, backend.ExecuteOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, len(results.Actions), "Expected a single action to be scheduled")
	createTimerAction := results.Actions[0].GetCreateTimer()
	require.NotNil(t, createTimerAction, "Expected the scheduled action to be a timer")
	require.WithinDuration(t, startEvent.Timestamp.AsTime().Add(timerDuration), createTimerAction.FireAt.AsTime(), 0)
	require.Equal(t, "MyEvent", createTimerAction.GetName())
	require.NotNil(t, createTimerAction.GetExternalEvent())
	require.Equal(t, "MyEvent", createTimerAction.GetExternalEvent().GetName())
}

// Verifies that the WaitForSingleEvent API always creates a timer even when no timeout is specified (indefinite wait),
// with a far-future fireAt and the ExternalEvent origin set.
func Test_Executor_WaitForEventWithoutTimeout_CreatesInfiniteTimer(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Orchestration", func(ctx *task.WorkflowContext) (any, error) {
		var value int
		ctx.WaitForSingleEvent("MyEvent", -1).Await(&value)
		return value, nil
	})

	iid := api.InstanceID("abc123")
	newEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Orchestration",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	}

	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteWorkflow(ctx, iid, []*protos.HistoryEvent{}, newEvents, backend.ExecuteOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, len(results.Actions), "Expected a timer to be created even for indefinite waits")
	createTimerAction := results.Actions[0].GetCreateTimer()
	require.NotNil(t, createTimerAction, "Expected the action to be a timer")
	expectedInfiniteFireAt := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
	require.Equal(t, expectedInfiniteFireAt, createTimerAction.FireAt.AsTime())
	require.NotNil(t, createTimerAction.GetExternalEvent())
	require.Equal(t, "MyEvent", createTimerAction.GetExternalEvent().GetName())
}

// Verifies that the CreateTimer API sets the CreateTimer origin on the resulting action.
func Test_Executor_CreateTimer_SetsCreateTimerOrigin(t *testing.T) {
	timerDuration := 5 * time.Second
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Orchestration", func(ctx *task.WorkflowContext) (any, error) {
		return nil, ctx.CreateTimer(timerDuration).Await(nil)
	})

	iid := api.InstanceID("abc123")
	newEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Orchestration",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	}

	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteWorkflow(ctx, iid, []*protos.HistoryEvent{}, newEvents, backend.ExecuteOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, len(results.Actions), "Expected a single action to be scheduled")
	createTimerAction := results.Actions[0].GetCreateTimer()
	require.NotNil(t, createTimerAction, "Expected the scheduled action to be a timer")
	require.NotNil(t, createTimerAction.GetCreateTimer(), "Expected the timer action to carry CreateTimer origin")
}

// Verifies that when a timer fires before the external event arrives, the WaitForSingleEvent task
// is cancelled and the orchestration completes with a failure (ErrTaskCanceled).
func Test_Executor_WaitForEvent_TimerFiresCancelsTask(t *testing.T) {
	timerDuration := 5 * time.Second
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Orchestration", func(ctx *task.WorkflowContext) (any, error) {
		var value int
		if err := ctx.WaitForSingleEvent("MyEvent", timerDuration).Await(&value); err != nil {
			return nil, err
		}
		return value, nil
	})

	iid := api.InstanceID("abc123")
	executionID := uuid.New().String()
	startTime := time.Now()

	// oldEvents: replay of the first execution where the timer was created
	oldEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Orchestration",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(executionID),
					},
				},
			},
		},
		{
			EventId:   0,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_TimerCreated{
				TimerCreated: &protos.TimerCreatedEvent{
					FireAt: timestamppb.New(startTime.Add(timerDuration)),
				},
			},
		},
	}

	// newEvents: the timer fires (no external event arrived)
	newEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime.Add(timerDuration)),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime.Add(timerDuration)),
			EventType: &protos.HistoryEvent_TimerFired{
				TimerFired: &protos.TimerFiredEvent{
					TimerId: 0,
					FireAt:  timestamppb.New(startTime.Add(timerDuration)),
				},
			},
		},
	}

	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteWorkflow(ctx, iid, oldEvents, newEvents, backend.ExecuteOptions{})
	require.NoError(t, err)
	require.Equal(t, 1, len(results.Actions), "Expected a single completion action")

	completeAction := results.Actions[0].GetCompleteWorkflow()
	require.NotNil(t, completeAction, "Expected a CompleteOrchestration action")
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, completeAction.WorkflowStatus)
	require.NotNil(t, completeAction.FailureDetails)
	require.Contains(t, completeAction.FailureDetails.ErrorMessage, "the task was canceled")
}

// This is a regression test for an issue where suspended workflows would continue to return
// actions prior to being resumed. In this case, the `WaitForSingleEvent` action would continue
// return a timer action even after the workflow was suspended, which is not correct.
// The correct behavior is that a suspended workflow should not return any actions.
func Test_Executor_SuspendStopsAllActions(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SuspendResumeWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		var value int
		ctx.WaitForSingleEvent("MyEvent", 5*time.Second).Await(&value)
		return value, nil
	})

	executor := task.NewTaskExecutor(r)
	iid := api.InstanceID("abc123")
	oldEvents := []*protos.HistoryEvent{}
	newEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "SuspendResumeWorkflow",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionSuspended{
				ExecutionSuspended: &protos.ExecutionSuspendedEvent{},
			},
		},
	}

	// Execute the workflow function and expect to get back no actions
	results, err := executor.ExecuteWorkflow(ctx, iid, oldEvents, newEvents, backend.ExecuteOptions{})
	require.NoError(t, err)
	require.Empty(t, results.Actions, "Suspended workflows should not have any actions")
}

// Regression test: replaying a history produced before WaitForSingleEvent
// started emitting a synthetic CreateTimer action for negative timeouts
// must still be deterministic. In pre-patch histories, an indefinite
// WaitForSingleEvent left no TimerCreated event behind, so subsequent actions
// (e.g. CallActivity) carry the lower sequence numbers. The SDK must drop the
// optional pending timer on replay and shift later pending ids down so
// TaskScheduled at EventId=0 continues to match the activity.
func Test_Executor_Replay_PrePatchIndefiniteWaitForEvent(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddActivityN("MyActivity", func(ctx task.ActivityContext) (any, error) {
		return "ok", nil
	})
	r.AddWorkflowN("Orchestration", func(ctx *task.WorkflowContext) (any, error) {
		var v int
		if err := ctx.WaitForSingleEvent("MyEvent", -1).Await(&v); err != nil {
			return nil, err
		}
		var out string
		if err := ctx.CallActivity("MyActivity").Await(&out); err != nil {
			return nil, err
		}
		return out, nil
	})

	iid := api.InstanceID("abc123")
	executionID := uuid.New().String()
	startTime := time.Now()

	// oldEvents simulate a history produced by a prior release: the indefinite
	// WaitForSingleEvent consumed no sequence number, so the activity's
	// TaskScheduled was written with EventId=0.
	oldEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Orchestration",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(executionID),
					},
				},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_EventRaised{
				EventRaised: &protos.EventRaisedEvent{
					Name:  "MyEvent",
					Input: wrapperspb.String("42"),
				},
			},
		},
		{
			EventId:   0,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_TaskScheduled{
				TaskScheduled: &protos.TaskScheduledEvent{
					Name: "MyActivity",
				},
			},
		},
	}

	// newEvents: the activity completes with its result.
	newEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_TaskCompleted{
				TaskCompleted: &protos.TaskCompletedEvent{
					TaskScheduledId: 0,
					Result:          wrapperspb.String(`"ok"`),
				},
			},
		},
	}

	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteWorkflow(ctx, iid, oldEvents, newEvents, backend.ExecuteOptions{})
	require.NoError(t, err, "Replay of pre-patch history must not fail the determinism check")
	require.Equal(t, 1, len(results.Actions), "Expected a single CompleteWorkflow action after replay")

	complete := results.Actions[0].GetCompleteWorkflow()
	require.NotNil(t, complete, "Expected CompleteWorkflow action")
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, complete.WorkflowStatus)
	require.NotNil(t, complete.Result)
	require.Equal(t, `"ok"`, complete.Result.Value)

	// No optional CreateTimer should leak through: the replay must consume
	// the optional timer entirely rather than emit it to the backend.
	for _, a := range results.Actions {
		require.Nil(t, a.GetCreateTimer(), "Optional CreateTimer must not be emitted on replay of a pre-patch history")
	}
}

// Regression test: when replaying a post-patch history where the indefinite
// WaitForSingleEvent already emitted a TimerCreated(origin=ExternalEvent,
// fireAt=9999) event, the SDK must match it normally and must NOT drop any
// pending action. This guards against a too-aggressive shift that would also
// trigger for histories produced by the current runtime.
func Test_Executor_Replay_PostPatchIndefiniteWaitForEvent(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Orchestration", func(ctx *task.WorkflowContext) (any, error) {
		var v int
		if err := ctx.WaitForSingleEvent("MyEvent", -1).Await(&v); err != nil {
			return nil, err
		}
		return v, nil
	})

	iid := api.InstanceID("abc123")
	executionID := uuid.New().String()
	startTime := time.Now()
	infiniteFireAt := time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)

	// oldEvents simulate a history produced by the current runtime: the
	// indefinite WaitForSingleEvent emitted its optional timer at EventId=0.
	oldEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Orchestration",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(executionID),
					},
				},
			},
		},
		{
			EventId:   0,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_TimerCreated{
				TimerCreated: &protos.TimerCreatedEvent{
					FireAt: timestamppb.New(infiniteFireAt),
					Origin: &protos.TimerCreatedEvent_ExternalEvent{
						ExternalEvent: &protos.TimerOriginExternalEvent{Name: "MyEvent"},
					},
				},
			},
		},
	}

	// newEvents: the external event finally arrives.
	newEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_EventRaised{
				EventRaised: &protos.EventRaisedEvent{
					Name:  "MyEvent",
					Input: wrapperspb.String("42"),
				},
			},
		},
	}

	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteWorkflow(ctx, iid, oldEvents, newEvents, backend.ExecuteOptions{})
	require.NoError(t, err, "Post-patch replay must succeed")
	require.Equal(t, 1, len(results.Actions), "Expected a single CompleteWorkflow action")

	complete := results.Actions[0].GetCompleteWorkflow()
	require.NotNil(t, complete)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, complete.WorkflowStatus)
	require.Equal(t, `42`, complete.Result.Value)
}

// Regression test: pre-patch history where an indefinite WaitForSingleEvent is
// followed by CallChildWorkflow. Exercises the onChildWorkflowScheduled shift
// path (distinct from onTaskScheduled).
func Test_Executor_Replay_PrePatchIndefiniteWaitForEvent_ChildWorkflow(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Child", func(ctx *task.WorkflowContext) (any, error) {
		return "child-result", nil
	})
	r.AddWorkflowN("Parent", func(ctx *task.WorkflowContext) (any, error) {
		var v int
		if err := ctx.WaitForSingleEvent("MyEvent", -1).Await(&v); err != nil {
			return nil, err
		}
		var out string
		if err := ctx.CallChildWorkflow("Child").Await(&out); err != nil {
			return nil, err
		}
		return out, nil
	})

	iid := api.InstanceID("parent-1")
	executionID := uuid.New().String()
	startTime := time.Now()

	oldEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Parent",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(executionID),
					},
				},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_EventRaised{
				EventRaised: &protos.EventRaisedEvent{
					Name:  "MyEvent",
					Input: wrapperspb.String("1"),
				},
			},
		},
		{
			EventId:   0,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{
				ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{
					InstanceId: "child-1",
					Name:       "Child",
				},
			},
		},
	}

	newEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_ChildWorkflowInstanceCompleted{
				ChildWorkflowInstanceCompleted: &protos.ChildWorkflowInstanceCompletedEvent{
					TaskScheduledId: 0,
					Result:          wrapperspb.String(`"child-result"`),
				},
			},
		},
	}

	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteWorkflow(ctx, iid, oldEvents, newEvents, backend.ExecuteOptions{})
	require.NoError(t, err, "Replay of pre-patch history with a child workflow must not fail")
	require.Equal(t, 1, len(results.Actions), "Expected a single CompleteWorkflow action")

	complete := results.Actions[0].GetCompleteWorkflow()
	require.NotNil(t, complete)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, complete.WorkflowStatus)
	require.Equal(t, `"child-result"`, complete.Result.Value)
	for _, a := range results.Actions {
		require.Nil(t, a.GetCreateTimer(), "Optional CreateTimer must not be emitted")
	}
}

// Regression test: pre-patch history where an indefinite WaitForSingleEvent is
// followed by a user CreateTimer. Exercises the onTimerCreated shift path —
// specifically the "pending is optional, incoming real CreateTimer-origin" case
// where the incoming TimerCreated is NOT itself optional so the shift must
// trigger despite both being CreateTimer actions.
func Test_Executor_Replay_PrePatchIndefiniteWaitForEvent_RealTimer(t *testing.T) {
	timerDuration := 5 * time.Second
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Orchestration", func(ctx *task.WorkflowContext) (any, error) {
		var v int
		if err := ctx.WaitForSingleEvent("MyEvent", -1).Await(&v); err != nil {
			return nil, err
		}
		if err := ctx.CreateTimer(timerDuration).Await(nil); err != nil {
			return nil, err
		}
		return v, nil
	})

	iid := api.InstanceID("abc123")
	executionID := uuid.New().String()
	startTime := time.Now()

	oldEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Orchestration",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(executionID),
					},
				},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_EventRaised{
				EventRaised: &protos.EventRaisedEvent{
					Name:  "MyEvent",
					Input: wrapperspb.String("7"),
				},
			},
		},
		{
			EventId:   0,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_TimerCreated{
				TimerCreated: &protos.TimerCreatedEvent{
					FireAt: timestamppb.New(startTime.Add(timerDuration)),
					Origin: &protos.TimerCreatedEvent_CreateTimer{
						CreateTimer: &protos.TimerOriginCreateTimer{},
					},
				},
			},
		},
	}

	newEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime.Add(timerDuration)),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime.Add(timerDuration)),
			EventType: &protos.HistoryEvent_TimerFired{
				TimerFired: &protos.TimerFiredEvent{
					TimerId: 0,
					FireAt:  timestamppb.New(startTime.Add(timerDuration)),
				},
			},
		},
	}

	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteWorkflow(ctx, iid, oldEvents, newEvents, backend.ExecuteOptions{})
	require.NoError(t, err, "Replay of pre-patch history with a real timer must not fail")
	require.Equal(t, 1, len(results.Actions), "Expected a single CompleteWorkflow action")

	complete := results.Actions[0].GetCompleteWorkflow()
	require.NotNil(t, complete)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, complete.WorkflowStatus)
	require.Equal(t, `7`, complete.Result.Value)
	for _, a := range results.Actions {
		require.Nil(t, a.GetCreateTimer(), "Optional CreateTimer must not be emitted")
	}
}

// Regression test: two consecutive indefinite WaitForSingleEvent calls each
// followed by a CallActivity. In a pre-patch history both activities carry
// sequence numbers 0 and 1. The runtime must drop two separate optional
// timers and shift the pending map on each occurrence.
func Test_Executor_Replay_PrePatchIndefiniteWaitForEvent_Multiple(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddActivityN("ActA", func(ctx task.ActivityContext) (any, error) {
		return "a", nil
	})
	r.AddActivityN("ActB", func(ctx task.ActivityContext) (any, error) {
		return "b", nil
	})
	r.AddWorkflowN("Orchestration", func(ctx *task.WorkflowContext) (any, error) {
		var v int
		if err := ctx.WaitForSingleEvent("EventA", -1).Await(&v); err != nil {
			return nil, err
		}
		var outA string
		if err := ctx.CallActivity("ActA").Await(&outA); err != nil {
			return nil, err
		}
		if err := ctx.WaitForSingleEvent("EventB", -1).Await(&v); err != nil {
			return nil, err
		}
		var outB string
		if err := ctx.CallActivity("ActB").Await(&outB); err != nil {
			return nil, err
		}
		return outA + outB, nil
	})

	iid := api.InstanceID("abc123")
	executionID := uuid.New().String()
	startTime := time.Now()

	oldEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Orchestration",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(executionID),
					},
				},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_EventRaised{
				EventRaised: &protos.EventRaisedEvent{Name: "EventA", Input: wrapperspb.String("1")},
			},
		},
		{
			EventId:   0,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_TaskScheduled{
				TaskScheduled: &protos.TaskScheduledEvent{Name: "ActA"},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_TaskCompleted{
				TaskCompleted: &protos.TaskCompletedEvent{
					TaskScheduledId: 0,
					Result:          wrapperspb.String(`"a"`),
				},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_EventRaised{
				EventRaised: &protos.EventRaisedEvent{Name: "EventB", Input: wrapperspb.String("2")},
			},
		},
		{
			EventId:   1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_TaskScheduled{
				TaskScheduled: &protos.TaskScheduledEvent{Name: "ActB"},
			},
		},
	}

	// newEvents: second activity completes.
	newEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_WorkflowStarted{
				WorkflowStarted: &protos.WorkflowStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_TaskCompleted{
				TaskCompleted: &protos.TaskCompletedEvent{
					TaskScheduledId: 1,
					Result:          wrapperspb.String(`"b"`),
				},
			},
		},
	}

	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteWorkflow(ctx, iid, oldEvents, newEvents, backend.ExecuteOptions{})
	require.NoError(t, err, "Replay of pre-patch history with multiple indefinite waits must not fail")
	require.Equal(t, 1, len(results.Actions), "Expected a single CompleteWorkflow action")

	complete := results.Actions[0].GetCompleteWorkflow()
	require.NotNil(t, complete)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, complete.WorkflowStatus)
	require.Equal(t, `"ab"`, complete.Result.Value)
	for _, a := range results.Actions {
		require.Nil(t, a.GetCreateTimer(), "No optional CreateTimer must leak through")
	}
}
