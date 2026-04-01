// This file contains tests for the task executor, which is used only for orchestrations authored in Go.
package tests

import (
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
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
	r.AddOrchestratorN("Orchestration", func(ctx *task.OrchestrationContext) (any, error) {
		var value int
		ctx.WaitForSingleEvent("MyEvent", timerDuration).Await(&value)
		return value, nil
	})

	iid := api.InstanceID("abc123")
	startEvent := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_OrchestratorStarted{
			OrchestratorStarted: &protos.OrchestratorStartedEvent{},
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
					Name: "Orchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	}

	// Execute the orchestrator function and expect to get back a single timer action
	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteOrchestrator(ctx, iid, oldEvents, newEvents)
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
	r.AddOrchestratorN("Orchestration", func(ctx *task.OrchestrationContext) (any, error) {
		var value int
		ctx.WaitForSingleEvent("MyEvent", -1).Await(&value)
		return value, nil
	})

	iid := api.InstanceID("abc123")
	newEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_OrchestratorStarted{
				OrchestratorStarted: &protos.OrchestratorStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Orchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	}

	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteOrchestrator(ctx, iid, []*protos.HistoryEvent{}, newEvents)
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
	r.AddOrchestratorN("Orchestration", func(ctx *task.OrchestrationContext) (any, error) {
		return nil, ctx.CreateTimer(timerDuration).Await(nil)
	})

	iid := api.InstanceID("abc123")
	newEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_OrchestratorStarted{
				OrchestratorStarted: &protos.OrchestratorStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Orchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  string(iid),
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	}

	executor := task.NewTaskExecutor(r)
	results, err := executor.ExecuteOrchestrator(ctx, iid, []*protos.HistoryEvent{}, newEvents)
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
	r.AddOrchestratorN("Orchestration", func(ctx *task.OrchestrationContext) (any, error) {
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
			EventType: &protos.HistoryEvent_OrchestratorStarted{
				OrchestratorStarted: &protos.OrchestratorStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(startTime),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Orchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
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
			EventType: &protos.HistoryEvent_OrchestratorStarted{
				OrchestratorStarted: &protos.OrchestratorStartedEvent{},
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
	results, err := executor.ExecuteOrchestrator(ctx, iid, oldEvents, newEvents)
	require.NoError(t, err)
	require.Equal(t, 1, len(results.Actions), "Expected a single completion action")

	completeAction := results.Actions[0].GetCompleteOrchestration()
	require.NotNil(t, completeAction, "Expected a CompleteOrchestration action")
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, completeAction.OrchestrationStatus)
	require.NotNil(t, completeAction.FailureDetails)
	require.Contains(t, completeAction.FailureDetails.ErrorMessage, "the task was canceled")
}

// This is a regression test for an issue where suspended orchestrations would continue to return
// actions prior to being resumed. In this case, the `WaitForSingleEvent` action would continue
// return a timer action even after the orchestration was suspended, which is not correct.
// The correct behavior is that a suspended orchestration should not return any actions.
func Test_Executor_SuspendStopsAllActions(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddOrchestratorN("SuspendResumeOrchestration", func(ctx *task.OrchestrationContext) (any, error) {
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
			EventType: &protos.HistoryEvent_OrchestratorStarted{
				OrchestratorStarted: &protos.OrchestratorStartedEvent{},
			},
		},
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "SuspendResumeOrchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
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

	// Execute the orchestrator function and expect to get back no actions
	results, err := executor.ExecuteOrchestrator(ctx, iid, oldEvents, newEvents)
	require.NoError(t, err)
	require.Empty(t, results.Actions, "Suspended orchestrations should not have any actions")
}
