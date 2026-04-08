package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/runtimestate"
	"github.com/dapr/durabletask-go/task"
)

// Verifies runtime state created from an ExecutionStarted event
func Test_NewWorkflow(t *testing.T) {
	const iid = "abc"
	const expectedName = "myworkflow"
	createdAt := time.Now().UTC()

	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(createdAt),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				WorkflowInstance: &protos.WorkflowInstance{InstanceId: iid},
				Name:             expectedName,
			},
		},
	}

	s := runtimestate.NewWorkflowRuntimeState(iid, nil, []*protos.HistoryEvent{e})
	assert.Equal(t, api.InstanceID(iid), api.InstanceID(s.InstanceId))

	actualName, err := runtimestate.Name(s)
	if assert.NoError(t, err) {
		assert.Equal(t, expectedName, actualName)
	}

	actualTime, err := runtimestate.CreatedTime(s)
	if assert.NoError(t, err) {
		assert.WithinDuration(t, createdAt, actualTime, 0)
	}

	_, err = runtimestate.CompletedTime(s)
	if assert.Error(t, err) {
		assert.Equal(t, api.ErrNotCompleted, err)
	}

	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING, runtimestate.RuntimeStatus(s))

	oldEvents := s.OldEvents
	if assert.Equal(t, 1, len(oldEvents)) {
		assert.Equal(t, e, oldEvents[0])
	}

	assert.Equal(t, 0, len(s.NewEvents))
}

func Test_CompletedWorkflow(t *testing.T) {
	const iid = "abc"
	const expectedName = "myworkflow"
	createdAt := time.Now().UTC()
	completedAt := createdAt.Add(10 * time.Second)

	events := []*protos.HistoryEvent{{
		EventId:   -1,
		Timestamp: timestamppb.New(createdAt),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				WorkflowInstance: &protos.WorkflowInstance{InstanceId: iid},
				Name:             expectedName,
			},
		},
	}, {
		EventId:   -1,
		Timestamp: timestamppb.New(completedAt),
		EventType: &protos.HistoryEvent_ExecutionCompleted{
			ExecutionCompleted: &protos.ExecutionCompletedEvent{
				WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			},
		},
	}}

	s := runtimestate.NewWorkflowRuntimeState(iid, nil, events)
	assert.Equal(t, api.InstanceID(iid), api.InstanceID(s.InstanceId))

	actualName, err := runtimestate.Name(s)
	if assert.NoError(t, err) {
		assert.Equal(t, expectedName, actualName)
	}

	actualCreatedTime, err := runtimestate.CreatedTime(s)
	if assert.NoError(t, err) {
		assert.WithinDuration(t, createdAt, actualCreatedTime, 0)
	}

	actualCompletedTime, err := runtimestate.CompletedTime(s)
	if assert.NoError(t, err) {
		assert.WithinDuration(t, completedAt, actualCompletedTime, 0)
	}

	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, runtimestate.RuntimeStatus(s))

	assert.Equal(t, events, s.OldEvents)
	assert.Equal(t, 0, len(s.NewEvents))
}

func Test_CompletedChildWorkflow(t *testing.T) {
	expectedOutput := "\"done!\""
	expectedTaskID := int32(3)

	// TODO: Loop through different completion status values
	status := protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED

	s := runtimestate.NewWorkflowRuntimeState("abc", nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Child",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  "child_id",
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
					ParentInstance: &protos.ParentInstanceInfo{
						TaskScheduledId:  expectedTaskID,
						Name:             wrapperspb.String("Parent"),
						WorkflowInstance: &protos.WorkflowInstance{InstanceId: "parent_id"},
					},
				},
			},
		},
	})

	actions := []*protos.WorkflowAction{
		{
			Id: expectedTaskID,
			WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{
				CompleteWorkflow: &protos.CompleteWorkflowAction{
					WorkflowStatus:  status,
					Result:          wrapperspb.String(expectedOutput),
					CarryoverEvents: []*protos.HistoryEvent{},
				},
			},
		},
	}

	applier := runtimestate.NewApplier("example")
	continuedAsNew, err := applier.Actions(s, nil, actions, nil)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, s.NewEvents, 1) {
			e := s.NewEvents[0]
			assert.NotNil(t, e.Timestamp)
			if ec := e.GetExecutionCompleted(); assert.NotNil(t, ec) {
				assert.Equal(t, expectedTaskID, e.EventId)
				assert.Equal(t, status, ec.WorkflowStatus)
				assert.Equal(t, expectedOutput, ec.Result.GetValue())
				assert.Nil(t, ec.FailureDetails)
			}
		}
		if assert.Len(t, s.PendingMessages, 1) {
			e := s.PendingMessages[0]
			assert.NotNil(t, e.HistoryEvent.Timestamp)
			if soc := e.HistoryEvent.GetChildWorkflowInstanceCompleted(); assert.NotNil(t, soc) {
				assert.Equal(t, expectedTaskID, soc.TaskScheduledId)
				assert.Equal(t, expectedOutput, soc.Result.GetValue())
			}
		}
	}
}

func Test_RuntimeState_ContinueAsNew(t *testing.T) {
	iid := "abc"
	expectedName := "MyWorkflow"
	continueAsNewInput := "\"done!\""
	expectedTaskID := int32(3)
	eventName := "MyRaisedEvent"
	eventPayload := "MyEventPayload"

	state := runtimestate.NewWorkflowRuntimeState(iid, nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: expectedName,
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  iid,
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	})

	carryoverEvents := []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_EventRaised{
				EventRaised: &protos.EventRaisedEvent{Name: eventName, Input: wrapperspb.String(eventPayload)},
			},
		},
	}
	actions := []*protos.WorkflowAction{
		{
			Id: expectedTaskID,
			WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{
				CompleteWorkflow: &protos.CompleteWorkflowAction{
					WorkflowStatus:  protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
					Result:          wrapperspb.String(continueAsNewInput),
					CarryoverEvents: carryoverEvents,
				},
			},
		},
	}

	applier := runtimestate.NewApplier("example")
	continuedAsNew, err := applier.Actions(state, nil, actions, nil)
	if assert.NoError(t, err) && assert.True(t, continuedAsNew) {
		if assert.Len(t, state.NewEvents, 3) {
			assert.NotNil(t, state.NewEvents[0].Timestamp)
			assert.NotNil(t, state.NewEvents[0].GetWorkflowStarted())
			assert.NotNil(t, state.NewEvents[1].Timestamp)
			if ec := state.NewEvents[1].GetExecutionStarted(); assert.NotNil(t, ec) {
				assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING, runtimestate.RuntimeStatus(state))
				assert.Equal(t, state.InstanceId, ec.WorkflowInstance.InstanceId)
				if name, err := runtimestate.Name(state); assert.NoError(t, err) {
					assert.Equal(t, expectedName, name)
					assert.Equal(t, expectedName, ec.Name)
				}
				if input, err := runtimestate.Input(state); assert.NoError(t, err) {
					assert.Equal(t, continueAsNewInput, input.GetValue())
				}
			}
			assert.NotNil(t, state.NewEvents[2].Timestamp)
			if er := state.NewEvents[2].GetEventRaised(); assert.NotNil(t, er) {
				assert.Equal(t, eventName, er.Name)
				assert.Equal(t, eventPayload, er.Input.GetValue())
			}
		}
		assert.Empty(t, state.PendingMessages)
		assert.Empty(t, state.PendingTasks)
		assert.Empty(t, state.PendingTimers)
	}
}

func Test_CreateTimer(t *testing.T) {
	const iid = "abc"
	timerName := "foo"
	expectedFireAt := time.Now().UTC().Add(72 * time.Hour)

	s := runtimestate.NewWorkflowRuntimeState(iid, nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyWorkflow",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  iid,
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	})

	var actions []*protos.WorkflowAction
	timerCount := 3
	for i := 1; i <= timerCount; i++ {
		actions = append(actions, &protos.WorkflowAction{
			Id: int32(i),
			WorkflowActionType: &protos.WorkflowAction_CreateTimer{
				CreateTimer: &protos.CreateTimerAction{
					FireAt: timestamppb.New(expectedFireAt),
					Name:   &timerName,
					Origin: &protos.CreateTimerAction_CreateTimer{
						CreateTimer: &protos.TimerOriginCreateTimer{},
					},
				},
			},
		})
	}

	applier := runtimestate.NewApplier("example")
	continuedAsNew, err := applier.Actions(s, nil, actions, nil)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, s.NewEvents, timerCount) {
			for _, e := range s.NewEvents {
				assert.NotNil(t, e.Timestamp)
				if timerCreated := e.GetTimerCreated(); assert.NotNil(t, timerCreated) {
					assert.WithinDuration(t, expectedFireAt, timerCreated.FireAt.AsTime(), 0)
					assert.Equal(t, timerName, timerCreated.GetName())
					assert.NotNil(t, timerCreated.GetCreateTimer(), "expected TimerCreatedEvent to carry CreateTimer origin")
				}
			}
		}
		if assert.Len(t, s.PendingTimers, timerCount) {
			for i, e := range s.PendingTimers {
				assert.NotNil(t, e.Timestamp)
				if timerFired := e.GetTimerFired(); assert.NotNil(t, timerFired) {
					expectedTimerID := int32(i + 1)
					assert.WithinDuration(t, expectedFireAt, timerFired.FireAt.AsTime(), 0)
					assert.Equal(t, expectedTimerID, timerFired.TimerId)
				}
			}
		}
	}
}

func Test_CreateTimer_ExternalEventOrigin(t *testing.T) {
	const iid = "abc"
	eventName := "myEvent"
	expectedFireAt := time.Now().UTC().Add(30 * time.Minute)

	s := runtimestate.NewWorkflowRuntimeState(iid, nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyOrchestration",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  iid,
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	})

	actions := []*protos.WorkflowAction{
		{
			Id: 1,
			WorkflowActionType: &protos.WorkflowAction_CreateTimer{
				CreateTimer: &protos.CreateTimerAction{
					FireAt: timestamppb.New(expectedFireAt),
					Name:   &eventName,
					Origin: &protos.CreateTimerAction_ExternalEvent{
						ExternalEvent: &protos.TimerOriginExternalEvent{
							Name: eventName,
						},
					},
				},
			},
		},
	}

	applier := runtimestate.NewApplier("example")
	continuedAsNew, err := applier.Actions(s, nil, actions, nil)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, s.NewEvents, 1) {
			timerCreated := s.NewEvents[0].GetTimerCreated()
			if assert.NotNil(t, timerCreated) {
				assert.WithinDuration(t, expectedFireAt, timerCreated.FireAt.AsTime(), 0)
				assert.Equal(t, eventName, timerCreated.GetName())
				externalEvent := timerCreated.GetExternalEvent()
				if assert.NotNil(t, externalEvent, "expected TimerCreatedEvent to carry ExternalEvent origin") {
					assert.Equal(t, eventName, externalEvent.Name)
				}
			}
		}
	}
}

func Test_CreateTimer_ActivityRetryOrigin(t *testing.T) {
	const iid = "abc"
	timerName := "myActivity-retry"
	taskExecutionId := "task-exec-123"
	expectedFireAt := time.Now().UTC().Add(10 * time.Second)

	s := runtimestate.NewWorkflowRuntimeState(iid, nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyOrchestration",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  iid,
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	})

	actions := []*protos.WorkflowAction{
		{
			Id: 1,
			WorkflowActionType: &protos.WorkflowAction_CreateTimer{
				CreateTimer: &protos.CreateTimerAction{
					FireAt: timestamppb.New(expectedFireAt),
					Name:   &timerName,
					Origin: &protos.CreateTimerAction_ActivityRetry{
						ActivityRetry: &protos.TimerOriginActivityRetry{
							TaskExecutionId: taskExecutionId,
						},
					},
				},
			},
		},
	}

	applier := runtimestate.NewApplier("example")
	continuedAsNew, err := applier.Actions(s, nil, actions, nil)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, s.NewEvents, 1) {
			timerCreated := s.NewEvents[0].GetTimerCreated()
			if assert.NotNil(t, timerCreated) {
				assert.WithinDuration(t, expectedFireAt, timerCreated.FireAt.AsTime(), 0)
				assert.Equal(t, timerName, timerCreated.GetName())
				activityRetry := timerCreated.GetActivityRetry()
				if assert.NotNil(t, activityRetry, "expected TimerCreatedEvent to carry ActivityRetry origin") {
					assert.Equal(t, taskExecutionId, activityRetry.TaskExecutionId)
				}
			}
		}
	}
}

func Test_CreateTimer_ChildWorkflowRetryOrigin(t *testing.T) {
	const iid = "abc"
	timerName := "myWorkflow-retry"
	childInstanceId := "child-instance-456"
	expectedFireAt := time.Now().UTC().Add(30 * time.Second)

	s := runtimestate.NewWorkflowRuntimeState(iid, nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyOrchestration",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  iid,
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	})

	actions := []*protos.WorkflowAction{
		{
			Id: 1,
			WorkflowActionType: &protos.WorkflowAction_CreateTimer{
				CreateTimer: &protos.CreateTimerAction{
					FireAt: timestamppb.New(expectedFireAt),
					Name:   &timerName,
					Origin: &protos.CreateTimerAction_ChildWorkflowRetry{
						ChildWorkflowRetry: &protos.TimerOriginChildWorkflowRetry{
							InstanceId: childInstanceId,
						},
					},
				},
			},
		},
	}

	applier := runtimestate.NewApplier("example")
	continuedAsNew, err := applier.Actions(s, nil, actions, nil)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, s.NewEvents, 1) {
			timerCreated := s.NewEvents[0].GetTimerCreated()
			if assert.NotNil(t, timerCreated) {
				assert.WithinDuration(t, expectedFireAt, timerCreated.FireAt.AsTime(), 0)
				assert.Equal(t, timerName, timerCreated.GetName())
				childWorkflowRetry := timerCreated.GetChildWorkflowRetry()
				if assert.NotNil(t, childWorkflowRetry, "expected TimerCreatedEvent to carry ChildWorkflowRetry origin") {
					assert.Equal(t, childInstanceId, childWorkflowRetry.InstanceId)
				}
			}
		}
	}
}

func Test_ChildWorkflowRetry_TimerOriginPointsToFirstChild(t *testing.T) {
	const parentID = "parent-instance"

	// Register a parent workflow that calls a child with retry policy (no explicit instance ID).
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Parent", func(ctx *task.WorkflowContext) (any, error) {
		err := ctx.CallChildWorkflow("Child", task.WithChildWorkflowRetryPolicy(&task.RetryPolicy{
			MaxAttempts:          4,
			InitialRetryInterval: 1 * time.Second,
		})).Await(nil)
		return nil, err
	})
	r.AddWorkflowN("Child", func(ctx *task.WorkflowContext) (any, error) {
		return nil, errors.New("child failed")
	})

	executor := task.NewTaskExecutor(r)
	applier := runtimestate.NewApplier("test")

	startEvent := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name: "Parent",
				WorkflowInstance: &protos.WorkflowInstance{
					InstanceId:  parentID,
					ExecutionId: wrapperspb.String(uuid.New().String()),
				},
			},
		},
	}

	// Round 1: Parent starts, produces CreateChildWorkflow action.
	resp, err := executor.ExecuteWorkflow(context.Background(), parentID, nil, []*protos.HistoryEvent{startEvent})
	require.NoError(t, err)
	require.Len(t, resp.Actions, 1)
	childAction := resp.Actions[0].GetCreateChildWorkflow()
	require.NotNil(t, childAction)

	// Apply round 1 to get the ChildWorkflowInstanceCreated event.
	state := runtimestate.NewWorkflowRuntimeState(parentID, nil, []*protos.HistoryEvent{startEvent})
	_, err = applier.Actions(state, nil, resp.Actions, nil)
	require.NoError(t, err)

	// The first child's instance ID (auto-generated by the applier).
	firstChildInstanceID := childAction.InstanceId

	// Simulate child failure (TaskScheduledId must match the CreateChildWorkflow action ID).
	childFailedEvent := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ChildWorkflowInstanceFailed{
			ChildWorkflowInstanceFailed: &protos.ChildWorkflowInstanceFailedEvent{
				TaskScheduledId: 0,
				FailureDetails: &protos.TaskFailureDetails{
					ErrorMessage: "child failed",
				},
			},
		},
	}

	// Round 2: Parent replays, sees child failure, produces CreateTimer#1.
	oldEvents := append([]*protos.HistoryEvent{startEvent}, state.NewEvents...)
	resp, err = executor.ExecuteWorkflow(context.Background(), parentID, oldEvents, []*protos.HistoryEvent{childFailedEvent})
	require.NoError(t, err)
	require.Len(t, resp.Actions, 1)

	// Verify first retry timer has ChildWorkflowRetry origin pointing to the first child.
	timer1 := resp.Actions[0].GetCreateTimer()
	require.NotNil(t, timer1)
	retry1 := timer1.GetChildWorkflowRetry()
	if assert.NotNil(t, retry1, "first retry timer should have ChildWorkflowRetry origin") {
		assert.Equal(t, firstChildInstanceID, retry1.InstanceId,
			"first retry timer origin should point to the first child instance ID")
	}

	// Apply round 2 to get TimerCreated event.
	state2 := runtimestate.NewWorkflowRuntimeState(parentID, nil, oldEvents)
	_, err = applier.Actions(state2, nil, resp.Actions, nil)
	require.NoError(t, err)

	// Round 3: Timer fires, produces CreateChildWorkflow#2.
	timerFiredEvent := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_TimerFired{
			TimerFired: &protos.TimerFiredEvent{
				FireAt:  timer1.FireAt,
				TimerId: resp.Actions[0].Id,
			},
		},
	}
	oldEvents2 := make([]*protos.HistoryEvent, 0)
	oldEvents2 = append(oldEvents2, oldEvents...)
	oldEvents2 = append(oldEvents2, childFailedEvent)
	oldEvents2 = append(oldEvents2, state2.NewEvents...)
	resp, err = executor.ExecuteWorkflow(context.Background(), parentID, oldEvents2, []*protos.HistoryEvent{timerFiredEvent})
	require.NoError(t, err)
	require.Len(t, resp.Actions, 1)
	childAction2 := resp.Actions[0].GetCreateChildWorkflow()
	require.NotNil(t, childAction2)
	assert.NotEqual(t, firstChildInstanceID, childAction2.InstanceId,
		"each retry should get a different auto-generated instance ID")

	// Apply round 3 to get ChildWorkflowInstanceCreated event.
	state3 := runtimestate.NewWorkflowRuntimeState(parentID, nil, oldEvents2)
	_, err = applier.Actions(state3, nil, resp.Actions, nil)
	require.NoError(t, err)

	// Round 4: Second child fails, produces CreateTimer#3.
	childFailedEvent2 := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ChildWorkflowInstanceFailed{
			ChildWorkflowInstanceFailed: &protos.ChildWorkflowInstanceFailedEvent{
				TaskScheduledId: 2,
				FailureDetails: &protos.TaskFailureDetails{
					ErrorMessage: "child failed",
				},
			},
		},
	}
	oldEvents3 := make([]*protos.HistoryEvent, 0)
	oldEvents3 = append(oldEvents3, oldEvents2...)
	oldEvents3 = append(oldEvents3, timerFiredEvent)
	oldEvents3 = append(oldEvents3, state3.NewEvents...)
	resp, err = executor.ExecuteWorkflow(context.Background(), parentID, oldEvents3, []*protos.HistoryEvent{childFailedEvent2})
	require.NoError(t, err)
	require.Len(t, resp.Actions, 1)

	// Verify second retry timer also points to the FIRST child's instance ID.
	timer2 := resp.Actions[0].GetCreateTimer()
	require.NotNil(t, timer2)
	retry2 := timer2.GetChildWorkflowRetry()
	if assert.NotNil(t, retry2, "second retry timer should have ChildWorkflowRetry origin") {
		assert.Equal(t, firstChildInstanceID, retry2.InstanceId,
			"second retry timer origin should also point to the first child instance ID")
	}
}

func Test_ScheduleTask(t *testing.T) {
	const iid = "abc"
	expectedTaskID := int32(1)
	expectedName := "MyActivity"
	expectedInput := "{\"Foo\":5}"

	state := runtimestate.NewWorkflowRuntimeState(iid, nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyWorkflow",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  iid,
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
					Input: wrapperspb.String(expectedInput),
				},
			},
		},
	})

	actions := []*protos.WorkflowAction{
		{
			Id: expectedTaskID,
			WorkflowActionType: &protos.WorkflowAction_ScheduleTask{
				ScheduleTask: &protos.ScheduleTaskAction{Name: expectedName, Input: wrapperspb.String(expectedInput)},
			},
		},
	}

	tc := &protos.TraceContext{TraceParent: "trace", TraceState: wrapperspb.String("state")}
	applier := runtimestate.NewApplier("example")
	continuedAsNew, err := applier.Actions(state, nil, actions, tc)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, state.NewEvents, 1) {
			e := state.NewEvents[0]
			if taskScheduled := e.GetTaskScheduled(); assert.NotNil(t, taskScheduled) {
				assert.Equal(t, expectedTaskID, e.EventId)
				assert.Equal(t, expectedName, taskScheduled.Name)
				assert.Equal(t, expectedInput, taskScheduled.Input.GetValue())
				if assert.NotNil(t, taskScheduled.ParentTraceContext) {
					assert.Equal(t, "trace", taskScheduled.ParentTraceContext.TraceParent)
					assert.Equal(t, "state", taskScheduled.ParentTraceContext.TraceState.GetValue())
				}
			}
		}
		if assert.Len(t, state.PendingTasks, 1) {
			e := state.PendingTasks[0]
			if taskScheduled := e.GetTaskScheduled(); assert.NotNil(t, taskScheduled) {
				assert.Equal(t, expectedTaskID, e.EventId)
				assert.Equal(t, expectedName, taskScheduled.Name)
				assert.Equal(t, expectedInput, taskScheduled.Input.GetValue())
				if assert.NotNil(t, taskScheduled.ParentTraceContext) {
					assert.Equal(t, "trace", taskScheduled.ParentTraceContext.TraceParent)
					assert.Equal(t, "state", taskScheduled.ParentTraceContext.TraceState.GetValue())
				}
			}
		}
	}
}

func Test_CreateChildWorkflow(t *testing.T) {
	iid := "abc"
	expectedTaskID := int32(4)
	expectedInstanceID := "xyz"
	expectedName := "MyChildWorkflow"
	expectedInput := wrapperspb.String("{\"Foo\":5}")
	expectedTraceParent := "trace"
	expectedTraceState := "trace_state"

	state := runtimestate.NewWorkflowRuntimeState(iid, nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Parent",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  iid,
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	})

	actions := []*protos.WorkflowAction{
		{
			Id: expectedTaskID,
			WorkflowActionType: &protos.WorkflowAction_CreateChildWorkflow{
				CreateChildWorkflow: &protos.CreateChildWorkflowAction{
					Name:       expectedName,
					Input:      expectedInput,
					InstanceId: expectedInstanceID,
				},
			},
		},
	}

	tc := &protos.TraceContext{
		TraceParent: expectedTraceParent,
		TraceState:  wrapperspb.String(expectedTraceState),
	}
	applier := runtimestate.NewApplier("example")
	continuedAsNew, err := applier.Actions(state, nil, actions, tc)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, state.NewEvents, 1) {
			e := state.NewEvents[0]
			if orchCreated := e.GetChildWorkflowInstanceCreated(); assert.NotNil(t, orchCreated) {
				assert.Equal(t, expectedTaskID, e.EventId)
				assert.Equal(t, expectedInstanceID, orchCreated.InstanceId)
				assert.Equal(t, expectedName, orchCreated.Name)
				assert.Equal(t, expectedInput.GetValue(), orchCreated.Input.GetValue())
				if assert.NotNil(t, orchCreated.ParentTraceContext) {
					assert.Equal(t, expectedTraceParent, orchCreated.ParentTraceContext.TraceParent)
					assert.Equal(t, expectedTraceState, orchCreated.ParentTraceContext.TraceState.GetValue())
				}
			}
		}
		if assert.Len(t, state.PendingMessages, 1) {
			msg := state.PendingMessages[0]
			if executionStarted := msg.HistoryEvent.GetExecutionStarted(); assert.NotNil(t, executionStarted) {
				assert.Equal(t, int32(-1), msg.HistoryEvent.EventId)
				assert.Equal(t, expectedInstanceID, executionStarted.WorkflowInstance.InstanceId)
				assert.NotEmpty(t, executionStarted.WorkflowInstance.ExecutionId)
				assert.Equal(t, expectedName, executionStarted.Name)
				assert.Equal(t, expectedInput.GetValue(), executionStarted.Input.GetValue())
				if assert.NotNil(t, executionStarted.ParentInstance) {
					assert.Equal(t, "Parent", executionStarted.ParentInstance.Name.GetValue())
					assert.Equal(t, expectedTaskID, executionStarted.ParentInstance.TaskScheduledId)
					if assert.NotNil(t, executionStarted.ParentInstance.WorkflowInstance) {
						assert.Equal(t, iid, executionStarted.ParentInstance.WorkflowInstance.InstanceId)
					}
				}
				if assert.NotNil(t, executionStarted.ParentTraceContext) {
					assert.Equal(t, expectedTraceParent, executionStarted.ParentTraceContext.TraceParent)
					assert.Equal(t, expectedTraceState, executionStarted.ParentTraceContext.TraceState.GetValue())
				}
			}
		}
	}
}

func Test_SendEvent(t *testing.T) {
	expectedInstanceID := "xyz"
	expectedEventName := "MyEvent"
	expectedInput := "foo"

	s := runtimestate.NewWorkflowRuntimeState("abc", nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyWorkflow",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  "abc",
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
					Input: wrapperspb.String(expectedInput),
				},
			},
		},
	})

	actions := []*protos.WorkflowAction{
		{
			Id: -1,
			WorkflowActionType: &protos.WorkflowAction_SendEvent{
				SendEvent: &protos.SendEventAction{
					Instance: &protos.WorkflowInstance{InstanceId: expectedInstanceID},
					Name:     expectedEventName,
					Data:     wrapperspb.String(expectedInput),
				},
			},
		},
	}

	applier := runtimestate.NewApplier("example")
	continuedAsNew, err := applier.Actions(s, nil, actions, nil)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, s.NewEvents, 1) {
			e := s.NewEvents[0]
			if sendEvent := e.GetEventSent(); assert.NotNil(t, sendEvent) {
				assert.Equal(t, expectedEventName, sendEvent.Name)
				assert.Equal(t, expectedInput, sendEvent.Input.GetValue())
				assert.Equal(t, expectedInstanceID, sendEvent.InstanceId)
			}
		}
		if assert.Len(t, s.PendingMessages, 1) {
			msg := s.PendingMessages[0]
			if sendEvent := msg.HistoryEvent.GetEventSent(); assert.NotNil(t, sendEvent) {
				assert.Equal(t, expectedEventName, sendEvent.Name)
				assert.Equal(t, expectedInput, sendEvent.Input.GetValue())
				assert.Equal(t, expectedInstanceID, sendEvent.InstanceId)
			}
		}
	}
}

func Test_StateIsValid(t *testing.T) {
	s := runtimestate.NewWorkflowRuntimeState("abc", nil, []*protos.HistoryEvent{})
	assert.True(t, runtimestate.IsValid(s))
	s = runtimestate.NewWorkflowRuntimeState("abc", nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyWorkflow",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  "abc",
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	})
	assert.True(t, runtimestate.IsValid(s))
	s = runtimestate.NewWorkflowRuntimeState("abc", nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_TaskCompleted{
				TaskCompleted: &protos.TaskCompletedEvent{
					TaskScheduledId: 1,
				},
			},
		},
	})
	assert.False(t, runtimestate.IsValid(s))
}

func Test_DuplicateEvents(t *testing.T) {
	s := runtimestate.NewWorkflowRuntimeState("abc", nil, []*protos.HistoryEvent{})
	err := runtimestate.AddEvent(s, &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name: "MyWorkflow",
				WorkflowInstance: &protos.WorkflowInstance{
					InstanceId:  "abc",
					ExecutionId: wrapperspb.String(uuid.New().String()),
				},
			},
		},
	})
	if assert.NoError(t, err) {
		err = runtimestate.AddEvent(s, &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyWorkflow",
					WorkflowInstance: &protos.WorkflowInstance{
						InstanceId:  "abc",
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		})
		assert.ErrorIs(t, err, runtimestate.ErrDuplicateEvent)
	} else {
		return
	}

	// TODO: Add other types of duplicate events (task completion, external events, child workflow, etc.)
	err = runtimestate.AddEvent(s, &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionCompleted{
			ExecutionCompleted: &protos.ExecutionCompletedEvent{
				WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			},
		},
	})
	if assert.NoError(t, err) {
		err = runtimestate.AddEvent(s, &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_ExecutionCompleted{
				ExecutionCompleted: &protos.ExecutionCompletedEvent{
					WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
				},
			},
		})
		assert.ErrorIs(t, err, runtimestate.ErrDuplicateEvent)
	} else {
		return
	}
}
