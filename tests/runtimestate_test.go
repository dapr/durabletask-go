package tests

import (
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/runtimestate"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Verifies runtime state created from an ExecutionStarted event
func Test_NewOrchestration(t *testing.T) {
	const iid = "abc"
	const expectedName = "myorchestration"
	createdAt := time.Now().UTC()

	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(createdAt),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: iid},
				Name:                  expectedName,
			},
		},
	}

	s := runtimestate.NewOrchestrationRuntimeState(iid, nil, []*protos.HistoryEvent{e})
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

func Test_CompletedOrchestration(t *testing.T) {
	const iid = "abc"
	const expectedName = "myorchestration"
	createdAt := time.Now().UTC()
	completedAt := createdAt.Add(10 * time.Second)

	events := []*protos.HistoryEvent{{
		EventId:   -1,
		Timestamp: timestamppb.New(createdAt),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: iid},
				Name:                  expectedName,
			},
		},
	}, {
		EventId:   -1,
		Timestamp: timestamppb.New(completedAt),
		EventType: &protos.HistoryEvent_ExecutionCompleted{
			ExecutionCompleted: &protos.ExecutionCompletedEvent{
				OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			},
		},
	}}

	s := runtimestate.NewOrchestrationRuntimeState(iid, nil, events)
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

func Test_CompletedSubOrchestration(t *testing.T) {
	expectedOutput := "\"done!\""
	expectedTaskID := int32(3)

	// TODO: Loop through different completion status values
	status := protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED

	s := runtimestate.NewOrchestrationRuntimeState("abc", nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Child",
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  "child_id",
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
					ParentInstance: &protos.ParentInstanceInfo{
						TaskScheduledId:       expectedTaskID,
						Name:                  wrapperspb.String("Parent"),
						OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: "parent_id"},
					},
				},
			},
		},
	})

	actions := []*protos.OrchestratorAction{
		{
			Id: expectedTaskID,
			OrchestratorActionType: &protos.OrchestratorAction_CompleteOrchestration{
				CompleteOrchestration: &protos.CompleteOrchestrationAction{
					OrchestrationStatus: status,
					Result:              wrapperspb.String(expectedOutput),
					CarryoverEvents:     []*protos.HistoryEvent{},
				},
			},
		},
	}

	continuedAsNew, err := runtimestate.ApplyActions(s, nil, actions, nil)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, s.NewEvents, 1) {
			e := s.NewEvents[0]
			assert.NotNil(t, e.Timestamp)
			if ec := e.GetExecutionCompleted(); assert.NotNil(t, ec) {
				assert.Equal(t, expectedTaskID, e.EventId)
				assert.Equal(t, status, ec.OrchestrationStatus)
				assert.Equal(t, expectedOutput, ec.Result.GetValue())
				assert.Nil(t, ec.FailureDetails)
			}
		}
		if assert.Len(t, s.PendingMessages, 1) {
			e := s.PendingMessages[0]
			assert.NotNil(t, e.HistoryEvent.Timestamp)
			if soc := e.HistoryEvent.GetSubOrchestrationInstanceCompleted(); assert.NotNil(t, soc) {
				assert.Equal(t, expectedTaskID, soc.TaskScheduledId)
				assert.Equal(t, expectedOutput, soc.Result.GetValue())
			}
		}
	}
}

func Test_RuntimeState_ContinueAsNew(t *testing.T) {
	iid := "abc"
	expectedName := "MyOrchestration"
	continueAsNewInput := "\"done!\""
	expectedTaskID := int32(3)
	eventName := "MyRaisedEvent"
	eventPayload := "MyEventPayload"

	state := runtimestate.NewOrchestrationRuntimeState(iid, nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: expectedName,
					OrchestrationInstance: &protos.OrchestrationInstance{
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
	actions := []*protos.OrchestratorAction{
		{
			Id: expectedTaskID,
			OrchestratorActionType: &protos.OrchestratorAction_CompleteOrchestration{
				CompleteOrchestration: &protos.CompleteOrchestrationAction{
					OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW,
					Result:              wrapperspb.String(continueAsNewInput),
					CarryoverEvents:     carryoverEvents,
				},
			},
		},
	}

	continuedAsNew, err := runtimestate.ApplyActions(state, nil, actions, nil)
	if assert.NoError(t, err) && assert.True(t, continuedAsNew) {
		if assert.Len(t, state.NewEvents, 3) {
			assert.NotNil(t, state.NewEvents[0].Timestamp)
			assert.NotNil(t, state.NewEvents[0].GetOrchestratorStarted())
			assert.NotNil(t, state.NewEvents[1].Timestamp)
			if ec := state.NewEvents[1].GetExecutionStarted(); assert.NotNil(t, ec) {
				assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING, runtimestate.RuntimeStatus(state))
				assert.Equal(t, state.InstanceId, ec.OrchestrationInstance.InstanceId)
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

	s := runtimestate.NewOrchestrationRuntimeState(iid, nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyOrchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  iid,
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	})

	var actions []*protos.OrchestratorAction
	timerCount := 3
	for i := 1; i <= timerCount; i++ {
		actions = append(actions, &protos.OrchestratorAction{
			Id: int32(i),
			OrchestratorActionType: &protos.OrchestratorAction_CreateTimer{
				CreateTimer: &protos.CreateTimerAction{
					FireAt: timestamppb.New(expectedFireAt),
					Name:   &timerName,
				},
			},
		})

	}

	continuedAsNew, err := runtimestate.ApplyActions(s, nil, actions, nil)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, s.NewEvents, timerCount) {
			for _, e := range s.NewEvents {
				assert.NotNil(t, e.Timestamp)
				if timerCreated := e.GetTimerCreated(); assert.NotNil(t, timerCreated) {
					assert.WithinDuration(t, expectedFireAt, timerCreated.FireAt.AsTime(), 0)
					assert.Equal(t, timerName, timerCreated.GetName())
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

func Test_ScheduleTask(t *testing.T) {
	const iid = "abc"
	expectedTaskID := int32(1)
	expectedName := "MyActivity"
	expectedInput := "{\"Foo\":5}"

	state := runtimestate.NewOrchestrationRuntimeState(iid, nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyOrchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  iid,
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
					Input: wrapperspb.String(expectedInput),
				},
			},
		},
	})

	actions := []*protos.OrchestratorAction{
		{
			Id: expectedTaskID,
			OrchestratorActionType: &protos.OrchestratorAction_ScheduleTask{
				ScheduleTask: &protos.ScheduleTaskAction{Name: expectedName, Input: wrapperspb.String(expectedInput)},
			},
		},
	}

	tc := &protos.TraceContext{TraceParent: "trace", TraceState: wrapperspb.String("state")}
	continuedAsNew, err := runtimestate.ApplyActions(state, nil, actions, tc)
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

func Test_CreateSubOrchestration(t *testing.T) {
	iid := "abc"
	expectedTaskID := int32(4)
	expectedInstanceID := "xyz"
	expectedName := "MySubOrchestration"
	expectedInput := wrapperspb.String("{\"Foo\":5}")
	expectedTraceParent := "trace"
	expectedTraceState := "trace_state"

	state := runtimestate.NewOrchestrationRuntimeState(iid, nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "Parent",
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  iid,
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	})

	actions := []*protos.OrchestratorAction{
		{
			Id: expectedTaskID,
			OrchestratorActionType: &protos.OrchestratorAction_CreateSubOrchestration{
				CreateSubOrchestration: &protos.CreateSubOrchestrationAction{
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
	continuedAsNew, err := runtimestate.ApplyActions(state, nil, actions, tc)
	if assert.NoError(t, err) && assert.False(t, continuedAsNew) {
		if assert.Len(t, state.NewEvents, 1) {
			e := state.NewEvents[0]
			if orchCreated := e.GetSubOrchestrationInstanceCreated(); assert.NotNil(t, orchCreated) {
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
				assert.Equal(t, expectedInstanceID, executionStarted.OrchestrationInstance.InstanceId)
				assert.NotEmpty(t, executionStarted.OrchestrationInstance.ExecutionId)
				assert.Equal(t, expectedName, executionStarted.Name)
				assert.Equal(t, expectedInput.GetValue(), executionStarted.Input.GetValue())
				if assert.NotNil(t, executionStarted.ParentInstance) {
					assert.Equal(t, "Parent", executionStarted.ParentInstance.Name.GetValue())
					assert.Equal(t, expectedTaskID, executionStarted.ParentInstance.TaskScheduledId)
					if assert.NotNil(t, executionStarted.ParentInstance.OrchestrationInstance) {
						assert.Equal(t, iid, executionStarted.ParentInstance.OrchestrationInstance.InstanceId)
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

	s := runtimestate.NewOrchestrationRuntimeState("abc", nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyOrchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  "abc",
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
					Input: wrapperspb.String(expectedInput),
				},
			},
		},
	})

	actions := []*protos.OrchestratorAction{
		{
			Id: -1,
			OrchestratorActionType: &protos.OrchestratorAction_SendEvent{
				SendEvent: &protos.SendEventAction{
					Instance: &protos.OrchestrationInstance{InstanceId: expectedInstanceID},
					Name:     expectedEventName,
					Data:     wrapperspb.String(expectedInput),
				},
			},
		},
	}

	continuedAsNew, err := runtimestate.ApplyActions(s, nil, actions, nil)
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
	s := runtimestate.NewOrchestrationRuntimeState("abc", nil, []*protos.HistoryEvent{})
	assert.True(t, runtimestate.IsValid(s))
	s = runtimestate.NewOrchestrationRuntimeState("abc", nil, []*protos.HistoryEvent{
		{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "MyOrchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  "abc",
						ExecutionId: wrapperspb.String(uuid.New().String()),
					},
				},
			},
		},
	})
	assert.True(t, runtimestate.IsValid(s))
	s = runtimestate.NewOrchestrationRuntimeState("abc", nil, []*protos.HistoryEvent{
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
	s := runtimestate.NewOrchestrationRuntimeState("abc", nil, []*protos.HistoryEvent{})
	err := runtimestate.AddEvent(s, &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name: "MyOrchestration",
				OrchestrationInstance: &protos.OrchestrationInstance{
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
					Name: "MyOrchestration",
					OrchestrationInstance: &protos.OrchestrationInstance{
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

	// TODO: Add other types of duplicate events (task completion, external events, sub-orchestration, etc.)
	err = runtimestate.AddEvent(s, &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionCompleted{
			ExecutionCompleted: &protos.ExecutionCompletedEvent{
				OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			},
		},
	})
	if assert.NoError(t, err) {
		err = runtimestate.AddEvent(s, &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_ExecutionCompleted{
				ExecutionCompleted: &protos.ExecutionCompletedEvent{
					OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
				},
			},
		})
		assert.ErrorIs(t, err, runtimestate.ErrDuplicateEvent)
	} else {
		return
	}
}
