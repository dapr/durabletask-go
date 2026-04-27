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

package runtimestate

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/ptr"
)

type Applier struct {
	appID string
}

func NewApplier(appID string) *Applier {
	return &Applier{
		appID: appID,
	}
}

// Actions takes a set of actions and updates its internal state, including populating the outbox.
func (a *Applier) Actions(s *protos.WorkflowRuntimeState, customStatus *wrapperspb.StringValue, actions []*protos.WorkflowAction, currentTraceContext *protos.TraceContext) (bool, error) {
	s.CustomStatus = customStatus
	s.Stalled = nil

	for _, action := range actions {
		if action.Router == nil {
			action.Router = &protos.TaskRouter{
				SourceAppID: a.appID,
			}
		} else {
			action.Router.SourceAppID = a.appID
		}

		if completedAction := action.GetCompleteWorkflow(); completedAction != nil {
			if completedAction.WorkflowStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
				newState := NewWorkflowRuntimeState(s.InstanceId, customStatus, []*protos.HistoryEvent{})
				newState.ContinuedAsNew = true
				_ = AddEvent(newState, &protos.HistoryEvent{
					EventId:   -1,
					Timestamp: timestamppb.Now(),
					EventType: &protos.HistoryEvent_WorkflowStarted{
						WorkflowStarted: &protos.WorkflowStartedEvent{},
					},
					Router: action.Router,
				})

				// Duplicate the start event info, updating just the input
				_ = AddEvent(newState,
					&protos.HistoryEvent{
						EventId:   -1,
						Timestamp: timestamppb.New(time.Now()),
						EventType: &protos.HistoryEvent_ExecutionStarted{
							ExecutionStarted: &protos.ExecutionStartedEvent{
								Name:           s.StartEvent.Name,
								ParentInstance: s.StartEvent.ParentInstance,
								Input:          completedAction.Result,
								WorkflowInstance: &protos.WorkflowInstance{
									InstanceId:  s.InstanceId,
									ExecutionId: wrapperspb.String(uuid.New().String()),
								},
								ParentTraceContext: s.StartEvent.ParentTraceContext,
							},
						},
						Router: action.Router,
					},
				)

				// Unprocessed "carryover" events
				for _, e := range completedAction.CarryoverEvents {
					_ = AddEvent(newState, e)
				}

				// Overwrite the current state object with a new one
				*s = *newState

				// ignore all remaining actions
				return true, nil
			} else {
				AddEvent(s, &protos.HistoryEvent{
					EventId:   action.Id,
					Timestamp: timestamppb.Now(),
					EventType: &protos.HistoryEvent_ExecutionCompleted{
						ExecutionCompleted: &protos.ExecutionCompletedEvent{
							WorkflowStatus: completedAction.WorkflowStatus,
							Result:         completedAction.Result,
							FailureDetails: completedAction.FailureDetails,
						},
					},
					Router: action.Router,
				})
				if parentInstance := s.StartEvent.GetParentInstance(); parentInstance != nil {
					var completionRouter *protos.TaskRouter

					if parentInstance.AppID != nil {
						completionRouter = &protos.TaskRouter{
							SourceAppID: action.Router.GetSourceAppID(),
							TargetAppID: ptr.Of(parentInstance.GetAppID()),
						}
					} else {
						completionRouter = action.Router
					}

					msg := &protos.WorkflowRuntimeStateMessage{
						HistoryEvent: &protos.HistoryEvent{
							EventId:   -1,
							Timestamp: timestamppb.Now(),
							Router:    completionRouter,
						},
						TargetInstanceId: s.StartEvent.GetParentInstance().WorkflowInstance.InstanceId,
					}
					if completedAction.WorkflowStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
						msg.HistoryEvent.EventType = &protos.HistoryEvent_ChildWorkflowInstanceCompleted{
							ChildWorkflowInstanceCompleted: &protos.ChildWorkflowInstanceCompletedEvent{
								TaskScheduledId: s.StartEvent.ParentInstance.TaskScheduledId,
								Result:          completedAction.Result,
							},
						}
					} else {
						// TODO: What is the expected result for termination?
						msg.HistoryEvent.EventType = &protos.HistoryEvent_ChildWorkflowInstanceFailed{
							ChildWorkflowInstanceFailed: &protos.ChildWorkflowInstanceFailedEvent{
								TaskScheduledId: s.StartEvent.ParentInstance.TaskScheduledId,
								FailureDetails:  completedAction.FailureDetails,
							},
						}
					}
					s.PendingMessages = append(s.PendingMessages, msg)
				}
			}
		} else if timerAction := action.GetCreateTimer(); timerAction != nil {
			timerCreated := &protos.TimerCreatedEvent{
				FireAt: timerAction.FireAt,
				Name:   timerAction.Name,
			}
			switch o := timerAction.GetOrigin().(type) {
			case *protos.CreateTimerAction_CreateTimer:
				timerCreated.Origin = &protos.TimerCreatedEvent_CreateTimer{CreateTimer: o.CreateTimer}
			case *protos.CreateTimerAction_ExternalEvent:
				timerCreated.Origin = &protos.TimerCreatedEvent_ExternalEvent{ExternalEvent: o.ExternalEvent}
			case *protos.CreateTimerAction_ActivityRetry:
				timerCreated.Origin = &protos.TimerCreatedEvent_ActivityRetry{ActivityRetry: o.ActivityRetry}
			case *protos.CreateTimerAction_ChildWorkflowRetry:
				timerCreated.Origin = &protos.TimerCreatedEvent_ChildWorkflowRetry{ChildWorkflowRetry: o.ChildWorkflowRetry}
			default:
				// Origin is nil or an unrecognized type; timerCreated.Origin stays nil.
			}
			_ = AddEvent(s, &protos.HistoryEvent{
				EventId:   action.Id,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_TimerCreated{
					TimerCreated: timerCreated,
				},
				Router: action.Router,
			})
			// TODO cant pass trace context
			s.PendingTimers = append(s.PendingTimers, &protos.HistoryEvent{
				EventId:   -1,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_TimerFired{
					TimerFired: &protos.TimerFiredEvent{
						TimerId: action.Id,
						FireAt:  timerAction.FireAt,
					},
				},
			})
		} else if scheduleTask := action.GetScheduleTask(); scheduleTask != nil {
			scheduledEvent := &protos.HistoryEvent{
				EventId:   action.Id,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_TaskScheduled{
					TaskScheduled: &protos.TaskScheduledEvent{
						Name:               scheduleTask.Name,
						TaskExecutionId:    scheduleTask.TaskExecutionId,
						Version:            scheduleTask.Version,
						Input:              scheduleTask.Input,
						ParentTraceContext: currentTraceContext,
						InProcess:         scheduleTask.GetInProcess(),
					},
				},
				Router: action.Router,
			}
			_ = AddEvent(s, scheduledEvent)
			s.PendingTasks = append(s.PendingTasks, scheduledEvent)
		} else if createSO := action.GetCreateChildWorkflow(); createSO != nil {
			// Autogenerate an instance ID for the child workflow if none is provided, using a
			// deterministic algorithm based on the parent instance ID to help enable de-duplication.
			if createSO.InstanceId == "" {
				createSO.InstanceId = helpers.GenerateChildWorkflowInstanceID(s.InstanceId, action.Id)
			}
			_ = AddEvent(s, &protos.HistoryEvent{
				EventId:   action.Id,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{
					ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{
						Name:               createSO.Name,
						Version:            createSO.Version,
						Input:              createSO.Input,
						InstanceId:         createSO.InstanceId,
						ParentTraceContext: currentTraceContext,
					},
				},
				Router: action.Router,
			})
			startEvent := &protos.HistoryEvent{
				EventId:   -1,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_ExecutionStarted{
					ExecutionStarted: &protos.ExecutionStartedEvent{
						Name: createSO.Name,
						ParentInstance: &protos.ParentInstanceInfo{
							TaskScheduledId:  action.Id,
							Name:             wrapperspb.String(s.StartEvent.Name),
							WorkflowInstance: &protos.WorkflowInstance{InstanceId: string(s.InstanceId)},
							AppID:            ptr.Of(action.Router.GetSourceAppID()),
						},
						Input: createSO.Input,
						WorkflowInstance: &protos.WorkflowInstance{
							InstanceId:  createSO.InstanceId,
							ExecutionId: wrapperspb.String(uuid.New().String()),
						},
						ParentTraceContext: currentTraceContext,
						InProcess:         createSO.GetInProcess(),
					},
				},
				Router: action.Router,
			}

			s.PendingMessages = append(s.PendingMessages, &protos.WorkflowRuntimeStateMessage{HistoryEvent: startEvent, TargetInstanceId: createSO.InstanceId})
		} else if sendEvent := action.GetSendEvent(); sendEvent != nil {
			e := &protos.HistoryEvent{
				EventId:   action.Id,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_EventSent{
					EventSent: &protos.EventSentEvent{
						InstanceId: sendEvent.Instance.InstanceId,
						Name:       sendEvent.Name,
						Input:      sendEvent.Data,
					},
				},
				Router: action.Router,
			}
			_ = AddEvent(s, e)
			s.PendingMessages = append(s.PendingMessages, &protos.WorkflowRuntimeStateMessage{HistoryEvent: e, TargetInstanceId: sendEvent.Instance.InstanceId})
		} else if terminate := action.GetTerminateWorkflow(); terminate != nil {
			// Send a message to terminate the target workflow
			msg := &protos.WorkflowRuntimeStateMessage{
				TargetInstanceId: terminate.InstanceId,
				HistoryEvent: &protos.HistoryEvent{
					EventId:   -1,
					Timestamp: timestamppb.Now(),
					EventType: &protos.HistoryEvent_ExecutionTerminated{
						ExecutionTerminated: &protos.ExecutionTerminatedEvent{
							Input:   terminate.Reason,
							Recurse: terminate.Recurse,
						},
					},
					Router: action.Router,
				},
			}
			s.PendingMessages = append(s.PendingMessages, msg)
		} else if versionNotAvailable := action.GetWorkflowVersionNotAvailable(); versionNotAvailable != nil {
			versionName := ""
			for _, e := range s.OldEvents {
				if es := e.GetWorkflowStarted(); es != nil {
					versionName = es.GetVersion().GetName()
					break
				}
			}

			msg := &protos.WorkflowRuntimeStateMessage{
				HistoryEvent: &protos.HistoryEvent{
					EventId:   -1,
					Timestamp: timestamppb.Now(),
					EventType: &protos.HistoryEvent_ExecutionStalled{
						ExecutionStalled: &protos.ExecutionStalledEvent{
							Reason:      protos.StalledReason_VERSION_NOT_AVAILABLE,
							Description: ptr.Of(fmt.Sprintf("Version not available: %s", versionName)),
						},
					},
					Router: action.Router,
				},
			}
			s.PendingMessages = append(s.PendingMessages, msg)
		} else {
			return false, fmt.Errorf("unknown action type: %v", action)
		}
	}

	return false, nil
}
