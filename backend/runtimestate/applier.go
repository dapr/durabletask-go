package runtimestate

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

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
func (a *Applier) Actions(s *protos.OrchestrationRuntimeState, customStatus *wrapperspb.StringValue, actions []*protos.OrchestratorAction, currentTraceContext *protos.TraceContext) (bool, error) {
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

		if completedAction := action.GetCompleteOrchestration(); completedAction != nil {
			if completedAction.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
				newState := NewOrchestrationRuntimeState(s.InstanceId, customStatus, []*protos.HistoryEvent{})
				newState.ContinuedAsNew = true
				_ = AddEvent(newState, &protos.HistoryEvent{
					EventId:   -1,
					Timestamp: timestamppb.Now(),
					EventType: &protos.HistoryEvent_OrchestratorStarted{
						OrchestratorStarted: &protos.OrchestratorStartedEvent{},
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
								OrchestrationInstance: &protos.OrchestrationInstance{
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
							OrchestrationStatus: completedAction.OrchestrationStatus,
							Result:              completedAction.Result,
							FailureDetails:      completedAction.FailureDetails,
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

					msg := &protos.OrchestrationRuntimeStateMessage{
						HistoryEvent: &protos.HistoryEvent{
							EventId:   -1,
							Timestamp: timestamppb.Now(),
							Router:    completionRouter,
						},
						TargetInstanceID: s.StartEvent.GetParentInstance().OrchestrationInstance.InstanceId,
					}
					if completedAction.OrchestrationStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
						msg.HistoryEvent.EventType = &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
							SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
								TaskScheduledId: s.StartEvent.ParentInstance.TaskScheduledId,
								Result:          completedAction.Result,
							},
						}
					} else {
						// TODO: What is the expected result for termination?
						msg.HistoryEvent.EventType = &protos.HistoryEvent_SubOrchestrationInstanceFailed{
							SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{
								TaskScheduledId: s.StartEvent.ParentInstance.TaskScheduledId,
								FailureDetails:  completedAction.FailureDetails,
							},
						}
					}
					s.PendingMessages = append(s.PendingMessages, msg)
				}
			}
		} else if createtimer := action.GetCreateTimer(); createtimer != nil {
			_ = AddEvent(s, &protos.HistoryEvent{
				EventId:   action.Id,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_TimerCreated{
					TimerCreated: &protos.TimerCreatedEvent{
						FireAt: createtimer.FireAt,
						Name:   createtimer.Name,
					},
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
						FireAt:  createtimer.FireAt,
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
					},
				},
				Router: action.Router,
			}
			_ = AddEvent(s, scheduledEvent)
			s.PendingTasks = append(s.PendingTasks, scheduledEvent)
		} else if createSO := action.GetCreateSubOrchestration(); createSO != nil {
			// Autogenerate an instance ID for the sub-orchestration if none is provided, using a
			// deterministic algorithm based on the parent instance ID to help enable de-duplication.
			if createSO.InstanceId == "" {
				createSO.InstanceId = fmt.Sprintf("%s:%04x", s.InstanceId, action.Id)
			}
			_ = AddEvent(s, &protos.HistoryEvent{
				EventId:   action.Id,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_SubOrchestrationInstanceCreated{
					SubOrchestrationInstanceCreated: &protos.SubOrchestrationInstanceCreatedEvent{
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
							TaskScheduledId:       action.Id,
							Name:                  wrapperspb.String(s.StartEvent.Name),
							OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: string(s.InstanceId)},
							AppID:                 ptr.Of(action.Router.GetSourceAppID()),
						},
						Input: createSO.Input,
						OrchestrationInstance: &protos.OrchestrationInstance{
							InstanceId:  createSO.InstanceId,
							ExecutionId: wrapperspb.String(uuid.New().String()),
						},
						ParentTraceContext: currentTraceContext,
					},
				},
				Router: action.Router,
			}

			s.PendingMessages = append(s.PendingMessages, &protos.OrchestrationRuntimeStateMessage{HistoryEvent: startEvent, TargetInstanceID: createSO.InstanceId})
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
			s.PendingMessages = append(s.PendingMessages, &protos.OrchestrationRuntimeStateMessage{HistoryEvent: e, TargetInstanceID: sendEvent.Instance.InstanceId})
		} else if terminate := action.GetTerminateOrchestration(); terminate != nil {
			// Send a message to terminate the target orchestration
			msg := &protos.OrchestrationRuntimeStateMessage{
				TargetInstanceID: terminate.InstanceId,
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
		} else if versionNotAvailable := action.GetOrchestratorVersionNotAvailable(); versionNotAvailable != nil {
			versionName := ""
			for _, e := range s.OldEvents {
				if es := e.GetOrchestratorStarted(); es != nil {
					versionName = es.GetVersion().GetName()
					break
				}
			}

			msg := &protos.OrchestrationRuntimeStateMessage{
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
