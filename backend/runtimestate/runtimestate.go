package runtimestate

import (
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
)

var ErrDuplicateEvent = errors.New("duplicate event")

func NewOrchestrationRuntimeState(instanceID string, customStatus *wrapperspb.StringValue, existingHistory []*protos.HistoryEvent) *protos.OrchestrationRuntimeState {
	s := &protos.OrchestrationRuntimeState{
		InstanceId:   instanceID,
		OldEvents:    make([]*protos.HistoryEvent, 0, len(existingHistory)),
		NewEvents:    make([]*protos.HistoryEvent, 0, 10),
		CustomStatus: customStatus,
	}

	for _, e := range existingHistory {
		addEvent(s, e, false)
	}

	return s
}

// AddEvent appends a new history event to the orchestration history
func AddEvent(s *protos.OrchestrationRuntimeState, e *protos.HistoryEvent) error {
	return addEvent(s, e, true)
}

func addEvent(s *protos.OrchestrationRuntimeState, e *protos.HistoryEvent, isNew bool) error {
	if startEvent := e.GetExecutionStarted(); startEvent != nil {
		if s.StartEvent != nil {
			return ErrDuplicateEvent
		}
		s.StartEvent = startEvent
		s.CreatedTime = timestamppb.New(e.Timestamp.AsTime())
	} else if completedEvent := e.GetExecutionCompleted(); completedEvent != nil {
		if s.CompletedEvent != nil {
			return ErrDuplicateEvent
		}
		s.CompletedEvent = completedEvent
		s.CompletedTime = timestamppb.New(e.Timestamp.AsTime())
	} else if e.GetExecutionSuspended() != nil {
		s.IsSuspended = true
	} else if e.GetExecutionResumed() != nil {
		s.IsSuspended = false
	} else if e.GetExecutionStalled() != nil {
		s.Stalled = &protos.RuntimeStateStalled{
			Reason:      e.GetExecutionStalled().Reason,
			Description: e.GetExecutionStalled().Description,
		}
	} else {
		// TODO: Check for other possible duplicates using task IDs
	}

	if isNew {
		s.NewEvents = append(s.NewEvents, e)
	} else {
		s.OldEvents = append(s.OldEvents, e)
	}

	s.LastUpdatedTime = timestamppb.New(e.Timestamp.AsTime())
	return nil
}

func IsValid(s *protos.OrchestrationRuntimeState) bool {
	if len(s.OldEvents) == 0 && len(s.NewEvents) == 0 {
		// empty orchestration state
		return true
	} else if s.StartEvent != nil {
		// orchestration history has a start event
		return true
	}
	return false
}

func Name(s *protos.OrchestrationRuntimeState) (string, error) {
	if s.StartEvent == nil {
		return "", api.ErrNotStarted
	}

	return s.StartEvent.Name, nil
}

func Input(s *protos.OrchestrationRuntimeState) (*wrapperspb.StringValue, error) {
	if s.StartEvent == nil {
		return nil, api.ErrNotStarted
	}

	// REVIEW: Should we distinguish between no input and the empty string?
	return s.StartEvent.Input, nil
}

func Output(s *protos.OrchestrationRuntimeState) (*wrapperspb.StringValue, error) {
	if s.CompletedEvent == nil {
		return nil, api.ErrNotCompleted
	}

	// REVIEW: Should we distinguish between no output and the empty string?
	return s.CompletedEvent.Result, nil
}

func RuntimeStatus(s *protos.OrchestrationRuntimeState) protos.OrchestrationStatus {
	if s.StartEvent == nil {
		return protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	} else if s.CompletedEvent != nil {
		return s.CompletedEvent.GetOrchestrationStatus()
	} else if s.Stalled != nil {
		return protos.OrchestrationStatus_ORCHESTRATION_STATUS_STALLED
	} else if s.IsSuspended {
		return protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED
	}

	return protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING
}

func CreatedTime(s *protos.OrchestrationRuntimeState) (time.Time, error) {
	if s.StartEvent == nil {
		return time.Time{}, api.ErrNotStarted
	}

	return s.CreatedTime.AsTime(), nil
}

func LastUpdatedTime(s *protos.OrchestrationRuntimeState) (time.Time, error) {
	if s.StartEvent == nil {
		return time.Time{}, api.ErrNotStarted
	}

	return s.LastUpdatedTime.AsTime(), nil
}

func CompletedTime(s *protos.OrchestrationRuntimeState) (time.Time, error) {
	if s.CompletedEvent == nil {
		return time.Time{}, api.ErrNotCompleted
	}

	return s.CompletedTime.AsTime(), nil
}

func FailureDetails(s *protos.OrchestrationRuntimeState) (*protos.TaskFailureDetails, error) {
	if s.CompletedEvent == nil {
		return nil, api.ErrNotCompleted
	} else if s.CompletedEvent.FailureDetails == nil {
		return nil, api.ErrNoFailures
	}

	return s.CompletedEvent.FailureDetails, nil
}

// useful for abruptly stopping any execution of an orchestration from the backend
func CancelPending(s *protos.OrchestrationRuntimeState) {
	s.NewEvents = []*protos.HistoryEvent{}
	s.PendingMessages = []*protos.OrchestrationRuntimeStateMessage{}
	s.PendingTasks = []*protos.HistoryEvent{}
	s.PendingTimers = []*protos.HistoryEvent{}
}

func String(s *protos.OrchestrationRuntimeState) string {
	return fmt.Sprintf("%v:%v", s.InstanceId, helpers.ToRuntimeStatusString(RuntimeStatus(s)))
}

func GetStartedTime(s *protos.OrchestrationRuntimeState) time.Time {
	var startTime time.Time
	if len(s.OldEvents) > 0 {
		startTime = s.OldEvents[0].Timestamp.AsTime()
	} else if len(s.NewEvents) > 0 {
		startTime = s.NewEvents[0].Timestamp.AsTime()
	}
	return startTime
}

func IsCompleted(s *protos.OrchestrationRuntimeState) bool {
	return s.CompletedEvent != nil
}
