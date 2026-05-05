package runtimestate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/runtimestate/dedup"
)

func startedEvent() *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name: "test-orchestrator",
			},
		},
	}
}

func stalledEvent(reason protos.StalledReason, description string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionStalled{
			ExecutionStalled: &protos.ExecutionStalledEvent{
				Reason:      reason,
				Description: proto.String(description),
			},
		},
	}
}

func TestAddEvent_StalledClearedOnNewEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		newEvent *protos.HistoryEvent
	}{
		{
			name: "stalled cleared by ExecutionSuspended",
			newEvent: &protos.HistoryEvent{
				EventId:   -1,
				Timestamp: timestamppb.Now(),
				EventType: &protos.HistoryEvent_ExecutionSuspended{
					ExecutionSuspended: &protos.ExecutionSuspendedEvent{},
				},
			},
		},
		{
			name: "stalled cleared by ExecutionResumed",
			newEvent: &protos.HistoryEvent{
				EventId:   -1,
				Timestamp: timestamppb.Now(),
				EventType: &protos.HistoryEvent_ExecutionResumed{
					ExecutionResumed: &protos.ExecutionResumedEvent{},
				},
			},
		},
		{
			name: "stalled cleared by TaskScheduled",
			newEvent: &protos.HistoryEvent{
				EventId:   -1,
				Timestamp: timestamppb.Now(),
				EventType: &protos.HistoryEvent_TaskScheduled{
					TaskScheduled: &protos.TaskScheduledEvent{
						Name: "test-activity",
					},
				},
			},
		},
		{
			name: "stalled cleared by ExecutionCompleted",
			newEvent: &protos.HistoryEvent{
				EventId:   -1,
				Timestamp: timestamppb.Now(),
				EventType: &protos.HistoryEvent_ExecutionCompleted{
					ExecutionCompleted: &protos.ExecutionCompletedEvent{
						OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
					},
				},
			},
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := NewOrchestrationRuntimeState("test-instance", nil, nil)

			// Add a start event so RuntimeStatus can reach the stalled check.
			require.NoError(t, AddEvent(s, startedEvent()))

			// Add the stalled event.
			require.NoError(t, AddEvent(s, stalledEvent(protos.StalledReason_PATCH_MISMATCH, "test stall")))
			require.NotNil(t, s.Stalled, "expected Stalled to be set after stalled event")
			assert.Equal(t, protos.StalledReason_PATCH_MISMATCH, s.Stalled.Reason)
			assert.Equal(t, "test stall", s.Stalled.GetDescription())
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_STALLED, RuntimeStatus(s))

			// Add another event which should clear the stalled state.
			require.NoError(t, AddEvent(s, tt.newEvent))
			assert.Nil(t, s.Stalled, "expected Stalled to be cleared after new event")
			assert.NotEqual(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_STALLED, RuntimeStatus(s))
		})
	}
}

func TestAddEvent_StalledSetFromOldEvents(t *testing.T) {
	t.Parallel()

	s := NewOrchestrationRuntimeState("test-instance", nil, []*protos.HistoryEvent{
		startedEvent(),
		stalledEvent(protos.StalledReason_VERSION_NOT_AVAILABLE, "old stall"),
	})
	require.NotNil(t, s.Stalled)
	assert.Equal(t, protos.StalledReason_VERSION_NOT_AVAILABLE, s.Stalled.Reason)
	assert.Equal(t, "old stall", s.Stalled.GetDescription())
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_STALLED, RuntimeStatus(s))
}

func TestAddEvent_StalledClearedBySubsequentOldEvent(t *testing.T) {
	t.Parallel()

	taskScheduled := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TaskScheduled{
			TaskScheduled: &protos.TaskScheduledEvent{
				Name: "test-activity",
			},
		},
	}

	// If the history contains a stalled event followed by another event,
	// the stalled state should be cleared.
	s := NewOrchestrationRuntimeState("test-instance", nil, []*protos.HistoryEvent{
		startedEvent(),
		stalledEvent(protos.StalledReason_PATCH_MISMATCH, "stalled"),
		taskScheduled,
	})
	assert.Nil(t, s.Stalled, "expected Stalled to be cleared by subsequent old event")
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING, RuntimeStatus(s))
}

func TestAddEvent_StalledPreservedOnDuplicateError(t *testing.T) {
	t.Parallel()

	s := NewOrchestrationRuntimeState("test-instance", nil, nil)
	require.NoError(t, AddEvent(s, startedEvent()))
	require.NoError(t, AddEvent(s, stalledEvent(protos.StalledReason_PATCH_MISMATCH, "stalled")))
	require.NotNil(t, s.Stalled)

	// A duplicate ExecutionStarted should return an error and NOT clear
	// the stalled state.
	err := AddEvent(s, startedEvent())
	require.ErrorIs(t, err, ErrDuplicateEvent)
	assert.NotNil(t, s.Stalled, "expected Stalled to be preserved on error")
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_STALLED, RuntimeStatus(s))
}

func TestAddEvent_StalledReplacedByNewStalled(t *testing.T) {
	t.Parallel()

	s := NewOrchestrationRuntimeState("test-instance", nil, nil)
	require.NoError(t, AddEvent(s, startedEvent()))

	require.NoError(t, AddEvent(s, stalledEvent(protos.StalledReason_PATCH_MISMATCH, "first stall")))
	require.NotNil(t, s.Stalled)
	assert.Equal(t, "first stall", s.Stalled.GetDescription())

	// A second stalled event should replace the existing stalled state with the new one.
	require.NoError(t, AddEvent(s, stalledEvent(protos.StalledReason_VERSION_NOT_AVAILABLE, "second stall")))
	require.NotNil(t, s.Stalled)
	assert.Equal(t, protos.StalledReason_VERSION_NOT_AVAILABLE, s.Stalled.Reason)
	assert.Equal(t, "second stall", s.Stalled.GetDescription())
}

func taskCompletedEvent(taskScheduledID int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TaskCompleted{
			TaskCompleted: &protos.TaskCompletedEvent{
				TaskScheduledId: taskScheduledID,
			},
		},
	}
}

func taskFailedEvent(taskScheduledID int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TaskFailed{
			TaskFailed: &protos.TaskFailedEvent{
				TaskScheduledId: taskScheduledID,
			},
		},
	}
}

func timerFiredEvent(timerID int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TimerFired{
			TimerFired: &protos.TimerFiredEvent{
				TimerId: timerID,
			},
		},
	}
}

func subOrchestrationCompletedEvent(taskScheduledID int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_SubOrchestrationInstanceCompleted{
			SubOrchestrationInstanceCompleted: &protos.SubOrchestrationInstanceCompletedEvent{
				TaskScheduledId: taskScheduledID,
			},
		},
	}
}

func subOrchestrationFailedEvent(taskScheduledID int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_SubOrchestrationInstanceFailed{
			SubOrchestrationInstanceFailed: &protos.SubOrchestrationInstanceFailedEvent{
				TaskScheduledId: taskScheduledID,
			},
		},
	}
}

func TestAddEvent_DuplicateTaskCompleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		first     *protos.HistoryEvent
		duplicate *protos.HistoryEvent
	}{
		{
			name:      "TaskCompleted then TaskCompleted",
			first:     taskCompletedEvent(1),
			duplicate: taskCompletedEvent(1),
		},
		{
			name:      "TaskCompleted then TaskFailed for same id",
			first:     taskCompletedEvent(2),
			duplicate: taskFailedEvent(2),
		},
		{
			name:      "TaskFailed then TaskCompleted for same id",
			first:     taskFailedEvent(3),
			duplicate: taskCompletedEvent(3),
		},
		{
			name:      "TimerFired then TimerFired",
			first:     timerFiredEvent(7),
			duplicate: timerFiredEvent(7),
		},
		{
			name:      "SubOrchestrationInstanceCompleted then SubOrchestrationInstanceCompleted",
			first:     subOrchestrationCompletedEvent(4),
			duplicate: subOrchestrationCompletedEvent(4),
		},
		{
			name:      "SubOrchestrationInstanceCompleted then SubOrchestrationInstanceFailed for same id",
			first:     subOrchestrationCompletedEvent(5),
			duplicate: subOrchestrationFailedEvent(5),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := NewOrchestrationRuntimeState("test-instance", nil, nil)
			require.NoError(t, AddEvent(s, startedEvent()))
			require.NoError(t, AddEvent(s, tt.first))

			before := len(s.NewEvents)
			err := AddEvent(s, tt.duplicate)
			require.ErrorIs(t, err, ErrDuplicateEvent)
			assert.Len(t, s.NewEvents, before, "duplicate event must not be appended")
		})
	}
}

func TestAddEvent_DistinctIDsAndKindsAreNotDuplicates(t *testing.T) {
	t.Parallel()

	s := NewOrchestrationRuntimeState("test-instance", nil, nil)
	require.NoError(t, AddEvent(s, startedEvent()))

	// Different task ids must each be accepted.
	require.NoError(t, AddEvent(s, taskCompletedEvent(1)))
	require.NoError(t, AddEvent(s, taskCompletedEvent(2)))

	// Same id under a different kind is not a duplicate: the "task" and
	// "timer" namespaces are independent.
	require.NoError(t, AddEvent(s, timerFiredEvent(1)))
	require.NoError(t, AddEvent(s, timerFiredEvent(2)))

	// Same id under the "child" kind likewise distinct from "task".
	require.NoError(t, AddEvent(s, subOrchestrationCompletedEvent(1)))
	require.NoError(t, AddEvent(s, subOrchestrationCompletedEvent(2)))

	assert.Len(t, s.NewEvents, 7, "started + 2 task + 2 timer + 2 child = 7 events")
}

func TestAddEvent_DuplicateAgainstOldEvents(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		committed *protos.HistoryEvent
		duplicate *protos.HistoryEvent
	}{
		{
			name:      "TaskCompleted committed, TaskCompleted redelivered",
			committed: taskCompletedEvent(1),
			duplicate: taskCompletedEvent(1),
		},
		{
			name:      "TaskCompleted committed, TaskFailed redelivered for same id",
			committed: taskCompletedEvent(1),
			duplicate: taskFailedEvent(1),
		},
		{
			name:      "TaskFailed committed, TaskCompleted redelivered for same id",
			committed: taskFailedEvent(1),
			duplicate: taskCompletedEvent(1),
		},
		{
			name:      "TimerFired committed, TimerFired redelivered",
			committed: timerFiredEvent(2),
			duplicate: timerFiredEvent(2),
		},
		{
			name:      "SubOrchestrationInstanceCompleted committed, SubOrchestrationInstanceFailed redelivered for same id",
			committed: subOrchestrationCompletedEvent(3),
			duplicate: subOrchestrationFailedEvent(3),
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := NewOrchestrationRuntimeState("test-instance", nil, []*protos.HistoryEvent{
				startedEvent(),
				tt.committed,
			})

			err := AddEvent(s, tt.duplicate)
			require.ErrorIs(t, err, ErrDuplicateEvent)
			assert.Empty(t, s.NewEvents, "duplicate event must not be appended to NewEvents")
			assert.Len(t, s.OldEvents, 2, "OldEvents unchanged")
		})
	}
}

func TestAddEvent_StalledPreservedOnDuplicateCompletion(t *testing.T) {
	t.Parallel()

	s := NewOrchestrationRuntimeState("test-instance", nil, nil)
	require.NoError(t, AddEvent(s, startedEvent()))
	require.NoError(t, AddEvent(s, taskCompletedEvent(1)))
	require.NoError(t, AddEvent(s, stalledEvent(protos.StalledReason_PATCH_MISMATCH, "stalled")))
	require.NotNil(t, s.Stalled)

	// A duplicate completion must NOT clear the stalled state, mirroring
	// the behaviour for duplicate ExecutionStarted.
	err := AddEvent(s, taskCompletedEvent(1))
	require.ErrorIs(t, err, ErrDuplicateEvent)
	assert.NotNil(t, s.Stalled, "Stalled must be preserved across a duplicate-completion error")
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_STALLED, RuntimeStatus(s))
}

func TestAddEvent_NewOrchestrationRuntimeStateDropsHistoryDuplicates(t *testing.T) {
	t.Parallel()

	// If existing history somehow contains duplicates (e.g. written by a
	// previous version that didn't dedup), NewOrchestrationRuntimeState must not
	// surface them: it ignores AddEvent errors and the duplicate is simply
	// not re-added to OldEvents. Without this property, upgrading would
	// regress workflows that already accumulated duplicate history.
	history := []*protos.HistoryEvent{
		startedEvent(),
		taskCompletedEvent(1),
		taskCompletedEvent(1), // duplicate that NewOrchestrationRuntimeState should silently drop
		taskCompletedEvent(2),
	}
	s := NewOrchestrationRuntimeState("test-instance", nil, history)

	assert.Len(t, s.OldEvents, 3, "duplicate must not be re-added to OldEvents")
	assert.True(t, dedup.IsPresent(s.OldEvents, dedup.KindTask, 1))
	assert.True(t, dedup.IsPresent(s.OldEvents, dedup.KindTask, 2))
}
