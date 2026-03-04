package runtimestate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dapr/durabletask-go/api/protos"
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

	// A second stalled event should first clear, then set the new stalled state.
	require.NoError(t, AddEvent(s, stalledEvent(protos.StalledReason_VERSION_NOT_AVAILABLE, "second stall")))
	require.NotNil(t, s.Stalled)
	assert.Equal(t, protos.StalledReason_VERSION_NOT_AVAILABLE, s.Stalled.Reason)
	assert.Equal(t, "second stall", s.Stalled.GetDescription())
}
