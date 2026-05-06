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

package dedup_test

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/runtimestate"
	"github.com/dapr/durabletask-go/backend/runtimestate/dedup"
)

func taskCompleted(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   id,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TaskCompleted{
			TaskCompleted: &protos.TaskCompletedEvent{TaskScheduledId: id},
		},
	}
}

func taskFailed(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   id,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TaskFailed{
			TaskFailed: &protos.TaskFailedEvent{TaskScheduledId: id},
		},
	}
}

func timerFired(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   id,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TimerFired{
			TimerFired: &protos.TimerFiredEvent{TimerId: id},
		},
	}
}

func TestOf(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		event    *protos.HistoryEvent
		wantKind dedup.Kind
		wantID   int32
		wantOK   bool
	}{
		{"TaskCompleted", taskCompleted(7), dedup.KindTask, 7, true},
		{"TaskFailed shares Task kind", taskFailed(7), dedup.KindTask, 7, true},
		{"TimerFired", timerFired(3), dedup.KindTimer, 3, true},
		{
			"ExecutionStarted has no resolution",
			&protos.HistoryEvent{EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{Name: "n"},
			}},
			dedup.KindNone, 0, false,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			k, id, ok := dedup.Of(tt.event)
			assert.Equal(t, tt.wantOK, ok)
			assert.Equal(t, tt.wantKind, k)
			assert.Equal(t, tt.wantID, id)
		})
	}
}

func TestSet_Add(t *testing.T) {
	t.Parallel()

	s := dedup.New(0)

	assert.False(t, s.Has(dedup.KindTask, 1))
	assert.False(t, s.Add(dedup.KindTask, 1), "first add should not be a duplicate")
	assert.True(t, s.Has(dedup.KindTask, 1))
	assert.True(t, s.Add(dedup.KindTask, 1), "second add of same key is a duplicate")

	// Same id under a different kind is independent.
	assert.False(t, s.Add(dedup.KindTimer, 1))
	assert.True(t, s.Has(dedup.KindTimer, 1))
}

func TestSet_NilSentinel(t *testing.T) {
	t.Parallel()

	var s dedup.Set
	assert.False(t, s.Has(dedup.KindTask, 1))
	assert.False(t, s.Add(dedup.KindTask, 1))
}

func TestNewForState_PrePopulates(t *testing.T) {
	t.Parallel()

	state := runtimestate.NewWorkflowRuntimeState("inst", nil, []*protos.HistoryEvent{
		{EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{Name: "wf"},
		}, Timestamp: timestamppb.Now()},
		taskCompleted(1),
		timerFired(2),
	})

	s := dedup.NewForState(state)
	assert.True(t, s.Has(dedup.KindTask, 1))
	assert.True(t, s.Has(dedup.KindTimer, 2))
	assert.False(t, s.Has(dedup.KindTask, 99))
}

func TestIsPresent(t *testing.T) {
	t.Parallel()

	events := []*protos.HistoryEvent{
		taskCompleted(1),
		timerFired(5),
		taskFailed(2),
	}

	assert.True(t, dedup.IsPresent(events, dedup.KindTask, 1))
	assert.True(t, dedup.IsPresent(events, dedup.KindTask, 2), "TaskFailed satisfies KindTask presence")
	assert.True(t, dedup.IsPresent(events, dedup.KindTimer, 5))
	assert.False(t, dedup.IsPresent(events, dedup.KindTimer, 1), "different kind, same id is not a match")
	assert.False(t, dedup.IsPresent(events, dedup.KindTask, 999))
}

func BenchmarkLoadHistory(b *testing.B) {
	for _, n := range []int{100, 1000, 5000} {
		history := make([]*protos.HistoryEvent, 0, n+1)
		history = append(history, &protos.HistoryEvent{
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{Name: "wf"},
			},
			Timestamp: timestamppb.Now(),
		})
		for i := 0; i < n; i++ {
			history = append(history, taskCompleted(int32(i)))
		}

		b.Run("N="+strconv.Itoa(n), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = runtimestate.NewWorkflowRuntimeState("inst", wrapperspb.String(""), history)
			}
		})
	}
}
