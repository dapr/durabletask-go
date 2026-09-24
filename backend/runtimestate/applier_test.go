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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/runtimestate/dedup"
)

// A resolution that reached the state before its scheduling action was
// committed (delivered to the workflow from the replay buffer) must still get
// its scheduling event recorded, so the history replays and the resolution
// keeps its match, but the work must not be dispatched a second time.
func TestActions_ResolvedScheduleIsRecordedNotDispatched(t *testing.T) {
	scheduleTask := func(id int32) *protos.WorkflowAction {
		return &protos.WorkflowAction{Id: id, WorkflowActionType: &protos.WorkflowAction_ScheduleTask{
			ScheduleTask: &protos.ScheduleTaskAction{Name: "act"},
		}}
	}
	createTimer := func(id int32) *protos.WorkflowAction {
		return &protos.WorkflowAction{Id: id, WorkflowActionType: &protos.WorkflowAction_CreateTimer{
			CreateTimer: &protos.CreateTimerAction{FireAt: timestamppb.New(time.Now().Add(time.Hour))},
		}}
	}
	createChild := func(id int32) *protos.WorkflowAction {
		return &protos.WorkflowAction{Id: id, WorkflowActionType: &protos.WorkflowAction_CreateChildWorkflow{
			CreateChildWorkflow: &protos.CreateChildWorkflowAction{Name: "child"},
		}}
	}
	resolution := func(kind dedup.Kind, id int32) *protos.HistoryEvent {
		e := &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now()}
		switch kind {
		case dedup.KindTask:
			e.EventType = &protos.HistoryEvent_TaskCompleted{TaskCompleted: &protos.TaskCompletedEvent{TaskScheduledId: id}}
		case dedup.KindTimer:
			e.EventType = &protos.HistoryEvent_TimerFired{TimerFired: &protos.TimerFiredEvent{TimerId: id}}
		case dedup.KindChild:
			e.EventType = &protos.HistoryEvent_ChildWorkflowInstanceCompleted{ChildWorkflowInstanceCompleted: &protos.ChildWorkflowInstanceCompletedEvent{TaskScheduledId: id}}
		}
		return e
	}

	tests := []struct {
		name      string
		kind      dedup.Kind
		action    func(int32) *protos.WorkflowAction
		scheduled func(*protos.HistoryEvent) bool
		pending   func(*protos.WorkflowRuntimeState) []int32
	}{
		{
			name: "task", kind: dedup.KindTask, action: scheduleTask,
			scheduled: func(e *protos.HistoryEvent) bool { return e.GetTaskScheduled() != nil },
			pending: func(s *protos.WorkflowRuntimeState) []int32 {
				ids := make([]int32, 0, len(s.GetPendingTasks()))
				for _, e := range s.GetPendingTasks() {
					ids = append(ids, e.GetEventId())
				}
				return ids
			},
		},
		{
			name: "timer", kind: dedup.KindTimer, action: createTimer,
			scheduled: func(e *protos.HistoryEvent) bool { return e.GetTimerCreated() != nil },
			pending: func(s *protos.WorkflowRuntimeState) []int32 {
				ids := make([]int32, 0, len(s.GetPendingTimers()))
				for _, e := range s.GetPendingTimers() {
					ids = append(ids, e.GetTimerFired().GetTimerId())
				}
				return ids
			},
		},
		{
			name: "child", kind: dedup.KindChild, action: createChild,
			scheduled: func(e *protos.HistoryEvent) bool { return e.GetChildWorkflowInstanceCreated() != nil },
			pending: func(s *protos.WorkflowRuntimeState) []int32 {
				ids := make([]int32, 0, len(s.GetPendingMessages()))
				for _, m := range s.GetPendingMessages() {
					ids = append(ids, m.GetHistoryEvent().GetExecutionStarted().GetParentInstance().GetTaskScheduledId())
				}
				return ids
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewWorkflowRuntimeState("parent", nil, []*protos.HistoryEvent{startedEvent()})
			require.NoError(t, AddEvent(s, resolution(tt.kind, 1)))

			_, err := NewApplier("app", "ns").Actions(s, nil, []*protos.WorkflowAction{tt.action(1), tt.action(2)}, nil, nil)
			require.NoError(t, err)

			var scheduled []int32
			for _, e := range s.GetNewEvents() {
				if tt.scheduled(e) {
					scheduled = append(scheduled, e.GetEventId())
				}
			}
			assert.Equal(t, []int32{1, 2}, scheduled, "both scheduling events must be recorded")
			assert.Equal(t, []int32{2}, tt.pending(s), "only the unresolved work is dispatched")
			assert.True(t, dedup.IsPresent(s.GetNewEvents(), tt.kind, 1), "the early resolution must be retained")
		})
	}
}
