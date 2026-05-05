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

// Package dedup detects duplicate task-resolution events
// (TaskCompleted/TaskFailed/TimerFired/SubOrchestrationInstance{Completed,Failed})
// inside an orchestration's runtime state.
package dedup

import "github.com/dapr/durabletask-go/api/protos"

// Kind names the family of correlator that identifies a task-resolution event.
// Two events are duplicates only when they share both kind and id.
type Kind uint8

const (
	KindNone Kind = iota
	// KindTask covers TaskCompleted and TaskFailed, both correlated by
	// TaskScheduledId. A failure arriving after a completion (or vice versa) for
	// the same id is still a duplicate.
	KindTask
	KindTimer
	// KindChild covers SubOrchestrationInstanceCompleted and
	// SubOrchestrationInstanceFailed, correlated by TaskScheduledId.
	KindChild
)

// Of returns the (kind, id) correlator for e. Returns (KindNone, 0, false)
// for events with no resolution semantics.
func Of(e *protos.HistoryEvent) (Kind, int32, bool) {
	switch {
	case e.GetTaskCompleted() != nil:
		return KindTask, e.GetTaskCompleted().GetTaskScheduledId(), true
	case e.GetTaskFailed() != nil:
		return KindTask, e.GetTaskFailed().GetTaskScheduledId(), true
	case e.GetTimerFired() != nil:
		return KindTimer, e.GetTimerFired().GetTimerId(), true
	case e.GetSubOrchestrationInstanceCompleted() != nil:
		return KindChild, e.GetSubOrchestrationInstanceCompleted().GetTaskScheduledId(), true
	case e.GetSubOrchestrationInstanceFailed() != nil:
		return KindChild, e.GetSubOrchestrationInstanceFailed().GetTaskScheduledId(), true
	}
	return KindNone, 0, false
}

// IsPresent reports whether events contains a resolution event with the
// given (kind, id) correlator.
func IsPresent(events []*protos.HistoryEvent, kind Kind, id int32) bool {
	for _, e := range events {
		if k, existingID, ok := Of(e); ok && k == kind && existingID == id {
			return true
		}
	}
	return false
}
