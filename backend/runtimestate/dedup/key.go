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
