package backend

import (
	"fmt"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

type WorkItem interface {
	fmt.Stringer
	IsWorkItem() bool
}

type WorkflowWorkItem struct {
	InstanceID api.InstanceID
	NewEvents  []*HistoryEvent
	LockedBy   string
	RetryCount int32
	State      *protos.WorkflowRuntimeState
	Properties map[string]interface{}
	// Internal indicates if a worker can route to an internal executor for internal workflow execution.
	Internal bool
}

// String implements core.WorkItem and fmt.Stringer
func (wi WorkflowWorkItem) String() string {
	return fmt.Sprintf("%s (%d event(s))", wi.InstanceID, len(wi.NewEvents))
}

// IsWorkItem implements core.WorkItem
func (wi WorkflowWorkItem) IsWorkItem() bool {
	return true
}

func (wi *WorkflowWorkItem) GetAbandonDelay() time.Duration {
	switch {
	case wi.RetryCount == 0:
		return time.Duration(0) // no delay
	case wi.RetryCount > 100:
		return 5 * time.Minute // max delay
	default:
		return time.Duration(wi.RetryCount) * time.Second // linear backoff
	}
}

type ActivityWorkItem struct {
	SequenceNumber int64
	InstanceID     api.InstanceID
	NewEvent       *HistoryEvent
	Result         *HistoryEvent
	LockedBy       string
	Properties     map[string]interface{}
	// Internal indicates if a worker can route to an internal executor for internal workflow execution.
	Internal bool
}

// String implements core.WorkItem and fmt.Stringer
func (wi ActivityWorkItem) String() string {
	name := wi.NewEvent.GetTaskScheduled().GetName()
	taskID := wi.NewEvent.EventId
	return fmt.Sprintf("%s/%s#%d", wi.InstanceID, name, taskID)
}

// IsWorkItem implements core.WorkItem
func (wi ActivityWorkItem) IsWorkItem() bool {
	return true
}
