package workflow

import (
	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

const (
	StatusRunning        = api.RUNTIME_STATUS_RUNNING
	StatusCompleted      = api.RUNTIME_STATUS_COMPLETED
	StatusContinuedAsNew = api.RUNTIME_STATUS_CONTINUED_AS_NEW
	StatusFailed         = api.RUNTIME_STATUS_FAILED
	StatusCanceled       = api.RUNTIME_STATUS_CANCELED
	StatusTerminated     = api.RUNTIME_STATUS_TERMINATED
	StatusPending        = api.RUNTIME_STATUS_PENDING
	StatusSuspended      = api.RUNTIME_STATUS_SUSPENDED
	StatusStalled        = api.RUNTIME_STATUS_STALLED
)

type WorkflowMetadata protos.WorkflowMetadata
type ListInstanceIDsResponse protos.ListInstanceIDsResponse
type GetInstanceHistoryResponse protos.GetInstanceHistoryResponse

func (w WorkflowMetadata) String() string {
	switch w.RuntimeStatus {
	case api.RUNTIME_STATUS_RUNNING:
		return "RUNNING"
	case api.RUNTIME_STATUS_COMPLETED:
		return "COMPLETED"
	case api.RUNTIME_STATUS_CONTINUED_AS_NEW:
		return "CONTINUED_AS_NEW"
	case api.RUNTIME_STATUS_FAILED:
		return "FAILED"
	case api.RUNTIME_STATUS_CANCELED:
		return "CANCELED"
	case api.RUNTIME_STATUS_TERMINATED:
		return "TERMINATED"
	case api.RUNTIME_STATUS_PENDING:
		return "PENDING"
	case api.RUNTIME_STATUS_SUSPENDED:
		return "SUSPENDED"
	case api.RUNTIME_STATUS_STALLED:
		return "STALLED"
	default:
		return ""
	}
}

// ScheduleResult is the result of scheduling a new workflow.
type ScheduleResult struct {
	// InstanceID is the ID of the new workflow instance.
	InstanceID string
	// Staged is true when the sidecar accepted the start with no workflow
	// worker connected. The workflow starts once a worker connects.
	Staged bool
}

// RerunResult is the result of rerunning a workflow from an event.
type RerunResult struct {
	// NewInstanceID is the ID of the new workflow instance.
	NewInstanceID string
	// Staged is true when the sidecar accepted the rerun with no workflow
	// worker connected. The rerun runs once a worker connects.
	Staged bool
}

// RaiseEventResult is the result of raising an event.
type RaiseEventResult = api.RaiseEventResult

// TerminateResult is the result of terminating a workflow.
type TerminateResult = api.TerminateResult

// SuspendResult is the result of suspending a workflow.
type SuspendResult = api.SuspendResult

// ResumeResult is the result of resuming a workflow.
type ResumeResult = api.ResumeResult
