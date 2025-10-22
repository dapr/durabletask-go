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
	StatusPendingVersion = api.RUNTIME_STATUS_PENDING_VERSION
)

type WorkflowMetadata protos.OrchestrationMetadata
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
	case api.RUNTIME_STATUS_PENDING_VERSION:
		return "PENDING_VERSION"
	default:
		return ""
	}
}
