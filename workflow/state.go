package workflow

import "github.com/dapr/durabletask-go/api"

const (
	StatusRunning        = api.RUNTIME_STATUS_RUNNING
	StatusCompleted      = api.RUNTIME_STATUS_COMPLETED
	StatusContinuedAsNew = api.RUNTIME_STATUS_CONTINUED_AS_NEW
	StatusFailed         = api.RUNTIME_STATUS_FAILED
	StatusCanceled       = api.RUNTIME_STATUS_CANCELED
	StatusTerminated     = api.RUNTIME_STATUS_TERMINATED
	StatusPending        = api.RUNTIME_STATUS_PENDING
	StatusSuspended      = api.RUNTIME_STATUS_SUSPENDED
)
