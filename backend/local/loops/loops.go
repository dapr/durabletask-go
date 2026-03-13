package loops

import (
	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

type taskbase struct{}

func (*taskbase) isEventTask() {}

// EventTask is the marker interface for local task backend events.
type EventTask interface{ isEventTask() }

// RegisterPendingOrchestrator registers a new pending orchestrator waiter.
type RegisterPendingOrchestrator struct {
	*taskbase
	InstanceID string
	Response   chan<- *protos.OrchestratorResponse
}

// CompleteOrchestrator signals completion of an orchestrator task.
type CompleteOrchestrator struct {
	*taskbase
	InstanceID string
	Response   *protos.OrchestratorResponse
	ErrCh      chan<- error
}

// CancelOrchestrator cancels a pending orchestrator task.
type CancelOrchestrator struct {
	*taskbase
	InstanceID api.InstanceID
	ErrCh      chan<- error
}

// RegisterPendingActivity registers a new pending activity waiter.
type RegisterPendingActivity struct {
	*taskbase
	Key      string
	Response chan<- *protos.ActivityResponse
}

// CompleteActivity signals completion of an activity task.
type CompleteActivity struct {
	*taskbase
	InstanceID string
	TaskID     int32
	Response   *protos.ActivityResponse
	ErrCh      chan<- error
}

// CancelActivity cancels a pending activity task.
type CancelActivity struct {
	*taskbase
	InstanceID api.InstanceID
	TaskID     int32
	ErrCh      chan<- error
}

// Shutdown signals the loop to stop.
type Shutdown struct {
	*taskbase
}
