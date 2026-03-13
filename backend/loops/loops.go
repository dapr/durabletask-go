package loops

import (
	"github.com/dapr/durabletask-go/api/protos"
)

type workerbase struct{}

func (*workerbase) isEventWorker() {}

// EventWorker is the marker interface for worker loop events.
type EventWorker interface{ isEventWorker() }

// Shutdown signals the worker loop to stop.
type Shutdown struct{ *workerbase }

// DispatchWorkItem delivers a work item directly to the worker loop from
// the backend. The worker processes it inline and signals completion via
// the Callback channel (nil = completed, non-nil error = abandoned).
type DispatchWorkItem struct {
	*workerbase
	WorkItem any
	Callback chan<- error
}

type executorbase struct{}

func (*executorbase) isEventExecutor() {}

// EventExecutor is the marker interface for executor loop events.
type EventExecutor interface{ isEventExecutor() }

// ExecuteOrchestrator delivers an orchestrator work item to the connected
// stream. The caller blocks on Dispatched until the item has been sent to
// a stream (or an error occurs), preserving back-pressure semantics.
type ExecuteOrchestrator struct {
	*executorbase
	InstanceID string
	WorkItem   *protos.WorkItem
	Dispatched chan<- error
}

// ExecuteActivity delivers an activity work item to the connected stream.
// Same back-pressure semantics as ExecuteOrchestrator.
type ExecuteActivity struct {
	*executorbase
	Key        string
	InstanceID string
	TaskID     int32
	WorkItem   *protos.WorkItem
	Dispatched chan<- error
}

// ConnectStream signals that a new GetWorkItems stream has connected.
type ConnectStream struct {
	*executorbase
	StreamID string
	Stream   protos.TaskHubSidecarService_GetWorkItemsServer
	// ErrCh receives the final error when the stream should terminate.
	ErrCh chan<- error
}

// DisconnectStream signals that a GetWorkItems stream has disconnected.
type DisconnectStream struct {
	*executorbase
	StreamID string
}

// StreamShutdown signals that the external shutdown channel has fired.
type StreamShutdown struct{ *executorbase }

// ShutdownExecutor signals the executor loop to stop. All pending items
// are cancelled and the loop terminates.
type ShutdownExecutor struct{ *executorbase }
