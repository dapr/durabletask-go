package backend

import (
	"context"
	"errors"
	"fmt"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/runtimestate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	ErrTaskHubExists         = errors.New("task hub already exists")
	ErrTaskHubNotFound       = errors.New("task hub not found")
	ErrNotInitialized        = errors.New("backend not initialized")
	ErrWorkItemLockLost      = errors.New("lock on work-item was lost")
	ErrBackendAlreadyStarted = errors.New("backend is already started")
)

type (
	HistoryEvent                     = protos.HistoryEvent
	TaskFailureDetails               = protos.TaskFailureDetails
	WorkflowState                    = protos.WorkflowState
	CreateWorkflowInstanceRequest    = protos.CreateWorkflowInstanceRequest
	ActivityRequest                  = protos.ActivityRequest
	OrchestrationMetadata            = protos.OrchestrationMetadata
	OrchestrationStatus              = protos.OrchestrationStatus
	WorkflowStateMetadata            = protos.WorkflowStateMetadata
	DurableTimer                     = protos.DurableTimer
	OrchestrationRuntimeState        = protos.OrchestrationRuntimeState
	OrchestrationRuntimeStateMessage = protos.OrchestrationRuntimeStateMessage
	RerunWorkflowFromEventRequest    = protos.RerunWorkflowFromEventRequest
)

type OrchestrationIdReusePolicyOptions func(*protos.OrchestrationIdReusePolicy) error

func WithOrchestrationIdReusePolicy(policy *protos.OrchestrationIdReusePolicy) OrchestrationIdReusePolicyOptions {
	return func(po *protos.OrchestrationIdReusePolicy) error {
		if policy != nil {
			po.OperationStatus = policy.OperationStatus
			po.Action = policy.Action
		}
		return nil
	}
}

type Backend interface {
	// CreateTaskHub creates a new task hub for the current backend. Task hub creation must be idempotent.
	//
	// If the task hub for this backend already exists, an error of type [ErrTaskHubExists] is returned.
	CreateTaskHub(context.Context) error

	// DeleteTaskHub deletes an existing task hub configured for the current backend. It's up to the backend
	// implementation to determine how the task hub data is deleted.
	//
	// If the task hub for this backend doesn't exist, an error of type [ErrTaskHubNotFound] is returned.
	DeleteTaskHub(context.Context) error

	// Start starts any background processing done by this backend.
	Start(context.Context) error

	// Stop stops any background processing done by this backend.
	Stop(context.Context) error

	// CreateOrchestrationInstance creates a new orchestration instance with a history event that
	// wraps a ExecutionStarted event.
	CreateOrchestrationInstance(context.Context, *HistoryEvent, ...OrchestrationIdReusePolicyOptions) error

	// RerunWorkflowFromEvent reruns a workflow from a specific event ID of some
	// source instance ID. If not given, a random new instance ID will be
	// generated and returned. Can optionally give a new input to the target
	// event ID to rerun from.
	RerunWorkflowFromEvent(ctx context.Context, req *protos.RerunWorkflowFromEventRequest) (api.InstanceID, error)

	// AddNewEvent adds a new orchestration event to the specified orchestration instance.
	AddNewOrchestrationEvent(context.Context, api.InstanceID, *HistoryEvent) error

	// NextOrchestrationWorkItem blocks and returns the next orchestration work
	// item from the task hub. Should only return an error when shutting down.
	NextOrchestrationWorkItem(context.Context) (*OrchestrationWorkItem, error)

	// GetOrchestrationRuntimeState gets the runtime state of an orchestration instance.
	GetOrchestrationRuntimeState(context.Context, *OrchestrationWorkItem) (*OrchestrationRuntimeState, error)

	// WatchOrchestrationRuntimeStatus is a streaming API to watch for changes to
	// the OrchestrtionMetadata, receiving events as and when the state changes.
	// When the given condition is true, returns.
	// Used over polling the metadata.
	WatchOrchestrationRuntimeStatus(ctx context.Context, id api.InstanceID, condition func(*OrchestrationMetadata) bool) error

	// GetOrchestrationMetadata gets the metadata associated with the given orchestration instance ID.
	//
	// Returns [api.ErrInstanceNotFound] if the orchestration instance doesn't exist.
	GetOrchestrationMetadata(context.Context, api.InstanceID) (*OrchestrationMetadata, error)

	// CompleteOrchestrationWorkItem completes a work item by saving the updated runtime state to durable storage.
	//
	// Returns [ErrWorkItemLockLost] if the work-item couldn't be completed due to a lock-lost conflict (e.g., split-brain).
	CompleteOrchestrationWorkItem(context.Context, *OrchestrationWorkItem) error

	// AbandonOrchestrationWorkItem undoes any state changes and returns the work item to the work item queue.
	//
	// This is called if an internal failure happens in the processing of an orchestration work item. It is
	// not called if the orchestration work item is processed successfully (note that an orchestration that
	// completes with a failure is still considered a successfully processed work item).
	AbandonOrchestrationWorkItem(context.Context, *OrchestrationWorkItem) error

	// NextActivityWorkItem blocks and returns the next activity work item from
	// the task hub. Should only return an error when shutting down.
	NextActivityWorkItem(context.Context) (*ActivityWorkItem, error)

	// CompleteActivityWorkItem sends a message to the parent orchestration indicating activity completion.
	//
	// Returns [ErrWorkItemLockLost] if the work-item couldn't be completed due to a lock-lost conflict (e.g., split-brain).
	CompleteActivityWorkItem(context.Context, *ActivityWorkItem) error

	// AbandonActivityWorkItem returns the work-item back to the queue without committing any other chances.
	//
	// This is called when an internal failure occurs during activity work-item processing.
	AbandonActivityWorkItem(context.Context, *ActivityWorkItem) error

	// PurgeOrchestrationState deletes all saved state for the specified orchestration instance.
	//
	// [api.ErrInstanceNotFound] is returned if the specified orchestration instance doesn't exist.
	// [api.ErrNotCompleted] is returned if the specified orchestration instance is still running.
	PurgeOrchestrationState(context.Context, api.InstanceID) error

	// CompleteOrchestratorTask completes the orchestrator task by saving the updated runtime state to durable storage.
	CompleteOrchestratorTask(context.Context, *protos.OrchestratorResponse) error

	// CancelOrchestratorTask cancels the orchestrator task so instances of WaitForOrchestratorCompletion will return an error.
	CancelOrchestratorTask(context.Context, api.InstanceID) error

	// WaitForOrchestratorCompletion blocks until the orchestrator completes and returns the final response.
	//
	// [api.ErrTaskCancelled] is returned if the task was cancelled.
	WaitForOrchestratorCompletion(context.Context, *protos.OrchestratorRequest) (*protos.OrchestratorResponse, error)

	// CompleteActivityTask completes the activity task by saving the updated runtime state to durable storage.
	CompleteActivityTask(context.Context, *protos.ActivityResponse) error

	// CancelActivityTask cancels the activity task so instances of WaitForActivityCompletion will return an error.
	CancelActivityTask(context.Context, api.InstanceID, int32) error

	// WaitForActivityCompletion blocks until the activity completes and returns the final response.
	//
	// [api.ErrTaskCancelled] is returned if the task was cancelled.
	WaitForActivityCompletion(context.Context, *protos.ActivityRequest) (*protos.ActivityResponse, error)
}

// MarshalHistoryEvent serializes the [HistoryEvent] into a protobuf byte array.
func MarshalHistoryEvent(e *HistoryEvent) ([]byte, error) {
	if bytes, err := proto.Marshal(e); err != nil {
		return nil, fmt.Errorf("failed to marshal history event: %w", err)
	} else {
		return bytes, nil
	}
}

// UnmarshalHistoryEvent deserializes a [HistoryEvent] from a protobuf byte array.
func UnmarshalHistoryEvent(bytes []byte) (*HistoryEvent, error) {
	e := &protos.HistoryEvent{}
	if err := proto.Unmarshal(bytes, e); err != nil {
		return nil, fmt.Errorf("unreadable history event payload: %w", err)
	}
	return e, nil
}

// purgeOrchestrationState purges the orchestration state, including sub-orchestrations if [recursive] is true.
// Returns (deletedInstanceCount, error), where deletedInstanceCount is the number of instances deleted.
func purgeOrchestrationState(ctx context.Context, be Backend, iid api.InstanceID, recursive bool) (int, error) {
	deletedInstanceCount := 0
	if recursive {
		owi := &OrchestrationWorkItem{
			InstanceID: iid,
		}
		state, err := be.GetOrchestrationRuntimeState(ctx, owi)
		if err != nil {
			return 0, fmt.Errorf("failed to fetch orchestration state: %w", err)
		}
		if len(state.NewEvents)+len(state.OldEvents) == 0 {
			// If there are no events, the orchestration instance doesn't exist
			return 0, api.ErrInstanceNotFound
		}
		if !runtimestate.IsCompleted(state) {
			// Orchestration must be completed before purging its state
			return 0, api.ErrNotCompleted
		}
		subOrchestrationInstances := getSubOrchestrationInstances(state.OldEvents, state.NewEvents)
		for _, subOrchestrationInstance := range subOrchestrationInstances {
			// Recursively purge sub-orchestrations
			count, err := purgeOrchestrationState(ctx, be, subOrchestrationInstance, recursive)
			// `count` sub-orchestrations have been successfully purged (even in case of error)
			deletedInstanceCount += count
			if err != nil {
				return deletedInstanceCount, fmt.Errorf("failed to purge sub-orchestration: %w", err)
			}
		}
	}
	// Purging root orchestration
	if err := be.PurgeOrchestrationState(ctx, iid); err != nil {
		return deletedInstanceCount, err
	}
	return deletedInstanceCount + 1, nil
}

// terminateSubOrchestrationInstances submits termination requests to sub-orchestrations if [et.Recurse] is true.
func terminateSubOrchestrationInstances(ctx context.Context, be Backend, iid api.InstanceID, state *OrchestrationRuntimeState, et *protos.ExecutionTerminatedEvent) error {
	if !et.Recurse {
		return nil
	}
	subOrchestrationInstances := getSubOrchestrationInstances(state.OldEvents, state.NewEvents)
	for _, subOrchestrationInstance := range subOrchestrationInstances {
		e := &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_ExecutionTerminated{
				ExecutionTerminated: &protos.ExecutionTerminatedEvent{
					Input:   et.Input,
					Recurse: et.Recurse,
				},
			},
		}
		// Adding terminate event to sub-orchestration instance
		if err := be.AddNewOrchestrationEvent(ctx, subOrchestrationInstance, e); err != nil {
			return fmt.Errorf("failed to submit termination request to sub-orchestration: %w", err)
		}
	}
	return nil
}

// getSubOrchestrationInstances returns the instance IDs of all sub-orchestrations in the specified events.
func getSubOrchestrationInstances(oldEvents []*HistoryEvent, newEvents []*HistoryEvent) []api.InstanceID {
	subOrchestrationInstancesMap := make(map[api.InstanceID]struct{}, len(oldEvents)+len(newEvents))
	for _, e := range oldEvents {
		if created := e.GetSubOrchestrationInstanceCreated(); created != nil {
			subOrchestrationInstancesMap[api.InstanceID(created.InstanceId)] = struct{}{}
		}
	}
	for _, e := range newEvents {
		if created := e.GetSubOrchestrationInstanceCreated(); created != nil {
			subOrchestrationInstancesMap[api.InstanceID(created.InstanceId)] = struct{}{}
		}
	}
	subOrchestrationInstances := make([]api.InstanceID, 0, len(subOrchestrationInstancesMap))
	for orch := range subOrchestrationInstancesMap {
		subOrchestrationInstances = append(subOrchestrationInstances, orch)
	}
	return subOrchestrationInstances
}
