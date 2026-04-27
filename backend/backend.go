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
	HistoryEvent                  = protos.HistoryEvent
	TaskFailureDetails            = protos.TaskFailureDetails
	WorkflowState                 = protos.WorkflowState
	CreateWorkflowInstanceRequest = protos.CreateWorkflowInstanceRequest
	ActivityRequest               = protos.ActivityRequest
	WorkflowMetadata              = protos.WorkflowMetadata
	OrchestrationStatus           = protos.OrchestrationStatus
	BackendWorkflowStateMetadata  = protos.BackendWorkflowStateMetadata
	DurableTimer                  = protos.DurableTimer
	WorkflowRuntimeState          = protos.WorkflowRuntimeState
	WorkflowRuntimeStateMessage   = protos.WorkflowRuntimeStateMessage
	SigningCertificate            = protos.SigningCertificate
	HistorySignature              = protos.HistorySignature
	RerunWorkflowFromEventRequest = protos.RerunWorkflowFromEventRequest
	ListInstanceIDsRequest        = protos.ListInstanceIDsRequest
	ListInstanceIDsResponse       = protos.ListInstanceIDsResponse
	GetInstanceHistoryRequest     = protos.GetInstanceHistoryRequest
	GetInstanceHistoryResponse    = protos.GetInstanceHistoryResponse
)

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

	// CreateWorkflowInstance creates a new workflow instance with a history event that
	// wraps a ExecutionStarted event.
	CreateWorkflowInstance(context.Context, *HistoryEvent) error

	// RerunWorkflowFromEvent reruns a workflow from a specific event ID of some
	// source instance ID. If not given, a random new instance ID will be
	// generated and returned. Can optionally give a new input to the target
	// event ID to rerun from.
	RerunWorkflowFromEvent(ctx context.Context, req *protos.RerunWorkflowFromEventRequest) (api.InstanceID, error)

	// AddNewEvent adds a new workflow event to the specified workflow instance.
	AddNewWorkflowEvent(context.Context, api.InstanceID, *HistoryEvent) error

	// NextWorkflowWorkItem blocks and returns the next workflow work
	// item from the task hub. Should only return an error when shutting down.
	NextWorkflowWorkItem(context.Context) (*WorkflowWorkItem, error)

	// GetWorkflowRuntimeState gets the runtime state of a workflow instance.
	GetWorkflowRuntimeState(context.Context, *WorkflowWorkItem) (*WorkflowRuntimeState, error)

	// WatchWorkflowRuntimeStatus is a streaming API to watch for changes to
	// the OrchestrtionMetadata, receiving events as and when the state changes.
	// When the given condition is true, returns.
	// Used over polling the metadata.
	WatchWorkflowRuntimeStatus(ctx context.Context, id api.InstanceID, condition func(*WorkflowMetadata) bool) error

	// GetWorkflowMetadata gets the metadata associated with the given workflow instance ID.
	//
	// Returns [api.ErrInstanceNotFound] if the workflow instance doesn't exist.
	GetWorkflowMetadata(context.Context, api.InstanceID) (*WorkflowMetadata, error)

	// CompleteWorkflowWorkItem completes a work item by saving the updated runtime state to durable storage.
	//
	// Returns [ErrWorkItemLockLost] if the work-item couldn't be completed due to a lock-lost conflict (e.g., split-brain).
	CompleteWorkflowWorkItem(context.Context, *WorkflowWorkItem) error

	// AbandonWorkflowWorkItem undoes any state changes and returns the work item to the work item queue.
	//
	// This is called if an internal failure happens in the processing of a workflow work item. It is
	// not called if the workflow work item is processed successfully (note that a workflow that
	// completes with a failure is still considered a successfully processed work item).
	AbandonWorkflowWorkItem(context.Context, *WorkflowWorkItem) error

	// NextActivityWorkItem blocks and returns the next activity work item from
	// the task hub. Should only return an error when shutting down.
	NextActivityWorkItem(context.Context) (*ActivityWorkItem, error)

	// CompleteActivityWorkItem sends a message to the parent workflow indicating activity completion.
	//
	// Returns [ErrWorkItemLockLost] if the work-item couldn't be completed due to a lock-lost conflict (e.g., split-brain).
	CompleteActivityWorkItem(context.Context, *ActivityWorkItem) error

	// AbandonActivityWorkItem returns the work-item back to the queue without committing any other chances.
	//
	// This is called when an internal failure occurs during activity work-item processing.
	AbandonActivityWorkItem(context.Context, *ActivityWorkItem) error

	// PurgeWorkflowState deletes saved workflow state. When router is nil or
	// targets the local app, this is a single-instance purge of id and the
	// returned count is 1 on success. When router carries a foreign TargetAppID
	// set by the recursive purge driver for a child started cross-app the
	// backend is expected to delegate the entire recursive purge of that subtree
	// to the target app (the local recursion stops walking through it) and
	// return the number of instances purged on the remote side.
	// [api.ErrInstanceNotFound] is returned if the specified workflow instance doesn't exist.
	// [api.ErrNotCompleted] is returned if the specified workflow instance is still running.
	PurgeWorkflowState(ctx context.Context, id api.InstanceID, router *protos.TaskRouter, force bool) (int, error)

	// CompleteWorkflowTask completes the workflow task by saving the updated runtime state to durable storage.
	CompleteWorkflowTask(context.Context, *protos.WorkflowResponse) error

	// CancelWorkflowTask cancels the workflow task so instances of WaitForWorkflowTaskCompletion will return an error.
	CancelWorkflowTask(context.Context, api.InstanceID) error

	// WaitForWorkflowTaskCompletion blocks until the workflow completes and returns the final response.
	//
	// [api.ErrTaskCancelled] is returned if the task was cancelled.
	WaitForWorkflowTaskCompletion(*protos.WorkflowRequest) func(context.Context) (*protos.WorkflowResponse, error)

	// CompleteActivityTask completes the activity task by saving the updated runtime state to durable storage.
	CompleteActivityTask(context.Context, *protos.ActivityResponse) error

	// CancelActivityTask cancels the activity task so instances of WaitForActivityCompletion will return an error.
	CancelActivityTask(context.Context, api.InstanceID, int32) error

	// WaitForActivityCompletion blocks until the activity completes and returns the final response.
	//
	// [api.ErrTaskCancelled] is returned if the task was cancelled.
	WaitForActivityCompletion(*protos.ActivityRequest) func(context.Context) (*protos.ActivityResponse, error)

	// ListInstanceIDs lists workflow instance IDs based on the provided
	// query parameters.
	ListInstanceIDs(ctx context.Context, req *protos.ListInstanceIDsRequest) (*protos.ListInstanceIDsResponse, error)

	// GetInstanceHistory returns the full current history of a workflow instance.
	GetInstanceHistory(ctx context.Context, req *protos.GetInstanceHistoryRequest) (*protos.GetInstanceHistoryResponse, error)
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

// purgeWorkflowState purges the workflow state, including child workflows if [recursive] is true.
// Returns (deletedInstanceCount, error), where deletedInstanceCount is the number of instances deleted.
func purgeWorkflowState(ctx context.Context, be Backend, iid api.InstanceID, recursive bool, force bool) (int, error) {
	deletedInstanceCount := 0
	if recursive {
		owi := &WorkflowWorkItem{
			InstanceID: iid,
		}
		state, err := be.GetWorkflowRuntimeState(ctx, owi)
		if err != nil {
			return 0, fmt.Errorf("failed to fetch workflow state: %w", err)
		}
		if len(state.NewEvents)+len(state.OldEvents) == 0 {
			// If there are no events, the workflow instance doesn't exist
			return 0, api.ErrInstanceNotFound
		}
		if !runtimestate.IsCompleted(state) {
			// Workflow must be completed before purging its state
			return 0, api.ErrNotCompleted
		}
		childWorkflowInstances := getChildWorkflowInstances(state.OldEvents, state.NewEvents)
		for _, child := range childWorkflowInstances {
			if isRemoteRouter(child.Router) {
				// Child workflow started cross-app: its state (and any further
				// descendants) live on a different app, so delegate the entire subtree
				// to the backend rather than trying to walk it from here.
				count, err := be.PurgeWorkflowState(ctx, child.InstanceID, child.Router, force)
				deletedInstanceCount += count
				if err != nil {
					return deletedInstanceCount, fmt.Errorf("failed to purge cross-app child workflow: %w", err)
				}
				continue
			}
			// Same-app child: walk locally.
			count, err := purgeWorkflowState(ctx, be, child.InstanceID, recursive, force)
			// `count` child workflows have been successfully purged (even in case of
			// error)
			deletedInstanceCount += count
			if err != nil {
				return deletedInstanceCount, fmt.Errorf("failed to purge child workflow: %w", err)
			}
		}
	}
	// Purging root workflow
	count, err := be.PurgeWorkflowState(ctx, iid, nil, force)
	if err != nil {
		return deletedInstanceCount, err
	}
	return deletedInstanceCount + count, nil
}

// isRemoteRouter reports whether the given router targets a sub-orchestration
// hosted on a different app, i.e. whether the recursion driver should delegate
// the entire subtree to the backend's cross-app dispatch path. The applier
// stamps every action's router with at least SourceAppID, so same-app children
// arrive here with a non-nil router whose TargetAppID is empty — match on the
// effective target string to stay aligned with the per-backend dispatch
// predicates.
func isRemoteRouter(r *protos.TaskRouter) bool {
	return r.GetTargetAppID() != ""
}

// childWorkflowRef captures a child workflow's instance ID together with the
// routing metadata recorded on its ChildWorkflowInstanceCreated history event.
// The router carries the target app ID for cross-app sub-orchestrations and
// must be preserved when synthesising new events (e.g. recursive terminate) so
// the backend can dispatch them to the correct app.
type childWorkflowRef struct {
	InstanceID api.InstanceID
	Router     *protos.TaskRouter
}

// terminateChildWorkflowInstances submits termination requests to child workflows if [et.Recurse] is true.
func terminateChildWorkflowInstances(ctx context.Context, be Backend, iid api.InstanceID, state *WorkflowRuntimeState, et *protos.ExecutionTerminatedEvent) error {
	if !et.Recurse {
		return nil
	}
	childWorkflowInstances := getChildWorkflowInstances(state.OldEvents, state.NewEvents)
	for _, child := range childWorkflowInstances {
		e := &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_ExecutionTerminated{
				ExecutionTerminated: &protos.ExecutionTerminatedEvent{
					Input:   et.Input,
					Recurse: et.Recurse,
				},
			},
			Router: child.Router,
		}
		// Adding terminate event to child workflow instance
		if err := be.AddNewWorkflowEvent(ctx, child.InstanceID, e); err != nil {
			return fmt.Errorf("failed to submit termination request to child workflow: %w", err)
		}
	}
	return nil
}

// getChildWorkflowInstances returns each child workflow recorded in the given
// events, paired with the router that was attached to its
// ChildWorkflowInstanceCreated history event. The applier always populates a
// router on emitted history events (at minimum the parent's SourceAppID), so
// same-app children are represented by a router whose TargetAppID is unset
// or empty rather than by a nil router.
func getChildWorkflowInstances(oldEvents []*HistoryEvent, newEvents []*HistoryEvent) []childWorkflowRef {
	childWorkflowInstancesMap := make(map[api.InstanceID]*protos.TaskRouter, len(oldEvents)+len(newEvents))
	collect := func(events []*HistoryEvent) {
		for _, e := range events {
			created := e.GetChildWorkflowInstanceCreated()
			if created == nil {
				continue
			}
			id := api.InstanceID(created.InstanceId)
			if _, ok := childWorkflowInstancesMap[id]; ok {
				continue
			}
			childWorkflowInstancesMap[id] = e.GetRouter()
		}
	}
	collect(oldEvents)
	collect(newEvents)

	childWorkflowInstances := make([]childWorkflowRef, 0, len(childWorkflowInstancesMap))
	for id, router := range childWorkflowInstancesMap {
		childWorkflowInstances = append(childWorkflowInstances, childWorkflowRef{
			InstanceID: id,
			Router:     router,
		})
	}
	return childWorkflowInstances
}
