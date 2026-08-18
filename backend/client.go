package backend

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/payloadstore"
)

type TaskHubClient interface {
	ScheduleNewWorkflow(ctx context.Context, workflow interface{}, opts ...api.NewWorkflowOptions) (api.InstanceID, error)
	FetchWorkflowMetadata(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*WorkflowMetadata, error)
	WaitForWorkflowStart(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*WorkflowMetadata, error)
	WaitForWorkflowCompletion(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*WorkflowMetadata, error)
	TerminateWorkflow(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error
	RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error
	SuspendWorkflow(ctx context.Context, id api.InstanceID, reason string, opts ...api.SuspendOptions) error
	ResumeWorkflow(ctx context.Context, id api.InstanceID, reason string, opts ...api.ResumeOptions) error
	PurgeWorkflowState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error
	RerunWorkflowFromEvent(ctx context.Context, source api.InstanceID, eventID uint32, opts ...api.RerunOptions) (api.InstanceID, error)
}

type backendClient struct {
	be           Backend
	payloadStore payloadstore.Store
	logger       Logger
}

// TaskHubClientOption configures NewTaskHubClient.
type TaskHubClientOption func(*backendClient)

// WithClientPayloadStore makes the client resolve offloaded payload
// references in workflow metadata (FetchWorkflowMetadata and the
// WaitForWorkflow* calls) back to full payloads, best-effort. Resolution
// failures are logged through logger when non-nil.
func WithClientPayloadStore(store payloadstore.Store, logger Logger) TaskHubClientOption {
	return func(c *backendClient) {
		c.payloadStore = store
		c.logger = logger
	}
}

func NewTaskHubClient(be Backend, opts ...TaskHubClientOption) TaskHubClient {
	c := &backendClient{
		be: be,
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

func (c *backendClient) ScheduleNewWorkflow(ctx context.Context, workflow interface{}, opts ...api.NewWorkflowOptions) (api.InstanceID, error) {
	name := helpers.GetTaskFunctionName(workflow)
	req := &protos.CreateInstanceRequest{Name: name}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to configure create instance request: %w", err)
		}
	}
	if req.InstanceId == "" {
		u, err := uuid.NewRandom()
		if err != nil {
			return api.EmptyInstanceID, fmt.Errorf("failed to generate instance ID: %w", err)
		}
		req.InstanceId = u.String()
	}
	if err := api.ValidateTaskRouter(req.GetRouter()); err != nil {
		return api.EmptyInstanceID, err
	}

	var span trace.Span
	ctx, span = helpers.StartNewCreateWorkflowSpan(ctx, req.Name, req.Version.GetValue(), req.InstanceId)
	defer span.End()

	tc := helpers.TraceContextFromSpan(span)
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name:  req.Name,
				Input: req.Input,
				WorkflowInstance: &protos.WorkflowInstance{
					InstanceId:  req.InstanceId,
					ExecutionId: wrapperspb.String(uuid.New().String()),
				},
				ParentTraceContext:      tc,
				ScheduledStartTimestamp: req.ScheduledStartTimestamp,
			},
		},
		Router: req.GetRouter(),
	}
	if err := c.be.CreateWorkflowInstance(ctx, &CreateWorkflowInstanceRequest{
		StartEvent:              e,
		EnforceUniqueInstanceId: req.EnforceUniqueInstanceId,
	}); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return api.EmptyInstanceID, fmt.Errorf("failed to start workflow: %w", err)
	}
	return api.InstanceID(req.InstanceId), nil
}

// FetchWorkflowMetadata fetches metadata for the specified workflow from the configured task hub.
//
// ErrInstanceNotFound is returned when the specified workflow doesn't exist.
func (c *backendClient) FetchWorkflowMetadata(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*WorkflowMetadata, error) {
	req, err := fetchRequest(id, opts)
	if err != nil {
		return nil, err
	}
	metadata, err := c.be.GetWorkflowMetadata(ctx, id, req.GetRouter())
	if err != nil {
		return nil, fmt.Errorf("failed to fetch workflow metadata: %w", err)
	}
	resolveMetadataPayloads(ctx, c.payloadStore, c.logger, metadata)
	return metadata, nil
}

// WaitForWorkflowStart waits for a workflow to start running and returns an [WorkflowMetadata] object that contains
// metadata about the started instance.
//
// ErrInstanceNotFound is returned when the specified workflow doesn't exist.
func (c *backendClient) WaitForWorkflowStart(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*WorkflowMetadata, error) {
	return c.waitForWorkflowCondition(ctx, id, opts, func(metadata *WorkflowMetadata) bool {
		return metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	})
}

// WaitForWorkflowCompletion waits for a workflow to complete and returns an [WorkflowMetadata] object that contains
// metadata about the completed instance.
//
// ErrInstanceNotFound is returned when the specified workflow doesn't exist.
func (c *backendClient) WaitForWorkflowCompletion(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*WorkflowMetadata, error) {
	return c.waitForWorkflowCondition(ctx, id, opts, api.WorkflowMetadataIsComplete)
}

func (c *backendClient) waitForWorkflowCondition(ctx context.Context, id api.InstanceID, opts []api.FetchWorkflowMetadataOptions, condition func(metadata *WorkflowMetadata) bool) (*WorkflowMetadata, error) {
	req, err := fetchRequest(id, opts)
	if err != nil {
		return nil, err
	}
	var metadata *protos.WorkflowMetadata
	err = c.be.WatchWorkflowRuntimeStatus(ctx, id, req.GetRouter(), func(m *WorkflowMetadata) bool {
		metadata = m
		return condition(m)
	})
	if err != nil {
		return metadata, err
	}

	// Resolve once on the final metadata rather than on every status
	// update observed while waiting.
	resolveMetadataPayloads(ctx, c.payloadStore, c.logger, metadata)
	return metadata, nil
}

func fetchRequest(id api.InstanceID, opts []api.FetchWorkflowMetadataOptions) (*protos.GetInstanceRequest, error) {
	req := &protos.GetInstanceRequest{InstanceId: string(id)}
	for _, configure := range opts {
		configure(req)
	}
	if err := api.ValidateTaskRouter(req.GetRouter()); err != nil {
		return nil, err
	}
	return req, nil
}

// TerminateWorkflow enqueues a message to terminate a running workflow, causing it to stop receiving new events and
// go directly into the TERMINATED state. This operation is asynchronous. An workflow worker must
// dequeue the termination event before the workflow will be terminated.
func (c *backendClient) TerminateWorkflow(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	req := &protos.TerminateRequest{InstanceId: string(id), Recursive: true}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure termination request: %w", err)
		}
	}
	if err := api.ValidateTaskRouter(req.GetRouter()); err != nil {
		return err
	}
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionTerminated{
			ExecutionTerminated: &protos.ExecutionTerminatedEvent{
				Input:   req.Output,
				Recurse: req.Recursive,
			},
		},
		Router: req.GetRouter(),
	}
	if err := c.be.AddNewWorkflowEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to submit termination request:: %w", err)
	}
	return nil
}

// RaiseEvent implements TaskHubClient and sends an asynchronous event notification to a waiting workflow.
//
// In order to handle the event, the target workflow instance must be waiting for an event named [eventName]
// using the [WaitForSingleEvent] method of the workflow context parameter. If the target workflow instance
// is not yet waiting for an event named [eventName], then the event will be bufferred in memory until a task
// subscribing to that event name is created.
//
// Raised events for a completed or non-existent workflow instance will be silently discarded.
func (c *backendClient) RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error {
	req := &protos.RaiseEventRequest{InstanceId: string(id), Name: eventName}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure raise event request: %w", err)
		}
	}
	if err := api.ValidateTaskRouter(req.GetRouter()); err != nil {
		return err
	}

	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_EventRaised{
			EventRaised: &protos.EventRaisedEvent{Name: req.Name, Input: req.Input},
		},
		Router: req.GetRouter(),
	}
	if err := c.be.AddNewWorkflowEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to raise event: %w", err)
	}
	return nil
}

// SuspendWorkflow suspends a workflow instance, halting processing of its events until a "resume" operation resumes it.
//
// Note that suspended workflows are still considered to be "running" even though they will not process events.
func (c *backendClient) SuspendWorkflow(ctx context.Context, id api.InstanceID, reason string, opts ...api.SuspendOptions) error {
	var input *wrapperspb.StringValue
	if reason != "" {
		input = wrapperspb.String(reason)
	}
	req := &protos.SuspendRequest{InstanceId: string(id), Reason: input}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure suspend request: %w", err)
		}
	}
	if err := api.ValidateTaskRouter(req.GetRouter()); err != nil {
		return err
	}
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionSuspended{
			ExecutionSuspended: &protos.ExecutionSuspendedEvent{
				Input: input,
			},
		},
		Router: req.GetRouter(),
	}
	if err := c.be.AddNewWorkflowEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to suspend workflow: %w", err)
	}
	return nil
}

// ResumeWorkflow resumes a workflow instance that was previously suspended.
func (c *backendClient) ResumeWorkflow(ctx context.Context, id api.InstanceID, reason string, opts ...api.ResumeOptions) error {
	var input *wrapperspb.StringValue
	if reason != "" {
		input = wrapperspb.String(reason)
	}
	req := &protos.ResumeRequest{InstanceId: string(id), Reason: input}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure resume request: %w", err)
		}
	}
	if err := api.ValidateTaskRouter(req.GetRouter()); err != nil {
		return err
	}
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionResumed{
			ExecutionResumed: &protos.ExecutionResumedEvent{
				Input: input,
			},
		},
		Router: req.GetRouter(),
	}
	if err := c.be.AddNewWorkflowEvent(ctx, id, e); err != nil {
		return fmt.Errorf("failed to resume workflow: %w", err)
	}
	return nil
}

// PurgeWorkflowState deletes the state of the specified workflow instance.
//
// [api.ErrInstanceNotFound] is returned if the specified workflow instance doesn't exist.
// [api.ErrNotCompleted] is returned if the specified workflow instance is still running.
func (c *backendClient) PurgeWorkflowState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error {
	req := &protos.PurgeInstancesRequest{Request: &protos.PurgeInstancesRequest_InstanceId{InstanceId: string(id)}, Recursive: true}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure purge request: %w", err)
		}
	}
	if err := api.ValidateTaskRouter(req.GetRouter()); err != nil {
		return err
	}
	if _, err := purgeWorkflowState(ctx, c.be, id, req.GetRouter(), req.Recursive, req.GetForce()); err != nil {
		return fmt.Errorf("failed to purge workflow state: %w", err)
	}
	return nil
}

// RerunWorkflowFromEvent reruns a workflow from a specific event ID of some
// source instance ID. If not given, a random new instance ID will be generated
// and returned. Can optionally give a new input to the target event ID to
// rerun from.
func (c *backendClient) RerunWorkflowFromEvent(ctx context.Context, id api.InstanceID, eventID uint32, opts ...api.RerunOptions) (api.InstanceID, error) {
	req := &protos.RerunWorkflowFromEventRequest{SourceInstanceID: string(id), EventID: eventID}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return "", fmt.Errorf("failed to configure rerun request: %w", err)
		}
	}
	if err := api.ValidateTaskRouter(req.GetRouter()); err != nil {
		return "", err
	}

	id, err := c.be.RerunWorkflowFromEvent(ctx, req)
	return id, err
}
