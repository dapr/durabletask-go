package client

import (
	"context"
	"fmt"

	"github.com/cenkalti/backoff/v4"
	"github.com/google/uuid"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/kit/ptr"
)

// REVIEW: Can this be merged with backend/client.go somehow?

type TaskHubGrpcClient struct {
	client protos.TaskHubSidecarServiceClient
	logger backend.Logger
}

// NewTaskHubGrpcClient creates a client that can be used to manage workflows over a gRPC connection.
// The gRPC connection must be to a task hub worker that understands the Durable Task gRPC protocol.
func NewTaskHubGrpcClient(cc grpc.ClientConnInterface, logger backend.Logger) *TaskHubGrpcClient {
	return &TaskHubGrpcClient{
		client: protos.NewTaskHubSidecarServiceClient(cc),
		logger: logger,
	}
}

// ScheduleNewWorkflow schedules a new workflow instance with a specified set of options for execution.
func (c *TaskHubGrpcClient) ScheduleNewWorkflow(ctx context.Context, workflow string, opts ...api.NewWorkflowOptions) (api.InstanceID, error) {
	req := &protos.CreateInstanceRequest{Name: workflow}
	for _, configure := range opts {
		configure(req)
	}
	if req.InstanceId == "" {
		req.InstanceId = uuid.NewString()
	}

	xspan := trace.SpanFromContext(ctx)
	if sctx := xspan.SpanContext(); sctx.IsValid() {
		req.ParentTraceContext = &protos.TraceContext{
			TraceParent: sctx.TraceID().String(),
			SpanID:      sctx.SpanID().String(),
			TraceState:  wrapperspb.String(sctx.TraceState().String()),
		}
	}

	resp, err := c.client.StartInstance(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return api.EmptyInstanceID, ctx.Err()
		}
		return api.EmptyInstanceID, fmt.Errorf("failed to start workflow: %w", err)
	}
	return api.InstanceID(resp.InstanceId), nil
}

// FetchWorkflowMetadata fetches metadata for the specified workflow from the configured task hub.
//
// api.ErrInstanceNotFound is returned when the specified workflow doesn't exist.
func (c *TaskHubGrpcClient) FetchWorkflowMetadata(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*backend.WorkflowMetadata, error) {
	req := makeGetInstanceRequest(id, opts)
	resp, err := c.client.GetInstance(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to fetch workflow metadata: %w", err)
	}
	return makeWorkflowMetadata(resp)
}

// WaitForWorkflowStart waits for a workflow to start running and returns an [backend.WorkflowMetadata] object that contains
// metadata about the started instance.
//
// api.ErrInstanceNotFound is returned when the specified workflow doesn't exist.
func (c *TaskHubGrpcClient) WaitForWorkflowStart(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*backend.WorkflowMetadata, error) {
	var resp *protos.GetInstanceResponse
	var err error
	err = backoff.Retry(func() error {
		req := makeGetInstanceRequest(id, opts)
		resp, err = c.client.WaitForInstanceStart(ctx, req)
		if err != nil {
			// if its context cancelled stop retrying
			if ctx.Err() != nil {
				return backoff.Permanent(ctx.Err())
			}
			return fmt.Errorf("failed to wait for workflow start: %w", err)
		}
		return nil
	}, backoff.WithContext(newInfiniteRetries(), ctx))
	if err != nil {
		return nil, err
	}
	return makeWorkflowMetadata(resp)
}

// WaitForWorkflowCompletion waits for a workflow to complete and returns an [backend.WorkflowMetadata] object that contains
// metadata about the completed instance.
//
// api.ErrInstanceNotFound is returned when the specified workflow doesn't exist.
func (c *TaskHubGrpcClient) WaitForWorkflowCompletion(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*backend.WorkflowMetadata, error) {
	var resp *protos.GetInstanceResponse
	var err error
	err = backoff.Retry(func() error {
		req := makeGetInstanceRequest(id, opts)
		resp, err = c.client.WaitForInstanceCompletion(ctx, req)
		if err != nil {
			// if its context cancelled stop retrying
			if ctx.Err() != nil {
				return backoff.Permanent(ctx.Err())
			}
			return fmt.Errorf("failed to wait for workflow completion: %w", err)
		}
		return nil
	}, backoff.WithContext(newInfiniteRetries(), ctx))
	if err != nil {
		return nil, err
	}
	return makeWorkflowMetadata(resp)
}

// TerminateWorkflow terminates a running workflow by causing it to stop receiving new events and
// putting it directly into the TERMINATED state.
func (c *TaskHubGrpcClient) TerminateWorkflow(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	req := &protos.TerminateRequest{InstanceId: string(id), Recursive: true}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure termination request: %w", err)
		}
	}

	_, err := c.client.TerminateInstance(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to terminate instance: %w", err)
	}
	return nil
}

// RaiseEvent sends an asynchronous event notification to a waiting workflow.
func (c *TaskHubGrpcClient) RaiseEvent(ctx context.Context, id api.InstanceID, eventName string, opts ...api.RaiseEventOptions) error {
	req := &protos.RaiseEventRequest{InstanceId: string(id), Name: eventName}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure raise event request: %w", err)
		}
	}

	if _, err := c.client.RaiseEvent(ctx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to raise event: %w", err)
	}
	return nil
}

// SuspendWorkflow suspends a workflow instance, halting processing of its events until a "resume" operation resumes it.
//
// Note that suspended workflows are still considered to be "running" even though they will not process events.
func (c *TaskHubGrpcClient) SuspendWorkflow(ctx context.Context, id api.InstanceID, reason string) error {
	req := &protos.SuspendRequest{
		InstanceId: string(id),
		Reason:     wrapperspb.String(reason),
	}
	if _, err := c.client.SuspendInstance(ctx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to suspend workflow: %w", err)
	}
	return nil
}

// ResumeWorkflow resumes a workflow instance that was previously suspended.
func (c *TaskHubGrpcClient) ResumeWorkflow(ctx context.Context, id api.InstanceID, reason string) error {
	req := &protos.ResumeRequest{
		InstanceId: string(id),
		Reason:     wrapperspb.String(reason),
	}
	if _, err := c.client.ResumeInstance(ctx, req); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to resume workflow: %w", err)
	}
	return nil
}

// PurgeWorkflowState deletes the state of the specified workflow instance.
//
// [api.api.ErrInstanceNotFound] is returned if the specified workflow instance doesn't exist.
func (c *TaskHubGrpcClient) PurgeWorkflowState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error {
	req := &protos.PurgeInstancesRequest{
		Request: &protos.PurgeInstancesRequest_InstanceId{InstanceId: string(id)},
	}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return fmt.Errorf("failed to configure purge request: %w", err)
		}
	}

	res, err := c.client.PurgeInstances(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return fmt.Errorf("failed to purge workflow state: %w", err)
	} else if res.GetDeletedInstanceCount() == 0 {
		return api.ErrInstanceNotFound
	}
	return nil
}

// RerunWorkflowFromEvent reruns a workflow from a specific event ID of some
// source instance ID. If not given, a random new instance ID will be
// generated and returned. Can optionally give a new input to the target
// event ID to rerun from.
func (c *TaskHubGrpcClient) RerunWorkflowFromEvent(ctx context.Context, id api.InstanceID, eventID uint32, opts ...api.RerunOptions) (api.InstanceID, error) {
	req := &protos.RerunWorkflowFromEventRequest{
		SourceInstanceID: string(id),
		EventID:          eventID,
	}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return "", fmt.Errorf("failed to configure rerun request: %w", err)
		}
	}

	resp, err := c.client.RerunWorkflowFromEvent(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return "", ctx.Err()
		}
		return "", err
	}

	return api.InstanceID(resp.GetNewInstanceID()), nil
}

func (c *TaskHubGrpcClient) ListInstanceIDs(ctx context.Context, opts ...api.ListInstanceIDsOptions) (*backend.ListInstanceIDsResponse, error) {
	req := protos.ListInstanceIDsRequest{
		PageSize: ptr.Of(uint32(1024)),
	}

	for _, configure := range opts {
		configure(&req)
	}

	resp, err := c.client.ListInstanceIDs(ctx, &req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to list instance IDs: %w", err)
	}

	return resp, nil
}

func (c *TaskHubGrpcClient) GetInstanceHistory(ctx context.Context, id api.InstanceID, opts ...api.GetInstanceHistoryOptions) (*backend.GetInstanceHistoryResponse, error) {
	req := protos.GetInstanceHistoryRequest{
		InstanceId: id.String(),
	}

	for _, configure := range opts {
		configure(&req)
	}

	resp, err := c.client.GetInstanceHistory(ctx, &req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to get instance history: %w", err)
	}

	return resp, nil
}

func makeGetInstanceRequest(id api.InstanceID, opts []api.FetchWorkflowMetadataOptions) *protos.GetInstanceRequest {
	req := &protos.GetInstanceRequest{
		InstanceId:          string(id),
		GetInputsAndOutputs: true,
	}
	for _, configure := range opts {
		configure(req)
	}
	return req
}

// makeWorkflowMetadata validates and converts protos.GetInstanceResponse to backend.WorkflowMetadata
// api.ErrInstanceNotFound is returned when the specified workflow doesn't exist.
func makeWorkflowMetadata(resp *protos.GetInstanceResponse) (*backend.WorkflowMetadata, error) {
	if !resp.Exists {
		return nil, api.ErrInstanceNotFound
	}
	if resp.WorkflowState == nil {
		return nil, fmt.Errorf("workflow state is nil")
	}
	metadata := &backend.WorkflowMetadata{
		InstanceId:     resp.WorkflowState.InstanceId,
		Name:           resp.WorkflowState.Name,
		RuntimeStatus:  resp.WorkflowState.WorkflowStatus,
		Input:          resp.WorkflowState.Input,
		CustomStatus:   resp.WorkflowState.CustomStatus,
		Output:         resp.WorkflowState.Output,
		CreatedAt:      resp.WorkflowState.CreatedTimestamp,
		LastUpdatedAt:  resp.WorkflowState.LastUpdatedTimestamp,
		FailureDetails: resp.WorkflowState.FailureDetails,
	}
	return metadata, nil
}
