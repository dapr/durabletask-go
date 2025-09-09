package workflow

import (
	"context"

	"google.golang.org/grpc"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/client"
)

type Client struct {
	thgc *client.TaskHubGrpcClient
}

// NewClient creates a client that can be used to manage worfklows.
func NewClient(cc grpc.ClientConnInterface) *Client {
	return NewClientWithLogger(cc, backend.DefaultLogger())
}

func NewClientWithLogger(cc grpc.ClientConnInterface, logger backend.Logger) *Client {
	return &Client{
		thgc: client.NewTaskHubGrpcClient(cc, logger),
	}
}

// StartWorker starts the workflow runtime to process workflows.
func (c *Client) StartWorker(ctx context.Context, r *TaskRegistry) error {
	return c.thgc.StartWorkItemListener(ctx, r.registry)
}

// ScheduleNewWorkflow schedules a new workflow instance with a specified set
// of options for execution.
func (c *Client) ScheduleNewWorkflow(ctx context.Context, orchestrator string, opts ...NewWorkflowOptions) (string, error) {
	oopts := make([]api.NewOrchestrationOptions, len(opts))
	for i, o := range opts {
		oopts[i] = api.NewOrchestrationOptions(o)
	}

	id, err := c.thgc.ScheduleNewOrchestration(ctx, orchestrator, oopts...)
	return string(id), err
}

// FetchWorkflowMetadata fetches metadata for the specified workflow from the
// configured task hub.
//
// api.ErrInstanceNotFound is returned when the specified workflow doesn't
// exist.
func (c *Client) FetchWorkflowMetadata(ctx context.Context, id string, opts ...FetchWorkflowMetadataOptions) (*WorkflowMetadata, error) {
	oops := make([]api.FetchOrchestrationMetadataOptions, len(opts))
	for i, o := range opts {
		oops[i] = api.FetchOrchestrationMetadataOptions(o)
	}
	meta, err := c.thgc.FetchOrchestrationMetadata(ctx, api.InstanceID(id), oops...)
	return (*WorkflowMetadata)(meta), err
}

// WaitForWorkflowStart waits for an workflow to start running and returns an
// [backend.WorkflowMetadata] object that contains metadata about the started
// instance.
//
// api.ErrInstanceNotFound is returned when the specified workflow doesn't
// exist.
func (c *Client) WaitForWorkfowStart(ctx context.Context, id string, opts ...FetchWorkflowMetadataOptions) (*WorkflowMetadata, error) {
	oops := make([]api.FetchOrchestrationMetadataOptions, len(opts))
	for i, o := range opts {
		oops[i] = api.FetchOrchestrationMetadataOptions(o)
	}
	meta, err := c.thgc.WaitForOrchestrationStart(ctx, api.InstanceID(id), oops...)
	return (*WorkflowMetadata)(meta), err
}

// WaitForWorkflowCompletion waits for an workflow to complete and returns an
// [backend.WorkflowMetadata] object that contains metadata about the completed
// instance.
//
// api.ErrInstanceNotFound is returned when the specified workflow doesn't
// exist.
func (c *Client) WaitForWorkflowCompletion(ctx context.Context, id string, opts ...FetchWorkflowMetadataOptions) (*WorkflowMetadata, error) {
	oops := make([]api.FetchOrchestrationMetadataOptions, len(opts))
	for i, o := range opts {
		oops[i] = api.FetchOrchestrationMetadataOptions(o)
	}
	meta, err := c.thgc.WaitForOrchestrationCompletion(ctx, api.InstanceID(id), oops...)
	return (*WorkflowMetadata)(meta), err
}

// TerminateWorkflow terminates a running workflow by causing it to stop
// receiving new events and putting it directly into the TERMINATED state.
func (c *Client) TerminateWorkflow(ctx context.Context, id string, opts ...TerminateOptions) error {
	toops := make([]api.TerminateOptions, len(opts))
	for i, o := range opts {
		toops[i] = api.TerminateOptions(o)
	}
	return c.thgc.TerminateOrchestration(ctx, api.InstanceID(id), toops...)
}

// RaiseEvent sends an asynchronous event notification to a waiting workflow.
func (c *Client) RaiseEvent(ctx context.Context, id, eventName string, opts ...RaiseEventOptions) error {
	oops := make([]api.RaiseEventOptions, len(opts))
	for i, o := range opts {
		oops[i] = api.RaiseEventOptions(o)
	}
	return c.thgc.RaiseEvent(ctx, api.InstanceID(id), eventName, oops...)
}

// SuspendWorkflow suspends an workflow instance, halting processing of its
// events until a "resume" operation resumes it.
//
// Note that suspended workflows are still considered to be "running" even
// though they will not process events.
func (c *Client) SuspendWorkflow(ctx context.Context, id, reason string) error {
	return c.thgc.SuspendOrchestration(ctx, api.InstanceID(id), reason)
}

// ResumeWorkflow resumes an orchestration instance that was previously
// suspended.
func (c *Client) ResumeWorkflow(ctx context.Context, id, reason string) error {
	return c.thgc.ResumeOrchestration(ctx, api.InstanceID(id), reason)
}

// PurgeWorkflowState deletes the state of the specified workflow instance.
//
// [api.api.ErrInstanceNotFound] is returned if the specified workflow instance
// doesn't exist.
func (c *Client) PurgeWorkflowState(ctx context.Context, id string, opts ...PurgeOptions) error {
	oops := make([]api.PurgeOptions, len(opts))
	for i, o := range opts {
		oops[i] = api.PurgeOptions(o)
	}
	return c.thgc.PurgeOrchestrationState(ctx, api.InstanceID(id), oops...)
}

// RerunWorkflowFromEvent reruns a workflow from a specific event ID of some
// source instance ID. If not given, a random new instance ID will be generated
// and returned. Can optionally give a new input to the target event ID to
// rerun from.
func (c *Client) RerunWorkflowFromEvent(ctx context.Context, id string, eventID uint32, opts ...RerunOptions) (string, error) {
	oops := make([]api.RerunOptions, len(opts))
	for i, o := range opts {
		oops[i] = api.RerunOptions(o)
	}
	newID, err := c.thgc.RerunWorkflowFromEvent(ctx, api.InstanceID(id), eventID, oops...)
	return string(newID), err
}
