package backend

import (
	"context"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

// This file contains deprecated aliases for backward compatibility.
// TODO: rm in a future release

// Deprecated: Use WorkflowWorkItem instead.
type OrchestrationWorkItem = WorkflowWorkItem

// Deprecated: Use WorkflowRuntimeState instead.
type OrchestrationRuntimeState = WorkflowRuntimeState

// Deprecated: Use WorkflowRuntimeStateMessage instead.
type OrchestrationRuntimeStateMessage = WorkflowRuntimeStateMessage

// Deprecated: Use WorkflowMetadata instead.
type OrchestrationMetadata = WorkflowMetadata

// Deprecated: Use BackendWorkflowStateMetadata instead.
type WorkflowStateMetadata = BackendWorkflowStateMetadata

// Deprecated: Use protos.BackendWorkflowState instead.
type BackendWorkflowState = protos.BackendWorkflowState

// --- Deprecated methods on backendClient for TaskHubClient interface ---

// Deprecated: Use ScheduleNewWorkflow instead.
func (c *backendClient) ScheduleNewOrchestration(ctx context.Context, workflow interface{}, opts ...api.NewWorkflowOptions) (api.InstanceID, error) {
	return c.ScheduleNewWorkflow(ctx, workflow, opts...)
}

// Deprecated: Use FetchWorkflowMetadata instead.
func (c *backendClient) FetchOrchestrationMetadata(ctx context.Context, id api.InstanceID) (*WorkflowMetadata, error) {
	return c.FetchWorkflowMetadata(ctx, id)
}

// Deprecated: Use WaitForWorkflowStart instead.
func (c *backendClient) WaitForOrchestrationStart(ctx context.Context, id api.InstanceID) (*WorkflowMetadata, error) {
	return c.WaitForWorkflowStart(ctx, id)
}

// Deprecated: Use WaitForWorkflowCompletion instead.
func (c *backendClient) WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID) (*WorkflowMetadata, error) {
	return c.WaitForWorkflowCompletion(ctx, id)
}

// Deprecated: Use TerminateWorkflow instead.
func (c *backendClient) TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	return c.TerminateWorkflow(ctx, id, opts...)
}

// Deprecated: Use SuspendWorkflow instead.
func (c *backendClient) SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	return c.SuspendWorkflow(ctx, id, reason)
}

// Deprecated: Use ResumeWorkflow instead.
func (c *backendClient) ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	return c.ResumeWorkflow(ctx, id, reason)
}

// Deprecated: Use PurgeWorkflowState instead.
func (c *backendClient) PurgeOrchestrationState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error {
	return c.PurgeWorkflowState(ctx, id, opts...)
}
