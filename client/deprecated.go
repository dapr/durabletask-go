/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client

import (
	"context"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/backend"
)

// This file contains deprecated aliases for backward compatibility.
// TODO: rm in a future release

// Deprecated: Use ScheduleNewWorkflow instead.
func (c *TaskHubGrpcClient) ScheduleNewOrchestration(ctx context.Context, workflow string, opts ...api.NewWorkflowOptions) (api.InstanceID, error) {
	return c.ScheduleNewWorkflow(ctx, workflow, opts...)
}

// Deprecated: Use WaitForWorkflowCompletion instead.
func (c *TaskHubGrpcClient) WaitForOrchestrationCompletion(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*backend.WorkflowMetadata, error) {
	return c.WaitForWorkflowCompletion(ctx, id, opts...)
}

// Deprecated: Use WaitForWorkflowStart instead.
func (c *TaskHubGrpcClient) WaitForOrchestrationStart(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*backend.WorkflowMetadata, error) {
	return c.WaitForWorkflowStart(ctx, id, opts...)
}

// Deprecated: Use FetchWorkflowMetadata instead.
func (c *TaskHubGrpcClient) FetchOrchestrationMetadata(ctx context.Context, id api.InstanceID, opts ...api.FetchWorkflowMetadataOptions) (*backend.WorkflowMetadata, error) {
	return c.FetchWorkflowMetadata(ctx, id, opts...)
}

// Deprecated: Use TerminateWorkflow instead.
func (c *TaskHubGrpcClient) TerminateOrchestration(ctx context.Context, id api.InstanceID, opts ...api.TerminateOptions) error {
	return c.TerminateWorkflow(ctx, id, opts...)
}

// Deprecated: Use SuspendWorkflow instead.
func (c *TaskHubGrpcClient) SuspendOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	return c.SuspendWorkflow(ctx, id, reason)
}

// Deprecated: Use ResumeWorkflow instead.
func (c *TaskHubGrpcClient) ResumeOrchestration(ctx context.Context, id api.InstanceID, reason string) error {
	return c.ResumeWorkflow(ctx, id, reason)
}

// Deprecated: Use PurgeWorkflowState instead.
func (c *TaskHubGrpcClient) PurgeOrchestrationState(ctx context.Context, id api.InstanceID, opts ...api.PurgeOptions) error {
	return c.PurgeWorkflowState(ctx, id, opts...)
}
