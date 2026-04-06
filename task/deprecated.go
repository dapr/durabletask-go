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

package task

import "google.golang.org/protobuf/types/known/wrapperspb"

// This file contains deprecated aliases for backward compatibility.
// TODO: rm in a future release

// Deprecated: Use WorkflowContext instead.
type OrchestrationContext = WorkflowContext

// Deprecated: Use Workflow instead.
type Orchestrator = Workflow

// Deprecated: Use ChildWorkflowOption instead.
type SubOrchestratorOption = ChildWorkflowOption

// Deprecated: Use AddWorkflow instead.
func (r *TaskRegistry) AddOrchestrator(o Workflow) error {
	return r.AddWorkflow(o)
}

// Deprecated: Use AddWorkflowN instead.
func (r *TaskRegistry) AddOrchestratorN(name string, o Workflow) error {
	return r.AddWorkflowN(name, o)
}

// Deprecated: Use CallChildWorkflow instead.
func (ctx *WorkflowContext) CallSubOrchestrator(workflow interface{}, opts ...ChildWorkflowOption) Task {
	return ctx.CallChildWorkflow(workflow, opts...)
}

// Deprecated: Use WithChildWorkflowInput instead.
func WithSubOrchestratorInput(input any) ChildWorkflowOption {
	return WithChildWorkflowInput(input)
}

// Deprecated: Use WithRawChildWorkflowInput instead.
func WithRawSubOrchestratorInput(input *wrapperspb.StringValue) ChildWorkflowOption {
	return WithRawChildWorkflowInput(input)
}

// Deprecated: Use WithChildWorkflowAppID instead.
func WithSubOrchestratorAppID(appID string) ChildWorkflowOption {
	return WithChildWorkflowAppID(appID)
}

// Deprecated: Use WithChildWorkflowInstanceID instead.
func WithSubOrchestrationInstanceID(instanceID string) ChildWorkflowOption {
	return WithChildWorkflowInstanceID(instanceID)
}

// Deprecated: Use WithChildWorkflowRetryPolicy instead.
func WithSubOrchestrationRetryPolicy(policy *RetryPolicy) ChildWorkflowOption {
	return WithChildWorkflowRetryPolicy(policy)
}

// Deprecated: Use AddVersionedWorkflowN instead.
func (r *TaskRegistry) AddVersionedOrchestratorN(canonicalName string, name string, isLatest bool, o Workflow) error {
	return r.AddVersionedWorkflowN(canonicalName, name, isLatest, o)
}

// Deprecated: Use AddVersionedWorkflow instead.
func (r *TaskRegistry) AddVersionedOrchestrator(canonicalName string, isLatest bool, o Workflow) error {
	return r.AddVersionedWorkflow(canonicalName, isLatest, o)
}
