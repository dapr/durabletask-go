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
