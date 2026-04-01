package protos

import "google.golang.org/protobuf/types/known/wrapperspb"

// This file contains deprecated aliases for backward compatibility with
// code that references the old Orchestration/SubOrchestration proto type
// and getter names.
// TODO: rm in a future release

// --- Type aliases ---

// Deprecated: Use WorkflowStartedEvent instead.
type OrchestratorStartedEvent = WorkflowStartedEvent

// Deprecated: Use WorkflowCompletedEvent instead.
type OrchestratorCompletedEvent = WorkflowCompletedEvent

// Deprecated: Use WorkflowInstance instead.
type OrchestrationInstance = WorkflowInstance

// Deprecated: Use WorkflowState instead.
type OrchestrationState = WorkflowState

// Deprecated: Use WorkflowRuntimeState instead.
type OrchestrationRuntimeState = WorkflowRuntimeState

// Deprecated: Use WorkflowRuntimeStateMessage instead.
type OrchestrationRuntimeStateMessage = WorkflowRuntimeStateMessage

// Deprecated: Use WorkflowMetadata instead.
type OrchestrationMetadata = WorkflowMetadata

// Deprecated: Use WorkflowAction instead.
type OrchestratorAction = WorkflowAction

// Deprecated: Use WorkflowRequest instead.
type OrchestratorRequest = WorkflowRequest

// Deprecated: Use WorkflowResponse instead.
type OrchestratorResponse = WorkflowResponse

// Deprecated: Use WorkflowMessage instead.
type OrchestratorMessage = WorkflowMessage

// Deprecated: Use CreateChildWorkflowAction instead.
type CreateSubOrchestrationAction = CreateChildWorkflowAction

// Deprecated: Use CompleteWorkflowAction instead.
type CompleteOrchestrationAction = CompleteWorkflowAction

// Deprecated: Use TerminateWorkflowAction instead.
type TerminateOrchestrationAction = TerminateWorkflowAction

// Deprecated: Use WorkflowVersionNotAvailableAction instead.
type OrchestratorVersionNotAvailableAction = WorkflowVersionNotAvailableAction

// Deprecated: Use ChildWorkflowInstanceCreatedEvent instead.
type SubOrchestrationInstanceCreatedEvent = ChildWorkflowInstanceCreatedEvent

// Deprecated: Use ChildWorkflowInstanceCompletedEvent instead.
type SubOrchestrationInstanceCompletedEvent = ChildWorkflowInstanceCompletedEvent

// Deprecated: Use ChildWorkflowInstanceFailedEvent instead.
type SubOrchestrationInstanceFailedEvent = ChildWorkflowInstanceFailedEvent

// Deprecated: Use CompleteWorkflowWorkItemRequest instead.
type CompleteOrchestrationWorkItemRequest = CompleteWorkflowWorkItemRequest

// Deprecated: Use CompleteWorkflowWorkItemResponse instead.
type CompleteOrchestrationWorkItemResponse = CompleteWorkflowWorkItemResponse

// --- Oneof wrapper type aliases ---

// Deprecated: Use HistoryEvent_WorkflowStarted instead.
type HistoryEvent_OrchestratorStarted = HistoryEvent_WorkflowStarted

// Deprecated: Use HistoryEvent_WorkflowCompleted instead.
type HistoryEvent_OrchestratorCompleted = HistoryEvent_WorkflowCompleted

// Deprecated: Use HistoryEvent_ChildWorkflowInstanceCreated instead.
type HistoryEvent_SubOrchestrationInstanceCreated = HistoryEvent_ChildWorkflowInstanceCreated

// Deprecated: Use HistoryEvent_ChildWorkflowInstanceCompleted instead.
type HistoryEvent_SubOrchestrationInstanceCompleted = HistoryEvent_ChildWorkflowInstanceCompleted

// Deprecated: Use HistoryEvent_ChildWorkflowInstanceFailed instead.
type HistoryEvent_SubOrchestrationInstanceFailed = HistoryEvent_ChildWorkflowInstanceFailed

// Deprecated: Use WorkflowAction_ScheduleTask instead.
type OrchestratorAction_ScheduleTask = WorkflowAction_ScheduleTask

// Deprecated: Use WorkflowAction_CreateChildWorkflow instead.
type OrchestratorAction_CreateSubOrchestration = WorkflowAction_CreateChildWorkflow

// Deprecated: Use WorkflowAction_CreateTimer instead.
type OrchestratorAction_CreateTimer = WorkflowAction_CreateTimer

// Deprecated: Use WorkflowAction_SendEvent instead.
type OrchestratorAction_SendEvent = WorkflowAction_SendEvent

// Deprecated: Use WorkflowAction_CompleteWorkflow instead.
type OrchestratorAction_CompleteOrchestration = WorkflowAction_CompleteWorkflow

// Deprecated: Use WorkflowAction_TerminateWorkflow instead.
type OrchestratorAction_TerminateOrchestration = WorkflowAction_TerminateWorkflow

// Deprecated: Use WorkflowAction_WorkflowVersionNotAvailable instead.
type OrchestratorAction_OrchestratorVersionNotAvailable = WorkflowAction_WorkflowVersionNotAvailable

// Deprecated: Use WorkItem_WorkflowRequest instead.
type WorkItem_OrchestratorRequest = WorkItem_WorkflowRequest

// --- Deprecated getter methods ---

// Deprecated: Use GetWorkflowStarted instead.
func (x *HistoryEvent) GetOrchestratorStarted() *WorkflowStartedEvent {
	return x.GetWorkflowStarted()
}

// Deprecated: Use GetWorkflowCompleted instead.
func (x *HistoryEvent) GetOrchestratorCompleted() *WorkflowCompletedEvent {
	return x.GetWorkflowCompleted()
}

// Deprecated: Use GetWorkflowInstance instead.
func (x *ExecutionStartedEvent) GetOrchestrationInstance() *WorkflowInstance {
	return x.GetWorkflowInstance()
}

// Deprecated: Use GetWorkflowStatus on ExecutionCompletedEvent instead.
func (x *ExecutionCompletedEvent) GetOrchestrationStatus() OrchestrationStatus {
	return x.GetWorkflowStatus()
}

// Deprecated: Use GetWorkflowStatus on CompleteWorkflowAction instead.
func (x *CompleteWorkflowAction) GetOrchestrationStatus() OrchestrationStatus {
	return x.GetWorkflowStatus()
}

// Deprecated: Use GetChildWorkflowInstanceCreated instead.
func (x *HistoryEvent) GetSubOrchestrationInstanceCreated() *ChildWorkflowInstanceCreatedEvent {
	return x.GetChildWorkflowInstanceCreated()
}

// Deprecated: Use GetChildWorkflowInstanceCompleted instead.
func (x *HistoryEvent) GetSubOrchestrationInstanceCompleted() *ChildWorkflowInstanceCompletedEvent {
	return x.GetChildWorkflowInstanceCompleted()
}

// Deprecated: Use GetChildWorkflowInstanceFailed instead.
func (x *HistoryEvent) GetSubOrchestrationInstanceFailed() *ChildWorkflowInstanceFailedEvent {
	return x.GetChildWorkflowInstanceFailed()
}

// Deprecated: Use GetWorkflowInstance instead.
func (x *ParentInstanceInfo) GetOrchestrationInstance() *WorkflowInstance {
	return x.GetWorkflowInstance()
}

// Deprecated: Use GetWorkflowSpanID instead.
func (x *ExecutionStartedEvent) GetOrchestrationSpanID() *wrapperspb.StringValue {
	return x.GetWorkflowSpanID()
}

// Deprecated: Use GetWorkflowRequest instead.
func (x *WorkItem) GetOrchestratorRequest() *WorkflowRequest {
	return x.GetWorkflowRequest()
}

// Deprecated: Use GetCompleteWorkflow instead.
func (x *WorkflowAction) GetCompleteOrchestration() *CompleteWorkflowAction {
	return x.GetCompleteWorkflow()
}

// Deprecated: Use GetCreateChildWorkflow instead.
func (x *WorkflowAction) GetCreateSubOrchestration() *CreateChildWorkflowAction {
	return x.GetCreateChildWorkflow()
}

// Deprecated: Use GetTerminateWorkflow instead.
func (x *WorkflowAction) GetTerminateOrchestration() *TerminateWorkflowAction {
	return x.GetTerminateWorkflow()
}

// Deprecated: Use GetWorkflowVersionNotAvailable instead.
func (x *WorkflowAction) GetOrchestratorVersionNotAvailable() *WorkflowVersionNotAvailableAction {
	return x.GetWorkflowVersionNotAvailable()
}
