// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// This file provides backward-compatible type aliases for the old
// Orchestration*/Orchestrator* names that have been renamed to Workflow* in the
// generated protobuf code.

package protos

// OrchestrationInstance is the deprecated name for WorkflowInstance.
// Deprecated: Use WorkflowInstance instead.
type OrchestrationInstance = WorkflowInstance

// OrchestrationStatus is the deprecated name for WorkflowStatus.
// Deprecated: Use WorkflowStatus instead.
type OrchestrationStatus = WorkflowStatus

// OrchestrationState is the deprecated name for WorkflowState.
// Deprecated: Use WorkflowState instead.
type OrchestrationState = WorkflowState

// OrchestrationIdReusePolicy is the deprecated name for WorkflowIdReusePolicy.
// Deprecated: Use WorkflowIdReusePolicy instead.
type OrchestrationIdReusePolicy = WorkflowIdReusePolicy

// OrchestrationVersion is the deprecated name for WorkflowVersion.
// Deprecated: Use WorkflowVersion instead.
type OrchestrationVersion = WorkflowVersion

// OrchestratorAction is the deprecated name for WorkflowAction.
// Deprecated: Use WorkflowAction instead.
type OrchestratorAction = WorkflowAction

// OrchestratorRequest is the deprecated name for WorkflowRequest.
// Deprecated: Use WorkflowRequest instead.
type OrchestratorRequest = WorkflowRequest

// OrchestratorResponse is the deprecated name for WorkflowResponse.
// Deprecated: Use WorkflowResponse instead.
type OrchestratorResponse = WorkflowResponse

// CreateOrchestrationAction is the deprecated name for CreateWorkflowAction.
// Deprecated: Use CreateWorkflowAction instead.
type CreateOrchestrationAction = CreateWorkflowAction

// OrchestratorStartedEvent is the deprecated name for WorkflowExecutorStartedEvent.
// Deprecated: Use WorkflowExecutorStartedEvent instead.
type OrchestratorStartedEvent = WorkflowExecutorStartedEvent

// OrchestratorCompletedEvent is the deprecated name for WorkflowExecutorCompletedEvent.
// Deprecated: Use WorkflowExecutorCompletedEvent instead.
type OrchestratorCompletedEvent = WorkflowExecutorCompletedEvent

// SubOrchestrationInstanceCreatedEvent is the deprecated name for SubWorkflowInstanceCreatedEvent.
// Deprecated: Use SubWorkflowInstanceCreatedEvent instead.
type SubOrchestrationInstanceCreatedEvent = SubWorkflowInstanceCreatedEvent

// SubOrchestrationInstanceCompletedEvent is the deprecated name for SubWorkflowInstanceCompletedEvent.
// Deprecated: Use SubWorkflowInstanceCompletedEvent instead.
type SubOrchestrationInstanceCompletedEvent = SubWorkflowInstanceCompletedEvent

// SubOrchestrationInstanceFailedEvent is the deprecated name for SubWorkflowInstanceFailedEvent.
// Deprecated: Use SubWorkflowInstanceFailedEvent instead.
type SubOrchestrationInstanceFailedEvent = SubWorkflowInstanceFailedEvent

// CreateSubOrchestrationAction is the deprecated name for CreateSubWorkflowAction.
// Deprecated: Use CreateSubWorkflowAction instead.
type CreateSubOrchestrationAction = CreateSubWorkflowAction

// CompleteOrchestrationAction is the deprecated name for CompleteWorkflowAction.
// Deprecated: Use CompleteWorkflowAction instead.
type CompleteOrchestrationAction = CompleteWorkflowAction

// TerminateOrchestrationAction is the deprecated name for TerminateWorkflowAction.
// Deprecated: Use TerminateWorkflowAction instead.
type TerminateOrchestrationAction = TerminateWorkflowAction

// OrchestratorVersionNotAvailableAction is the deprecated name for WorkflowVersionNotAvailableAction.
// Deprecated: Use WorkflowVersionNotAvailableAction instead.
type OrchestratorVersionNotAvailableAction = WorkflowVersionNotAvailableAction

// AbandonOrchestrationTaskRequest is the deprecated name for AbandonWorkflowTaskRequest.
// Deprecated: Use AbandonWorkflowTaskRequest instead.
type AbandonOrchestrationTaskRequest = AbandonWorkflowTaskRequest

// AbandonOrchestrationTaskResponse is the deprecated name for AbandonWorkflowTaskResponse.
// Deprecated: Use AbandonWorkflowTaskResponse instead.
type AbandonOrchestrationTaskResponse = AbandonWorkflowTaskResponse

// OrchestratorMessage is the deprecated name for WorkflowMessage.
// Deprecated: Use WorkflowMessage instead.
type OrchestratorMessage = WorkflowMessage

// OrchestrationMetadata is the deprecated name for WorkflowMetadata.
// Deprecated: Use WorkflowMetadata instead.
type OrchestrationMetadata = WorkflowMetadata

// CompleteOrchestrationWorkItemRequest is the deprecated name for CompleteWorkflowWorkItemRequest.
// Deprecated: Use CompleteWorkflowWorkItemRequest instead.
type CompleteOrchestrationWorkItemRequest = CompleteWorkflowWorkItemRequest

// CompleteOrchestrationWorkItemResponse is the deprecated name for CompleteWorkflowWorkItemResponse.
// Deprecated: Use CompleteWorkflowWorkItemResponse instead.
type CompleteOrchestrationWorkItemResponse = CompleteWorkflowWorkItemResponse

// AbandonOrchestrationWorkItemRequest is the deprecated name for AbandonWorkflowWorkItemRequest.
// Deprecated: Use AbandonWorkflowWorkItemRequest instead.
type AbandonOrchestrationWorkItemRequest = AbandonWorkflowWorkItemRequest

// AbandonOrchestrationWorkItemResponse is the deprecated name for AbandonWorkflowWorkItemResponse.
// Deprecated: Use AbandonWorkflowWorkItemResponse instead.
type AbandonOrchestrationWorkItemResponse = AbandonWorkflowWorkItemResponse

// GetOrchestrationRuntimeStateRequest is the deprecated name for GetWorkflowRuntimeStateRequest.
// Deprecated: Use GetWorkflowRuntimeStateRequest instead.
type GetOrchestrationRuntimeStateRequest = GetWorkflowRuntimeStateRequest

// GetOrchestrationRuntimeStateResponse is the deprecated name for GetWorkflowRuntimeStateResponse.
// Deprecated: Use GetWorkflowRuntimeStateResponse instead.
type GetOrchestrationRuntimeStateResponse = GetWorkflowRuntimeStateResponse

// OrchestrationRuntimeState is the deprecated name for WorkflowRuntimeState.
// Deprecated: Use WorkflowRuntimeState instead.
type OrchestrationRuntimeState = WorkflowRuntimeState

// OrchestrationRuntimeStateMessage is the deprecated name for WorkflowRuntimeStateMessage.
// Deprecated: Use WorkflowRuntimeStateMessage instead.
type OrchestrationRuntimeStateMessage = WorkflowRuntimeStateMessage

// Deprecated enum constant aliases for OrchestrationStatus.
const (
	OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING          = WorkflowStatus_WORKFLOW_STATUS_RUNNING
	OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED        = WorkflowStatus_WORKFLOW_STATUS_COMPLETED
	OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW = WorkflowStatus_WORKFLOW_STATUS_CONTINUED_AS_NEW
	OrchestrationStatus_ORCHESTRATION_STATUS_FAILED           = WorkflowStatus_WORKFLOW_STATUS_FAILED
	OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED         = WorkflowStatus_WORKFLOW_STATUS_CANCELED
	OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED       = WorkflowStatus_WORKFLOW_STATUS_TERMINATED
	OrchestrationStatus_ORCHESTRATION_STATUS_PENDING          = WorkflowStatus_WORKFLOW_STATUS_PENDING
	OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED        = WorkflowStatus_WORKFLOW_STATUS_SUSPENDED
	OrchestrationStatus_ORCHESTRATION_STATUS_STALLED          = WorkflowStatus_WORKFLOW_STATUS_STALLED
)

// Deprecated enum constant aliases for CreateOrchestrationAction.
const (
	CreateOrchestrationAction_ERROR     = CreateWorkflowAction_ERROR
	CreateOrchestrationAction_IGNORE    = CreateWorkflowAction_IGNORE
	CreateOrchestrationAction_TERMINATE = CreateWorkflowAction_TERMINATE
)

// OrchestratorAction oneof type aliases.
type (
	OrchestratorAction_ScheduleTask                    = WorkflowAction_ScheduleTask
	OrchestratorAction_CreateSubOrchestration          = WorkflowAction_CreateSubWorkflow
	OrchestratorAction_CreateTimer                     = WorkflowAction_CreateTimer
	OrchestratorAction_SendEvent                       = WorkflowAction_SendEvent
	OrchestratorAction_CompleteOrchestration           = WorkflowAction_CompleteWorkflow
	OrchestratorAction_TerminateOrchestration          = WorkflowAction_TerminateWorkflow
	OrchestratorAction_SendEntityMessage               = WorkflowAction_SendEntityMessage
	OrchestratorAction_OrchestratorVersionNotAvailable = WorkflowAction_WorkflowVersionNotAvailable
)

// HistoryEvent oneof type aliases.
type (
	HistoryEvent_OrchestratorStarted               = HistoryEvent_WorkflowExecutorStarted
	HistoryEvent_OrchestratorCompleted             = HistoryEvent_WorkflowExecutorCompleted
	HistoryEvent_SubOrchestrationInstanceCreated   = HistoryEvent_SubWorkflowInstanceCreated
	HistoryEvent_SubOrchestrationInstanceCompleted = HistoryEvent_SubWorkflowInstanceCompleted
	HistoryEvent_SubOrchestrationInstanceFailed    = HistoryEvent_SubWorkflowInstanceFailed
)

// WorkItem oneof type alias.
type WorkItem_OrchestratorRequest = WorkItem_WorkflowRequest
