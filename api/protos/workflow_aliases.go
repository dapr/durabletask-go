// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

// This file provides type aliases that map new workflow-centric names to
// the underlying types, and also provides backward-compatible aliases for
// the old orchestration-centric names.

package protos

// WorkflowInstance is the new name for OrchestrationInstance.
type WorkflowInstance = OrchestrationInstance

// WorkflowStatus is the new name for OrchestrationStatus.
type WorkflowStatus = OrchestrationStatus

// WorkflowState is the new name for OrchestrationState.
type WorkflowState = OrchestrationState

// WorkflowIdReusePolicy is the new name for OrchestrationIdReusePolicy.
type WorkflowIdReusePolicy = OrchestrationIdReusePolicy

// WorkflowVersion is the new name for OrchestrationVersion.
type WorkflowVersion = OrchestrationVersion

// WorkflowAction is the new name for OrchestratorAction.
type WorkflowAction = OrchestratorAction

// WorkflowRequest is the new name for OrchestratorRequest.
type WorkflowRequest = OrchestratorRequest

// WorkflowResponse is the new name for OrchestratorResponse.
type WorkflowResponse = OrchestratorResponse

// CreateWorkflowAction is the new name for CreateOrchestrationAction.
type CreateWorkflowAction = CreateOrchestrationAction

// SubWorkflowInstanceCreatedEvent is the new name for SubOrchestrationInstanceCreatedEvent.
type SubWorkflowInstanceCreatedEvent = SubOrchestrationInstanceCreatedEvent

// SubWorkflowInstanceCompletedEvent is the new name for SubOrchestrationInstanceCompletedEvent.
type SubWorkflowInstanceCompletedEvent = SubOrchestrationInstanceCompletedEvent

// SubWorkflowInstanceFailedEvent is the new name for SubOrchestrationInstanceFailedEvent.
type SubWorkflowInstanceFailedEvent = SubOrchestrationInstanceFailedEvent

// WorkflowExecutorStartedEvent is the new name for OrchestratorStartedEvent.
type WorkflowExecutorStartedEvent = OrchestratorStartedEvent

// CreateSubWorkflowAction is the new name for CreateSubOrchestrationAction.
type CreateSubWorkflowAction = CreateSubOrchestrationAction

// CompleteWorkflowAction is the new name for CompleteOrchestrationAction.
type CompleteWorkflowAction = CompleteOrchestrationAction

// TerminateWorkflowAction is the new name for TerminateOrchestrationAction.
type TerminateWorkflowAction = TerminateOrchestrationAction

// WorkflowVersionNotAvailableAction is the new name for OrchestratorVersionNotAvailableAction.
type WorkflowVersionNotAvailableAction = OrchestratorVersionNotAvailableAction

// AbandonWorkflowTaskRequest is the new name for AbandonOrchestrationTaskRequest.
type AbandonWorkflowTaskRequest = AbandonOrchestrationTaskRequest

// AbandonWorkflowTaskResponse is the new name for AbandonOrchestrationTaskResponse.
type AbandonWorkflowTaskResponse = AbandonOrchestrationTaskResponse

// Backward-compatible aliases (old names → new underlying types in backend_service.pb.go)

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

// Backward-compatible aliases for runtime state types

// OrchestrationRuntimeState is the deprecated name for WorkflowRuntimeState.
// Deprecated: Use WorkflowRuntimeState instead.
type OrchestrationRuntimeState = WorkflowRuntimeState

// OrchestrationRuntimeStateMessage is the deprecated name for WorkflowRuntimeStateMessage.
// Deprecated: Use WorkflowRuntimeStateMessage instead.
type OrchestrationRuntimeStateMessage = WorkflowRuntimeStateMessage

// New enum constants using the WORKFLOW_STATUS_* naming convention.
// These map directly to the ORCHESTRATION_STATUS_* constants.
const (
	WorkflowStatus_WORKFLOW_STATUS_RUNNING          WorkflowStatus = OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING
	WorkflowStatus_WORKFLOW_STATUS_COMPLETED        WorkflowStatus = OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED
	WorkflowStatus_WORKFLOW_STATUS_CONTINUED_AS_NEW WorkflowStatus = OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW
	WorkflowStatus_WORKFLOW_STATUS_FAILED           WorkflowStatus = OrchestrationStatus_ORCHESTRATION_STATUS_FAILED
	WorkflowStatus_WORKFLOW_STATUS_CANCELED         WorkflowStatus = OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED
	WorkflowStatus_WORKFLOW_STATUS_TERMINATED       WorkflowStatus = OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED
	WorkflowStatus_WORKFLOW_STATUS_PENDING          WorkflowStatus = OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	WorkflowStatus_WORKFLOW_STATUS_SUSPENDED        WorkflowStatus = OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED
	WorkflowStatus_WORKFLOW_STATUS_STALLED          WorkflowStatus = OrchestrationStatus_ORCHESTRATION_STATUS_STALLED
)

// New enum constants using the workflow-centric naming convention for CreateWorkflowAction.
const (
	CreateWorkflowAction_ERROR     CreateWorkflowAction = CreateOrchestrationAction_ERROR
	CreateWorkflowAction_IGNORE    CreateWorkflowAction = CreateOrchestrationAction_IGNORE
	CreateWorkflowAction_TERMINATE CreateWorkflowAction = CreateOrchestrationAction_TERMINATE
)
