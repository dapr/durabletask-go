package backend

import "github.com/dapr/durabletask-go/api/protos"

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
// This was the old backend WorkflowState with Inbox/History/Generation fields.
type BackendWorkflowState = protos.BackendWorkflowState
