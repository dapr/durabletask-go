package api

import (
	"github.com/dapr/durabletask-go/api/protos"
)

// This file contains deprecated aliases for backward compatibility.
// TODO: rm in a future release

// Deprecated: Use NewWorkflowOptions instead.
type NewOrchestrationOptions = NewWorkflowOptions

// Deprecated: Use FetchWorkflowMetadataOptions instead.
type FetchOrchestrationMetadataOptions = FetchWorkflowMetadataOptions

// Deprecated: Use WorkflowMetadataIsComplete instead.
func OrchestrationMetadataIsComplete(o *protos.WorkflowMetadata) bool {
	return WorkflowMetadataIsComplete(o)
}

// Deprecated: Use WorkflowMetadataIsRunning instead.
func OrchestrationMetadataIsRunning(o *protos.WorkflowMetadata) bool {
	return WorkflowMetadataIsRunning(o)
}

// Deprecated: Use WithOrchestrationIdReusePolicy is no longer supported.
func WithOrchestrationIdReusePolicy(policy interface{}) NewWorkflowOptions {
	return func(req *protos.CreateInstanceRequest) error {
		return nil
	}
}
