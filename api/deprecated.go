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
