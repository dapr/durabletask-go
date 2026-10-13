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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

func TestMakeGetInstanceRequest_Defaults(t *testing.T) {
	req := makeGetInstanceRequest("abc", nil)
	assert.Equal(t, "abc", req.GetInstanceId())
	assert.True(t, req.GetGetInputsAndOutputs())
	assert.Nil(t, req.GetRouter())
}

func TestMakeGetInstanceRequest_AppliesOptions(t *testing.T) {
	req := makeGetInstanceRequest("abc", []api.FetchWorkflowMetadataOptions{
		api.WithFetchPayloads(false),
		api.WithFetchAppID("other-app"),
	})
	assert.False(t, req.GetGetInputsAndOutputs())
	assert.Equal(t, "other-app", req.GetRouter().GetTargetAppID())
}

func TestMakeWorkflowMetadata_InstanceNotFound(t *testing.T) {
	_, err := makeWorkflowMetadata(&protos.GetInstanceResponse{Exists: false})
	assert.ErrorIs(t, err, api.ErrInstanceNotFound)
}

func TestMakeWorkflowMetadata_NilWorkflowState(t *testing.T) {
	_, err := makeWorkflowMetadata(&protos.GetInstanceResponse{Exists: true})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "workflow state is nil")
}

func TestMakeWorkflowMetadata_Populated(t *testing.T) {
	resp := &protos.GetInstanceResponse{
		Exists: true,
		WorkflowState: &protos.WorkflowState{
			InstanceId:     "abc",
			Name:           "MyWorkflow",
			WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
			Version:        wrapperspb.String("v1"),
		},
	}
	metadata, err := makeWorkflowMetadata(resp)
	require.NoError(t, err)
	assert.Equal(t, "abc", metadata.InstanceId)
	assert.Equal(t, "MyWorkflow", metadata.Name)
	assert.Equal(t, "v1", metadata.Version.GetValue())
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
}

func TestNewInfiniteRetries(t *testing.T) {
	b := newInfiniteRetries()
	assert.Equal(t, 15*time.Second, b.MaxInterval)
	assert.Equal(t, time.Duration(0), b.MaxElapsedTime)
}
