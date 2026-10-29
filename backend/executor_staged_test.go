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

package backend

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

// stagingBackend returns err from every operation and counts the waits that
// follow them.
type stagingBackend struct {
	Backend

	err     error
	watches int
}

func (b *stagingBackend) CreateWorkflowInstance(context.Context, *CreateWorkflowInstanceRequest) error {
	return b.err
}

func (b *stagingBackend) AddNewWorkflowEvent(context.Context, api.InstanceID, *HistoryEvent) error {
	return b.err
}

func (b *stagingBackend) RerunWorkflowFromEvent(context.Context, *RerunWorkflowFromEventRequest) (api.InstanceID, error) {
	return "rerun", b.err
}

func (b *stagingBackend) WatchWorkflowRuntimeStatus(_ context.Context, id api.InstanceID, _ *protos.TaskRouter, condition func(*WorkflowMetadata) bool) error {
	b.watches++
	condition(&protos.WorkflowMetadata{
		InstanceId:    string(id),
		RuntimeStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
	})
	return nil
}

func TestGrpcExecutor_StagedOperation(t *testing.T) {
	ctx := t.Context()

	ops := map[string]func(protos.TaskHubSidecarServiceServer) error{
		"start": func(s protos.TaskHubSidecarServiceServer) error {
			_, err := s.StartInstance(ctx, &protos.CreateInstanceRequest{InstanceId: "iid", Name: "wf"})
			return err
		},
		"rerun": func(s protos.TaskHubSidecarServiceServer) error {
			res, err := s.RerunWorkflowFromEvent(ctx, &protos.RerunWorkflowFromEventRequest{SourceInstanceID: "iid"})
			if err == nil {
				assert.Equal(t, "rerun", res.GetNewInstanceID())
			}
			return err
		},
		"terminate": func(s protos.TaskHubSidecarServiceServer) error {
			_, err := s.TerminateInstance(ctx, &protos.TerminateRequest{InstanceId: "iid"})
			return err
		},
		"suspend": func(s protos.TaskHubSidecarServiceServer) error {
			_, err := s.SuspendInstance(ctx, &protos.SuspendRequest{InstanceId: "iid"})
			return err
		},
		"resume": func(s protos.TaskHubSidecarServiceServer) error {
			_, err := s.ResumeInstance(ctx, &protos.ResumeRequest{InstanceId: "iid"})
			return err
		},
	}

	for name, op := range ops {
		t.Run(name+" staged returns without waiting", func(t *testing.T) {
			be := &stagingBackend{err: fmt.Errorf("on fallback host: %w", ErrOperationStaged)}
			exec, _ := NewGrpcExecutor(be, DefaultLogger())
			require.NoError(t, op(exec.(*grpcExecutor)))
			assert.Zero(t, be.watches)
		})

		t.Run(name+" applied waits", func(t *testing.T) {
			be := new(stagingBackend)
			exec, _ := NewGrpcExecutor(be, DefaultLogger())
			require.NoError(t, op(exec.(*grpcExecutor)))
			assert.Equal(t, 1, be.watches)
		})

		t.Run(name+" failed returns the error", func(t *testing.T) {
			be := &stagingBackend{err: errors.New("boom")}
			exec, _ := NewGrpcExecutor(be, DefaultLogger())
			require.ErrorContains(t, op(exec.(*grpcExecutor)), "boom")
			assert.Zero(t, be.watches)
		})
	}

	t.Run("raise event staged succeeds", func(t *testing.T) {
		be := &stagingBackend{err: ErrOperationStaged}
		exec, _ := NewGrpcExecutor(be, DefaultLogger())
		_, err := exec.(*grpcExecutor).RaiseEvent(ctx, &protos.RaiseEventRequest{InstanceId: "iid", Name: "ev"})
		require.NoError(t, err)
	})
}

func TestBackendClient_StagedOperation(t *testing.T) {
	ctx := t.Context()
	be := &stagingBackend{err: ErrOperationStaged}
	c := NewTaskHubClient(be)

	_, err := c.ScheduleNewWorkflow(ctx, "wf")
	require.NoError(t, err)
	require.NoError(t, c.RaiseEvent(ctx, "iid", "ev"))
	require.NoError(t, c.TerminateWorkflow(ctx, "iid"))
	require.NoError(t, c.SuspendWorkflow(ctx, "iid", ""))
	require.NoError(t, c.ResumeWorkflow(ctx, "iid", ""))
	id, err := c.RerunWorkflowFromEvent(ctx, "iid", 0)
	require.NoError(t, err)
	assert.Equal(t, api.InstanceID("rerun"), id)
}
