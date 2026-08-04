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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/ptr"
)

// fakeRouterBackend records the events and routers reaching the Backend so
// tests can assert what the in-proc TaskHubClient stamps on each operation.
type fakeRouterBackend struct {
	Backend

	createdEvent *protos.HistoryEvent
	addedEvent   *protos.HistoryEvent
	getRouter    *protos.TaskRouter
	watchRouter  *protos.TaskRouter
}

func (f *fakeRouterBackend) CreateWorkflowInstance(_ context.Context, req *CreateWorkflowInstanceRequest) error {
	f.createdEvent = req.GetStartEvent()
	return nil
}

func (f *fakeRouterBackend) AddNewWorkflowEvent(_ context.Context, _ api.InstanceID, e *HistoryEvent) error {
	f.addedEvent = e
	return nil
}

func (f *fakeRouterBackend) GetWorkflowMetadata(_ context.Context, id api.InstanceID, router *protos.TaskRouter) (*WorkflowMetadata, error) {
	f.getRouter = router
	return &protos.WorkflowMetadata{InstanceId: string(id)}, nil
}

func (f *fakeRouterBackend) WatchWorkflowRuntimeStatus(_ context.Context, id api.InstanceID, router *protos.TaskRouter, condition func(*WorkflowMetadata) bool) error {
	f.watchRouter = router
	condition(&protos.WorkflowMetadata{
		InstanceId:    string(id),
		RuntimeStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
	})
	return nil
}

func TestBackendClient_RouterStamping(t *testing.T) {
	ctx := t.Context()
	const iid = api.InstanceID("iid")

	t.Run("schedule", func(t *testing.T) {
		be := new(fakeRouterBackend)
		c := NewTaskHubClient(be)
		_, err := c.ScheduleNewWorkflow(ctx, "wf", api.WithAppID("app2"))
		require.NoError(t, err)
		require.NotNil(t, be.createdEvent)
		assert.Equal(t, "app2", be.createdEvent.GetRouter().GetTargetAppID())
	})

	t.Run("terminate", func(t *testing.T) {
		be := new(fakeRouterBackend)
		c := NewTaskHubClient(be)
		require.NoError(t, c.TerminateWorkflow(ctx, iid, api.WithTerminateAppID("app2")))
		require.NotNil(t, be.addedEvent)
		require.NotNil(t, be.addedEvent.GetExecutionTerminated())
		assert.Equal(t, "app2", be.addedEvent.GetRouter().GetTargetAppID())
	})

	t.Run("raise event", func(t *testing.T) {
		be := new(fakeRouterBackend)
		c := NewTaskHubClient(be)
		require.NoError(t, c.RaiseEvent(ctx, iid, "ev", api.WithRaiseEventAppID("app2")))
		require.NotNil(t, be.addedEvent)
		require.NotNil(t, be.addedEvent.GetEventRaised())
		assert.Equal(t, "app2", be.addedEvent.GetRouter().GetTargetAppID())
	})

	t.Run("suspend", func(t *testing.T) {
		be := new(fakeRouterBackend)
		c := NewTaskHubClient(be)
		require.NoError(t, c.SuspendWorkflow(ctx, iid, "why", api.WithSuspendAppID("app2")))
		require.NotNil(t, be.addedEvent)
		require.NotNil(t, be.addedEvent.GetExecutionSuspended())
		assert.Equal(t, "app2", be.addedEvent.GetRouter().GetTargetAppID())
	})

	t.Run("resume", func(t *testing.T) {
		be := new(fakeRouterBackend)
		c := NewTaskHubClient(be)
		require.NoError(t, c.ResumeWorkflow(ctx, iid, "why", api.WithResumeAppID("app2")))
		require.NotNil(t, be.addedEvent)
		require.NotNil(t, be.addedEvent.GetExecutionResumed())
		assert.Equal(t, "app2", be.addedEvent.GetRouter().GetTargetAppID())
	})

	t.Run("fetch", func(t *testing.T) {
		be := new(fakeRouterBackend)
		c := NewTaskHubClient(be)
		_, err := c.FetchWorkflowMetadata(ctx, iid, api.WithFetchAppID("app2"))
		require.NoError(t, err)
		assert.Equal(t, "app2", be.getRouter.GetTargetAppID())
	})

	t.Run("wait for completion", func(t *testing.T) {
		be := new(fakeRouterBackend)
		c := NewTaskHubClient(be)
		_, err := c.WaitForWorkflowCompletion(ctx, iid, api.WithFetchAppID("app2"))
		require.NoError(t, err)
		assert.Equal(t, "app2", be.watchRouter.GetTargetAppID())
	})

	t.Run("no option means nil router", func(t *testing.T) {
		be := new(fakeRouterBackend)
		c := NewTaskHubClient(be)
		require.NoError(t, c.TerminateWorkflow(ctx, iid))
		require.NotNil(t, be.addedEvent)
		assert.Nil(t, be.addedEvent.GetRouter())
	})

	t.Run("namespace without app id is rejected", func(t *testing.T) {
		be := new(fakeRouterBackend)
		c := NewTaskHubClient(be)
		// No exported option sets the namespace; craft one directly to prove
		// the client still validates routers before they reach the backend.
		nsOnly := api.TerminateOptions(func(req *protos.TerminateRequest) error {
			req.Router = &protos.TaskRouter{TargetAppNamespace: ptr.Of("ns2")}
			return nil
		})
		require.Error(t, c.TerminateWorkflow(ctx, iid, nsOnly))
		assert.Nil(t, be.addedEvent, "invalid router must not reach the backend")
	})
}
