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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/ptr"
)

func TestWithAppIDOptions_SetTargetOnRouter(t *testing.T) {
	t.Run("schedule", func(t *testing.T) {
		req := new(protos.CreateInstanceRequest)
		require.NoError(t, WithAppID("app2")(req))
		assert.Equal(t, "app2", req.GetRouter().GetTargetAppID())
		assert.Empty(t, req.GetRouter().GetTargetAppNamespace())
	})

	t.Run("schedule composes with other options", func(t *testing.T) {
		req := new(protos.CreateInstanceRequest)
		require.NoError(t, WithInstanceID("iid")(req))
		require.NoError(t, WithAppID("app2")(req))
		assert.Equal(t, "iid", req.GetInstanceId())
		assert.Equal(t, "app2", req.GetRouter().GetTargetAppID())
	})

	t.Run("fetch", func(t *testing.T) {
		req := new(protos.GetInstanceRequest)
		WithFetchAppID("app2")(req)
		assert.Equal(t, "app2", req.GetRouter().GetTargetAppID())
	})

	t.Run("raise event", func(t *testing.T) {
		req := new(protos.RaiseEventRequest)
		require.NoError(t, WithRaiseEventAppID("app2")(req))
		assert.Equal(t, "app2", req.GetRouter().GetTargetAppID())
	})

	t.Run("terminate", func(t *testing.T) {
		req := new(protos.TerminateRequest)
		require.NoError(t, WithTerminateAppID("app2")(req))
		assert.Equal(t, "app2", req.GetRouter().GetTargetAppID())
	})

	t.Run("suspend", func(t *testing.T) {
		req := new(protos.SuspendRequest)
		require.NoError(t, WithSuspendAppID("app2")(req))
		assert.Equal(t, "app2", req.GetRouter().GetTargetAppID())
	})

	t.Run("resume", func(t *testing.T) {
		req := new(protos.ResumeRequest)
		require.NoError(t, WithResumeAppID("app2")(req))
		assert.Equal(t, "app2", req.GetRouter().GetTargetAppID())
	})

	t.Run("purge", func(t *testing.T) {
		req := new(protos.PurgeInstancesRequest)
		require.NoError(t, WithPurgeAppID("app2")(req))
		assert.Equal(t, "app2", req.GetRouter().GetTargetAppID())
	})

	t.Run("rerun", func(t *testing.T) {
		req := new(protos.RerunWorkflowFromEventRequest)
		require.NoError(t, WithRerunAppID("app2")(req))
		assert.Equal(t, "app2", req.GetRouter().GetTargetAppID())
	})
}

func TestValidateTaskRouter(t *testing.T) {
	assert.NoError(t, ValidateTaskRouter(nil))
	assert.NoError(t, ValidateTaskRouter(&protos.TaskRouter{}))
	assert.NoError(t, ValidateTaskRouter(&protos.TaskRouter{TargetAppID: ptr.Of("app2")}))
	assert.NoError(t, ValidateTaskRouter(&protos.TaskRouter{
		TargetAppID:        ptr.Of("app2"),
		TargetAppNamespace: ptr.Of("ns2"),
	}))
	assert.Error(t, ValidateTaskRouter(&protos.TaskRouter{TargetAppNamespace: ptr.Of("ns2")}))
}
