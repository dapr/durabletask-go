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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api/protos"
)

func TestStartInstance_Ordering(t *testing.T) {
	be := new(fakeRouterBackend)
	exec, _ := NewGrpcExecutor(be, DefaultLogger())
	_, err := exec.(*grpcExecutor).StartInstance(t.Context(), &protos.CreateInstanceRequest{
		InstanceId: "iid",
		Name:       "wf",
		Ordering:   &protos.WorkflowOrdering{Key: "k"},
		Router:     &protos.TaskRouter{SourceAppID: "a"},
	})
	require.NoError(t, err)
	assert.Equal(t, "k", be.createdEvent.GetExecutionStarted().GetOrdering().GetKey())
	assert.Nil(t, be.watchRouter, "a keyed start returns without waiting")
}
