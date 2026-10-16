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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

var errNoHost = errors.New("no host for the workflow")

// acceptOnlyBackend accepts every new event but cannot report on the
// instance, the way a backend behaves when it has held a request for an
// instance that no worker hosts yet.
type acceptOnlyBackend struct {
	Backend

	added []*HistoryEvent
}

func (b *acceptOnlyBackend) AddNewWorkflowEvent(_ context.Context, _ api.InstanceID, e *HistoryEvent) error {
	b.added = append(b.added, e)
	return nil
}

func (b *acceptOnlyBackend) WatchWorkflowRuntimeStatus(context.Context, api.InstanceID, *protos.TaskRouter, func(*WorkflowMetadata) bool) error {
	return errNoHost
}

func TestGrpcExecutor_SkipWaitForStateChange(t *testing.T) {
	const iid = "iid"

	calls := map[string]func(*grpcExecutor) error{
		"terminate": func(g *grpcExecutor) error {
			_, err := g.TerminateInstance(t.Context(), &protos.TerminateRequest{InstanceId: iid})
			return err
		},
		"suspend": func(g *grpcExecutor) error {
			_, err := g.SuspendInstance(t.Context(), &protos.SuspendRequest{InstanceId: iid, Reason: wrapperspb.String("r")})
			return err
		},
		"resume": func(g *grpcExecutor) error {
			_, err := g.ResumeInstance(t.Context(), &protos.ResumeRequest{InstanceId: iid, Reason: wrapperspb.String("r")})
			return err
		},
	}

	for name, call := range calls {
		t.Run(name+" waits by default", func(t *testing.T) {
			be := new(acceptOnlyBackend)
			exec, _ := NewGrpcExecutor(be, defaultLogger)
			g, ok := exec.(*grpcExecutor)
			require.True(t, ok)

			require.ErrorIs(t, call(g), errNoHost)
			assert.Len(t, be.added, 1)
		})

		t.Run(name+" returns once accepted with skip-wait", func(t *testing.T) {
			be := new(acceptOnlyBackend)
			exec, _ := NewGrpcExecutor(be, defaultLogger, WithSkipWaitForStateChange())
			g, ok := exec.(*grpcExecutor)
			require.True(t, ok)

			require.NoError(t, call(g))
			assert.Len(t, be.added, 1)
		})
	}
}
