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

package tests_grpc

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/client"
	"github.com/dapr/durabletask-go/tests/mocks"
)

// Test_Grpc_GetInstance_CompletedAt checks that the completion time in the
// backend metadata reaches the client through GetInstance.
func Test_Grpc_GetInstance_CompletedAt(t *testing.T) {
	be := mocks.NewBackend(t)
	_, register := backend.NewGrpcExecutor(be, backend.DefaultLogger())
	srv := grpc.NewServer()
	register(srv)
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	c := client.NewTaskHubGrpcClient(conn, backend.DefaultLogger())

	created := time.Date(2026, 11, 20, 10, 0, 0, 0, time.UTC)
	completed := created.Add(time.Minute)
	be.EXPECT().GetWorkflowMetadata(mock.Anything, api.InstanceID("done"), mock.Anything).Return(&protos.WorkflowMetadata{
		InstanceId:    "done",
		Name:          "foo",
		RuntimeStatus: api.RUNTIME_STATUS_COMPLETED,
		CreatedAt:     timestamppb.New(created),
		StartedAt:     timestamppb.New(created),
		LastUpdatedAt: timestamppb.New(completed),
		CompletedAt:   timestamppb.New(completed),
	}, nil).Once()
	be.EXPECT().GetWorkflowMetadata(mock.Anything, api.InstanceID("running"), mock.Anything).Return(&protos.WorkflowMetadata{
		InstanceId:    "running",
		Name:          "foo",
		RuntimeStatus: api.RUNTIME_STATUS_RUNNING,
		CreatedAt:     timestamppb.New(created),
		LastUpdatedAt: timestamppb.New(created),
	}, nil).Once()

	md, err := c.FetchWorkflowMetadata(ctx, "done")
	require.NoError(t, err)
	require.NotNil(t, md.GetCompletedAt())
	assert.True(t, completed.Equal(md.GetCompletedAt().AsTime()), "completedAt %v", md.GetCompletedAt().AsTime())
	assert.True(t, created.Equal(md.GetStartedAt().AsTime()), "startedAt %v", md.GetStartedAt().AsTime())

	md, err = c.FetchWorkflowMetadata(ctx, "running")
	require.NoError(t, err)
	assert.Nil(t, md.GetCompletedAt())
}
