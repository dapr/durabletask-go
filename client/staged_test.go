package client

import (
	"context"
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
)

// stagedServer answers every client operation with the given staged value.
type stagedServer struct {
	protos.UnimplementedTaskHubSidecarServiceServer
	staged bool
}

func (s *stagedServer) StartInstance(_ context.Context, req *protos.CreateInstanceRequest) (*protos.CreateInstanceResponse, error) {
	return &protos.CreateInstanceResponse{InstanceId: req.GetInstanceId(), Staged: s.staged}, nil
}

func (s *stagedServer) RaiseEvent(context.Context, *protos.RaiseEventRequest) (*protos.RaiseEventResponse, error) {
	return &protos.RaiseEventResponse{Staged: s.staged}, nil
}

func (s *stagedServer) TerminateInstance(context.Context, *protos.TerminateRequest) (*protos.TerminateResponse, error) {
	return &protos.TerminateResponse{Staged: s.staged}, nil
}

func (s *stagedServer) SuspendInstance(context.Context, *protos.SuspendRequest) (*protos.SuspendResponse, error) {
	return &protos.SuspendResponse{Staged: s.staged}, nil
}

func (s *stagedServer) ResumeInstance(context.Context, *protos.ResumeRequest) (*protos.ResumeResponse, error) {
	return &protos.ResumeResponse{Staged: s.staged}, nil
}

func (s *stagedServer) RerunWorkflowFromEvent(_ context.Context, req *protos.RerunWorkflowFromEventRequest) (*protos.RerunWorkflowFromEventResponse, error) {
	return &protos.RerunWorkflowFromEventResponse{NewInstanceID: req.GetNewInstanceID(), Staged: s.staged}, nil
}

func newStagedClient(t *testing.T, staged bool) *TaskHubGrpcClient {
	t.Helper()

	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	protos.RegisterTaskHubSidecarServiceServer(srv, &stagedServer{staged: staged})
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { conn.Close() })

	return NewTaskHubGrpcClient(conn, backend.DefaultLogger())
}

func TestStagedResults(t *testing.T) {
	for _, staged := range []bool{true, false} {
		c := newStagedClient(t, staged)
		ctx := t.Context()

		start, err := c.ScheduleNewWorkflowWithResult(ctx, "wf", api.WithInstanceID("abc"))
		require.NoError(t, err)
		assert.Equal(t, api.InstanceID("abc"), start.InstanceID)
		assert.Equal(t, staged, start.Staged)

		raise, err := c.RaiseEventWithResult(ctx, "abc", "event")
		require.NoError(t, err)
		assert.Equal(t, staged, raise.Staged)

		terminate, err := c.TerminateWorkflowWithResult(ctx, "abc")
		require.NoError(t, err)
		assert.Equal(t, staged, terminate.Staged)

		suspend, err := c.SuspendWorkflowWithResult(ctx, "abc", "")
		require.NoError(t, err)
		assert.Equal(t, staged, suspend.Staged)

		resume, err := c.ResumeWorkflowWithResult(ctx, "abc", "")
		require.NoError(t, err)
		assert.Equal(t, staged, resume.Staged)

		rerun, err := c.RerunWorkflowFromEventWithResult(ctx, "abc", 0, api.WithRerunNewInstanceID("def"))
		require.NoError(t, err)
		assert.Equal(t, api.InstanceID("def"), rerun.NewInstanceID)
		assert.Equal(t, staged, rerun.Staged)
	}
}

func TestStagedOptions(t *testing.T) {
	for _, staged := range []bool{true, false} {
		c := newStagedClient(t, staged)
		ctx := t.Context()

		// Start each destination at the opposite value, to see the call set it.
		dst := func() *bool { v := !staged; return &v }

		got := dst()
		id, err := c.ScheduleNewWorkflow(ctx, "wf", api.WithInstanceID("abc"), api.WithStartStaged(got))
		require.NoError(t, err)
		assert.Equal(t, api.InstanceID("abc"), id)
		assert.Equal(t, staged, *got)

		got = dst()
		require.NoError(t, c.RaiseEvent(ctx, "abc", "event", api.WithRaiseEventStaged(got)))
		assert.Equal(t, staged, *got)

		got = dst()
		require.NoError(t, c.TerminateWorkflow(ctx, "abc", api.WithTerminateStaged(got)))
		assert.Equal(t, staged, *got)

		got = dst()
		require.NoError(t, c.SuspendWorkflow(ctx, "abc", "", api.WithSuspendStaged(got)))
		assert.Equal(t, staged, *got)

		got = dst()
		require.NoError(t, c.ResumeWorkflow(ctx, "abc", "", api.WithResumeStaged(got)))
		assert.Equal(t, staged, *got)

		got = dst()
		newID, err := c.RerunWorkflowFromEvent(ctx, "abc", 0, api.WithRerunNewInstanceID("def"), api.WithRerunStaged(got))
		require.NoError(t, err)
		assert.Equal(t, api.InstanceID("def"), newID)
		assert.Equal(t, staged, *got)
	}
}

func TestStagedOptionNotSetOnError(t *testing.T) {
	c := newStagedClient(t, true)

	staged := false
	failing := func(*protos.TerminateRequest) error { return errors.New("bad option") }
	err := c.TerminateWorkflow(t.Context(), "abc", api.WithTerminateStaged(&staged), failing)
	require.Error(t, err)
	assert.False(t, staged)
}
