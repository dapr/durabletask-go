package tests

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// mockSink records delivered work items for verification.
type mockSink struct {
	mu          sync.Mutex
	items       []*protos.WorkItem
	closeCalled bool
}

func (s *mockSink) DeliverWorkItem(_ context.Context, wi *protos.WorkItem) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.items = append(s.items, wi)
	return nil
}

func (s *mockSink) Close(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closeCalled = true
	return nil
}

func (s *mockSink) Items() []*protos.WorkItem {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*protos.WorkItem{}, s.items...)
}

func (s *mockSink) WasCloseCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closeCalled
}

func newTestExecutorWithSQLite(t *testing.T) (backend.Executor, backend.Backend) {
	t.Helper()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), backend.DefaultLogger())
	ctx := context.Background()
	require.NoError(t, be.CreateTaskHub(ctx))
	t.Cleanup(func() { _ = be.DeleteTaskHub(ctx) })

	exec, _ := backend.NewGrpcExecutor(be, backend.DefaultLogger())
	return exec, be
}

// TestSinkRegistrar_TypeAssertion verifies that the executor returned from
// NewGrpcExecutor implements SinkRegistrar.
func TestSinkRegistrar_TypeAssertion(t *testing.T) {
	exec, _ := newTestExecutorWithSQLite(t)
	_, ok := exec.(backend.SinkRegistrar)
	require.True(t, ok, "executor should implement SinkRegistrar")
}

// TestSinkRegistrar_RegisterValidation tests input validation on RegisterSink.
func TestSinkRegistrar_RegisterValidation(t *testing.T) {
	exec, _ := newTestExecutorWithSQLite(t)
	reg := exec.(backend.SinkRegistrar)

	t.Run("empty prefixes", func(t *testing.T) {
		err := reg.RegisterSink(backend.SinkOptions{}, &mockSink{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "at least one")
	})

	t.Run("nil sink", func(t *testing.T) {
		err := reg.RegisterSink(backend.SinkOptions{WorkflowNamePrefix: "x."}, nil)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "must not be nil")
	})

	t.Run("happy path", func(t *testing.T) {
		err := reg.RegisterSink(backend.SinkOptions{WorkflowNamePrefix: "test."}, &mockSink{})
		require.NoError(t, err)
	})

	t.Run("duplicate key", func(t *testing.T) {
		err := reg.RegisterSink(backend.SinkOptions{WorkflowNamePrefix: "test."}, &mockSink{})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "already registered")
	})
}

// TestSinkRegistrar_Unregister tests UnregisterSink.
func TestSinkRegistrar_Unregister(t *testing.T) {
	exec, _ := newTestExecutorWithSQLite(t)
	reg := exec.(backend.SinkRegistrar)

	sink := &mockSink{}
	require.NoError(t, reg.RegisterSink(backend.SinkOptions{WorkflowNamePrefix: "foo."}, sink))

	t.Run("unregister existing", func(t *testing.T) {
		err := reg.UnregisterSink("foo.")
		require.NoError(t, err)
	})

	t.Run("unregister missing", func(t *testing.T) {
		err := reg.UnregisterSink("foo.")
		require.Error(t, err)
		assert.Contains(t, err.Error(), "no sink registered")
	})
}

// TestSinkRouting_WorkflowRoutedToSink verifies that a workflow whose name
// matches a registered sink prefix is delivered to the sink rather than the
// default gRPC work-item queue.
func TestSinkRouting_WorkflowRoutedToSink(t *testing.T) {
	exec, be := newTestExecutorWithSQLite(t)
	reg := exec.(backend.SinkRegistrar)

	sink := &mockSink{}
	require.NoError(t, reg.RegisterSink(backend.SinkOptions{
		WorkflowNamePrefix: "dapr.mcp.",
		ActivityNamePrefix: "dapr-mcp-",
	}, sink))

	iid := api.InstanceID("test-mcp-wf-1")

	// Create the workflow instance in the backend so the engine knows about it.
	startEvent := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name: "dapr.mcp.myserver.CallTool",
				WorkflowInstance: &protos.WorkflowInstance{
					InstanceId:  string(iid),
					ExecutionId: wrapperspb.String("exec-1"),
				},
			},
		},
	}
	require.NoError(t, be.CreateWorkflowInstance(context.Background(), startEvent))

	// ExecuteWorkflow blocks on WaitForWorkflowTaskCompletion. Use a short
	// timeout context so the test doesn't hang — we only care that the sink
	// received the work item, not that the workflow completed.
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	newEvents := []*protos.HistoryEvent{startEvent}
	_, _ = exec.ExecuteWorkflow(ctx, iid, nil, newEvents)

	// The sink should have received exactly one work item.
	items := sink.Items()
	require.Len(t, items, 1, "sink should have received one work item")
	assert.NotNil(t, items[0].GetWorkflowRequest())
	assert.Equal(t, string(iid), items[0].GetWorkflowRequest().GetInstanceId())
}

// TestSinkRouting_ActivityRoutedToSink verifies activity name-prefix routing.
func TestSinkRouting_ActivityRoutedToSink(t *testing.T) {
	exec, be := newTestExecutorWithSQLite(t)
	reg := exec.(backend.SinkRegistrar)

	sink := &mockSink{}
	require.NoError(t, reg.RegisterSink(backend.SinkOptions{
		WorkflowNamePrefix: "dapr.mcp.",
		ActivityNamePrefix: "dapr-mcp-",
	}, sink))

	iid := api.InstanceID("test-mcp-act-1")

	// We need a workflow in the backend for activity completion to coordinate
	// against.
	startEvent := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name: "dapr.mcp.myserver.CallTool",
				WorkflowInstance: &protos.WorkflowInstance{
					InstanceId:  string(iid),
					ExecutionId: wrapperspb.String("exec-1"),
				},
			},
		},
	}
	require.NoError(t, be.CreateWorkflowInstance(context.Background(), startEvent))

	activityEvent := &protos.HistoryEvent{
		EventId:   42,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_TaskScheduled{
			TaskScheduled: &protos.TaskScheduledEvent{
				Name:  "dapr-mcp-call-tool",
				Input: wrapperspb.String(`{"toolName":"echo"}`),
			},
		},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	_, _ = exec.ExecuteActivity(ctx, iid, activityEvent)

	items := sink.Items()
	require.Len(t, items, 1, "sink should have received one activity work item")
	assert.NotNil(t, items[0].GetActivityRequest())
	assert.Equal(t, "dapr-mcp-call-tool", items[0].GetActivityRequest().GetName())
}

// TestSinkRouting_NonMatchingGoesToQueue verifies that non-matching names are
// NOT routed to the sink (they go to the default work-item queue).
func TestSinkRouting_NonMatchingGoesToQueue(t *testing.T) {
	exec, be := newTestExecutorWithSQLite(t)
	reg := exec.(backend.SinkRegistrar)

	sink := &mockSink{}
	require.NoError(t, reg.RegisterSink(backend.SinkOptions{
		WorkflowNamePrefix: "dapr.mcp.",
	}, sink))

	iid := api.InstanceID("test-user-wf-1")
	startEvent := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name: "user.MyWorkflow",
				WorkflowInstance: &protos.WorkflowInstance{
					InstanceId:  string(iid),
					ExecutionId: wrapperspb.String("exec-user"),
				},
			},
		},
	}
	require.NoError(t, be.CreateWorkflowInstance(context.Background(), startEvent))

	// The non-matching workflow should try to enqueue on the gRPC queue.
	// Since nobody is draining that queue, this will block until ctx expires.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	_, _ = exec.ExecuteWorkflow(ctx, iid, nil, []*protos.HistoryEvent{startEvent})

	// The sink should NOT have received anything.
	assert.Empty(t, sink.Items(), "sink should not receive non-matching work items")
}

// TestShutdown_ClosesSinksFirst verifies that Shutdown calls sink.Close
// before closing the work-item queue.
func TestShutdown_ClosesSinksFirst(t *testing.T) {
	exec, _ := newTestExecutorWithSQLite(t)
	reg := exec.(backend.SinkRegistrar)

	sink := &mockSink{}
	require.NoError(t, reg.RegisterSink(backend.SinkOptions{
		WorkflowNamePrefix: "internal.",
	}, sink))

	require.NoError(t, exec.Shutdown(context.Background()))

	assert.True(t, sink.WasCloseCalled(), "sink.Close should have been called during Shutdown")
}
