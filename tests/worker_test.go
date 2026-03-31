package tests

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/runtimestate"
	"github.com/dapr/durabletask-go/tests/mocks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// https://github.com/stretchr/testify/issues/519
var (
	anyContext = mock.Anything
)

func Test_TryProcessSingleOrchestrationWorkItem_BasicFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	wi := &backend.OrchestrationWorkItem{
		InstanceID: "test123",
		NewEvents: []*protos.HistoryEvent{
			{
				EventId:   -1,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_ExecutionStarted{
					ExecutionStarted: &protos.ExecutionStartedEvent{
						Name: "MyOrch",
						OrchestrationInstance: &protos.OrchestrationInstance{
							InstanceId:  "test123",
							ExecutionId: wrapperspb.String(uuid.New().String()),
						},
					},
				},
			},
		},
	}
	state := &backend.OrchestrationRuntimeState{}
	result := &protos.OrchestratorResponse{}

	be := mocks.NewBackend(t)
	be.EXPECT().GetOrchestrationRuntimeState(anyContext, wi).Return(state, nil).Once()
	be.EXPECT().CompleteOrchestrationWorkItem(anyContext, wi).Return(nil).Once()

	ex := mocks.NewExecutor(t)
	ex.EXPECT().ExecuteOrchestrator(anyContext, wi.InstanceID, state.OldEvents, mock.Anything).Return(result, nil).Once()

	worker := backend.NewOrchestrationWorker(backend.OrchestratorOptions{
		Backend:  be,
		Executor: ex,
		Logger:   logger,
		AppID:    "testapp",
	})
	go worker.Start(ctx)

	callback := make(chan error, 1)
	worker.Dispatch(wi, callback)

	select {
	case err := <-callback:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("dispatch callback not received within timeout")
	}

	cancel()

	t.Logf("state.NewEvents: %v", state.NewEvents)
	require.Len(t, state.NewEvents, 2)
	require.NotNil(t, wi.State.NewEvents[0].GetOrchestratorStarted())
	require.NotNil(t, wi.State.NewEvents[1].GetExecutionStarted())
}

func Test_TryProcessSingleOrchestrationWorkItem_Idempotency(t *testing.T) {
	workflowID := "test123"
	wi := &backend.OrchestrationWorkItem{
		InstanceID: api.InstanceID(workflowID),
		NewEvents: []*protos.HistoryEvent{
			{
				EventId:   -1,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_ExecutionStarted{
					ExecutionStarted: &protos.ExecutionStartedEvent{
						Name: "MyOrch",
						OrchestrationInstance: &protos.OrchestrationInstance{
							InstanceId:  workflowID,
							ExecutionId: wrapperspb.String(uuid.New().String()),
						},
					},
				},
			},
		},
		State: runtimestate.NewOrchestrationRuntimeState(workflowID, nil, []*protos.HistoryEvent{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	be := mocks.NewBackend(t)
	ex := mocks.NewExecutor(t)

	callNumber := 0
	ex.EXPECT().ExecuteOrchestrator(anyContext, wi.InstanceID, wi.State.OldEvents, mock.Anything).RunAndReturn(func(ctx context.Context, iid api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*protos.OrchestratorResponse, error) {
		callNumber++
		logger.Debugf("execute orchestrator called %d times", callNumber)
		if callNumber == 1 {
			return nil, errors.New("dummy error")
		}
		return &protos.OrchestratorResponse{}, nil
	}).Times(2)

	be.EXPECT().AbandonOrchestrationWorkItem(anyContext, wi).Return(nil).Once()
	be.EXPECT().CompleteOrchestrationWorkItem(anyContext, wi).Return(nil).Once()

	worker := backend.NewOrchestrationWorker(backend.OrchestratorOptions{
		Backend:  be,
		Executor: ex,
		Logger:   logger,
		AppID:    "testapp",
	}, backend.WithMaxParallelism(1))
	go worker.Start(ctx)

	// First dispatch: orchestrator returns an error, work item should be abandoned.
	cb1 := make(chan error, 1)
	worker.Dispatch(wi, cb1)
	select {
	case <-cb1:
	case <-time.After(1 * time.Second):
		t.Fatal("first dispatch callback not received within timeout")
	}

	// Second dispatch: orchestrator succeeds, work item should be completed.
	cb2 := make(chan error, 1)
	worker.Dispatch(wi, cb2)
	select {
	case <-cb2:
	case <-time.After(1 * time.Second):
		t.Fatal("second dispatch callback not received within timeout")
	}

	cancel()

	t.Logf("state.NewEvents: %v", wi.State.NewEvents)
	require.Len(t, wi.State.NewEvents, 3)
	require.NotNil(t, wi.State.NewEvents[0].GetOrchestratorStarted())
	require.NotNil(t, wi.State.NewEvents[1].GetExecutionStarted())
	require.NotNil(t, wi.State.NewEvents[2].GetOrchestratorStarted())
}

func Test_TryProcessSingleOrchestrationWorkItem_ExecutionStartedAndCompleted(t *testing.T) {
	iid := api.InstanceID("test123")

	// Simulate getting an ExecutionStarted message from the orchestration queue
	wi := &backend.OrchestrationWorkItem{
		InstanceID: iid,
		NewEvents: []*protos.HistoryEvent{
			{
				EventId:   -1,
				Timestamp: timestamppb.New(time.Now()),
				EventType: &protos.HistoryEvent_ExecutionStarted{
					ExecutionStarted: &protos.ExecutionStartedEvent{
						Name: "MyOrchestration",
						OrchestrationInstance: &protos.OrchestrationInstance{
							InstanceId:  string(iid),
							ExecutionId: wrapperspb.String(uuid.New().String()),
						},
					},
				},
			},
		},
	}

	// Empty orchestration runtime state since we're starting a new execution from scratch
	state := runtimestate.NewOrchestrationRuntimeState(string(iid), nil, []*protos.HistoryEvent{})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	be := mocks.NewBackend(t)
	be.EXPECT().GetOrchestrationRuntimeState(anyContext, wi).Return(state, nil).Once()

	ex := mocks.NewExecutor(t)

	// Return an execution completed action to simulate the completion of the orchestration (a no-op)
	resultValue := "done"
	result := &protos.OrchestratorResponse{
		Actions: []*protos.OrchestratorAction{
			{
				Id: -1,
				OrchestratorActionType: &protos.OrchestratorAction_CompleteOrchestration{
					CompleteOrchestration: &protos.CompleteOrchestrationAction{
						OrchestrationStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
						Result:              wrapperspb.String(resultValue),
					},
				},
			},
		},
	}

	// Execute should be called with an empty oldEvents list. NewEvents should contain two items,
	// but there doesn't seem to be a good way to assert this.
	ex.EXPECT().ExecuteOrchestrator(anyContext, iid, []*protos.HistoryEvent{}, mock.Anything).Return(result, nil).Once()

	// After execution, the Complete action should be called
	be.EXPECT().CompleteOrchestrationWorkItem(anyContext, wi).Return(nil).Once()

	// Set up and run the test
	worker := backend.NewOrchestrationWorker(backend.OrchestratorOptions{
		Backend:  be,
		Executor: ex,
		Logger:   logger,
		AppID:    "testapp",
	})
	go worker.Start(ctx)

	callback := make(chan error, 1)
	worker.Dispatch(wi, callback)

	select {
	case err := <-callback:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("dispatch callback not received within timeout")
	}

	cancel()

	t.Logf("state.NewEvents: %v", state.NewEvents)
	require.Len(t, state.NewEvents, 3)
	require.NotNil(t, wi.State.NewEvents[0].GetOrchestratorStarted())
	require.NotNil(t, wi.State.NewEvents[1].GetExecutionStarted())
	require.NotNil(t, wi.State.NewEvents[2].GetExecutionCompleted())
}

func Test_TaskWorker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp := mocks.NewTestTaskPocessor[*backend.ActivityWorkItem]("test")
	tp.UnblockProcessing()

	first := &backend.ActivityWorkItem{
		SequenceNumber: 1,
	}
	second := &backend.ActivityWorkItem{
		SequenceNumber: 2,
	}

	worker := backend.NewTaskWorker[*backend.ActivityWorkItem](tp, logger, backend.WithMaxParallelism(1))
	go worker.Start(ctx)

	cb1 := make(chan error, 1)
	cb2 := make(chan error, 1)
	worker.Dispatch(first, cb1)
	worker.Dispatch(second, cb2)

	select {
	case <-cb1:
	case <-time.After(1 * time.Second):
		t.Fatal("first dispatch callback not received within timeout")
	}
	select {
	case <-cb2:
	case <-time.After(1 * time.Second):
		t.Fatal("second dispatch callback not received within timeout")
	}

	require.Len(t, tp.AbandonedWorkItems(), 0)
	require.Len(t, tp.CompletedWorkItems(), 2)
	require.Equal(t, first, tp.CompletedWorkItems()[0])
	require.Equal(t, second, tp.CompletedWorkItems()[1])
}

func Test_StartAndStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	tp := mocks.NewTestTaskPocessor[*backend.ActivityWorkItem]("test")
	tp.BlockProcessing()

	first := &backend.ActivityWorkItem{
		SequenceNumber: 1,
	}

	worker := backend.NewTaskWorker[*backend.ActivityWorkItem](tp, logger, backend.WithMaxParallelism(1))

	startDone := make(chan error, 1)
	go func() {
		startDone <- worker.Start(ctx)
	}()

	// Dispatch a work item that will block on processing.
	cb := make(chan error, 1)
	worker.Dispatch(first, cb)

	// The work item should be in-flight (blocked). Give it a moment to start.
	time.Sleep(50 * time.Millisecond)

	// Cancel context to stop the worker. This unblocks processing and causes abandon.
	cancel()

	select {
	case <-startDone:
	case <-time.After(1 * time.Second):
		t.Fatalf("worker start not finished within timeout")
	}

	require.Len(t, tp.AbandonedWorkItems(), 1)
	require.Equal(t, first, tp.AbandonedWorkItems()[0])
	require.Len(t, tp.CompletedWorkItems(), 0)
}
