package tests

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/runtimestate"
	"github.com/dapr/durabletask-go/tests/mocks"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
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
	ctx := context.Background()
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
	result := &backend.ExecutionResults{Response: &protos.OrchestratorResponse{}}

	ctx, cancel := context.WithCancel(ctx)
	completed := atomic.Bool{}
	be := mocks.NewBackend(t)
	be.EXPECT().NextOrchestrationWorkItem(anyContext).Return(wi, nil).Once()
	be.EXPECT().NextOrchestrationWorkItem(anyContext).Return(nil, errors.New("")).Once().Run(func(mock.Arguments) {
		cancel()
	})
	be.EXPECT().GetOrchestrationRuntimeState(anyContext, wi).Return(state, nil).Once()
	be.EXPECT().CompleteOrchestrationWorkItem(anyContext, wi).RunAndReturn(func(ctx context.Context, owi *backend.OrchestrationWorkItem) error {
		completed.Store(true)
		return nil
	}).Once()

	ex := mocks.NewExecutor(t)
	ex.EXPECT().ExecuteOrchestrator(anyContext, wi.InstanceID, state.OldEvents, mock.Anything).Return(result, nil).Once()

	worker := backend.NewOrchestrationWorker(be, ex, logger)
	worker.Start(ctx)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		if !completed.Load() {
			collect.Errorf("process next not called CompleteOrchestrationWorkItem yet")
		}
	}, 1*time.Second, 100*time.Millisecond)

	worker.StopAndDrain()

	t.Logf("state.NewEvents: %v", state.NewEvents)
	require.Len(t, state.NewEvents, 2)
	require.True(t, state.NewEvents[0].GetOrchestratorStarted() != nil)
	require.True(t, state.NewEvents[1].GetExecutionStarted() != nil)
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

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)

	completed := atomic.Bool{}
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

	be.EXPECT().NextOrchestrationWorkItem(anyContext).Return(wi, nil).Once()
	be.EXPECT().AbandonOrchestrationWorkItem(anyContext, wi).Return(nil).Once()

	be.EXPECT().NextOrchestrationWorkItem(anyContext).Return(wi, nil).Once()
	be.EXPECT().CompleteOrchestrationWorkItem(anyContext, wi).RunAndReturn(func(ctx context.Context, owi *backend.OrchestrationWorkItem) error {
		completed.Store(true)
		return nil
	}).Once()

	be.EXPECT().NextOrchestrationWorkItem(anyContext).Return(nil, errors.New("")).Once().Run(func(mock.Arguments) {
		cancel()
	})

	worker := backend.NewOrchestrationWorker(be, ex, logger, backend.WithMaxParallelism(1))
	worker.Start(ctx)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		if !completed.Load() {
			collect.Errorf("process next not called CompleteOrchestrationWorkItem yet")
		}
	}, 2*time.Second, 100*time.Millisecond)

	worker.StopAndDrain()

	t.Logf("state.NewEvents: %v", wi.State.NewEvents)
	require.Len(t, wi.State.NewEvents, 3)
	require.True(t, wi.State.NewEvents[0].GetOrchestratorStarted() != nil)
	require.True(t, wi.State.NewEvents[1].GetExecutionStarted() != nil)
	require.True(t, wi.State.NewEvents[2].GetOrchestratorStarted() != nil)
}

func Test_TryProcessSingleOrchestrationWorkItem_ExecutionStartedAndCompleted(t *testing.T) {
	ctx := context.Background()
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

	ctx, cancel := context.WithCancel(ctx)
	be := mocks.NewBackend(t)
	be.EXPECT().NextOrchestrationWorkItem(anyContext).Return(wi, nil).Once()
	be.EXPECT().NextOrchestrationWorkItem(anyContext).Return(nil, errors.New("")).Once().Run(func(mock.Arguments) {
		cancel()
	})

	be.EXPECT().GetOrchestrationRuntimeState(anyContext, wi).Return(state, nil).Once()

	ex := mocks.NewExecutor(t)

	// Return an execution completed action to simulate the completion of the orchestration (a no-op)
	resultValue := "done"
	result := &backend.ExecutionResults{
		Response: &protos.OrchestratorResponse{
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
		},
	}

	// Execute should be called with an empty oldEvents list. NewEvents should contain two items,
	// but there doesn't seem to be a good way to assert this.
	ex.EXPECT().ExecuteOrchestrator(anyContext, iid, []*protos.HistoryEvent{}, mock.Anything).Return(result, nil).Once()

	// After execution, the Complete action should be called
	completed := atomic.Bool{}
	be.EXPECT().CompleteOrchestrationWorkItem(anyContext, wi).RunAndReturn(func(ctx context.Context, owi *backend.OrchestrationWorkItem) error {
		completed.Store(true)
		return nil
	}).Once()

	// Set up and run the test
	worker := backend.NewOrchestrationWorker(be, ex, logger)
	worker.Start(ctx)
	//ok, err := worker.ProcessNext(ctx)
	//// Successfully processing a work-item should result in a nil error
	//assert.Nil(t, err)
	//assert.True(t, ok)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		if !completed.Load() {
			collect.Errorf("process next not called CompleteOrchestrationWorkItem yet")
		}
	}, 1*time.Second, 100*time.Millisecond)

	worker.StopAndDrain()
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
	tp.AddWorkItems(first, second)

	worker := backend.NewTaskWorker[*backend.ActivityWorkItem](tp, logger)

	worker.Start(ctx)

	require.EventuallyWithT(t, func(collect *assert.CollectT) {
		if len(tp.PendingWorkItems()) == 0 {
			return
		}
		collect.Errorf("work items not consumed yet")
	}, 500*time.Millisecond, 100*time.Millisecond)

	require.Len(t, tp.PendingWorkItems(), 0)
	require.Len(t, tp.AbandonedWorkItems(), 0)
	require.Len(t, tp.CompletedWorkItems(), 2)
	require.Equal(t, first, tp.CompletedWorkItems()[0])
	require.Equal(t, second, tp.CompletedWorkItems()[1])

	drainFinished := make(chan bool)
	go func() {
		worker.StopAndDrain()
		drainFinished <- true
	}()

	select {
	case <-drainFinished:
		return
	case <-time.After(1 * time.Second):
		t.Fatalf("worker stop and drain not finished within timeout")
	}

}

func Test_StartAndStop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp := mocks.NewTestTaskPocessor[*backend.ActivityWorkItem]("test")
	tp.BlockProcessing()

	first := backend.ActivityWorkItem{
		SequenceNumber: 1,
	}
	second := backend.ActivityWorkItem{
		SequenceNumber: 2,
	}
	tp.AddWorkItems(&first, &second)

	worker := backend.NewTaskWorker[*backend.ActivityWorkItem](tp, logger)

	worker.Start(ctx)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Len(c, tp.PendingWorkItems(), 1)
	}, time.Second*5, 100*time.Millisecond)

	// due to the configuration of the TestTaskProcessor, now the work item is blocked on ProcessWorkItem until the context is cancelled
	drainFinished := make(chan bool)
	go func() {
		worker.StopAndDrain()
		drainFinished <- true
	}()

	select {
	case <-drainFinished:
		return
	case <-time.After(1 * time.Second):
		t.Fatalf("worker stop and drain not finished within timeout")
	}

	require.Len(t, tp.PendingWorkItems(), 1)
	require.Equal(t, second, tp.PendingWorkItems()[0])
	require.Len(t, tp.AbandonedWorkItems(), 1)
	require.Equal(t, first, tp.AbandonedWorkItems()[0])
	require.Len(t, tp.CompletedWorkItems(), 0)
}
