package tests

import (
	"context"
	"testing"

	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/tests/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func Test_TaskHubWorkerStartsDependencies(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	be := mocks.NewBackend(t)
	orchWorker := mocks.NewTaskWorker[*backend.OrchestrationWorkItem](t)
	actWorker := mocks.NewTaskWorker[*backend.ActivityWorkItem](t)

	be.EXPECT().CreateTaskHub(mock.Anything).Return(nil).Once()

	// All Start methods block until context is cancelled. Return nil on cancel.
	be.EXPECT().Start(mock.Anything).RunAndReturn(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}).Once()
	orchWorker.EXPECT().Start(mock.Anything).RunAndReturn(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}).Once()
	actWorker.EXPECT().Start(mock.Anything).RunAndReturn(func(ctx context.Context) error {
		<-ctx.Done()
		return nil
	}).Once()

	// The pollAndDispatch goroutines will call Next*WorkItem on the backend.
	be.On("NextOrchestrationWorkItem", mock.Anything).Return(nil, context.Canceled).Maybe()
	be.On("NextActivityWorkItem", mock.Anything).Return(nil, context.Canceled).Maybe()

	// Stop is called during cleanup.
	be.EXPECT().Stop(mock.Anything).Return(nil).Once()

	w := backend.NewTaskHubWorker(be, orchWorker, actWorker, logger)

	errCh := make(chan error, 1)
	go func() {
		errCh <- w.Start(ctx)
	}()

	// Cancel context to unblock Start.
	cancel()

	err := <-errCh
	assert.NoError(t, err)
}
