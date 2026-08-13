package backend

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
)

type TaskHubWorker interface {
	// Start starts the backend and the configured internal workers.
	Start(context.Context) error

	// Shutdown stops the backend and all internal workers.
	Shutdown(context.Context) error
}

type taskHubWorker struct {
	backend        Backend
	workflowWorker TaskWorker[*WorkflowWorkItem]
	activityWorker TaskWorker[*ActivityWorkItem]
	logger         *slog.Logger
}

func NewTaskHubWorker(be Backend, workflowWorker TaskWorker[*WorkflowWorkItem], activityWorker TaskWorker[*ActivityWorkItem], logger Logger) TaskHubWorker {
	return &taskHubWorker{
		backend:        be,
		workflowWorker: workflowWorker,
		activityWorker: activityWorker,
		logger:         SlogFromLogger(logger),
	}
}

func (w *taskHubWorker) Start(ctx context.Context) error {
	if err := w.backend.CreateTaskHub(ctx); err != nil && err != ErrTaskHubExists {
		return err
	}

	if err := w.backend.Start(ctx); err != nil {
		return err
	}

	w.logger.Info("worker started", "backend", lazyString(func() string { return fmt.Sprintf("%v", w.backend) }))

	w.workflowWorker.Start(ctx)
	w.activityWorker.Start(ctx)
	return nil
}

func (w *taskHubWorker) Shutdown(ctx context.Context) error {
	w.logger.Info("backend stopping...")
	if err := w.backend.Stop(ctx); err != nil {
		return err
	}

	w.logger.Info("workers stopping and draining...")
	defer w.logger.Info("finished stopping and draining workers!")

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		w.workflowWorker.StopAndDrain()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		w.activityWorker.StopAndDrain()
	}()

	wg.Wait()

	return nil
}
