package backend

import (
	"context"
	"errors"

	"github.com/dapr/kit/concurrency"
)

type TaskHubWorker interface {
	// Start starts the backend and the configured internal workers. Blocks
	// until the context is cancelled.
	Start(context.Context) error
}

type taskHubWorker struct {
	backend             Backend
	orchestrationWorker TaskWorker[*OrchestrationWorkItem]
	activityWorker      TaskWorker[*ActivityWorkItem]
	logger              Logger
}

func NewTaskHubWorker(be Backend, orchestrationWorker TaskWorker[*OrchestrationWorkItem], activityWorker TaskWorker[*ActivityWorkItem], logger Logger) TaskHubWorker {
	return &taskHubWorker{
		backend:             be,
		orchestrationWorker: orchestrationWorker,
		activityWorker:      activityWorker,
		logger:              logger,
	}
}

func (w *taskHubWorker) Start(ctx context.Context) error {
	if err := w.backend.CreateTaskHub(ctx); err != nil && err != ErrTaskHubExists {
		return err
	}

	w.logger.Infof("worker started with backend %v", w.backend)

	manager := concurrency.NewRunnerManager(
		w.backend.Start,
		w.orchestrationWorker.Start,
		w.activityWorker.Start,
		func(ctx context.Context) error {
			pollAndDispatch(ctx, w.backend.NextOrchestrationWorkItem, w.orchestrationWorker, w.logger)
			return nil
		},
		func(ctx context.Context) error {
			pollAndDispatch(ctx, w.backend.NextActivityWorkItem, w.activityWorker, w.logger)
			return nil
		},
	)

	defer func() {
		w.logger.Info("backend stopping...")
		w.backend.Stop(context.Background())
	}()

	return manager.Run(ctx)
}

// pollAndDispatch bridges a blocking NextWorkItem call into Dispatch. It runs
// until the context is cancelled.
func pollAndDispatch[T WorkItem](ctx context.Context, next func(context.Context) (T, error), worker TaskWorker[T], logger Logger) {
	for {
		wi, err := next(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return
			}
			logger.Errorf("failed to get next work item: %v", err)
			continue
		}

		// Fire-and-forget: the callback is not needed for poll-based
		// backends since they don't need completion signaling.
		worker.Dispatch(wi, make(chan error, 1))
	}
}
