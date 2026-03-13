package worker

import (
	"context"
	"fmt"

	"github.com/dapr/durabletask-go/backend/loops"
)

// Logger is the logging interface needed by the worker handler.
type Logger interface {
	Debugf(format string, v ...any)
	Infof(format string, v ...any)
	Errorf(format string, v ...any)
}

// Processor is the interface for processing work items.
type Processor[T any] interface {
	Name() string
	ProcessWorkItem(context.Context, T) error
	AbandonWorkItem(context.Context, T) error
	CompleteWorkItem(context.Context, T) error
}

// Options configures the worker loop handler.
type Options[T fmt.Stringer] struct {
	Processor Processor[T]
	Logger    Logger
}

type worker[T fmt.Stringer] struct {
	logger    Logger
	processor Processor[T]
}

// New creates a new worker handler. The caller is responsible for creating the
// loop and wiring this handler into it.
func New[T fmt.Stringer](opts Options[T]) *worker[T] {
	return &worker[T]{
		processor: opts.Processor,
		logger:    opts.Logger,
	}
}

// Handle implements loop.Handler[loops.EventWorker].
func (w *worker[T]) Handle(ctx context.Context, event loops.EventWorker) error {
	switch e := event.(type) {
	case *loops.DispatchWorkItem:
		w.handleDispatch(ctx, e)
	case *loops.Shutdown:
		w.logger.Infof("%v: worker stopped", w.processor.Name())
	}

	return nil
}

func (w *worker[T]) handleDispatch(ctx context.Context, e *loops.DispatchWorkItem) {
	wi := e.WorkItem.(T)
	w.processWorkItem(ctx, wi)
	e.Callback <- nil
}

func (w *worker[T]) processWorkItem(ctx context.Context, wi T) {
	w.logger.Debugf("%v: processing work item: %s", w.processor.Name(), wi)

	if err := w.processor.ProcessWorkItem(ctx, wi); err != nil {
		w.logger.Errorf("%v: failed to process work item: %v", w.processor.Name(), err)
		if err = w.processor.AbandonWorkItem(context.Background(), wi); err != nil {
			w.logger.Errorf("%v: failed to abandon work item: %v", w.processor.Name(), err)
		}
		return
	}

	if err := w.processor.CompleteWorkItem(ctx, wi); err != nil {
		w.logger.Errorf("%v: failed to complete work item: %v", w.processor.Name(), err)
		if err = w.processor.AbandonWorkItem(context.Background(), wi); err != nil {
			w.logger.Errorf("%v: failed to abandon work item: %v", w.processor.Name(), err)
		}
		return
	}

	w.logger.Debugf("%v: work item processed successfully", w.processor.Name())
}
