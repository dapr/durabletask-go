package backend

import (
	"context"
	"sync/atomic"

	"github.com/dapr/durabletask-go/backend/loops"
	loopworker "github.com/dapr/durabletask-go/backend/loops/worker"
	"github.com/dapr/kit/concurrency"
	"github.com/dapr/kit/events/loop"
)

type TaskWorker[T WorkItem] interface {
	// Start starts the worker loops and blocks until the context is cancelled.
	Start(context.Context) error

	// Dispatch pushes a work item directly to a worker loop. The callback
	// channel receives nil on completion or an error if the work item was
	// abandoned. Dispatch round-robins across worker loops.
	Dispatch(wi T, callback chan<- error)
}

type TaskProcessor[T WorkItem] interface {
	Name() string
	ProcessWorkItem(context.Context, T) error
	AbandonWorkItem(context.Context, T) error
	CompleteWorkItem(context.Context, T) error
}

type taskWorker[T WorkItem] struct {
	workers    []loop.Interface[loops.EventWorker]
	nextWorker atomic.Uint64
}

type NewTaskWorkerOptions func(*WorkerOptions)

type WorkerOptions struct {
	MaxParallelWorkItems *int32
}

func NewWorkerOptions() *WorkerOptions {
	return &WorkerOptions{}
}

func WithMaxParallelism(n int32) NewTaskWorkerOptions {
	return func(o *WorkerOptions) {
		o.MaxParallelWorkItems = &n
	}
}

func NewTaskWorker[T WorkItem](p TaskProcessor[T], logger Logger, opts ...NewTaskWorkerOptions) TaskWorker[T] {
	options := &WorkerOptions{}
	for _, configure := range opts {
		configure(options)
	}

	n := int32(1)
	if options.MaxParallelWorkItems != nil && *options.MaxParallelWorkItems > 1 {
		n = *options.MaxParallelWorkItems
	}

	workers := make([]loop.Interface[loops.EventWorker], n)
	for i := range workers {
		handler := loopworker.New(loopworker.Options[T]{
			Processor: p,
			Logger:    logger,
		})
		workers[i] = loop.New[loops.EventWorker](64).NewLoop(handler)
	}

	return &taskWorker[T]{
		workers: workers,
	}
}

func (w *taskWorker[T]) Start(ctx context.Context) error {
	manager := concurrency.NewRunnerManager()
	for _, worker := range w.workers {
		manager.Add(worker.Run)
	}
	// When context is cancelled, close all worker loops so their Run methods
	// unblock from the channel read and return.
	manager.Add(func(ctx context.Context) error {
		<-ctx.Done()
		for _, worker := range w.workers {
			worker.Close(new(loops.Shutdown))
		}
		return nil
	})
	return manager.Run(ctx)
}

func (w *taskWorker[T]) Dispatch(wi T, callback chan<- error) {
	// Round-robin across worker loops.
	idx := w.nextWorker.Add(1) - 1
	w.workers[idx%uint64(len(w.workers))].Enqueue(&loops.DispatchWorkItem{
		WorkItem: wi,
		Callback: callback,
	})
}

