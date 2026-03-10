package mocks

import (
	context "context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	backend "github.com/dapr/durabletask-go/backend"
)

// TestTaskProcessor implements a dummy task processor useful for testing
type TestTaskProcessor[T backend.WorkItem] struct {
	name string

	processingBlocked atomic.Bool

	abandonedWorkItemMu sync.Mutex
	abandonedWorkItems  []T

	completedWorkItemMu sync.Mutex
	completedWorkItems  []T
}

func NewTestTaskPocessor[T backend.WorkItem](name string) *TestTaskProcessor[T] {
	return &TestTaskProcessor[T]{
		name: name,
	}
}

func (t *TestTaskProcessor[T]) BlockProcessing() {
	t.processingBlocked.Store(true)
}

func (t *TestTaskProcessor[T]) UnblockProcessing() {
	t.processingBlocked.Store(false)
}

func (t *TestTaskProcessor[T]) AbandonedWorkItems() []T {
	t.abandonedWorkItemMu.Lock()
	defer t.abandonedWorkItemMu.Unlock()

	// copy array
	return append([]T{}, t.abandonedWorkItems...)
}

func (t *TestTaskProcessor[T]) CompletedWorkItems() []T {
	t.completedWorkItemMu.Lock()
	defer t.completedWorkItemMu.Unlock()

	// copy array
	return append([]T{}, t.completedWorkItems...)
}

func (t *TestTaskProcessor[T]) Name() string {
	return t.name
}

func (t *TestTaskProcessor[T]) ProcessWorkItem(ctx context.Context, wi T) error {
	if !t.processingBlocked.Load() {
		return nil
	}
	// wait for context cancellation or until processing is unblocked
	for {
		select {
		case <-ctx.Done():
			return errors.New("dummy error processing work item")
		default:
			if !t.processingBlocked.Load() {
				return nil
			}
			time.Sleep(time.Millisecond)
		}
	}
}

func (t *TestTaskProcessor[T]) AbandonWorkItem(ctx context.Context, wi T) error {
	t.abandonedWorkItemMu.Lock()
	defer t.abandonedWorkItemMu.Unlock()

	t.abandonedWorkItems = append(t.abandonedWorkItems, wi)
	return nil
}

func (t *TestTaskProcessor[T]) CompleteWorkItem(ctx context.Context, wi T) error {
	t.completedWorkItemMu.Lock()
	defer t.completedWorkItemMu.Unlock()

	t.completedWorkItems = append(t.completedWorkItems, wi)
	return nil
}
