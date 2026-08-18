package tests

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/tests/mocks"
)

// asyncTaskProcessor wraps the test processor with an AsyncTaskProcessor
// implementation whose completions are delivered by the test, standing in for
// the goroutine that delivers a real work item completion.
type asyncTaskProcessor struct {
	*mocks.TestTaskProcessor[*backend.ActivityWorkItem]

	// cancelOnCtxDone emulates the executor's context watcher, which settles
	// an in-flight item with the context error when the worker shuts down.
	cancelOnCtxDone bool

	mu   sync.Mutex
	done []func(error)
}

func newAsyncTaskProcessor(cancelOnCtxDone bool) *asyncTaskProcessor {
	return &asyncTaskProcessor{
		TestTaskProcessor: mocks.NewTestTaskPocessor[*backend.ActivityWorkItem]("asynctest"),
		cancelOnCtxDone:   cancelOnCtxDone,
	}
}

func (p *asyncTaskProcessor) ProcessWorkItemAsync(ctx context.Context, wi *backend.ActivityWorkItem, done func(error)) bool {
	p.mu.Lock()
	p.done = append(p.done, done)
	p.mu.Unlock()
	if p.cancelOnCtxDone {
		context.AfterFunc(ctx, func() {
			done(ctx.Err())
		})
	}
	return true
}

func (p *asyncTaskProcessor) inFlight() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.done)
}

func (p *asyncTaskProcessor) takeAll() []func(error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	done := p.done
	p.done = nil
	return done
}

func Test_TaskWorkerAsync_ConcurrentCompletions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp := newAsyncTaskProcessor(false)
	items := make([]*backend.ActivityWorkItem, 8)
	for i := range items {
		items[i] = &backend.ActivityWorkItem{SequenceNumber: int64(i + 1)}
	}
	tp.AddWorkItems(items...)

	worker := backend.NewTaskWorker[*backend.ActivityWorkItem](tp, logger, backend.WithMaxParallelism(4))
	worker.Start(ctx)

	// All four semaphore slots fill, and no fifth item is handed out while
	// they are held.
	require.Eventually(t, func() bool { return tp.inFlight() == 4 }, 2*time.Second, 10*time.Millisecond)
	time.Sleep(150 * time.Millisecond)
	require.Equal(t, 4, tp.inFlight())
	require.Len(t, tp.PendingWorkItems(), 4)

	// Deliver the four completions concurrently, as separate delivery
	// goroutines would.
	var wg sync.WaitGroup
	for _, done := range tp.takeAll() {
		wg.Add(1)
		go func(done func(error)) {
			defer wg.Done()
			done(nil)
		}(done)
	}
	wg.Wait()

	// The released slots admit the remaining four items.
	require.Eventually(t, func() bool { return tp.inFlight() == 4 }, 2*time.Second, 10*time.Millisecond)
	for _, done := range tp.takeAll() {
		done(nil)
	}

	require.Eventually(t, func() bool { return len(tp.CompletedWorkItems()) == 8 }, 2*time.Second, 10*time.Millisecond)

	worker.StopAndDrain()

	require.Len(t, tp.CompletedWorkItems(), 8)
	require.Empty(t, tp.AbandonedWorkItems())
	require.Empty(t, tp.PendingWorkItems())
}

func Test_TaskWorkerAsync_SemaphoreReleasedOnError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp := newAsyncTaskProcessor(false)
	first := &backend.ActivityWorkItem{SequenceNumber: 1}
	second := &backend.ActivityWorkItem{SequenceNumber: 2}
	tp.AddWorkItems(first, second)

	worker := backend.NewTaskWorker[*backend.ActivityWorkItem](tp, logger, backend.WithMaxParallelism(1))
	worker.Start(ctx)

	require.Eventually(t, func() bool { return tp.inFlight() == 1 }, 2*time.Second, 10*time.Millisecond)

	// Failing the first item must abandon it and release its slot, or the
	// second item can never start.
	tp.takeAll()[0](errors.New("dummy processing error"))

	require.Eventually(t, func() bool {
		return len(tp.AbandonedWorkItems()) == 1 && tp.inFlight() == 1
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, first, tp.AbandonedWorkItems()[0])

	tp.takeAll()[0](nil)
	require.Eventually(t, func() bool { return len(tp.CompletedWorkItems()) == 1 }, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, second, tp.CompletedWorkItems()[0])

	worker.StopAndDrain()
}

func Test_TaskWorkerAsync_ShutdownDrainsInFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp := newAsyncTaskProcessor(false)
	tp.AddWorkItems(&backend.ActivityWorkItem{SequenceNumber: 1})

	worker := backend.NewTaskWorker[*backend.ActivityWorkItem](tp, logger, backend.WithMaxParallelism(1))
	worker.Start(ctx)

	require.Eventually(t, func() bool { return tp.inFlight() == 1 }, 2*time.Second, 10*time.Millisecond)

	drained := make(chan struct{})
	go func() {
		worker.StopAndDrain()
		close(drained)
	}()

	select {
	case <-drained:
		t.Fatal("StopAndDrain returned while a work item was still in flight")
	case <-time.After(200 * time.Millisecond):
	}

	tp.takeAll()[0](nil)

	select {
	case <-drained:
	case <-time.After(2 * time.Second):
		t.Fatal("StopAndDrain did not finish after the in-flight item completed")
	}

	require.Len(t, tp.CompletedWorkItems(), 1)
	require.Empty(t, tp.AbandonedWorkItems())
}

func Test_TaskWorkerAsync_ShutdownCancelsViaContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tp := newAsyncTaskProcessor(true)
	tp.AddWorkItems(&backend.ActivityWorkItem{SequenceNumber: 1})

	worker := backend.NewTaskWorker[*backend.ActivityWorkItem](tp, logger, backend.WithMaxParallelism(1))
	worker.Start(ctx)

	require.Eventually(t, func() bool { return tp.inFlight() == 1 }, 2*time.Second, 10*time.Millisecond)

	// The context watcher settles the in-flight item with the context error,
	// so the drain completes without an external delivery and the item is
	// abandoned.
	worker.StopAndDrain()

	require.Len(t, tp.AbandonedWorkItems(), 1)
	require.Empty(t, tp.CompletedWorkItems())
}
