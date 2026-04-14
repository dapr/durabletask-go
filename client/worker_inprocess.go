package client

import (
	"context"
	"errors"
	"sync"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/task"
)

// InProcessClient is a work-item consumer that lives in the same process as
// the grpcExecutor and receives work items via a registered
// backend.WorkItemSink rather than a gRPC stream.
// It mirrors TaskHubGrpcClient: the caller provides a task.TaskRegistry
// describing the workflows and activities handled by this consumer, and the
// client dispatches incoming work items to an internal task.TaskExecutor
// backed by that registry. Completion results are written back to the
// backend directly (no gRPC round-trip) via backend.CompleteWorkflowTask /
// backend.CompleteActivityTask — the same API the gRPC server handlers call.
type InProcessClient struct {
	be     backend.Backend
	logger backend.Logger

	// workItems buffers deliveries from the executor side. The buffer lets
	// multiple work items queue up while the processor spawns goroutines;
	// when full, DeliverWorkItem blocks the executor (matching the
	// unbuffered gRPC workItemQueue's back-pressure semantics).
	workItems chan *protos.WorkItem

	mu      sync.Mutex
	started bool
	closed  bool

	// closed on Close; signals processors to stop
	done     chan struct{}
	executor backend.Executor

	// tracks in-flight work-item processors
	wg sync.WaitGroup
}

// NewTaskHubInProcessClient returns an in-process client that acts as a
// backend.WorkItemSink for a grpcExecutor. The caller must still register
// the returned client on the executor's SinkRegistrar and invoke
// StartWorkItemListener before work items will be drained.
func NewTaskHubInProcessClient(be backend.Backend, logger backend.Logger) *InProcessClient {
	return &InProcessClient{
		be:        be,
		logger:    logger,
		workItems: make(chan *protos.WorkItem, 64),
		done:      make(chan struct{}),
	}
}

// DeliverWorkItem implements backend.WorkItemSink. It enqueues the work item
// for the processor goroutine to pick up; the caller (grpcExecutor) blocks
// only until the item is accepted, matching the gRPC path's back-pressure.
func (c *InProcessClient) DeliverWorkItem(ctx context.Context, wi *protos.WorkItem) error {
	c.mu.Lock()
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return errors.New("in-process client is closed")
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.done:
		return errors.New("in-process client is closed")
	case c.workItems <- wi:
		return nil
	}
}

// Close implements backend.WorkItemSink. It stops accepting new work items
// and waits for in-flight processing to finish, bounded by ctx. Safe to
// call multiple times.
func (c *InProcessClient) Close(ctx context.Context) error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	close(c.done)
	executor := c.executor
	c.mu.Unlock()

	// Wait for in-flight processors, bounded by ctx.
	doneCh := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(doneCh)
	}()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-doneCh:
	}

	if executor != nil {
		if err := executor.Shutdown(ctx); err != nil {
			c.logger.Warnf("in-process client: executor shutdown returned error: %v", err)
		}
	}
	return nil
}

// StartWorkItemListener constructs an in-process task.TaskExecutor from r
// and launches a background processor that dispatches queued work items to
// it. Returns immediately once the processor is running. Only valid to call
// once per InProcessClient.
func (c *InProcessClient) StartWorkItemListener(ctx context.Context, r *task.TaskRegistry) error {
	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return errors.New("StartWorkItemListener has already been called")
	}
	if c.closed {
		c.mu.Unlock()
		return errors.New("in-process client is closed")
	}
	c.started = true
	c.executor = task.NewTaskExecutor(r)
	c.mu.Unlock()

	go c.processLoop(ctx)
	return nil
}

// processLoop drains the work-item channel and dispatches each item to an
// appropriate handler goroutine. Exits when ctx is canceled or Close is
// invoked.
func (c *InProcessClient) processLoop(ctx context.Context) {
	c.logger.Info("in-process client: starting background processor")
	defer c.logger.Info("in-process client: stopping background processor")

	for {
		select {
		case <-ctx.Done():
			return
		case <-c.done:
			return
		case wi, ok := <-c.workItems:
			if !ok {
				return
			}
			switch {
			case wi.GetWorkflowRequest() != nil:
				c.wg.Add(1)
				go func(req *protos.WorkflowRequest) {
					defer c.wg.Done()
					c.processWorkflowWorkItem(ctx, req)
				}(wi.GetWorkflowRequest())
			case wi.GetActivityRequest() != nil:
				c.wg.Add(1)
				go func(req *protos.ActivityRequest) {
					defer c.wg.Done()
					c.processActivityWorkItem(ctx, req)
				}(wi.GetActivityRequest())
			default:
				c.logger.Warnf("in-process client: received unsupported work item type: %T", wi.Request)
			}
		}
	}
}

func (c *InProcessClient) processWorkflowWorkItem(ctx context.Context, req *protos.WorkflowRequest) {
	executor := c.snapshotExecutor()
	if executor == nil {
		c.logger.Warnf("in-process client: workflow %q dispatched after shutdown; dropping", req.InstanceId)
		return
	}

	resp := dispatchWorkflow(ctx, executor, req)
	if err := c.be.CompleteWorkflowTask(ctx, resp); err != nil {
		if ctx.Err() != nil {
			c.logger.Warnf("in-process client: failed to complete workflow task: context canceled")
		} else {
			c.logger.Errorf("in-process client: failed to complete workflow task: %v", err)
		}
	}
}

func (c *InProcessClient) processActivityWorkItem(ctx context.Context, req *protos.ActivityRequest) {
	executor := c.snapshotExecutor()
	if executor == nil {
		c.logger.Warnf("in-process client: activity %q dispatched after shutdown; dropping", req.Name)
		return
	}

	resp := dispatchActivity(ctx, executor, c.logger, req)
	if err := c.be.CompleteActivityTask(ctx, resp); err != nil {
		if ctx.Err() != nil {
			c.logger.Warnf("in-process client: failed to complete activity task: context canceled")
		} else {
			c.logger.Errorf("in-process client: failed to complete activity task: %v", err)
		}
	}
}

// snapshotExecutor reads c.executor under the mutex so Close can set the
// pointer to nil safely even if work-item goroutines are still draining.
func (c *InProcessClient) snapshotExecutor() backend.Executor {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.executor
}
