package backend

import (
	"context"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
)

type activityProcessor struct {
	be                  Backend
	executor            ActivityExecutor
	inProcessExecutor   ActivityExecutor
	inProcessNamePrefix string
}

type ActivityExecutor interface {
	ExecuteActivity(ctx context.Context, iid api.InstanceID, e *protos.HistoryEvent, opts ExecuteOptions) (*protos.HistoryEvent, error)
}

// asyncActivityExecutor is implemented by executors that can deliver the
// activity result through a callback instead of blocking in ExecuteActivity.
type asyncActivityExecutor interface {
	canExecuteAsync() bool
	executeActivityAsync(ctx context.Context, iid api.InstanceID, e *protos.HistoryEvent, opts ExecuteOptions, done func(*protos.HistoryEvent, error))
}

// NewActivityTaskWorker constructs an activity worker.
func NewActivityTaskWorker(be Backend, executor ActivityExecutor, logger Logger, opts ...NewTaskWorkerOptions) TaskWorker[*ActivityWorkItem] {
	processor := newActivityProcessor(be, executor, nil, "")
	return NewTaskWorker(processor, logger, opts...)
}

// NewActivityTaskWorkerWithInProcess constructs an activity worker that dispatches
// activities whose name starts with inProcessNamePrefix to inProcessExecutor.
// An empty prefix disables in-process dispatch.
func NewActivityTaskWorkerWithInProcess(be Backend, executor, inProcessExecutor ActivityExecutor, inProcessNamePrefix string, logger Logger, opts ...NewTaskWorkerOptions) TaskWorker[*ActivityWorkItem] {
	processor := newActivityProcessor(be, executor, inProcessExecutor, inProcessNamePrefix)
	return NewTaskWorker(processor, logger, opts...)
}

func newActivityProcessor(be Backend, executor, inProcessExecutor ActivityExecutor, inProcessNamePrefix string) TaskProcessor[*ActivityWorkItem] {
	return &activityProcessor{
		be:                  be,
		executor:            executor,
		inProcessExecutor:   inProcessExecutor,
		inProcessNamePrefix: inProcessNamePrefix,
	}
}

// Name implements TaskProcessor
func (*activityProcessor) Name() string {
	return "activity-processor"
}

// NextWorkItem implements TaskDispatcher
func (ap *activityProcessor) NextWorkItem(ctx context.Context) (*ActivityWorkItem, error) {
	return ap.be.NextActivityWorkItem(ctx)
}

// ProcessWorkItemAsync implements TaskProcessor: the post-execution work is
// registered as a callback run by the goroutine that delivers the activity
// result, so no goroutine waits out the app roundtrip. Executors that cannot
// deliver by callback are invoked inline on the calling goroutine.
func (p *activityProcessor) ProcessWorkItemAsync(ctx context.Context, awi *ActivityWorkItem, done func(error)) {
	ts := awi.NewEvent.GetTaskScheduled()
	if ts == nil {
		done(fmt.Errorf("%v: invalid TaskScheduled event", awi.InstanceID))
		return
	}
	ctx, err := helpers.ContextFromTraceContext(ctx, ts.ParentTraceContext)
	if err != nil {
		done(fmt.Errorf("%v: failed to parse activity trace context: %w", awi.InstanceID, err))
		return
	}
	var span trace.Span
	ctx, span = helpers.StartNewActivitySpan(ctx, ts.Name, ts.Version.GetValue(), string(awi.InstanceID), awi.NewEvent.EventId)

	// set the parent trace context to be the newly created activity span
	ts.ParentTraceContext = helpers.TraceContextFromSpan(span)

	execOpts := ExecuteOptions{PropagatedHistory: awi.IncomingHistory}

	settle := func(result *protos.HistoryEvent, err error) {
		if span != nil {
			if err != nil {
				span.RecordError(err)
				span.SetStatus(codes.Error, err.Error())
			}
			span.End()
		}
		if err != nil {
			done(err)
			return
		}
		awi.Result = result
		done(nil)
	}

	executor := p.executor
	if p.inProcessExecutor != nil && p.inProcessNamePrefix != "" && strings.HasPrefix(ts.GetName(), p.inProcessNamePrefix) {
		executor = p.inProcessExecutor
	}
	if asyncExecutor, ok := executor.(asyncActivityExecutor); ok && asyncExecutor.canExecuteAsync() {
		asyncExecutor.executeActivityAsync(ctx, awi.InstanceID, awi.NewEvent, execOpts, settle)
		return
	}

	func() {
		defer func() {
			if r := recover(); r != nil {
				settle(nil, fmt.Errorf("%v: activity executor panicked: %v", awi.InstanceID, r))
			}
		}()
		settle(executor.ExecuteActivity(ctx, awi.InstanceID, awi.NewEvent, execOpts))
	}()
}

// CompleteWorkItem implements TaskDispatcher
func (ap *activityProcessor) CompleteWorkItem(ctx context.Context, awi *ActivityWorkItem) error {
	if awi.Result == nil {
		return fmt.Errorf("can't complete work item '%s' with nil result", awi)
	}
	if awi.Result.GetTaskCompleted() == nil && awi.Result.GetTaskFailed() == nil {
		return fmt.Errorf("can't complete work item '%s', which isn't TaskCompleted or TaskFailed", awi)
	}

	return ap.be.CompleteActivityWorkItem(ctx, awi)
}

// AbandonWorkItem implements TaskDispatcher
func (ap *activityProcessor) AbandonWorkItem(ctx context.Context, awi *ActivityWorkItem) error {
	return ap.be.AbandonActivityWorkItem(ctx, awi)
}
