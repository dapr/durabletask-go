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
	"github.com/dapr/durabletask-go/backend/payloadstore"
)

type activityProcessor struct {
	be                  Backend
	executor            ActivityExecutor
	inProcessExecutor   ActivityExecutor
	inProcessNamePrefix string
	payloadStore        payloadstore.Store
}

type ActivityExecutor interface {
	ExecuteActivity(ctx context.Context, iid api.InstanceID, e *protos.HistoryEvent, opts ExecuteOptions) (*protos.HistoryEvent, error)
}

// ActivityWorkerOptions configures NewActivityWorker.
type ActivityWorkerOptions struct {
	Backend  Backend
	Executor ActivityExecutor
	Logger   Logger
	// InProcessExecutor dispatches activities whose name has
	// InProcessNamePrefix as a prefix; an empty prefix disables it.
	InProcessExecutor   ActivityExecutor
	InProcessNamePrefix string
	// PayloadStore, when non-nil, resolves a payload-store reference in
	// the activity input back to the payload before it is handed to the
	// executor. Nil disables dereferencing.
	PayloadStore payloadstore.Store
}

// NewActivityWorker constructs an activity worker.
func NewActivityWorker(opts ActivityWorkerOptions, taskopts ...NewTaskWorkerOptions) TaskWorker[*ActivityWorkItem] {
	processor := &activityProcessor{
		be:                  opts.Backend,
		executor:            opts.Executor,
		inProcessExecutor:   opts.InProcessExecutor,
		inProcessNamePrefix: opts.InProcessNamePrefix,
		payloadStore:        opts.PayloadStore,
	}
	return NewTaskWorker(processor, opts.Logger, taskopts...)
}

// Name implements TaskProcessor
func (*activityProcessor) Name() string {
	return "activity-processor"
}

// NextWorkItem implements TaskDispatcher
func (ap *activityProcessor) NextWorkItem(ctx context.Context) (*ActivityWorkItem, error) {
	return ap.be.NextActivityWorkItem(ctx)
}

// ProcessWorkItem implements TaskDispatcher
func (p *activityProcessor) ProcessWorkItem(ctx context.Context, awi *ActivityWorkItem) error {
	ts := awi.NewEvent.GetTaskScheduled()
	if ts == nil {
		return fmt.Errorf("%v: invalid TaskScheduled event", awi.InstanceID)
	}
	// Create span as child of spanContext found in TaskScheduledEvent
	ctx, err := helpers.ContextFromTraceContext(ctx, ts.ParentTraceContext)
	if err != nil {
		return fmt.Errorf("%v: failed to parse activity trace context: %w", awi.InstanceID, err)
	}
	var span trace.Span
	ctx, span = helpers.StartNewActivitySpan(ctx, ts.Name, ts.Version.GetValue(), string(awi.InstanceID), awi.NewEvent.EventId)
	if span != nil {
		defer func() {
			if r := recover(); r != nil {
				span.SetStatus(codes.Error, fmt.Sprintf("%v", r))
			}
			span.End()
		}()
	}

	// set the parent trace context to be the newly created activity span
	ts.ParentTraceContext = helpers.TraceContextFromSpan(span)

	execOpts := ExecuteOptions{PropagatedHistory: awi.IncomingHistory}

	// Execute the activity and get its result.
	executor := p.executor
	if p.inProcessExecutor != nil && p.inProcessNamePrefix != "" && strings.HasPrefix(ts.GetName(), p.inProcessNamePrefix) {
		executor = p.inProcessExecutor
	}
	// Resolve an offloaded input on a copy of the event so the executor
	// sees the full payload while the work item keeps its reference.
	event := awi.NewEvent
	if p.payloadStore != nil {
		events, derr := payloadstore.Dereference(ctx, p.payloadStore, string(awi.InstanceID), []*protos.HistoryEvent{event})
		if derr != nil {
			if span != nil {
				span.RecordError(derr)
				span.SetStatus(codes.Error, derr.Error())
			}
			return fmt.Errorf("failed to resolve offloaded activity input: %w", derr)
		}
		event = events[0]
	}
	result, err := executor.ExecuteActivity(ctx, awi.InstanceID, event, execOpts)
	if err != nil {
		if span != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		return err
	}
	awi.Result = result
	return nil
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
