package backend

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/runtimestate"
)

type WorkflowExecutor interface {
	ExecuteWorkflow(
		ctx context.Context,
		iid api.InstanceID,
		oldEvents []*protos.HistoryEvent,
		newEvents []*protos.HistoryEvent) (*protos.WorkflowResponse, error)
}

type WorkflowWorkerOptions struct {
	Backend  Backend
	Executor WorkflowExecutor
	Logger   Logger
	AppID    string
	// InternalExecutor is forwork items whose WorkflowWorkItem.InProcess flag is true.
	// This is how internal dapr-side workflows (e.g. dapr.internal.mcp.*) run, inside the sidecar
	// instead of being shipped to an external SDK via the gRPC work-item stream.
	InternalExecutor WorkflowExecutor
}

type workflowProcessor struct {
	be               Backend
	executor         WorkflowExecutor
	internalExecutor WorkflowExecutor
	logger           Logger

	applier *runtimestate.Applier
}

func NewWorkflowWorker(opts WorkflowWorkerOptions, taskopts ...NewTaskWorkerOptions) TaskWorker[*WorkflowWorkItem] {
	processor := &workflowProcessor{
		be:               opts.Backend,
		executor:         opts.Executor,
		internalExecutor: opts.InternalExecutor,
		logger:           opts.Logger,
		applier:          runtimestate.NewApplier(opts.AppID),
	}
	return NewTaskWorker[*WorkflowWorkItem](processor, opts.Logger, taskopts...)
}

// Name implements TaskProcessor
func (*workflowProcessor) Name() string {
	return "workflow-processor"
}

// NextWorkItem implements TaskProcessor
func (p *workflowProcessor) NextWorkItem(ctx context.Context) (*WorkflowWorkItem, error) {
	return p.be.NextWorkflowWorkItem(ctx)
}

// ProcessWorkItem implements TaskProcessor
func (w *workflowProcessor) ProcessWorkItem(ctx context.Context, wi *WorkflowWorkItem) error {
	w.logger.Debugf("%v: received work item with %d new event(s): %v", wi.InstanceID, len(wi.NewEvents), helpers.HistoryListSummary(wi.NewEvents))

	// TODO: Caching
	// In the fullness of time, we should consider caching executors and runtime state
	// so that we can skip the loading of state and/or the creation of executors. A cached
	// executor should allow us to 1) skip runtime state loading and 2) execute only new events.
	if wi.State == nil {
		if state, err := w.be.GetWorkflowRuntimeState(ctx, wi); err != nil {
			return fmt.Errorf("failed to load workflow state: %w", err)
		} else {
			wi.State = state
		}
	}
	w.logger.Debugf("%v: got workflow runtime state: %s", wi.InstanceID, getWorkflowStateDescription(wi))

	var terminateEvent *protos.ExecutionTerminatedEvent = nil
	for _, e := range wi.NewEvents {
		if et := e.GetExecutionTerminated(); et != nil {
			terminateEvent = et
			break
		}
	}
	if ctx, span, ok := w.applyWorkItem(ctx, wi); ok {
		defer func() {
			// Note that the span and ctx references may be updated inside the continue-as-new loop.
			w.endWorkflowSpan(ctx, wi, span, false)
		}()

		for continueAsNewCount := 0; ; continueAsNewCount++ {
			if continueAsNewCount > 0 {
				w.logger.Debugf("%v: continuing-as-new with %d event(s): %s", wi.InstanceID, len(wi.State.NewEvents), helpers.HistoryListSummary(wi.State.NewEvents))
			} else {
				w.logger.Debugf("%v: invoking workflow", wi.InstanceID)
			}

			// Run the user workflow code, providing the old history and new events together.
			executor := w.executor
			if wi.State.GetStartEvent().GetInProcess() && w.internalExecutor != nil {
				executor = w.internalExecutor
			}
			results, err := executor.ExecuteWorkflow(ctx, wi.InstanceID, wi.State.OldEvents, wi.State.NewEvents)
			if err != nil {
				return fmt.Errorf("error executing workflow: %w", err)
			}
			w.logger.Debugf("%v: workflow returned %d action(s): %s", wi.InstanceID, len(results.Actions), helpers.ActionListSummary(results.Actions))

			if version := results.GetVersion(); version != nil && (version.GetPatches() != nil || version.Name != nil) {
				for _, e := range wi.State.NewEvents {
					if os := e.GetWorkflowStarted(); os != nil {
						os.Version = version
						if len(version.GetPatches()) > 0 {
							span.SetAttributes(attribute.StringSlice("applied_patches", version.GetPatches()))
						}
						break
					}
				}
			}

			// Apply the workflow outputs to the workflow state.
			continuedAsNew, err := w.applier.Actions(wi.State, results.CustomStatus, results.Actions, helpers.TraceContextFromSpan(span))
			if err != nil {
				return fmt.Errorf("failed to apply the execution result actions: %w", err)
			}

			// When continuing-as-new, we re-execute the workflow from the beginning with a truncated state in a tight loop
			// until the workflow performs some non-continue-as-new action.
			if continuedAsNew {
				const MaxContinueAsNewCount = 20
				if continueAsNewCount >= MaxContinueAsNewCount {
					return fmt.Errorf("exceeded tight-loop continue-as-new limit of %d iterations", MaxContinueAsNewCount)
				}

				// We create a new trace span for every continue-as-new
				w.endWorkflowSpan(ctx, wi, span, true)
				ctx, span = w.startOrResumeWorkflowSpan(ctx, wi)
				continue
			}

			if runtimestate.IsCompleted(wi.State) {
				name, _ := runtimestate.Name(wi.State)
				w.logger.Infof("%v: '%s' completed with a %s status.", wi.InstanceID, name, helpers.ToRuntimeStatusString(runtimestate.RuntimeStatus(wi.State)))
			}
			break
		}
	}
	if terminateEvent != nil && runtimestate.IsCompleted(wi.State) {
		if err := terminateChildWorkflowInstances(ctx, w.be, wi.InstanceID, wi.State, terminateEvent); err != nil {
			return err
		}
	}
	return nil
}

// CompleteWorkItem implements TaskProcessor
func (p *workflowProcessor) CompleteWorkItem(ctx context.Context, wi *WorkflowWorkItem) error {
	return p.be.CompleteWorkflowWorkItem(ctx, wi)
}

// AbandonWorkItem implements TaskProcessor
func (p *workflowProcessor) AbandonWorkItem(ctx context.Context, wi *WorkflowWorkItem) error {
	return p.be.AbandonWorkflowWorkItem(ctx, wi)
}

func (w *workflowProcessor) applyWorkItem(ctx context.Context, wi *WorkflowWorkItem) (context.Context, trace.Span, bool) {
	// Ignore work items for workflows that are completed or are in a corrupted state.
	if !runtimestate.IsValid(wi.State) {
		w.logger.Warnf("%v: workflow state is invalid; dropping work item", wi.InstanceID)
		return nil, nil, false
	} else if runtimestate.IsCompleted(wi.State) {
		w.logger.Warnf("%v: workflow already completed; dropping work item", wi.InstanceID)
		return nil, nil, false
	} else if len(wi.NewEvents) == 0 {
		w.logger.Warnf("%v: the work item had no events!", wi.InstanceID)
	}

	// The workflow started event is used primarily for updating the current time as reported
	// by the workflow context APIs.
	_ = runtimestate.AddEvent(wi.State, &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_WorkflowStarted{
			WorkflowStarted: &protos.WorkflowStartedEvent{},
		},
	})

	// Each workflow instance gets its own distributed tracing span. However, the implementation of
	// endWorkflowSpan will "cancel" the span mark the span as "unsampled" if the workflow isn't
	// complete. This is part of the strategy for producing one span for the entire workflow execution,
	// which isn't something that's natively supported by OTel today.
	ctx, span := w.startOrResumeWorkflowSpan(ctx, wi)

	// New events from the work item are appended to the workflow state, with duplicates automatically
	// filtered out. If all events are filtered out, return false so that the caller knows not to execute
	// the workflow logic for an empty set of events.
	for _, e := range wi.NewEvents {
		if err := runtimestate.AddEvent(wi.State, e); err != nil {
			if err == runtimestate.ErrDuplicateEvent {
				w.logger.Warnf("%v: dropping duplicate event: %v", wi.InstanceID, e)
			} else {
				w.logger.Warnf("%v: dropping event: %v, %v", wi.InstanceID, e, err)
			}
		}

		// Special case logic for specific event types
		if es := e.GetExecutionStarted(); es != nil {
			w.logger.Infof("%v: starting new '%s' instance with ID = '%s'.", wi.InstanceID, es.Name, es.WorkflowInstance.InstanceId)
		} else if timerFired := e.GetTimerFired(); timerFired != nil {
			// Timer spans are created and completed once the TimerFired event is received.
			// TODO: Ideally we don't emit spans for cancelled timers. Is there a way to support this?
			if err := helpers.StartAndEndNewTimerSpan(ctx, timerFired, e.Timestamp.AsTime(), string(wi.InstanceID)); err != nil {
				w.logger.Warnf("%v: failed to generate distributed trace span for durable timer: %v", wi.InstanceID, err)
			}
		}
	}

	if len(wi.State.NewEvents) == 0 {
		w.logger.Warnf("%v: no new events to process", wi.InstanceID)
		return ctx, span, false
	}

	return ctx, span, true
}

func getWorkflowStateDescription(wi *WorkflowWorkItem) string {
	name, err := runtimestate.Name(wi.State)
	if err != nil {
		if len(wi.NewEvents) > 0 {
			name = wi.NewEvents[0].GetExecutionStarted().GetName()
		}
	}
	if name == "" {
		name = "(unknown)"
	}

	ageStr := "(new)"
	createdAt, err := runtimestate.CreatedTime(wi.State)
	if err == nil {
		age := time.Since(createdAt)

		if age > 0 {
			ageStr = age.Round(time.Second).String()
		}
	}
	status := helpers.ToRuntimeStatusString(runtimestate.RuntimeStatus(wi.State))
	return fmt.Sprintf("name=%s, status=%s, events=%d, age=%s", name, status, len(wi.State.OldEvents), ageStr)
}

func (w *workflowProcessor) startOrResumeWorkflowSpan(ctx context.Context, wi *WorkflowWorkItem) (context.Context, trace.Span) {
	// Get the trace context from the ExecutionStarted history event
	var ptc *protos.TraceContext
	var es *protos.ExecutionStartedEvent
	if es = wi.State.StartEvent; es != nil {
		ptc = wi.State.StartEvent.ParentTraceContext
	} else {
		for _, e := range wi.NewEvents {
			if es = e.GetExecutionStarted(); es != nil {
				ptc = es.ParentTraceContext
				break
			}
		}
	}

	if ptc == nil {
		return ctx, helpers.NoopSpan()
	}

	ctx, err := helpers.ContextFromTraceContext(ctx, ptc)
	if err != nil {
		w.logger.Warnf("%v: failed to parse trace context: %v", wi.InstanceID, err)
		return ctx, helpers.NoopSpan()
	}

	// start a new span from the updated go context
	var span trace.Span
	ctx, span = helpers.StartNewRunWorkflowSpan(ctx, es, runtimestate.GetStartedTime(wi.State))

	// Assign or rehydrate the long-running workflow span ID
	if es.WorkflowSpanID == nil {
		// On the initial execution, assign the workflow span ID to be the
		// randomly generated span ID value. This will be persisted in the workflow history
		// and referenced on the next replay.
		es.WorkflowSpanID = wrapperspb.String(span.SpanContext().SpanID().String())
	} else {
		// On subsequent executions, replace the auto-generated span ID with the workflow
		// span ID. This allows us to have one long-running span that survives multiple replays
		// and process failures.
		if workflowSpanID, err := trace.SpanIDFromHex(es.WorkflowSpanID.Value); err == nil {
			helpers.ChangeSpanID(span, workflowSpanID)
		}
	}

	return ctx, span
}

func (w *workflowProcessor) endWorkflowSpan(ctx context.Context, wi *WorkflowWorkItem, span trace.Span, continuedAsNew bool) {
	if runtimestate.IsCompleted(wi.State) {
		if fd, err := runtimestate.FailureDetails(wi.State); err == nil {
			span.SetStatus(codes.Error, fd.ErrorMessage)
		}
		span.SetAttributes(attribute.KeyValue{
			Key:   "durabletask.runtime_status",
			Value: attribute.StringValue(helpers.ToRuntimeStatusString(runtimestate.RuntimeStatus(wi.State))),
		})
		addNotableEventsToSpan(wi.State.OldEvents, span)
		addNotableEventsToSpan(wi.State.NewEvents, span)
	} else if continuedAsNew {
		span.SetAttributes(attribute.KeyValue{
			Key:   "durabletask.runtime_status",
			Value: attribute.StringValue(helpers.ToRuntimeStatusString(protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW)),
		})
	} else {
		// Cancel the span - we want to publish it only when a workflow
		// completes or when it continue-as-new's.
		helpers.CancelSpan(span)
	}

	// We must always call End() on a span to ensure we don't leak resources.
	// See https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/api.md#span-creation
	span.End()
}

// Adds notable events to the span that are interesting to the user.
// More info: https://opentelemetry.io/docs/instrumentation/go/manual/#events
func addNotableEventsToSpan(events []*protos.HistoryEvent, span trace.Span) {
	for _, e := range events {
		if eventRaised := e.GetEventRaised(); eventRaised != nil {
			eventByteCount := len(eventRaised.Input.GetValue())
			span.AddEvent(
				"Received external event",
				trace.WithTimestamp(e.Timestamp.AsTime()),
				trace.WithAttributes(attribute.String("name", eventRaised.Name), attribute.Int("size", eventByteCount)))
		} else if suspended := e.GetExecutionSuspended(); suspended != nil {
			span.AddEvent(
				"Execution suspended",
				trace.WithTimestamp(e.Timestamp.AsTime()),
				trace.WithAttributes(attribute.String("reason", suspended.Input.GetValue())))
		} else if resumed := e.GetExecutionResumed(); resumed != nil {
			span.AddEvent(
				"Execution resumed",
				trace.WithTimestamp(e.Timestamp.AsTime()),
				trace.WithAttributes(attribute.String("reason", resumed.Input.GetValue())))
		} else if stalled := e.GetExecutionStalled(); stalled != nil {
			span.AddEvent(
				"Execution stalled",
				trace.WithTimestamp(e.Timestamp.AsTime()),
				trace.WithAttributes(
					attribute.String("reason", stalled.Reason.String()),
					attribute.String("description", stalled.GetDescription())),
			)
		}
	}
}
