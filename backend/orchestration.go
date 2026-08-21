package backend

import (
	"context"
	"fmt"
	"strings"
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
		newEvents []*protos.HistoryEvent,
		opts ExecuteOptions) (*protos.WorkflowResponse, error)
}

// asyncWorkflowExecutor is implemented by executors that can deliver the
// workflow response through a callback instead of blocking in ExecuteWorkflow.
type asyncWorkflowExecutor interface {
	canExecuteAsync() bool
	executeWorkflowAsync(ctx context.Context, iid api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent, opts ExecuteOptions, done func(*protos.WorkflowResponse, error))
}

// maxContinueAsNewCount bounds the tight continue-as-new re-execution loop.
const maxContinueAsNewCount = 20

type WorkflowWorkerOptions struct {
	Backend   Backend
	Executor  WorkflowExecutor
	Logger    Logger
	AppID     string
	Namespace string
	// InProcessExecutor is used to dispatch work items whose workflow name has
	// InProcessNamePrefix as a prefix. This is how internal dapr-side workflows
	// (e.g. dapr.internal.mcp.*) run inside the sidecar instead of being shipped
	// to an external SDK via the gRPC work-item stream.
	InProcessExecutor WorkflowExecutor
	// InProcessNamePrefix is the workflow-name prefix that selects InProcessExecutor.
	// Empty string disables prefix-based dispatch.
	InProcessNamePrefix string
}

type workflowProcessor struct {
	be                  Backend
	executor            WorkflowExecutor
	inProcessExecutor   WorkflowExecutor
	inProcessNamePrefix string
	logger              Logger

	applier *runtimestate.Applier
}

func NewWorkflowWorker(opts WorkflowWorkerOptions, taskopts ...NewTaskWorkerOptions) TaskWorker[*WorkflowWorkItem] {
	processor := &workflowProcessor{
		be:                  opts.Backend,
		executor:            opts.Executor,
		inProcessExecutor:   opts.InProcessExecutor,
		inProcessNamePrefix: opts.InProcessNamePrefix,
		logger:              opts.Logger,
		applier:             runtimestate.NewApplier(opts.AppID, opts.Namespace),
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
// ProcessWorkItemAsync implements TaskProcessor: the work that follows each
// workflow execution is registered as a completion callback, so no goroutine
// waits out the app roundtrip. A continue-as-new response starts its next
// execution from the goroutine that delivered the previous one. Executors
// that cannot deliver by callback are invoked inline by the turn.
func (w *workflowProcessor) ProcessWorkItemAsync(ctx context.Context, wi *WorkflowWorkItem, done func(error)) {
	w.logger.Debugf("%v: received work item with %d new event(s): %v", wi.InstanceID, len(wi.NewEvents), helpers.HistoryListSummary(wi.NewEvents))

	if wi.State == nil {
		state, err := w.be.GetWorkflowRuntimeState(ctx, wi)
		if err != nil {
			done(fmt.Errorf("failed to load workflow state: %w", err))
			return
		}
		wi.State = state
	}
	w.logger.Debugf("%v: got workflow runtime state: %s", wi.InstanceID, getWorkflowStateDescription(wi))

	var terminateEvent *protos.ExecutionTerminatedEvent
	for _, e := range wi.NewEvents {
		if et := e.GetExecutionTerminated(); et != nil {
			terminateEvent = et
			break
		}
	}

	spanCtx, span, ok := w.applyWorkItem(ctx, wi)
	if !ok {
		if terminateEvent != nil && runtimestate.IsCompleted(wi.State) {
			appendCascadeTerminateMessages(wi.State, terminateEvent)
		}
		done(nil)
		return
	}

	turn := &workflowTurn{
		processor:      w,
		wi:             wi,
		ctx:            spanCtx,
		span:           span,
		terminateEvent: terminateEvent,
		done:           done,
	}
	turn.execute()
}

// workflowTurn carries a workflow work item through its execute-apply loop in
// callback form: execute hands the response to applyResponse, which either
// finishes the work item or, on continue-as-new, executes again.
type workflowTurn struct {
	processor          *workflowProcessor
	wi                 *WorkflowWorkItem
	ctx                context.Context
	span               trace.Span
	terminateEvent     *protos.ExecutionTerminatedEvent
	continueAsNewCount int
	done               func(error)
}

func (t *workflowTurn) execute() {
	w := t.processor
	wi := t.wi

	if t.continueAsNewCount > 0 {
		w.logger.Debugf("%v: continuing-as-new with %d event(s): %s", wi.InstanceID, len(wi.State.NewEvents), helpers.HistoryListSummary(wi.State.NewEvents))
	} else {
		w.logger.Debugf("%v: invoking workflow", wi.InstanceID)
	}

	executor := w.executor
	if w.inProcessExecutor != nil && w.inProcessNamePrefix != "" {
		if name := wi.State.GetStartEvent().GetName(); strings.HasPrefix(name, w.inProcessNamePrefix) {
			executor = w.inProcessExecutor
		}
	}

	execOpts := ExecuteOptions{PropagatedHistory: wi.IncomingHistory}

	// An executor that cannot deliver by callback (e.g. the in-process one,
	// which runs the workflow code inline) is invoked synchronously on the
	// current goroutine.
	if asyncExecutor, ok := executor.(asyncWorkflowExecutor); ok && asyncExecutor.canExecuteAsync() {
		asyncExecutor.executeWorkflowAsync(t.ctx, wi.InstanceID, wi.State.OldEvents, wi.State.NewEvents, execOpts, func(results *protos.WorkflowResponse, err error) {
			t.applyResponse(results, err, execOpts)
		})
		return
	}

	results, err := executor.ExecuteWorkflow(t.ctx, wi.InstanceID, wi.State.OldEvents, wi.State.NewEvents, execOpts)
	t.applyResponse(results, err, execOpts)
}

func (t *workflowTurn) applyResponse(results *protos.WorkflowResponse, err error, execOpts ExecuteOptions) {
	w := t.processor
	wi := t.wi

	if err != nil {
		t.finish(fmt.Errorf("error executing workflow: %w", err))
		return
	}
	w.logger.Debugf("%v: workflow returned %d action(s): %s", wi.InstanceID, len(results.Actions), helpers.ActionListSummary(results.Actions))

	if version := results.GetVersion(); version != nil && (version.GetPatches() != nil || version.Name != nil) {
		for _, e := range wi.State.NewEvents {
			if os := e.GetWorkflowStarted(); os != nil {
				os.Version = version
				if len(version.GetPatches()) > 0 {
					t.span.SetAttributes(attribute.StringSlice("applied_patches", version.GetPatches()))
				}
				break
			}
		}
	}

	// A terminate in this batch must never be lost to ContinueAsNew; see
	// stripContinueAsNewOnTerminate.
	if t.terminateEvent != nil {
		stripContinueAsNewOnTerminate(results)
	}

	applyResult, err := w.applier.Actions(wi.State, results.CustomStatus, results.Actions, helpers.TraceContextFromSpan(t.span), execOpts.PropagatedHistory)
	if err != nil {
		t.finish(fmt.Errorf("failed to apply the execution result actions: %w", err))
		return
	}

	wi.OutgoingHistory = applyResult.OutgoingHistory

	if applyResult.ContinuedAsNew {
		if t.continueAsNewCount >= maxContinueAsNewCount {
			t.finish(fmt.Errorf("exceeded tight-loop continue-as-new limit of %d iterations", maxContinueAsNewCount))
			return
		}

		if applyResult.NewIncomingHistory != nil {
			wi.IncomingHistory = applyResult.NewIncomingHistory
		}

		w.endWorkflowSpan(t.ctx, wi, t.span, true)
		t.ctx, t.span = w.startOrResumeWorkflowSpan(t.ctx, wi)
		t.continueAsNewCount++
		t.execute()
		return
	}

	// Mirror ProcessWorkItem: a consumed terminate the executor ignored is
	// enforced here, since the event can never be re-delivered.
	if t.terminateEvent != nil && !runtimestate.IsCompleted(wi.State) {
		if ferr := w.forceTermination(wi, t.terminateEvent, t.span); ferr != nil {
			t.finish(ferr)
			return
		}
	}

	if runtimestate.IsCompleted(wi.State) {
		name, _ := runtimestate.Name(wi.State)
		w.logger.Infof("%v: '%s' completed with a %s status.", wi.InstanceID, name, helpers.ToRuntimeStatusString(runtimestate.RuntimeStatus(wi.State)))
	}
	t.finish(nil)
}

// stripContinueAsNewOnTerminate removes any continue-as-new completion from
// the executor's actions when the work item carried a terminate: applying it
// would replace the state with a fresh generation, so the forced termination
// would terminate that synthetic generation and the wipe would discard the
// child workflow history that cascade termination reads.
func stripContinueAsNewOnTerminate(results *protos.WorkflowResponse) {
	actions := results.Actions[:0]
	for _, a := range results.Actions {
		if co := a.GetCompleteWorkflow(); co != nil && co.WorkflowStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW {
			continue
		}
		actions = append(actions, a)
	}
	results.Actions = actions
}

// forceTermination drops the doomed execution's pending work and completes
// the workflow as TERMINATED: used when a work item carried an
// ExecutionTerminated event the executor did not honour, which is consumed
// with the work item and can never be re-delivered.
func (w *workflowProcessor) forceTermination(wi *WorkflowWorkItem, terminateEvent *protos.ExecutionTerminatedEvent, span trace.Span) error {
	w.logger.Warnf("%v: workflow was terminated but the executor did not complete it; forcing termination", wi.InstanceID)
	wi.State.PendingTasks = nil
	wi.State.PendingTimers = nil
	wi.State.PendingMessages = nil
	forced := []*protos.WorkflowAction{{
		Id: -1,
		WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{
			CompleteWorkflow: &protos.CompleteWorkflowAction{
				WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED,
				Result:         terminateEvent.Input,
			},
		},
	}}
	if _, err := w.applier.Actions(wi.State, wi.State.CustomStatus, forced, helpers.TraceContextFromSpan(span), nil); err != nil {
		return fmt.Errorf("failed to apply forced termination: %w", err)
	}
	return nil
}

func (t *workflowTurn) finish(err error) {
	t.processor.endWorkflowSpan(t.ctx, t.wi, t.span, false)
	if err == nil && t.terminateEvent != nil && runtimestate.IsCompleted(t.wi.State) {
		appendCascadeTerminateMessages(t.wi.State, t.terminateEvent)
	}
	t.done(err)
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
	errs := runtimestate.AddEvents(wi.State, wi.NewEvents)
	for i, e := range wi.NewEvents {
		if err := errs[i]; err != nil {
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
