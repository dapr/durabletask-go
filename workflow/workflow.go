package workflow

import (
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/task"
)

// Workflow is the functional interface for workflow functions.
type Workflow func(ctx *WorkflowContext) (any, error)

// ChildWorkflowOption is a functional option type for the CallChildWorkflow
// workflow method.
type ChildWorkflowOption task.ChildWorkflowOption

// WorkflowContext is the parameter type for workflow functions.
type WorkflowContext struct {
	oc *task.WorkflowContext
}

func (w *WorkflowContext) SetCustomStatus(cs string) {
	w.oc.SetCustomStatus(cs)
}

// GetInput unmarshals the serialized workflow input and stores it in [v].
func (w *WorkflowContext) GetInput(v any) error {
	return w.oc.GetInput(v)
}

func (w *WorkflowContext) ID() string {
	return string(w.oc.ID)
}

func (w *WorkflowContext) Name() string {
	return w.oc.Name
}

func (w *WorkflowContext) CurrentTimeUTC() time.Time {
	return w.oc.CurrentTimeUtc
}

func (w *WorkflowContext) IsReplaying() bool {
	return w.oc.IsReplaying
}

// CallActivity schedules an asynchronous invocation of an activity function.
// The [activity] parameter can be either the name of an activity as a string
// or can be a pointer to the function that implements the activity, in which
// case the name is obtained via reflection.
func (w *WorkflowContext) CallActivity(activity any, opts ...CallActivityOption) Task {
	oopts := make([]task.CallActivityOption, len(opts))
	for i, o := range opts {
		oopts[i] = task.CallActivityOption(o)
	}
	return w.oc.CallActivity(activity, oopts...)
}

func (w *WorkflowContext) CallChildWorkflow(workflow any, opts ...ChildWorkflowOption) Task {
	oopts := make([]task.ChildWorkflowOption, len(opts))
	for i, o := range opts {
		oopts[i] = task.ChildWorkflowOption(o)
	}
	return w.oc.CallChildWorkflow(workflow, oopts...)
}

// CreateTimer schedules a durable timer that expires after the specified
// delay.
func (w *WorkflowContext) CreateTimer(delay time.Duration, opts ...CreateTimerOption) Task {
	oopts := make([]task.CreateTimerOption, len(opts))
	for i, o := range opts {
		oopts[i] = task.CreateTimerOption(o)
	}
	return w.oc.CreateTimer(delay, oopts...)
}

// WaitForExternalEvent creates a task that is completed only after an event
// named [eventName] is received by this workflow or when the specified timeout
// expires.
//
// The [timeout] parameter can be used to define a timeout for receiving the
// event. If the timeout expires before the named event is received, the task
// will be completed and will return a timeout error value [ErrTaskCanceled]
// when awaited. Otherwise, the awaited task will return the deserialized
// payload of the received event. A Duration value of zero returns a canceled
// task if the event isn't already available in the history. Use a negative
// Duration to wait indefinitely for the event to be received.
//
// Workflows can wait for the same event name multiple times, so waiting for
// multiple events with the same name is allowed. Each event received by an
// workflow will complete just one task returned by this method.
//
// Note that event names are case-insensitive.
func (w *WorkflowContext) WaitForExternalEvent(eventName string, timeout time.Duration) Task {
	return w.oc.WaitForSingleEvent(eventName, timeout)
}

// Select blocks until the first of the given [tasks] completes and returns its index. Once Select
// returns, callers should call Await on the task at the returned index to obtain its result or
// error; the remaining tasks are left pending and may still be selected or awaited later (for
// example, in a loop that repeatedly selects over the tasks that have not yet completed).
//
// If more than one of the given tasks is found completed at the same time -- whether because they
// were already completed before Select was called, or a single event completed several of them at
// once -- the one with the lowest index wins; this is the only tie-break rule Select ever applies.
//
// Select requires at least one task and returns an error if no tasks are given, if any task is nil,
// or if any task was not obtained from this same WorkflowContext (e.g. via CallActivity, CreateTimer,
// or WaitForExternalEvent, with or without a retry policy) -- a task from a different WorkflowContext
// can never complete from this context's point of view, which would otherwise block the workflow
// indefinitely with no diagnostic. A Task implementation from outside this package is also rejected,
// with [ErrTaskNotSelectable].
//
// Like Await, Select may panic with [task.ErrTaskBlocked] as the panic value when none of the tasks
// have completed and there is no further history to process. This is normal control flow for
// workflow functions, which must never recover from such panics: doing so prevents the workflow
// runtime's own recovery from observing the signal, and the workflow will never emit its pending
// actions.
func (w *WorkflowContext) Select(tasks ...Task) (int, error) {
	otasks := make([]task.Task, len(tasks))
	for i, t := range tasks {
		otasks[i] = t
	}
	return w.oc.Select(otasks...)
}

func (w *WorkflowContext) ContinueAsNew(newInput any, options ...ContinueAsNewOption) {
	oopts := make([]task.ContinueAsNewOption, len(options))
	for i, o := range options {
		oopts[i] = task.ContinueAsNewOption(o)
	}
	w.oc.ContinueAsNew(newInput, oopts...)
}

func (w *WorkflowContext) IsPatched(patchName string) bool {
	return w.oc.IsPatched(patchName)
}

// GetPropagatedHistory returns the propagated history from a parent workflow,
// or nil if no history was propagated.
func (w *WorkflowContext) GetPropagatedHistory() *PropagatedHistory {
	return w.oc.GetPropagatedHistory()
}

// WithChildWorkflowAppID is a functional option type for the CallChildWorkflow
// workflow method that specifies the app ID of the target activity.
func WithChildWorkflowAppID(appID string) ChildWorkflowOption {
	return ChildWorkflowOption(task.WithChildWorkflowAppID(appID))
}

// WithChildWorkflowAppNamespace specifies the Dapr namespace that hosts the
// target child workflow. Must be combined with WithChildWorkflowAppID. See
// task.WithChildWorkflowAppNamespace for full semantics.
func WithChildWorkflowAppNamespace(namespace string) ChildWorkflowOption {
	return ChildWorkflowOption(task.WithChildWorkflowAppNamespace(namespace))
}

// ContinueAsNewOption is a functional option type for the ContinueAsNew
// workflow method.
type ContinueAsNewOption task.ContinueAsNewOption

// WithKeepUnprocessedEvents returns a ContinueAsNewOptions struct that
// instructs the runtime to carry forward any unprocessed external events to
// the new instance.
func WithKeepUnprocessedEvents() ContinueAsNewOption {
	return ContinueAsNewOption(task.WithKeepUnprocessedEvents())
}

// WithChildWorkflowInput is a functional option type for the CallChildWorkflow
// workflow method that takes an input value and marshals it to JSON.
func WithChildWorkflowInput(input any) ChildWorkflowOption {
	return ChildWorkflowOption(task.WithChildWorkflowInput(input))
}

// WithRawChildWorkflowInput is a functional option type for the
// CallChildWorkflow workflow method that takes a raw input value.
func WithRawChildWorkflowInput(input *wrapperspb.StringValue) ChildWorkflowOption {
	return ChildWorkflowOption(task.WithRawChildWorkflowInput(input))
}

// WithChildWorkflowInstanceID is a functional option type for the
// CallChildWorkflow workflow method that specifies the instance ID of the
// child-workflow.
func WithChildWorkflowInstanceID(instanceID string) ChildWorkflowOption {
	return ChildWorkflowOption(task.WithChildWorkflowInstanceID(instanceID))
}

func WithChildWorkflowRetryPolicy(policy *RetryPolicy) ChildWorkflowOption {
	return ChildWorkflowOption(task.WithChildWorkflowRetryPolicy(&task.RetryPolicy{
		MaxAttempts:          policy.MaxAttempts,
		InitialRetryInterval: policy.InitialRetryInterval,
		BackoffCoefficient:   policy.BackoffCoefficient,
		MaxRetryInterval:     policy.MaxRetryInterval,
		RetryTimeout:         policy.RetryTimeout,
		Handle:               policy.Handle,
	}))
}
