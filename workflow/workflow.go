package workflow

import (
	"time"

	"github.com/dapr/durabletask-go/task"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// Workflow is the functional interface for workflow functions.
type Workflow func(ctx *WorkflowContext) (any, error)

// ChildWorkflowOption is a functional option type for the CallChildWorkflow
// workflow method.
type ChildWorkflowOption task.SubOrchestratorOption

// WorkflowContext is the parameter type for workflow functions.
type WorkflowContext struct {
	oc *task.OrchestrationContext
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
	oopts := make([]task.SubOrchestratorOption, len(opts))
	for i, o := range opts {
		oopts[i] = task.SubOrchestratorOption(o)
	}
	return w.oc.CallSubOrchestrator(workflow, oopts...)
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

func (w *WorkflowContext) ContinueAsNew(newInput any, options ...ContinueAsNewOption) {
	oopts := make([]task.ContinueAsNewOption, len(options))
	for i, o := range options {
		oopts[i] = task.ContinueAsNewOption(o)
	}
	w.oc.ContinueAsNew(newInput, oopts...)
}

// WithChildWorkflowAppID is a functional option type for the CallChildWorkflow
// workflow method that specifies the app ID of the target activity.
func WithChildWorkflowAppID(appID string) ChildWorkflowOption {
	return ChildWorkflowOption(task.WithSubOrchestratorAppID(appID))
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
	return ChildWorkflowOption(task.WithSubOrchestratorInput(input))
}

// WithRawChildWorkflowInput is a functional option type for the
// CallChildWorkflow workflow method that takes a raw input value.
func WithRawChildWorkflowInput(input *wrapperspb.StringValue) ChildWorkflowOption {
	return ChildWorkflowOption(task.WithRawSubOrchestratorInput(input))
}

// WithChildWorkflowInstanceID is a functional option type for the
// CallChildWorkflow workflow method that specifies the instance ID of the
// child-workflow.
func WithChildWorkflowInstanceID(instanceID string) ChildWorkflowOption {
	return ChildWorkflowOption(task.WithSubOrchestrationInstanceID(instanceID))
}

func WithChildWorkflowRetryPolicy(policy *RetryPolicy) ChildWorkflowOption {
	return ChildWorkflowOption(task.WithSubOrchestrationRetryPolicy(&task.RetryPolicy{
		MaxAttempts:          policy.MaxAttempts,
		InitialRetryInterval: policy.InitialRetryInterval,
		BackoffCoefficient:   policy.BackoffCoefficient,
		MaxRetryInterval:     policy.MaxRetryInterval,
		RetryTimeout:         policy.RetryTimeout,
		Handle:               policy.Handle,
	}))
}
