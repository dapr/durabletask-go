package task

import (
	"fmt"
	"maps"
	"strconv"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
)

// scheduleNewWorkflowOptions is a struct that holds the options for the
// ScheduleNewWorkflow workflow method. It mirrors the fields of
// CreateInstanceRequest so that a workflow author can spawn a fully
// decoupled instance with the same surface area as the client API.
type scheduleNewWorkflowOptions struct {
	instanceID              *string
	rawInput                *wrapperspb.StringValue
	executionID             *wrapperspb.StringValue
	scheduledStartTimestamp *timestamppb.Timestamp
	tags                    map[string]string
	parentTraceContext      *protos.TraceContext
	targetAppID             *string
	targetAppNamespace      *string
}

// DetachedWorkflowOptions is the interface for options passed to
// ScheduleNewWorkflow. Unlike CallChildWorkflow, the spawned instance is
// fire-and-forget: the caller receives the instance ID synchronously but
// does not wait on (and is not notified of) the spawned workflow's
// completion.
type DetachedWorkflowOptions interface {
	applyDetachedWorkflowOption(*scheduleNewWorkflowOptions) error
}

// DetachedWorkflowOptionsFunc adapts a function to the
// DetachedWorkflowOptions interface.
type DetachedWorkflowOptionsFunc func(*scheduleNewWorkflowOptions) error

func (f DetachedWorkflowOptionsFunc) applyDetachedWorkflowOption(opts *scheduleNewWorkflowOptions) error {
	return f(opts)
}

// WithDetachedWorkflowInstanceID sets the instance ID of the detached
// workflow. When omitted, ScheduleNewWorkflow generates a deterministic
// ID of the form "<callerInstanceID>-<n>" where n increments per
// default-ID spawn within the execution. The '-' separator keeps the
// generated ID safe for consumers (e.g. dapr) that propagate the
// instance id into RFC 1123 subdomain identifiers. Passing an empty
// string is rejected as an error: callers either set a non-empty ID or
// omit the option entirely to opt into the default.
func WithDetachedWorkflowInstanceID(instanceID string) DetachedWorkflowOptionsFunc {
	return func(opts *scheduleNewWorkflowOptions) error {
		opts.instanceID = &instanceID
		return nil
	}
}

// WithDetachedWorkflowInput sets the input for the detached workflow,
// marshalling it to JSON.
func WithDetachedWorkflowInput(input any) DetachedWorkflowOptionsFunc {
	return func(opts *scheduleNewWorkflowOptions) error {
		bytes, err := marshalData(input)
		if err != nil {
			return fmt.Errorf("failed to marshal input to JSON: %w", err)
		}
		opts.rawInput = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawDetachedWorkflowInput sets a pre-marshalled raw input on the
// detached workflow.
func WithRawDetachedWorkflowInput(input *wrapperspb.StringValue) DetachedWorkflowOptionsFunc {
	return func(opts *scheduleNewWorkflowOptions) error {
		opts.rawInput = input
		return nil
	}
}

// WithDetachedWorkflowExecutionID sets an explicit execution ID on the
// detached workflow's first execution. When unset, the runtime mints a
// fresh UUID, matching the client ScheduleNewWorkflow behavior. Passing
// an empty string is rejected as an error: callers either set a
// non-empty execution ID or omit the option to opt into UUID minting.
func WithDetachedWorkflowExecutionID(executionID string) DetachedWorkflowOptionsFunc {
	return func(opts *scheduleNewWorkflowOptions) error {
		if executionID == "" {
			return fmt.Errorf("WithDetachedWorkflowExecutionID was passed an empty string; omit the option to let the runtime mint a UUID")
		}
		opts.executionID = wrapperspb.String(executionID)
		return nil
	}
}

// WithDetachedWorkflowStartTime defers the start of the detached workflow
// until the given time. Mirrors api.WithStartTime on the client API.
func WithDetachedWorkflowStartTime(startTime time.Time) DetachedWorkflowOptionsFunc {
	return func(opts *scheduleNewWorkflowOptions) error {
		opts.scheduledStartTimestamp = timestamppb.New(startTime)
		return nil
	}
}

// WithDetachedWorkflowTags sets the tag map on the detached workflow.
// The map is copied, so later mutations by the caller do not affect the
// scheduled action.
func WithDetachedWorkflowTags(tags map[string]string) DetachedWorkflowOptionsFunc {
	return func(opts *scheduleNewWorkflowOptions) error {
		opts.tags = maps.Clone(tags)
		return nil
	}
}

// WithDetachedWorkflowAppID specifies the app ID hosting the detached
// workflow. When set, the action carries a routing envelope so the
// dispatcher delivers the new instance to the target app.
func WithDetachedWorkflowAppID(appID string) DetachedWorkflowOptionsFunc {
	return func(opts *scheduleNewWorkflowOptions) error {
		opts.targetAppID = &appID
		return nil
	}
}

// WithDetachedWorkflowAppNamespace specifies the Dapr namespace hosting
// the detached workflow. Must be combined with WithDetachedWorkflowAppID;
// see WithChildWorkflowAppNamespace for full semantics.
func WithDetachedWorkflowAppNamespace(namespace string) DetachedWorkflowOptionsFunc {
	return func(opts *scheduleNewWorkflowOptions) error {
		opts.targetAppNamespace = &namespace
		return nil
	}
}

// ScheduleNewWorkflow schedules a new, fully decoupled workflow instance
// from the calling workflow. Unlike CallChildWorkflow, the spawned
// workflow has no parent linkage: its history's ExecutionStartedEvent
// carries no ParentInstanceInfo, completion and failure do not flow back
// to the caller, and the caller receives the new instance ID
// synchronously rather than an awaitable Task.
//
// If WithDetachedWorkflowInstanceID is not provided, a deterministic ID
// of the form "<callerInstanceID>-<n>" is generated, where n increments
// only for default-ID spawns within this execution (so the suffix
// reflects the order of those calls and is stable across replays). The
// '-' separator keeps the generated ID safe for consumers that
// propagate the instance id into RFC 1123 subdomain identifiers.
//
// If the workflow author needs the spawned workflow's result, they should
// model the dependency through external events (RaiseEvent / WaitForEvent)
// or shared state — there is no built-in completion channel.
func (ctx *WorkflowContext) ScheduleNewWorkflow(workflow any, opts ...DetachedWorkflowOptions) (api.InstanceID, error) {
	options := new(scheduleNewWorkflowOptions)
	for _, configure := range opts {
		if err := configure.applyDetachedWorkflowOption(options); err != nil {
			return api.EmptyInstanceID, err
		}
	}

	if options.targetAppNamespace != nil && options.targetAppID == nil {
		return api.EmptyInstanceID, fmt.Errorf("WithDetachedWorkflowAppNamespace requires WithDetachedWorkflowAppID to also be set")
	}

	var instanceID string
	if options.instanceID == nil {
		instanceID = string(ctx.ID) + "-" + strconv.FormatInt(int64(ctx.defaultDetachedWorkflowCounter), 10)
		ctx.defaultDetachedWorkflowCounter++
	} else if *options.instanceID == "" {
		return api.EmptyInstanceID, fmt.Errorf("WithDetachedWorkflowInstanceID was passed an empty string; omit the option to opt into the default ID")
	} else {
		instanceID = *options.instanceID
	}

	workflowName := helpers.GetTaskFunctionName(workflow)

	action := &protos.WorkflowAction{
		Id: ctx.getNextSequenceNumber(),
		WorkflowActionType: &protos.WorkflowAction_CreateDetachedWorkflow{
			CreateDetachedWorkflow: &protos.CreateDetachedWorkflowAction{
				InstanceId:              instanceID,
				Name:                    workflowName,
				Input:                   options.rawInput,
				ScheduledStartTimestamp: options.scheduledStartTimestamp,
				ExecutionId:             options.executionID,
				Tags:                    options.tags,
				ParentTraceContext:      options.parentTraceContext,
			},
		},
	}

	if r := taskRouterFromTarget(options.targetAppID, options.targetAppNamespace); r != nil {
		action.Router = r
	}

	ctx.pendingActions[action.Id] = action
	return api.InstanceID(instanceID), nil
}

// onDetachedWorkflowCreated retires the pending CreateDetachedWorkflowAction
// at this sequence position. Detached workflows are fire-and-forget, so
// there is no associated pending Task to complete: the call returned the
// instance ID synchronously when the action was emitted.
func (ctx *WorkflowContext) onDetachedWorkflowCreated(taskID int32, dw *protos.DetachedWorkflowInstanceCreatedEvent) error {
	a, ok := ctx.pendingActions[taskID]
	if !ok || a.GetCreateDetachedWorkflow() == nil {
		if ctx.dropOptionalExternalEventTimerAt(taskID) {
			a, ok = ctx.pendingActions[taskID]
		}
	}
	if !ok || a.GetCreateDetachedWorkflow() == nil {
		return fmt.Errorf(
			"a previous execution called ScheduleNewWorkflow for instance ID '%s' and sequence number %d at this point in the workflow logic, but the current execution doesn't have this action with this sequence number",
			dw.InstanceId,
			taskID,
		)
	}
	// The instance ID is the value ScheduleNewWorkflow returned to the
	// workflow code, so a mismatch means the current execution is
	// referencing an instance that was never started. Fail rather than
	// silently accepting the old history.
	if scheduled := a.GetCreateDetachedWorkflow().GetInstanceId(); scheduled != dw.InstanceId {
		return fmt.Errorf(
			"a previous execution called ScheduleNewWorkflow for instance ID '%s' and sequence number %d at this point in the workflow logic, but the current execution scheduled instance ID '%s'",
			dw.InstanceId,
			taskID,
			scheduled,
		)
	}
	delete(ctx.pendingActions, taskID)
	return nil
}
