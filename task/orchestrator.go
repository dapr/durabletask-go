package task

import (
	"container/list"
	"encoding/json"
	"fmt"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/kit/ptr"
)

// Workflow is the functional interface for workflow functions.
type Workflow func(ctx *WorkflowContext) (any, error)

// WorkflowContext is the parameter type for workflow functions.
type WorkflowContext struct {
	ID             api.InstanceID
	Name           string
	VersionName    *string
	IsReplaying    bool
	CurrentTimeUtc time.Time

	registry            *TaskRegistry
	rawInput            []byte
	oldEvents           []*protos.HistoryEvent
	newEvents           []*protos.HistoryEvent
	suspendedEvents     []*protos.HistoryEvent
	isSuspended         bool
	historyIndex        int
	sequenceNumber      int32
	pendingActions      map[int32]*protos.WorkflowAction
	pendingTasks        map[int32]*completableTask
	continuedAsNew      bool
	continuedAsNewInput any
	customStatus        string

	bufferedExternalEvents     map[string]*list.List
	pendingExternalEventTasks  map[string]*list.List
	saveBufferedExternalEvents bool
	historyPatches             map[string]bool
	appliedPatches             map[string]bool
	encounteredPatches         []string
}

// callChildWorkflowOptions is a struct that holds the options for the CallChildWorkflow workflow method.
type callChildWorkflowOptions struct {
	instanceID  string
	rawInput    *wrapperspb.StringValue
	targetAppID *string
	retryPolicy *RetryPolicy
}

// ChildWorkflowOption is a functional option type for the CallChildWorkflow workflow method.
type ChildWorkflowOption func(*callChildWorkflowOptions) error

// ContinueAsNewOption is a functional option type for the ContinueAsNew workflow method.
type ContinueAsNewOption func(*WorkflowContext)

// WithChildWorkflowAppID is a functional option type for the CallChildWorkflow workflow method that specifies the app ID of the target activity.
func WithChildWorkflowAppID(appID string) ChildWorkflowOption {
	return func(opts *callChildWorkflowOptions) error {
		opts.targetAppID = &appID
		return nil
	}
}

// WithKeepUnprocessedEvents returns a ContinueAsNewOptions struct that instructs the
// runtime to carry forward any unprocessed external events to the new instance.
func WithKeepUnprocessedEvents() ContinueAsNewOption {
	return func(ctx *WorkflowContext) {
		ctx.saveBufferedExternalEvents = true
	}
}

// WithChildWorkflowInput is a functional option type for the CallChildWorkflow
// workflow method that takes an input value and marshals it to JSON.
func WithChildWorkflowInput(input any) ChildWorkflowOption {
	return func(opts *callChildWorkflowOptions) error {
		bytes, err := marshalData(input)
		if err != nil {
			return fmt.Errorf("failed to marshal input to JSON: %w", err)
		}
		opts.rawInput = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawChildWorkflowInput is a functional option type for the CallChildWorkflow
// workflow method that takes a raw input value.
func WithRawChildWorkflowInput(input *wrapperspb.StringValue) ChildWorkflowOption {
	return func(opts *callChildWorkflowOptions) error {
		opts.rawInput = input
		return nil
	}
}

// WithChildWorkflowInstanceID is a functional option type for the CallChildWorkflow
// workflow method that specifies the instance ID of the child workflow.
func WithChildWorkflowInstanceID(instanceID string) ChildWorkflowOption {
	return func(opts *callChildWorkflowOptions) error {
		opts.instanceID = instanceID
		return nil
	}
}

func WithChildWorkflowRetryPolicy(policy *RetryPolicy) ChildWorkflowOption {
	return func(opt *callChildWorkflowOptions) error {
		if policy == nil {
			return nil
		}
		err := policy.Validate()
		if err != nil {
			return err
		}
		opt.retryPolicy = policy
		return nil
	}
}

// NewWorkflowContext returns a new [WorkflowContext] struct with the specified parameters.
func NewWorkflowContext(registry *TaskRegistry, id api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) *WorkflowContext {
	return &WorkflowContext{
		ID:                        id,
		registry:                  registry,
		oldEvents:                 oldEvents,
		newEvents:                 newEvents,
		bufferedExternalEvents:    make(map[string]*list.List),
		pendingExternalEventTasks: make(map[string]*list.List),
		historyPatches:            make(map[string]bool),
		appliedPatches:            make(map[string]bool),
		encounteredPatches:        make([]string, 0),
	}
}

func (ctx *WorkflowContext) start() (actions []*protos.WorkflowAction) {
	ctx.historyIndex = 0
	ctx.sequenceNumber = 0
	ctx.pendingActions = make(map[int32]*protos.WorkflowAction)
	ctx.pendingTasks = make(map[int32]*completableTask)

	defer func() {
		result := recover()
		if result == ErrTaskBlocked {
			// Expected, normal part of execution
			actions = ctx.actions()
		} else if result != nil {
			// Unexpected panic!
			panic(result)
		}
	}()

	for {
		if ok, err := ctx.processNextEvent(); err != nil {
			if api.IsUnsupportedVersionError(err) {
				ctx.setVersionNotRegistered()
				break
			}
			ctx.setFailed(err)
			break
		} else if !ok {
			// Workflow finished, break out of the loop and return any pending actions
			break
		}
	}
	return ctx.actions()
}

func (ctx *WorkflowContext) processNextEvent() (bool, error) {
	e, ok := ctx.getNextHistoryEvent()
	if !ok {
		// No more history
		return false, nil
	}

	if err := ctx.processEvent(e); err != nil {
		// Internal failure processing event
		return true, err
	}
	return true, nil
}

func (ctx *WorkflowContext) getNextHistoryEvent() (*protos.HistoryEvent, bool) {
	var historyList []*protos.HistoryEvent
	index := ctx.historyIndex
	if ctx.historyIndex >= len(ctx.oldEvents)+len(ctx.newEvents) {
		return nil, false
	} else if ctx.historyIndex < len(ctx.oldEvents) {
		ctx.IsReplaying = true
		historyList = ctx.oldEvents
	} else {
		ctx.IsReplaying = false
		historyList = ctx.newEvents
		index -= len(ctx.oldEvents)
	}

	ctx.historyIndex++
	e := historyList[index]
	return e, true
}

func (ctx *WorkflowContext) processEvent(e *backend.HistoryEvent) error {
	// Buffer certain events if we're in a suspended state
	if ctx.isSuspended && (e.GetExecutionResumed() == nil && e.GetExecutionTerminated() == nil) {
		ctx.suspendedEvents = append(ctx.suspendedEvents, e)
		return nil
	}

	var err error = nil
	if os := e.GetWorkflowStarted(); os != nil {
		// WorkflowStarted is only used to update the current workflow time and history patches
		ctx.CurrentTimeUtc = e.Timestamp.AsTime()
		if version := os.GetVersion(); version != nil {
			for _, p := range version.GetPatches() {
				ctx.historyPatches[p] = true
			}
			if version.Name != nil {
				ctx.VersionName = ptr.Of(version.GetName())
			}
		}
	} else if es := e.GetExecutionStarted(); es != nil {
		err = ctx.onExecutionStarted(es)
	} else if ts := e.GetTaskScheduled(); ts != nil {
		err = ctx.onTaskScheduled(e.EventId, ts)
	} else if tc := e.GetTaskCompleted(); tc != nil {
		err = ctx.onTaskCompleted(tc)
	} else if tf := e.GetTaskFailed(); tf != nil {
		err = ctx.onTaskFailed(tf)
	} else if ts := e.GetChildWorkflowInstanceCreated(); ts != nil {
		err = ctx.onChildWorkflowScheduled(e.EventId, ts)
	} else if sc := e.GetChildWorkflowInstanceCompleted(); sc != nil {
		err = ctx.onChildWorkflowCompleted(sc)
	} else if sf := e.GetChildWorkflowInstanceFailed(); sf != nil {
		err = ctx.onChildWorkflowFailed(sf)
	} else if tc := e.GetTimerCreated(); tc != nil {
		err = ctx.onTimerCreated(e)
	} else if tf := e.GetTimerFired(); tf != nil {
		err = ctx.onTimerFired(tf)
	} else if er := e.GetEventRaised(); er != nil {
		err = ctx.onExternalEventRaised(e)
	} else if es := e.GetExecutionSuspended(); es != nil {
		err = ctx.onExecutionSuspended(es)
	} else if er := e.GetExecutionResumed(); er != nil {
		err = ctx.onExecutionResumed(er)
	} else if et := e.GetExecutionTerminated(); et != nil {
		err = ctx.onExecutionTerminated(et)
	} else if e.GetExecutionStalled() != nil {
		// Nothing to do
	} else if oc := e.GetWorkflowCompleted(); oc != nil {
		// Nothing to do
	} else {
		err = fmt.Errorf("don't know how to handle event: %v", e)
	}
	return err
}

func (octx *WorkflowContext) SetCustomStatus(cs string) {
	octx.customStatus = cs
}

// GetInput unmarshals the serialized workflow input and stores it in [v].
func (octx *WorkflowContext) GetInput(v any) error {
	return unmarshalData(octx.rawInput, v)
}

// CallActivity schedules an asynchronous invocation of an activity function. The [activity]
// parameter can be either the name of an activity as a string or can be a pointer to the function
// that implements the activity, in which case the name is obtained via reflection.
func (ctx *WorkflowContext) CallActivity(activity interface{}, opts ...CallActivityOption) Task {
	options := new(callActivityOptions)
	for _, configure := range opts {
		if err := configure(options); err != nil {
			failedTask := newTask(ctx)
			failedTask.fail(&protos.TaskFailureDetails{
				ErrorType:    reflect.TypeOf(err).String(),
				ErrorMessage: err.Error(),
			})
			return failedTask
		}
	}

	activityName := helpers.GetTaskFunctionName(activity)

	if options.retryPolicy != nil {
		return ctx.internalScheduleTaskWithRetries(activityName+"-retry", ctx.CurrentTimeUtc, func(taskExecutionId string) Task {
			return ctx.internalScheduleActivity(activityName, taskExecutionId, options)
		}, *options.retryPolicy, 0, uuid.NewString())
	}

	return ctx.internalScheduleActivity(activityName, uuid.NewString(), options)
}

func (ctx *WorkflowContext) internalScheduleActivity(activityName, taskExecutionId string, options *callActivityOptions) Task {
	scheduleTaskAction := &protos.WorkflowAction{
		Id: ctx.getNextSequenceNumber(),
		WorkflowActionType: &protos.WorkflowAction_ScheduleTask{
			ScheduleTask: &protos.ScheduleTaskAction{Name: activityName, TaskExecutionId: taskExecutionId, Input: options.rawInput},
		},
	}

	if options.targetAppID != nil {
		scheduleTaskAction.Router = &protos.TaskRouter{
			TargetAppID: ptr.Of(*options.targetAppID),
		}
	}

	ctx.pendingActions[scheduleTaskAction.Id] = scheduleTaskAction

	task := newTask(ctx)
	ctx.pendingTasks[scheduleTaskAction.Id] = task
	return task
}

func (ctx *WorkflowContext) CallChildWorkflow(workflow interface{}, opts ...ChildWorkflowOption) Task {
	options := new(callChildWorkflowOptions)
	for _, configure := range opts {
		if err := configure(options); err != nil {
			failedTask := newTask(ctx)
			failedTask.fail(&protos.TaskFailureDetails{
				ErrorType:    reflect.TypeOf(err).String(),
				ErrorMessage: err.Error(),
			})
			return failedTask
		}
	}

	workflowName := helpers.GetTaskFunctionName(workflow)

	if options.retryPolicy != nil {
		return ctx.internalScheduleTaskWithRetries(workflowName+"-retry", ctx.CurrentTimeUtc, func(_ string) Task {
			return ctx.internalCallChildWorkflow(workflowName, options)
		}, *options.retryPolicy, 0, uuid.NewString())
	}

	return ctx.internalCallChildWorkflow(workflowName, options)
}

func (ctx *WorkflowContext) internalCallChildWorkflow(workflowName string, options *callChildWorkflowOptions) Task {
	createChildWorkflowAction := &protos.WorkflowAction{
		Id: ctx.getNextSequenceNumber(),
		WorkflowActionType: &protos.WorkflowAction_CreateChildWorkflow{
			CreateChildWorkflow: &protos.CreateChildWorkflowAction{
				Name:       workflowName,
				Input:      options.rawInput,
				InstanceId: options.instanceID,
			},
		},
	}

	if options.targetAppID != nil {
		createChildWorkflowAction.Router = &protos.TaskRouter{
			TargetAppID: ptr.Of(*options.targetAppID),
		}
	}

	ctx.pendingActions[createChildWorkflowAction.Id] = createChildWorkflowAction

	task := newTask(ctx)
	ctx.pendingTasks[createChildWorkflowAction.Id] = task
	return task
}

func (ctx *WorkflowContext) internalScheduleTaskWithRetries(name string, initialAttempt time.Time, schedule func(taskExecutionId string) Task, policy RetryPolicy, retryCount int, taskExecutionId string) Task {
	return &taskWrapper{
		delegate: schedule(taskExecutionId),
		onAwaitResult: func(v any, taskExecutionId string, err error) error {
			if err == nil {
				return nil
			}

			if retryCount+1 >= policy.MaxAttempts {
				// next try will exceed the max attempts, dont continue
				return err
			}

			nextDelay := computeNextDelay(ctx.CurrentTimeUtc, policy, retryCount, initialAttempt, err)
			if nextDelay == 0 {
				return err
			}
			timerErr := ctx.createTimerInternal(&name, nextDelay).Await(nil)
			if timerErr != nil {
				// TODO use errors.Join when updating golang
				return fmt.Errorf("%v %w", timerErr, err)
			}

			t := ctx.internalScheduleTaskWithRetries(name, initialAttempt, schedule, policy, retryCount+1, taskExecutionId)
			err = t.Await(v)
			if err == nil {
				return nil
			}

			return err
		},
	}
}

func computeNextDelay(currentTimeUtc time.Time, policy RetryPolicy, attempt int, firstAttempt time.Time, err error) time.Duration {
	if policy.Handle(err) {
		isExpired := false
		if policy.RetryTimeout != math.MaxInt64 {
			isExpired = currentTimeUtc.After(firstAttempt.Add(policy.RetryTimeout))
		}
		if !isExpired {
			nextDelayMs := float64(policy.InitialRetryInterval.Milliseconds()) * math.Pow(policy.BackoffCoefficient, float64(attempt))
			if nextDelayMs < float64(policy.MaxRetryInterval.Milliseconds()) {
				return time.Duration(int64(nextDelayMs) * int64(time.Millisecond))
			}
			return policy.MaxRetryInterval
		}
	}
	return 0
}

// CreateTimer schedules a durable timer that expires after the specified delay.
func (ctx *WorkflowContext) CreateTimer(delay time.Duration, opts ...CreateTimerOption) Task {
	options := new(createTimerOptions)
	for _, configure := range opts {
		if err := configure(options); err != nil {
			failedTask := newTask(ctx)
			failedTask.fail(&protos.TaskFailureDetails{
				ErrorType:    reflect.TypeOf(err).String(),
				ErrorMessage: err.Error(),
			})
			return failedTask
		}
	}
	return ctx.createTimerInternal(options.name, delay)
}

func (ctx *WorkflowContext) createTimerInternal(name *string, delay time.Duration) *completableTask {
	fireAt := ctx.CurrentTimeUtc.Add(delay)
	timerAction := &protos.WorkflowAction{
		Id: ctx.getNextSequenceNumber(),
		WorkflowActionType: &protos.WorkflowAction_CreateTimer{
			CreateTimer: &protos.CreateTimerAction{
				FireAt: timestamppb.New(fireAt),
				Name:   name,
				Origin: &protos.CreateTimerAction_CreateTimer{
					CreateTimer: &protos.TimerOriginCreateTimer{},
				},
			},
		},
	}
	ctx.pendingActions[timerAction.Id] = timerAction

	task := newTask(ctx)
	ctx.pendingTasks[timerAction.Id] = task
	return task
}

func (ctx *OrchestrationContext) createExternalEventTimerInternal(eventName string, fireAt time.Time) *completableTask {
	timerAction := &protos.OrchestratorAction{
		Id: ctx.getNextSequenceNumber(),
		OrchestratorActionType: &protos.OrchestratorAction_CreateTimer{
			CreateTimer: &protos.CreateTimerAction{
				FireAt: timestamppb.New(fireAt),
				Name:   &eventName,
				Origin: &protos.CreateTimerAction_ExternalEvent{
					ExternalEvent: &protos.TimerOriginExternalEvent{
						Name: eventName,
					},
				},
			},
		},
	}
	ctx.pendingActions[timerAction.Id] = timerAction

	task := newTask(ctx)
	ctx.pendingTasks[timerAction.Id] = task
	return task
}

// WaitForSingleEvent creates a task that is completed only after an event named [eventName] is received by this workflow
// or when the specified timeout expires.
//
// The [timeout] parameter can be used to define a timeout for receiving the event. If the timeout expires before the
// named event is received, the task will be completed and will return a timeout error value [ErrTaskCanceled] when
// awaited. Otherwise, the awaited task will return the deserialized payload of the received event. A Duration value
// of zero returns a canceled task if the event isn't already available in the history. Use a negative Duration to
// wait indefinitely for the event to be received.
//
// Workflows can wait for the same event name multiple times, so waiting for multiple events with the same name
// is allowed. Each event received by an workflow will complete just one task returned by this method.
//
// Note that event names are case-insensitive.
func (ctx *WorkflowContext) WaitForSingleEvent(eventName string, timeout time.Duration) Task {
	task := newTask(ctx)
	key := strings.ToUpper(eventName)
	if eventList, ok := ctx.bufferedExternalEvents[key]; ok {
		// An event with this name arrived already and can be consumed immediately.
		next := eventList.Front()
		if eventList.Len() > 1 {
			eventList.Remove(next)
		} else {
			delete(ctx.bufferedExternalEvents, key)
		}
		rawValue := []byte(next.Value.(*protos.HistoryEvent).GetEventRaised().GetInput().GetValue())
		task.complete(rawValue)
	} else if timeout == 0 {
		// Zero-timeout means fail immediately if the event isn't already buffered.
		task.cancel()
	} else {
		// Keep a reference to this task so we can complete it when the event of this name arrives
		var taskList *list.List
		var ok bool
		if taskList, ok = ctx.pendingExternalEventTasks[key]; !ok {
			taskList = list.New()
			ctx.pendingExternalEventTasks[key] = taskList
		}
		taskElement := taskList.PushBack(task)

		var fireAt time.Time
		if timeout > 0 {
			fireAt = ctx.CurrentTimeUtc.Add(timeout)
		} else {
			fireAt = time.Date(9999, 12, 31, 23, 59, 59, 999999999, time.UTC)
		}
		ctx.createExternalEventTimerInternal(eventName, fireAt).onCompleted(func() {
			task.cancel()
			if taskList.Len() > 1 {
				taskList.Remove(taskElement)
			} else {
				delete(ctx.pendingExternalEventTasks, key)
			}
		})
	}
	return task
}

func (ctx *WorkflowContext) ContinueAsNew(newInput any, options ...ContinueAsNewOption) {
	ctx.continuedAsNew = true
	ctx.continuedAsNewInput = newInput
	for _, option := range options {
		option(ctx)
	}
}

func (ctx *WorkflowContext) IsPatched(patchName string) bool {
	isPatched := ctx.isPatched(patchName)
	if isPatched {
		ctx.encounteredPatches = append(ctx.encounteredPatches, patchName)
	}
	return isPatched
}

func (ctx *WorkflowContext) isPatched(patchName string) bool {
	if patched, exists := ctx.appliedPatches[patchName]; exists {
		return patched
	}

	if ctx.historyPatches[patchName] {
		ctx.appliedPatches[patchName] = true
		return true
	}

	totalEvents := len(ctx.oldEvents) + len(ctx.newEvents)
	if ctx.historyIndex < totalEvents {
		// We're not at the end of the history stream, we assume the previous used the unpatched version
		ctx.appliedPatches[patchName] = false
		return false
	}

	// We're at the end of the history stream, we can run the patched version and save the decision for next rerun
	ctx.appliedPatches[patchName] = true
	return true
}

func (ctx *WorkflowContext) getWorkflow(es *protos.ExecutionStartedEvent) (Workflow, error) {
	workflow, ok := ctx.registry.workflows[es.Name]
	if ok {
		return workflow, nil
	}

	if versions, ok := ctx.registry.versionedWorkflows[es.Name]; ok {
		var versionToUse string
		if ctx.VersionName != nil {
			versionToUse = *ctx.VersionName
		} else {
			if latest, ok := ctx.registry.latestVersionedWorkflows[es.Name]; ok {
				versionToUse = latest
			} else {
				return nil, fmt.Errorf("versioned workflow '%s' does not have a latest version registered", es.Name)
			}
		}

		if workflow, ok = versions[versionToUse]; ok {
			ctx.VersionName = &versionToUse
			return workflow, nil
		} else {
			return nil, api.NewUnsupportedVersionError()
		}
	}

	if workflow, ok = ctx.registry.workflows["*"]; ok {
		return workflow, nil
	}

	return nil, fmt.Errorf("workflow named '%s' is not registered", es.Name)
}

func (ctx *WorkflowContext) onExecutionStarted(es *protos.ExecutionStartedEvent) error {
	workflow, err := ctx.getWorkflow(es)
	if err != nil {
		return err
	}
	ctx.Name = es.Name
	if es.Input != nil {
		ctx.rawInput = []byte(es.Input.Value)
	}

	output, appError := workflow(ctx)

	if appError != nil {
		err = ctx.setFailed(appError)
	} else if ctx.continuedAsNew {
		err = ctx.setContinuedAsNew()
	} else {
		err = ctx.setComplete(output)
	}

	if appError == nil && err != nil {
		completionErr := fmt.Errorf("failed to complete the workflow: %w", err)
		if err2 := ctx.setFailed(completionErr); err2 != nil {
			return completionErr
		}
	}
	return nil
}

func (ctx *WorkflowContext) onTaskScheduled(taskID int32, ts *protos.TaskScheduledEvent) error {
	if a, ok := ctx.pendingActions[taskID]; !ok || a.GetScheduleTask() == nil {
		return fmt.Errorf(
			"a previous execution called CallActivity for '%s' and sequence number %d at this point in the workflow logic, but the current execution doesn't have this action with this sequence number",
			ts.Name,
			taskID,
		)
	}
	delete(ctx.pendingActions, taskID)
	return nil
}

func (ctx *WorkflowContext) onTaskCompleted(tc *protos.TaskCompletedEvent) error {
	taskID := tc.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic workflow.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       workflow should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	if tc.Result != nil {
		task.complete([]byte(tc.Result.Value))
	} else {
		task.complete(nil)
	}
	return nil
}

func (ctx *WorkflowContext) onTaskFailed(tf *protos.TaskFailedEvent) error {
	taskID := tf.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic workflow.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       workflow should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	// completing a task will resume the corresponding Await() call
	task.fail(tf.FailureDetails)
	task.taskExecutionId = tf.TaskExecutionId
	return nil
}

func (ctx *WorkflowContext) onChildWorkflowScheduled(taskID int32, ts *protos.ChildWorkflowInstanceCreatedEvent) error {
	if a, ok := ctx.pendingActions[taskID]; !ok || a.GetCreateChildWorkflow() == nil {
		return fmt.Errorf(
			"a previous execution called CallChildWorkflow for '%s' and sequence number %d at this point in the workflow logic, but the current execution doesn't have this action with this sequence number",
			ts.Name,
			taskID,
		)
	}
	delete(ctx.pendingActions, taskID)
	return nil
}

func (ctx *WorkflowContext) onChildWorkflowCompleted(soc *protos.ChildWorkflowInstanceCompletedEvent) error {
	taskID := soc.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic workflow.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       workflow should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	// completing a task will resume the corresponding Await() call
	if soc.Result != nil {
		task.complete([]byte(soc.Result.Value))
	} else {
		task.complete(nil)
	}
	return nil
}

func (ctx *WorkflowContext) onChildWorkflowFailed(sof *protos.ChildWorkflowInstanceFailedEvent) error {
	taskID := sof.TaskScheduledId
	task, ok := ctx.pendingTasks[taskID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic workflow.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       workflow should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, taskID)

	// completing a task will resume the corresponding Await() call
	task.fail(sof.FailureDetails)
	return nil
}

func (ctx *WorkflowContext) onTimerCreated(e *protos.HistoryEvent) error {
	if a, ok := ctx.pendingActions[e.EventId]; !ok || a.GetCreateTimer() == nil {
		return fmt.Errorf(
			"a previous execution called CreateTimer with sequence number %d, but the current execution doesn't have this action with this sequence number",
			e.EventId,
		)
	}
	delete(ctx.pendingActions, e.EventId)
	return nil
}

func (ctx *WorkflowContext) onTimerFired(tf *protos.TimerFiredEvent) error {
	timerID := tf.TimerId
	task, ok := ctx.pendingTasks[timerID]
	if !ok {
		// TODO: This could be a duplicate event or it could be a non-deterministic workflow.
		//       Duplicate events should be handled gracefully with a warning. Otherwise, the
		//       workflow should probably fail with an error.
		return nil
	}
	delete(ctx.pendingTasks, timerID)

	// completing a task will resume the corresponding Await() call
	task.complete(nil)
	return nil
}

func (ctx *WorkflowContext) onExternalEventRaised(e *protos.HistoryEvent) error {
	er := e.GetEventRaised()
	key := strings.ToUpper(er.GetName())
	if pendingTasks, ok := ctx.pendingExternalEventTasks[key]; ok {
		// Complete the previously allocated task associated with this event name.
		elem := pendingTasks.Front()
		task := elem.Value.(*completableTask)
		if pendingTasks.Len() > 1 {
			pendingTasks.Remove(elem)
		} else {
			delete(ctx.pendingExternalEventTasks, key)
		}
		rawValue := []byte(er.Input.GetValue())
		task.complete(rawValue)
	} else {
		// Add this event to the buffered list of events with this name.
		var eventList *list.List
		var ok bool
		if eventList, ok = ctx.bufferedExternalEvents[key]; !ok {
			eventList = list.New()
			ctx.bufferedExternalEvents[key] = eventList
		}
		eventList.PushBack(e)
	}
	return nil
}

func (ctx *WorkflowContext) onExecutionSuspended(er *protos.ExecutionSuspendedEvent) error {
	ctx.isSuspended = true
	return nil
}

func (ctx *WorkflowContext) onExecutionResumed(er *protos.ExecutionResumedEvent) error {
	ctx.isSuspended = false
	for _, e := range ctx.suspendedEvents {
		if err := ctx.processEvent(e); err != nil {
			return err
		}
	}
	ctx.suspendedEvents = nil
	return nil
}

func (ctx *WorkflowContext) onExecutionTerminated(et *protos.ExecutionTerminatedEvent) error {
	if err := ctx.setCompleteInternal(et.Input, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, nil); err != nil {
		return err
	}
	return nil
}

func (ctx *WorkflowContext) setComplete(output any) error {
	status := protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED
	var rawOutput *wrapperspb.StringValue
	if output != nil {
		bytes, err := json.Marshal(output)
		if err != nil {
			return fmt.Errorf("failed to marshal output to JSON: %w", err)
		}
		rawOutput = wrapperspb.String(string(bytes))
	}
	if err := ctx.setCompleteInternal(rawOutput, status, nil); err != nil {
		return err
	}
	return nil
}

func (ctx *WorkflowContext) setFailed(appError error) error {
	fd := &protos.TaskFailureDetails{
		ErrorType:    reflect.TypeOf(appError).String(),
		ErrorMessage: appError.Error(),
	}
	failedStatus := protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED
	if err := ctx.setCompleteInternal(nil, failedStatus, fd); err != nil {
		return err
	}
	return nil
}

func (ctx *WorkflowContext) setContinuedAsNew() error {
	status := protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW
	var newRawInput *wrapperspb.StringValue
	if ctx.continuedAsNewInput != nil {
		bytes, err := json.Marshal(ctx.continuedAsNewInput)
		if err != nil {
			return fmt.Errorf("failed to marshal continue-as-new payload to JSON: %w", err)
		}
		newRawInput = wrapperspb.String(string(bytes))
	}
	if err := ctx.setCompleteInternal(newRawInput, status, nil); err != nil {
		return err
	}
	return nil
}

func (ctx *WorkflowContext) setCompleteInternal(
	rawResult *wrapperspb.StringValue,
	status protos.OrchestrationStatus,
	failureDetails *protos.TaskFailureDetails,
) error {
	sequenceNumber := ctx.getNextSequenceNumber()
	completedAction := &protos.WorkflowAction{
		Id: sequenceNumber,
		WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{
			CompleteWorkflow: &protos.CompleteWorkflowAction{
				WorkflowStatus: status,
				Result:              rawResult,
				FailureDetails:      failureDetails,
			},
		},
	}

	ctx.pendingActions[sequenceNumber] = completedAction
	return nil
}

func (ctx *WorkflowContext) setVersionNotRegistered() error {
	sequenceNumber := ctx.getNextSequenceNumber()
	ctx.pendingActions[sequenceNumber] = &protos.WorkflowAction{
		Id: sequenceNumber,
		WorkflowActionType: &protos.WorkflowAction_WorkflowVersionNotAvailable{
			WorkflowVersionNotAvailable: &protos.WorkflowVersionNotAvailableAction{},
		},
	}
	return nil
}

func (ctx *WorkflowContext) getNextSequenceNumber() int32 {
	current := ctx.sequenceNumber
	ctx.sequenceNumber++
	return current
}

func (ctx *WorkflowContext) actions() []*protos.WorkflowAction {
	if ctx.isSuspended {
		return nil
	}

	var actions []*protos.WorkflowAction
	for _, a := range ctx.pendingActions {
		actions = append(actions, a)
		if ctx.continuedAsNew && ctx.saveBufferedExternalEvents {
			if co := a.GetCompleteWorkflow(); co != nil {
				for _, eventList := range ctx.bufferedExternalEvents {
					for item := eventList.Front(); item != nil; item = item.Next() {
						e := item.Value.(*protos.HistoryEvent)
						co.CarryoverEvents = append(co.CarryoverEvents, e)
					}
				}
			}
		}
	}
	return actions
}
