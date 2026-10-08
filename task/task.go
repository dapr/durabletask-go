package task

import (
	"errors"
	"fmt"

	"github.com/dapr/durabletask-go/api/protos"
)

// ErrTaskBlocked is not an error, but rather a control flow signal indicating that a workflow
// function has executed as far as it can and that it now needs to unload, dispatch any scheduled tasks,
// and commit its current execution progress to durable storage.
var ErrTaskBlocked = errors.New("the current task is blocked")

// ErrTaskCanceled is used to indicate that a task was canceled. Tasks can be canceled, for example,
// when configured timeouts expire.
var ErrTaskCanceled = errors.New("the task was canceled") // CONSIDER: More specific info about the task

// ErrTaskNotSelectable is returned by [WorkflowContext.Select] when one of the given tasks was not
// created by a WorkflowContext method.
var ErrTaskNotSelectable = errors.New("task does not support Select")

// Task is an interface for asynchronous durable tasks. A task is conceptually similar to a future.
type Task interface {
	Await(v any) error
	TaskExecutionId() string
}

type completableTask struct {
	workflowCtx       *WorkflowContext
	isCompleted       bool
	isCanceled        bool
	rawResult         []byte
	failureDetails    *protos.TaskFailureDetails
	completedCallback func()
	taskExecutionId   string
	// advance, when set, runs on every poll (see pollCompleted) before isCompleted is read. Used by
	// a retry chain's outer task (see internalScheduleTaskWithRetries) to drive itself forward --
	// start the next attempt once a backoff timer fires, fail or complete once an attempt's outcome
	// is known -- only when something is actually polling for completion, not as a side effect of
	// history alone.
	advance func()
}

// pollCompleted runs t's advance hook, if any, then reports whether t is completed. Await and
// Select's poll both call this instead of reading isCompleted directly, so that observing a task is
// what drives a retry chain forward.
func (t *completableTask) pollCompleted() bool {
	if t.advance != nil {
		t.advance()
	}
	return t.isCompleted
}

func newTask(ctx *WorkflowContext) *completableTask {
	return &completableTask{
		workflowCtx: ctx,
	}
}

// Await blocks the current workflow until the task is complete and then saves the unmarshalled
// result of the task (if any) into [v].
//
// Await will return ErrTaskCanceled if the task was canceled - e.g. due to a timeout.
//
// Await may panic with ErrTaskBlocked as the panic value if called on a task that has not yet completed.
// This is normal control flow behavior for workflow functions and doesn't actually indicate a failure
// of any kind. However, workflow functions must never attempt to recover from such panics to ensure that
// the workflow execution can proceed normally.
func (t *completableTask) Await(v any) error {
	if err := t.workflowCtx.awaitUntil(t.pollCompleted); err != nil {
		return err
	}
	if err := t.completionError(); err != nil {
		return err
	}
	if v != nil && len(t.rawResult) > 0 {
		if err := unmarshalData(t.rawResult, v); err != nil {
			return fmt.Errorf("failed to decode task result: %w", err)
		}
	}
	return nil
}

func (t *completableTask) TaskExecutionId() string {
	return t.taskExecutionId
}

// completionError returns the error a completed task represents -- nil on success, the formatted
// failure on a failed task, or ErrTaskCanceled on a canceled one -- without touching the task's raw
// result. It must only be called once t.isCompleted is true.
func (t *completableTask) completionError() error {
	if t.failureDetails != nil {
		return fmt.Errorf("task failed with an error: %v", t.failureDetails.ErrorMessage)
	}
	if t.isCanceled {
		return ErrTaskCanceled
	}
	return nil
}

// onCompleted registers [callback] to run when the task completes. Only one callback may be
// registered on a task at a time; this package's only current caller, WaitForSingleEvent's internal
// timer plumbing, registers exactly one, on a task scoped to that single registration and never
// reused for another.
func (t *completableTask) onCompleted(callback func()) {
	t.completedCallback = callback
}

func (t *completableTask) complete(rawResult []byte) {
	t.rawResult = rawResult
	t.completeInternal()
}

func (t *completableTask) fail(fd *protos.TaskFailureDetails) {
	t.failureDetails = fd
	t.completeInternal()
}

func (t *completableTask) cancel() {
	t.isCanceled = true
	t.completeInternal()
}

func (t *completableTask) completeInternal() {
	t.isCompleted = true
	if t.completedCallback != nil {
		t.completedCallback()
	}
}
