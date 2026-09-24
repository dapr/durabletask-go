package workflow

import "github.com/dapr/durabletask-go/task"

// Task is an interface for asynchronous durable tasks. A task is conceptually
// similar to a future.
type Task task.Task

// ErrTaskCanceled is returned by [Task.Await] when the task was canceled, for example because a
// timeout configured via [WorkflowContext.WaitForExternalEvent] expired.
var ErrTaskCanceled = task.ErrTaskCanceled

// ErrTaskNotSelectable is returned by [WorkflowContext.Select] when one of the given tasks isn't
// selectable -- see Select's doc comment for the specific cases.
var ErrTaskNotSelectable = task.ErrTaskNotSelectable
