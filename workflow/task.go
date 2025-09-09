package workflow

import "github.com/dapr/durabletask-go/task"

// Task is an interface for asynchronous durable tasks. A task is conceptually
// similar to a future.
type Task task.Task
