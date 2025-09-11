package workflow

import "github.com/dapr/durabletask-go/task"

type CreateTimerOption task.CreateTimerOption

func WithTimerName(name string) CreateTimerOption {
	return CreateTimerOption(task.WithTimerName(name))
}
