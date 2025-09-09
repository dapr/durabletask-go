package workflow

import (
	"github.com/dapr/durabletask-go/task"
)

// TaskRegistry contains maps of names to corresponding orchestrator and
// activity functions.
type TaskRegistry struct {
	registry *task.TaskRegistry
}

// NewTaskRegistry returns a new [TaskRegistry] struct.
func NewTaskRegistry() *TaskRegistry {
	return &TaskRegistry{
		registry: task.NewTaskRegistry(),
	}
}

// AddWorkflow adds an orchestrator function to the registry. The name of the orchestrator
// function is determined using reflection.
func (r *TaskRegistry) AddWorkflow(w Workflow) error {
	return r.registry.AddOrchestrator(func(ctx *task.OrchestrationContext) (any, error) {
		return w(&WorkflowContext{ctx})
	})
}

// AddWorkflowN adds an orchestrator function to the registry with a
// specified name.
func (r *TaskRegistry) AddWorkflowN(name string, w Workflow) error {
	return r.registry.AddOrchestratorN(name, func(ctx *task.OrchestrationContext) (any, error) {
		return w(&WorkflowContext{ctx})
	})
}

// AddActivity adds an activity function to the registry. The name of the
// activity function is determined using reflection.
func (r *TaskRegistry) AddActivity(a Activity) error {
	return r.registry.AddActivity(func(ctx task.ActivityContext) (any, error) {
		return a(ctx.(ActivityContext))
	})
}

// AddActivityN adds an activity function to the registry with a specified
// name.
func (r *TaskRegistry) AddActivityN(name string, a Activity) error {
	return r.registry.AddActivityN(name, func(ctx task.ActivityContext) (any, error) {
		return a(ctx.(ActivityContext))
	})
}
