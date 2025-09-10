package workflow

import (
	"github.com/dapr/durabletask-go/task"
)

// Registry contains maps of names to corresponding orchestrator and activity
// functions.
type Registry struct {
	registry *task.TaskRegistry
}

// NewRegistry returns a new Registry struct.
func NewRegistry() *Registry {
	return &Registry{
		registry: task.NewTaskRegistry(),
	}
}

// AddWorkflow adds an orchestrator function to the registry. The name of the orchestrator
// function is determined using reflection.
func (r *Registry) AddWorkflow(w Workflow) error {
	return r.registry.AddOrchestrator(func(ctx *task.OrchestrationContext) (any, error) {
		return w(&WorkflowContext{ctx})
	})
}

// AddWorkflowN adds an orchestrator function to the registry with a
// specified name.
func (r *Registry) AddWorkflowN(name string, w Workflow) error {
	return r.registry.AddOrchestratorN(name, func(ctx *task.OrchestrationContext) (any, error) {
		return w(&WorkflowContext{ctx})
	})
}

// AddActivity adds an activity function to the registry. The name of the
// activity function is determined using reflection.
func (r *Registry) AddActivity(a Activity) error {
	return r.registry.AddActivity(func(ctx task.ActivityContext) (any, error) {
		return a(ctx.(ActivityContext))
	})
}

// AddActivityN adds an activity function to the registry with a specified
// name.
func (r *Registry) AddActivityN(name string, a Activity) error {
	return r.registry.AddActivityN(name, func(ctx task.ActivityContext) (any, error) {
		return a(ctx.(ActivityContext))
	})
}
