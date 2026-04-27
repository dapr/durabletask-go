package task

import (
	"fmt"
	"sync"

	"github.com/dapr/durabletask-go/api/helpers"
)

// TaskRegistry contains maps of names to corresponding workflow and activity functions.
type TaskRegistry struct {
	mu                       sync.RWMutex
	workflows                map[string]Workflow
	versionedWorkflows       map[string]map[string]Workflow
	latestVersionedWorkflows map[string]string
	activities               map[string]Activity
}

// NewTaskRegistry returns a new [TaskRegistry] struct.
func NewTaskRegistry() *TaskRegistry {
	r := &TaskRegistry{
		workflows:                make(map[string]Workflow),
		activities:               make(map[string]Activity),
		versionedWorkflows:       make(map[string]map[string]Workflow),
		latestVersionedWorkflows: make(map[string]string),
	}
	return r
}

// AddWorkflow adds a workflow function to the registry. The name of the workflow
// function is determined using reflection.
func (r *TaskRegistry) AddWorkflow(o Workflow) error {
	name := helpers.GetTaskFunctionName(o)
	return r.AddWorkflowN(name, o)
}

// AddWorkflowN adds a workflow function to the registry with a specified name.
func (r *TaskRegistry) AddWorkflowN(name string, o Workflow) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.workflows[name]; ok {
		return fmt.Errorf("workflow named '%s' is already registered", name)
	}
	r.workflows[name] = o
	return nil
}

// AddVersionedWorkflow adds a versioned workflow function to the registry with a specified name.
func (r *TaskRegistry) AddVersionedWorkflow(canonicalName string, isLatest bool, o Workflow) error {
	name := helpers.GetTaskFunctionName(o)
	return r.AddVersionedWorkflowN(canonicalName, name, isLatest, o)
}

// AddVersionedWorkflowN adds or replaces a versioned workflow function in the registry.
func (r *TaskRegistry) AddVersionedWorkflowN(canonicalName string, name string, isLatest bool, o Workflow) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.versionedWorkflows[canonicalName]; !ok {
		r.versionedWorkflows[canonicalName] = make(map[string]Workflow)
	}
	r.versionedWorkflows[canonicalName][name] = o
	if isLatest {
		r.latestVersionedWorkflows[canonicalName] = name
	}
	return nil
}

// AddActivity adds an activity function to the registry. The name of the activity
// function is determined using reflection.
func (r *TaskRegistry) AddActivity(a Activity) error {
	name := helpers.GetTaskFunctionName(a)
	return r.AddActivityN(name, a)
}

// AddActivityN adds or replaces an activity function in the registry.
func (r *TaskRegistry) AddActivityN(name string, a Activity) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.activities[name] = a
	return nil
}

// GetWorkflow returns a workflow by name, or nil if not found.
func (r *TaskRegistry) GetWorkflow(name string) (Workflow, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	w, ok := r.workflows[name]
	return w, ok
}

// GetVersionedWorkflow returns a versioned workflow by canonical name,
// resolving to the pinned version or the latest if no pin is provided.
func (r *TaskRegistry) GetVersionedWorkflow(canonicalName string, pinnedVersion *string) (Workflow, string, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	versions, ok := r.versionedWorkflows[canonicalName]
	if !ok {
		return nil, "", false
	}

	var versionToUse string
	if pinnedVersion != nil {
		versionToUse = *pinnedVersion
	} else if latest, ok := r.latestVersionedWorkflows[canonicalName]; ok {
		versionToUse = latest
	} else {
		return nil, "", false
	}

	w, ok := versions[versionToUse]
	return w, versionToUse, ok
}

// GetActivity returns an activity by name, or nil if not found.
func (r *TaskRegistry) GetActivity(name string) (Activity, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	a, ok := r.activities[name]
	return a, ok
}

// RemoveVersionedWorkflow removes all versions of a workflow from the registry.
func (r *TaskRegistry) RemoveVersionedWorkflow(canonicalName string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.versionedWorkflows, canonicalName)
	delete(r.latestVersionedWorkflows, canonicalName)
}

// RemoveActivity removes an activity from the registry.
func (r *TaskRegistry) RemoveActivity(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.activities, name)
}
