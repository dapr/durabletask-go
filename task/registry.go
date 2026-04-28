package task

import (
	"fmt"
	"sync"

	"github.com/dapr/durabletask-go/api"
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

// AddVersionedWorkflowN adds a versioned workflow function to the registry.
// Returns an error if a workflow with the same version name is already registered.
func (r *TaskRegistry) AddVersionedWorkflowN(canonicalName string, name string, isLatest bool, o Workflow) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if versions, ok := r.versionedWorkflows[canonicalName]; ok {
		if _, ok := versions[name]; ok {
			return fmt.Errorf("versioned workflow named '%s' is already registered", name)
		}
	} else {
		r.versionedWorkflows[canonicalName] = make(map[string]Workflow)
	}
	r.versionedWorkflows[canonicalName][name] = o
	if isLatest {
		r.latestVersionedWorkflows[canonicalName] = name
	}
	return nil
}

// UpsertVersionedWorkflowN adds or replaces a versioned workflow function in the registry.
// Used by callers that intentionally re-register (e.g. hot-reload).
func (r *TaskRegistry) UpsertVersionedWorkflowN(canonicalName string, name string, isLatest bool, o Workflow) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.versionedWorkflows[canonicalName]; !ok {
		r.versionedWorkflows[canonicalName] = make(map[string]Workflow)
	}
	r.versionedWorkflows[canonicalName][name] = o
	if isLatest {
		r.latestVersionedWorkflows[canonicalName] = name
	}
}

// AddActivity adds an activity function to the registry. The name of the activity
// function is determined using reflection.
func (r *TaskRegistry) AddActivity(a Activity) error {
	name := helpers.GetTaskFunctionName(a)
	return r.AddActivityN(name, a)
}

// AddActivityN adds an activity function to the registry with a specified name.
// Returns an error if an activity with the same name is already registered.
func (r *TaskRegistry) AddActivityN(name string, a Activity) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if _, ok := r.activities[name]; ok {
		return fmt.Errorf("activity named '%s' is already registered", name)
	}
	r.activities[name] = a
	return nil
}

// UpsertActivityN adds or replaces an activity function in the registry.
// Used by callers that intentionally re-register (e.g. hot-reload).
func (r *TaskRegistry) UpsertActivityN(name string, a Activity) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.activities[name] = a
}

// ResolveWorkflow looks up a workflow by name under a single read lock.
// It checks: exact match → versioned match → wildcard, returning the first hit.
// Returns the resolved workflow, an optional version string, or an error.
func (r *TaskRegistry) ResolveWorkflow(name string, pinnedVersion *string) (Workflow, *string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	// Exact non-versioned match.
	if w, ok := r.workflows[name]; ok {
		return w, nil, nil
	}

	// Versioned match.
	if versions, ok := r.versionedWorkflows[name]; ok {
		var versionToUse string
		if pinnedVersion != nil {
			versionToUse = *pinnedVersion
		} else if latest, ok := r.latestVersionedWorkflows[name]; ok {
			versionToUse = latest
		} else {
			return nil, nil, fmt.Errorf("versioned workflow '%s' does not have a latest version registered", name)
		}
		if w, ok := versions[versionToUse]; ok {
			return w, &versionToUse, nil
		}
		return nil, nil, api.NewUnsupportedVersionError()
	} else if pinnedVersion != nil {
		return nil, nil, api.NewUnsupportedVersionError()
	}

	// Wildcard fallback.
	if w, ok := r.workflows["*"]; ok {
		return w, nil, nil
	}

	return nil, nil, fmt.Errorf("workflow named '%s' is not registered", name)
}

// getActivity returns an activity by name. Package-internal.
func (r *TaskRegistry) getActivity(name string) (Activity, bool) {
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
