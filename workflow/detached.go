package workflow

import (
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/task"
)

// DetachedWorkflowOptions is a functional option type for the
// ScheduleNewWorkflow workflow method.
type DetachedWorkflowOptions task.DetachedWorkflowOptions

// ScheduleNewWorkflow schedules a new, fully decoupled workflow instance
// from this workflow. Returns the new instance ID synchronously. The
// spawned workflow has no parent linkage: completion and failure do not
// flow back to the caller. If WithDetachedWorkflowInstanceID is not
// provided, a deterministic default ID is generated.
func (w *WorkflowContext) ScheduleNewWorkflow(workflow any, opts ...DetachedWorkflowOptions) (string, error) {
	oopts := make([]task.DetachedWorkflowOptions, len(opts))
	for i, o := range opts {
		oopts[i] = task.DetachedWorkflowOptions(o)
	}
	id, err := w.oc.ScheduleNewWorkflow(workflow, oopts...)
	if err != nil {
		return "", err
	}
	return string(id), nil
}

// WithDetachedWorkflowInstanceID sets the instance ID of the detached
// workflow. When omitted, ScheduleNewWorkflow generates a deterministic
// ID of the form "<callerInstanceID>-<n>". Passing an empty string is
// rejected as an error: callers either set a non-empty ID or omit the
// option entirely to opt into the default.
func WithDetachedWorkflowInstanceID(instanceID string) DetachedWorkflowOptions {
	return DetachedWorkflowOptions(task.WithDetachedWorkflowInstanceID(instanceID))
}

// WithDetachedWorkflowInput sets the JSON-marshalled input for the
// detached workflow.
func WithDetachedWorkflowInput(input any) DetachedWorkflowOptions {
	return DetachedWorkflowOptions(task.WithDetachedWorkflowInput(input))
}

// WithRawDetachedWorkflowInput sets a pre-marshalled raw input on the
// detached workflow.
func WithRawDetachedWorkflowInput(input *wrapperspb.StringValue) DetachedWorkflowOptions {
	return DetachedWorkflowOptions(task.WithRawDetachedWorkflowInput(input))
}

// WithDetachedWorkflowVersion sets a version label on the detached
// workflow.
func WithDetachedWorkflowVersion(version string) DetachedWorkflowOptions {
	return DetachedWorkflowOptions(task.WithDetachedWorkflowVersion(version))
}

// WithDetachedWorkflowExecutionID sets an explicit execution ID on the
// detached workflow's first execution.
func WithDetachedWorkflowExecutionID(executionID string) DetachedWorkflowOptions {
	return DetachedWorkflowOptions(task.WithDetachedWorkflowExecutionID(executionID))
}

// WithDetachedWorkflowStartTime defers the start of the detached workflow
// until the given time.
func WithDetachedWorkflowStartTime(startTime time.Time) DetachedWorkflowOptions {
	return DetachedWorkflowOptions(task.WithDetachedWorkflowStartTime(startTime))
}

// WithDetachedWorkflowTags sets the tag map on the detached workflow.
func WithDetachedWorkflowTags(tags map[string]string) DetachedWorkflowOptions {
	return DetachedWorkflowOptions(task.WithDetachedWorkflowTags(tags))
}

// WithDetachedWorkflowAppID specifies the app ID hosting the detached
// workflow. When set, the action carries a routing envelope so the
// dispatcher delivers the new instance to the target app.
func WithDetachedWorkflowAppID(appID string) DetachedWorkflowOptions {
	return DetachedWorkflowOptions(task.WithDetachedWorkflowAppID(appID))
}

// WithDetachedWorkflowAppNamespace specifies the Dapr namespace hosting
// the detached workflow. Must be combined with WithDetachedWorkflowAppID.
func WithDetachedWorkflowAppNamespace(namespace string) DetachedWorkflowOptions {
	return DetachedWorkflowOptions(task.WithDetachedWorkflowAppNamespace(namespace))
}
