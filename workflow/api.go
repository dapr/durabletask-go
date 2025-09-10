package workflow

import (
	"time"

	"github.com/dapr/durabletask-go/api"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type NewWorkflowOptions api.NewOrchestrationOptions
type FetchWorkflowMetadataOptions api.FetchOrchestrationMetadataOptions
type RaiseEventOptions api.RaiseEventOptions
type TerminateOptions api.TerminateOptions
type PurgeOptions api.PurgeOptions
type RerunOptions api.RerunOptions

// WithInstanceID configures an explicit workflow instance ID. If not
// specified, a random UUID value will be used for the workflow instance ID.
func WithInstanceID(id string) NewWorkflowOptions {
	return NewWorkflowOptions(api.WithInstanceID(api.InstanceID(id)))
}

// WithInput configures an input for the workflow. The specified input must be
// serializable.
func WithInput(input any) NewWorkflowOptions {
	return NewWorkflowOptions(api.WithInput(input))
}

// WithRawInput configures an input for the workflow. The specified input must
// be a string.
func WithRawInput(rawInput *wrapperspb.StringValue) NewWorkflowOptions {
	return NewWorkflowOptions(api.WithRawInput(rawInput))
}

// WithStartTime configures a start time at which the workflow should start
// running. Note that the actual start time could be later than the specified
// start time if the task hub is under load or if the app is not running at the
// specified start time.
func WithStartTime(startTime time.Time) NewWorkflowOptions {
	return NewWorkflowOptions(api.WithStartTime(startTime))
}

// WithFetchPayloads configures whether to load workflow inputs, outputs, and
// custom status values, which could be large.
func WithFetchPayloads(fetchPayloads bool) FetchWorkflowMetadataOptions {
	return FetchWorkflowMetadataOptions(api.WithFetchPayloads(fetchPayloads))
}

// WithEventPayload configures an event payload. The specified payload must be
// serializable.
func WithEventPayload(data any) RaiseEventOptions {
	return RaiseEventOptions(api.WithEventPayload(data))
}

// WithRawEventData configures an event payload that is a raw, unprocessed
// string (e.g. JSON data).
func WithRawEventData(data *wrapperspb.StringValue) RaiseEventOptions {
	return RaiseEventOptions(api.WithRawEventData(data))
}

// WithOutput configures an output for the terminated workflow. The specified
// output must be serializable.
func WithOutput(data any) TerminateOptions {
	return TerminateOptions(api.WithOutput(data))
}

// WithRawOutput configures a raw, unprocessed output (i.e. pre-serialized) for
// the terminated workflow.
func WithRawOutput(data *wrapperspb.StringValue) TerminateOptions {
	return TerminateOptions(api.WithRawOutput(data))
}

// WithRecursiveTerminate configures whether to terminate all child-workflows
// created by the target workflow.
func WithRecursiveTerminate(recursive bool) TerminateOptions {
	return TerminateOptions(api.WithRecursiveTerminate(recursive))
}

// WithRecursivePurge configures whether to purge all child-workflows created
// by the target workflow.
func WithRecursivePurge(recursive bool) PurgeOptions {
	return PurgeOptions(api.WithRecursivePurge(recursive))
}

func WorkflowMetadataIsRunning(o *WorkflowMetadata) bool {
	return api.OrchestrationMetadataIsComplete(o.metadata)
}

func WorkflowMetadataIsComplete(o *WorkflowMetadata) bool {
	return api.OrchestrationMetadataIsComplete(o.metadata)
}

func WithRerunInput(input any) RerunOptions {
	return RerunOptions(api.WithRerunInput(input))
}

func WithRerunNewInstanceID(id string) RerunOptions {
	return RerunOptions(api.WithRerunNewInstanceID(api.InstanceID(id)))
}
