package api

import (
	"encoding/json"
	"time"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/ptr"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type OrchestrationStatus = protos.OrchestrationStatus

const (
	RUNTIME_STATUS_RUNNING          OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING
	RUNTIME_STATUS_COMPLETED        OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED
	RUNTIME_STATUS_CONTINUED_AS_NEW OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_CONTINUED_AS_NEW
	RUNTIME_STATUS_FAILED           OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED
	RUNTIME_STATUS_CANCELED         OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED
	RUNTIME_STATUS_TERMINATED       OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED
	RUNTIME_STATUS_PENDING          OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	RUNTIME_STATUS_SUSPENDED        OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED
	RUNTIME_STATUS_STALLED          OrchestrationStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_STALLED
)

// InstanceID is a unique identifier for a workflow instance.
type InstanceID string

func (i InstanceID) String() string {
	return string(i)
}

// NewWorkflowOptions configures options for starting a new workflow.
type NewWorkflowOptions func(*protos.CreateInstanceRequest) error

// GetWorkflowMetadataOptions is a set of options for fetching workflow metadata.
type FetchWorkflowMetadataOptions func(*protos.GetInstanceRequest)

// RaiseEventOptions is a set of options for raising a workflow event.
type RaiseEventOptions func(*protos.RaiseEventRequest) error

// TerminateOptions is a set of options for terminating a workflow.
type TerminateOptions func(*protos.TerminateRequest) error

// PurgeOptions is a set of options for purging a workflow.
type PurgeOptions func(*protos.PurgeInstancesRequest) error

type RerunOptions func(*protos.RerunWorkflowFromEventRequest) error

type ListInstanceIDsOptions func(*protos.ListInstanceIDsRequest) error

type GetInstanceHistoryOptions func(*protos.GetInstanceHistoryRequest) error

// WithInstanceID configures an explicit workflow instance ID. If not specified,
// a random UUID value will be used for the workflow instance ID.
func WithInstanceID(id InstanceID) NewWorkflowOptions {
	return func(req *protos.CreateInstanceRequest) error {
		req.InstanceId = string(id)
		return nil
	}
}

// WithInput configures an input for the workflow. The specified input must be serializable.
func WithInput(input any) NewWorkflowOptions {
	return func(req *protos.CreateInstanceRequest) error {
		bytes, err := json.Marshal(input)
		if err != nil {
			return err
		}
		req.Input = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawInput configures an input for the workflow. The specified input must be a string.
func WithRawInput(rawInput *wrapperspb.StringValue) NewWorkflowOptions {
	return func(req *protos.CreateInstanceRequest) error {
		req.Input = rawInput
		return nil
	}
}

// WithStartTime configures a start time at which the workflow should start running.
// Note that the actual start time could be later than the specified start time if the
// task hub is under load or if the app is not running at the specified start time.
func WithStartTime(startTime time.Time) NewWorkflowOptions {
	return func(req *protos.CreateInstanceRequest) error {
		req.ScheduledStartTimestamp = timestamppb.New(startTime)
		return nil
	}
}

// WithFetchPayloads configures whether to load workflow inputs, outputs, and custom status values, which could be large.
func WithFetchPayloads(fetchPayloads bool) FetchWorkflowMetadataOptions {
	return func(req *protos.GetInstanceRequest) {
		req.GetInputsAndOutputs = fetchPayloads
	}
}

// WithEventPayload configures an event payload. The specified payload must be serializable.
func WithEventPayload(data any) RaiseEventOptions {
	return func(req *protos.RaiseEventRequest) error {
		bytes, err := json.Marshal(data)
		if err != nil {
			return err
		}
		req.Input = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawEventData configures an event payload that is a raw, unprocessed string (e.g. JSON data).
func WithRawEventData(data *wrapperspb.StringValue) RaiseEventOptions {
	return func(req *protos.RaiseEventRequest) error {
		req.Input = data
		return nil
	}
}

// WithOutput configures an output for the terminated workflow. The specified output must be serializable.
func WithOutput(data any) TerminateOptions {
	return func(req *protos.TerminateRequest) error {
		bytes, err := json.Marshal(data)
		if err != nil {
			return err
		}
		req.Output = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawOutput configures a raw, unprocessed output (i.e. pre-serialized) for the terminated workflow.
func WithRawOutput(data *wrapperspb.StringValue) TerminateOptions {
	return func(req *protos.TerminateRequest) error {
		req.Output = data
		return nil
	}
}

// WithRecursiveTerminate configures whether to terminate all child workflows created by the target workflow.
func WithRecursiveTerminate(recursive bool) TerminateOptions {
	return func(req *protos.TerminateRequest) error {
		req.Recursive = recursive
		return nil
	}
}

// WithRecursivePurge configures whether to purge all child workflows created by the target workflow.
func WithRecursivePurge(recursive bool) PurgeOptions {
	return func(req *protos.PurgeInstancesRequest) error {
		req.Recursive = recursive
		return nil
	}
}

// WithForcePurge configures whether to purge a workflow, regardless of its
// state or if it is processable/being processed. Highly discouraged to use
// unless you know what you are doing.
func WithForcePurge(force bool) PurgeOptions {
	return func(req *protos.PurgeInstancesRequest) error {
		req.Force = &force
		return nil
	}
}

func WorkflowMetadataIsRunning(o *protos.WorkflowMetadata) bool {
	return !WorkflowMetadataIsComplete(o)
}

func WorkflowMetadataIsComplete(o *protos.WorkflowMetadata) bool {
	return o.GetRuntimeStatus() == protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED ||
		o.GetRuntimeStatus() == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED ||
		o.GetRuntimeStatus() == protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED ||
		o.GetRuntimeStatus() == protos.OrchestrationStatus_ORCHESTRATION_STATUS_CANCELED
}

func WithRerunInput(input any) RerunOptions {
	return func(req *protos.RerunWorkflowFromEventRequest) error {
		req.OverwriteInput = true

		if input == nil {
			return nil
		}

		bytes, err := json.Marshal(input)
		if err != nil {
			return err
		}

		req.Input = wrapperspb.String(string(bytes))

		return nil
	}
}

func WithRerunNewInstanceID(id InstanceID) RerunOptions {
	return func(req *protos.RerunWorkflowFromEventRequest) error {
		req.NewInstanceID = ptr.Of(id.String())
		return nil
	}
}

func WithListInstanceIDsPageSize(pageSize uint32) ListInstanceIDsOptions {
	return func(req *protos.ListInstanceIDsRequest) error {
		req.PageSize = &pageSize
		return nil
	}
}

func WithListInstanceIDsContinuationToken(token string) ListInstanceIDsOptions {
	return func(req *protos.ListInstanceIDsRequest) error {
		req.ContinuationToken = &token
		return nil
	}
}
