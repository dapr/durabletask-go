package api

import (
	"encoding/json"
	"errors"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/ptr"
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

// SuspendOptions is a set of options for suspending a workflow.
type SuspendOptions func(*protos.SuspendRequest) error

// ResumeOptions is a set of options for resuming a workflow.
type ResumeOptions func(*protos.ResumeRequest) error

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
// Proto message types are serialized with protojson; all other types use encoding/json.
func WithInput(input any) NewWorkflowOptions {
	return func(req *protos.CreateInstanceRequest) error {
		bytes, err := marshalData(input)
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
		bytes, err := marshalData(data)
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
		bytes, err := marshalData(data)
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

		bytes, err := marshalData(input)
		if err != nil {
			return err
		}

		req.Input = wrapperspb.String(string(bytes))

		return nil
	}
}

// protoMarshaler uses default protojson options so JSON output uses camelCase
// field names (the protojson default).
var protoMarshaler = protojson.MarshalOptions{}

// marshalData serializes v to JSON. Proto message types use protojson for
// correct handling of well-known types (e.g. google.protobuf.Struct);
// all other types use encoding/json.
func marshalData(v any) ([]byte, error) {
	if v == nil {
		return nil, nil
	}
	if msg, ok := v.(proto.Message); ok {
		return protoMarshaler.Marshal(msg)
	}
	return json.Marshal(v)
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

// routerWithTargetAppID returns r (allocating if nil) with the target app ID set.
func routerWithTargetAppID(r *protos.TaskRouter, appID string) *protos.TaskRouter {
	if r == nil {
		r = new(protos.TaskRouter)
	}
	r.TargetAppID = ptr.Of(appID)
	return r
}

// ValidateTaskRouter enforces the router invariant that cannot be checked
// inside a single option because options are order-independent: a target app
// namespace must be paired with a target app ID. Clients call this after
// applying all options. A nil router is valid.
func ValidateTaskRouter(r *protos.TaskRouter) error {
	if r.GetTargetAppNamespace() != "" && r.GetTargetAppID() == "" {
		return errors.New("a target app namespace requires a target app ID")
	}
	return nil
}

// WithAppID targets the new workflow at the app with the given app ID rather
// than the local app. The target app's access policy governs whether the
// schedule is permitted.
func WithAppID(appID string) NewWorkflowOptions {
	return func(req *protos.CreateInstanceRequest) error {
		req.Router = routerWithTargetAppID(req.GetRouter(), appID)
		return nil
	}
}

// WithFetchAppID targets the metadata fetch at the workflow instance owned by
// the app with the given app ID rather than the local app.
func WithFetchAppID(appID string) FetchWorkflowMetadataOptions {
	return func(req *protos.GetInstanceRequest) {
		req.Router = routerWithTargetAppID(req.GetRouter(), appID)
	}
}

// WithRaiseEventAppID targets the event at the workflow instance owned by the
// app with the given app ID rather than the local app.
func WithRaiseEventAppID(appID string) RaiseEventOptions {
	return func(req *protos.RaiseEventRequest) error {
		req.Router = routerWithTargetAppID(req.GetRouter(), appID)
		return nil
	}
}

// WithTerminateAppID targets the termination at the workflow instance owned by
// the app with the given app ID rather than the local app.
func WithTerminateAppID(appID string) TerminateOptions {
	return func(req *protos.TerminateRequest) error {
		req.Router = routerWithTargetAppID(req.GetRouter(), appID)
		return nil
	}
}

// WithSuspendAppID targets the suspension at the workflow instance owned by
// the app with the given app ID rather than the local app.
func WithSuspendAppID(appID string) SuspendOptions {
	return func(req *protos.SuspendRequest) error {
		req.Router = routerWithTargetAppID(req.GetRouter(), appID)
		return nil
	}
}

// WithResumeAppID targets the resumption at the workflow instance owned by the
// app with the given app ID rather than the local app.
func WithResumeAppID(appID string) ResumeOptions {
	return func(req *protos.ResumeRequest) error {
		req.Router = routerWithTargetAppID(req.GetRouter(), appID)
		return nil
	}
}

// WithPurgeAppID targets the purge at the workflow instance owned by the app
// with the given app ID rather than the local app. Cross-app purges are
// delegated to the target app in full, so they are always recursive on the
// remote side.
func WithPurgeAppID(appID string) PurgeOptions {
	return func(req *protos.PurgeInstancesRequest) error {
		req.Router = routerWithTargetAppID(req.GetRouter(), appID)
		return nil
	}
}

// WithRerunAppID targets the rerun at the workflow instance owned by the app
// with the given app ID rather than the local app.
func WithRerunAppID(appID string) RerunOptions {
	return func(req *protos.RerunWorkflowFromEventRequest) error {
		req.Router = routerWithTargetAppID(req.GetRouter(), appID)
		return nil
	}
}
