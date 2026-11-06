package workflow

import (
	"context"
	"time"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

type (
	WorkflowSchedule                = protos.WorkflowSchedule
	WorkflowScheduleStatus          = protos.WorkflowScheduleStatus
	WorkflowScheduleRun             = protos.WorkflowScheduleRun
	WorkflowScheduleState           = protos.WorkflowScheduleState
	CreateWorkflowScheduleResponse  = protos.CreateWorkflowScheduleResponse
	GetWorkflowScheduleResponse     = protos.GetWorkflowScheduleResponse
	ListWorkflowSchedulesResponse   = protos.ListWorkflowSchedulesResponse
	DeleteWorkflowScheduleResponse  = protos.DeleteWorkflowScheduleResponse
	TriggerWorkflowScheduleResponse = protos.TriggerWorkflowScheduleResponse

	CreateWorkflowScheduleOptions api.CreateWorkflowScheduleOptions
	ListWorkflowSchedulesOptions  api.ListWorkflowSchedulesOptions
)

const (
	WorkflowScheduleStateActive    = protos.WorkflowScheduleState_WORKFLOW_SCHEDULE_STATE_ACTIVE
	WorkflowScheduleStateCompleted = protos.WorkflowScheduleState_WORKFLOW_SCHEDULE_STATE_COMPLETED
	WorkflowScheduleStateFailed    = protos.WorkflowScheduleState_WORKFLOW_SCHEDULE_STATE_FAILED
)

// CreateWorkflowSchedule creates a schedule that starts the named workflow on
// the given schedule: a 6-field cron expression with seconds, "@every
// <duration>", or "@daily" and the like, as for the Dapr Jobs API. Each run
// is a new workflow instance with the ID "<name>_<owner>_<fire time>", where
// <owner> is a 6-character tag of the app that owns the schedule.
func (c *Client) CreateWorkflowSchedule(ctx context.Context, name, workflow, schedule string, opts ...CreateWorkflowScheduleOptions) (*CreateWorkflowScheduleResponse, error) {
	oops := make([]api.CreateWorkflowScheduleOptions, len(opts))
	for i, o := range opts {
		oops[i] = api.CreateWorkflowScheduleOptions(o)
	}
	return c.thgc.CreateWorkflowSchedule(ctx, name, workflow, schedule, oops...)
}

// GetWorkflowSchedule returns the schedule with its status, including how its
// last run went. A schedule that does not exist is a gRPC NotFound error.
func (c *Client) GetWorkflowSchedule(ctx context.Context, name string) (*GetWorkflowScheduleResponse, error) {
	return c.thgc.GetWorkflowSchedule(ctx, name)
}

// ListWorkflowSchedules returns a page of the app's schedules with their
// status.
func (c *Client) ListWorkflowSchedules(ctx context.Context, opts ...ListWorkflowSchedulesOptions) (*ListWorkflowSchedulesResponse, error) {
	oops := make([]api.ListWorkflowSchedulesOptions, len(opts))
	for i, o := range opts {
		oops[i] = api.ListWorkflowSchedulesOptions(o)
	}
	return c.thgc.ListWorkflowSchedules(ctx, oops...)
}

// DeleteWorkflowSchedule deletes the schedule. Runs it already started are
// not affected.
func (c *Client) DeleteWorkflowSchedule(ctx context.Context, name string) (*DeleteWorkflowScheduleResponse, error) {
	return c.thgc.DeleteWorkflowSchedule(ctx, name)
}

// TriggerWorkflowSchedule starts a run of the schedule now, without changing
// when its next scheduled run is. The response has the run's instance ID.
func (c *Client) TriggerWorkflowSchedule(ctx context.Context, name string) (*TriggerWorkflowScheduleResponse, error) {
	return c.thgc.TriggerWorkflowSchedule(ctx, name)
}

// WithScheduleInput sets the input every run is given, marshalled to JSON.
func WithScheduleInput(input any) CreateWorkflowScheduleOptions {
	return CreateWorkflowScheduleOptions(api.WithScheduleInput(input))
}

// WithRawScheduleInput sets the raw input every run is given.
func WithRawScheduleInput(input *wrapperspb.StringValue) CreateWorkflowScheduleOptions {
	return CreateWorkflowScheduleOptions(api.WithRawScheduleInput(input))
}

// WithScheduleDueTime sets the time of the first run.
func WithScheduleDueTime(t time.Time) CreateWorkflowScheduleOptions {
	return CreateWorkflowScheduleOptions(api.WithScheduleDueTime(t))
}

// WithScheduleExpirationTime sets the time after which no run starts.
func WithScheduleExpirationTime(t time.Time) CreateWorkflowScheduleOptions {
	return CreateWorkflowScheduleOptions(api.WithScheduleExpirationTime(t))
}

// WithScheduleRepeats sets the number of runs, scheduled or triggered, after
// which the schedule completes.
func WithScheduleRepeats(repeats uint32) CreateWorkflowScheduleOptions {
	return CreateWorkflowScheduleOptions(api.WithScheduleRepeats(repeats))
}

// WithScheduleTargetAppID sets the app ID that hosts the workflow each run
// starts.
func WithScheduleTargetAppID(appID string) CreateWorkflowScheduleOptions {
	return CreateWorkflowScheduleOptions(api.WithScheduleTargetAppID(appID))
}

// WithScheduleTargetAppNamespace sets the namespace of the target app ID.
func WithScheduleTargetAppNamespace(namespace string) CreateWorkflowScheduleOptions {
	return CreateWorkflowScheduleOptions(api.WithScheduleTargetAppNamespace(namespace))
}

// WithScheduleSkipMissed starts one run for the times the schedule missed
// while the app was down, rather than one run for each of them. By default
// every missed time starts its own run.
func WithScheduleSkipMissed() CreateWorkflowScheduleOptions {
	return CreateWorkflowScheduleOptions(api.WithScheduleSkipMissed())
}

// WithScheduleOverwrite replaces an existing schedule of the same name.
func WithScheduleOverwrite() CreateWorkflowScheduleOptions {
	return CreateWorkflowScheduleOptions(api.WithScheduleOverwrite())
}

func WithListWorkflowSchedulesPageSize(pageSize uint32) ListWorkflowSchedulesOptions {
	return ListWorkflowSchedulesOptions(api.WithListWorkflowSchedulesPageSize(pageSize))
}

func WithListWorkflowSchedulesContinuationToken(token string) ListWorkflowSchedulesOptions {
	return ListWorkflowSchedulesOptions(api.WithListWorkflowSchedulesContinuationToken(token))
}
