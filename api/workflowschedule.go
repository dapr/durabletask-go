package api

import (
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/ptr"
)

// CreateWorkflowScheduleOptions configures a workflow schedule being created.
type CreateWorkflowScheduleOptions func(*protos.CreateWorkflowScheduleRequest) error

// ListWorkflowSchedulesOptions configures a listing of workflow schedules.
type ListWorkflowSchedulesOptions func(*protos.ListWorkflowSchedulesRequest) error

// WithScheduleInput sets the input every run of the schedule is given,
// marshalled to JSON.
func WithScheduleInput(input any) CreateWorkflowScheduleOptions {
	return func(req *protos.CreateWorkflowScheduleRequest) error {
		bytes, err := marshalData(input)
		if err != nil {
			return fmt.Errorf("failed to marshal input data to JSON: %w", err)
		}
		req.Schedule.Input = wrapperspb.String(string(bytes))
		return nil
	}
}

// WithRawScheduleInput sets the raw input every run of the schedule is given.
func WithRawScheduleInput(input *wrapperspb.StringValue) CreateWorkflowScheduleOptions {
	return func(req *protos.CreateWorkflowScheduleRequest) error {
		req.Schedule.Input = input
		return nil
	}
}

// WithScheduleDueTime sets the time of the schedule's first run.
func WithScheduleDueTime(t time.Time) CreateWorkflowScheduleOptions {
	return func(req *protos.CreateWorkflowScheduleRequest) error {
		req.Schedule.DueTime = timestamppb.New(t)
		return nil
	}
}

// WithScheduleExpirationTime sets the time after which the schedule starts no
// run.
func WithScheduleExpirationTime(t time.Time) CreateWorkflowScheduleOptions {
	return func(req *protos.CreateWorkflowScheduleRequest) error {
		req.Schedule.ExpirationTime = timestamppb.New(t)
		return nil
	}
}

// WithScheduleRepeats sets the number of runs, scheduled or triggered, after
// which the schedule completes.
func WithScheduleRepeats(repeats uint32) CreateWorkflowScheduleOptions {
	return func(req *protos.CreateWorkflowScheduleRequest) error {
		req.Schedule.Repeats = &repeats
		return nil
	}
}

// WithScheduleTargetAppID sets the app ID that hosts the workflow each run
// starts.
func WithScheduleTargetAppID(appID string) CreateWorkflowScheduleOptions {
	return func(req *protos.CreateWorkflowScheduleRequest) error {
		req.Schedule.TargetAppID = &appID
		return nil
	}
}

// WithScheduleTargetAppNamespace sets the namespace of the target app ID.
func WithScheduleTargetAppNamespace(namespace string) CreateWorkflowScheduleOptions {
	return func(req *protos.CreateWorkflowScheduleRequest) error {
		req.Schedule.TargetAppNamespace = &namespace
		return nil
	}
}

// WithScheduleSkipMissed starts one run for the times the schedule missed
// while the app was down, rather than one run for each of them.
func WithScheduleSkipMissed() CreateWorkflowScheduleOptions {
	return func(req *protos.CreateWorkflowScheduleRequest) error {
		req.Schedule.SkipMissed = ptr.Of(true)
		return nil
	}
}

// WithScheduleOverwrite replaces an existing schedule of the same name.
func WithScheduleOverwrite() CreateWorkflowScheduleOptions {
	return func(req *protos.CreateWorkflowScheduleRequest) error {
		req.Overwrite = true
		return nil
	}
}

func WithListWorkflowSchedulesPageSize(pageSize uint32) ListWorkflowSchedulesOptions {
	return func(req *protos.ListWorkflowSchedulesRequest) error {
		req.PageSize = &pageSize
		return nil
	}
}

func WithListWorkflowSchedulesContinuationToken(token string) ListWorkflowSchedulesOptions {
	return func(req *protos.ListWorkflowSchedulesRequest) error {
		req.ContinuationToken = &token
		return nil
	}
}
