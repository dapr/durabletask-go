package client

import (
	"context"
	"fmt"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

// CreateWorkflowSchedule creates a schedule that starts the named workflow
// on the given schedule, in the syntax of the Dapr Jobs API.
func (c *TaskHubGrpcClient) CreateWorkflowSchedule(ctx context.Context, name, workflow, schedule string, opts ...api.CreateWorkflowScheduleOptions) (*protos.CreateWorkflowScheduleResponse, error) {
	req := &protos.CreateWorkflowScheduleRequest{
		Schedule: &protos.WorkflowSchedule{Name: name, WorkflowName: workflow, Schedule: schedule},
	}
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return nil, fmt.Errorf("failed to configure workflow schedule: %w", err)
		}
	}
	resp, err := c.client.CreateWorkflowSchedule(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to create workflow schedule %q: %w", name, err)
	}
	return resp, nil
}

// GetWorkflowSchedule returns the schedule with its status. A schedule that
// does not exist is a gRPC NotFound error.
func (c *TaskHubGrpcClient) GetWorkflowSchedule(ctx context.Context, name string) (*protos.GetWorkflowScheduleResponse, error) {
	resp, err := c.client.GetWorkflowSchedule(ctx, &protos.GetWorkflowScheduleRequest{Name: name})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to get workflow schedule %q: %w", name, err)
	}
	return resp, nil
}

// ListWorkflowSchedules returns a page of the app's schedules with their
// status.
func (c *TaskHubGrpcClient) ListWorkflowSchedules(ctx context.Context, opts ...api.ListWorkflowSchedulesOptions) (*protos.ListWorkflowSchedulesResponse, error) {
	req := new(protos.ListWorkflowSchedulesRequest)
	for _, configure := range opts {
		if err := configure(req); err != nil {
			return nil, fmt.Errorf("failed to configure workflow schedule listing: %w", err)
		}
	}
	resp, err := c.client.ListWorkflowSchedules(ctx, req)
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to list workflow schedules: %w", err)
	}
	return resp, nil
}

// DeleteWorkflowSchedule deletes the schedule. Runs it already started are
// not affected.
func (c *TaskHubGrpcClient) DeleteWorkflowSchedule(ctx context.Context, name string) (*protos.DeleteWorkflowScheduleResponse, error) {
	resp, err := c.client.DeleteWorkflowSchedule(ctx, &protos.DeleteWorkflowScheduleRequest{Name: name})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to delete workflow schedule %q: %w", name, err)
	}
	return resp, nil
}

// TriggerWorkflowSchedule starts a run of the schedule now, without changing
// when its next scheduled run is. The response has the instance ID of the run.
func (c *TaskHubGrpcClient) TriggerWorkflowSchedule(ctx context.Context, name string) (*protos.TriggerWorkflowScheduleResponse, error) {
	resp, err := c.client.TriggerWorkflowSchedule(ctx, &protos.TriggerWorkflowScheduleRequest{Name: name})
	if err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		return nil, fmt.Errorf("failed to trigger workflow schedule %q: %w", name, err)
	}
	return resp, nil
}
