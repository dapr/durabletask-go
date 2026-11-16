package backend

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/dapr/durabletask-go/api/protos"
)

// WorkflowScheduleHandler serves the workflow schedule calls of the
// TaskHubSidecarService. The runtime hosting the executor implements it.
type WorkflowScheduleHandler interface {
	CreateWorkflowSchedule(context.Context, *protos.CreateWorkflowScheduleRequest) (*protos.CreateWorkflowScheduleResponse, error)
	GetWorkflowSchedule(context.Context, *protos.GetWorkflowScheduleRequest) (*protos.GetWorkflowScheduleResponse, error)
	ListWorkflowSchedules(context.Context, *protos.ListWorkflowSchedulesRequest) (*protos.ListWorkflowSchedulesResponse, error)
	DeleteWorkflowSchedule(context.Context, *protos.DeleteWorkflowScheduleRequest) (*protos.DeleteWorkflowScheduleResponse, error)
	TriggerWorkflowSchedule(context.Context, *protos.TriggerWorkflowScheduleRequest) (*protos.TriggerWorkflowScheduleResponse, error)
}

// WithWorkflowScheduleHandler serves the workflow schedule calls with h.
// Without it they return codes.Unimplemented.
func WithWorkflowScheduleHandler(h WorkflowScheduleHandler) grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.workflowSchedules = h
	}
}

var errWorkflowSchedulesUnimplemented = status.Error(codes.Unimplemented, "workflow schedules are not supported by this runtime")

func (g *grpcExecutor) CreateWorkflowSchedule(ctx context.Context, req *protos.CreateWorkflowScheduleRequest) (*protos.CreateWorkflowScheduleResponse, error) {
	if g.workflowSchedules == nil {
		return nil, errWorkflowSchedulesUnimplemented
	}
	return g.workflowSchedules.CreateWorkflowSchedule(ctx, req)
}

func (g *grpcExecutor) GetWorkflowSchedule(ctx context.Context, req *protos.GetWorkflowScheduleRequest) (*protos.GetWorkflowScheduleResponse, error) {
	if g.workflowSchedules == nil {
		return nil, errWorkflowSchedulesUnimplemented
	}
	return g.workflowSchedules.GetWorkflowSchedule(ctx, req)
}

func (g *grpcExecutor) ListWorkflowSchedules(ctx context.Context, req *protos.ListWorkflowSchedulesRequest) (*protos.ListWorkflowSchedulesResponse, error) {
	if g.workflowSchedules == nil {
		return nil, errWorkflowSchedulesUnimplemented
	}
	return g.workflowSchedules.ListWorkflowSchedules(ctx, req)
}

func (g *grpcExecutor) DeleteWorkflowSchedule(ctx context.Context, req *protos.DeleteWorkflowScheduleRequest) (*protos.DeleteWorkflowScheduleResponse, error) {
	if g.workflowSchedules == nil {
		return nil, errWorkflowSchedulesUnimplemented
	}
	return g.workflowSchedules.DeleteWorkflowSchedule(ctx, req)
}

func (g *grpcExecutor) TriggerWorkflowSchedule(ctx context.Context, req *protos.TriggerWorkflowScheduleRequest) (*protos.TriggerWorkflowScheduleResponse, error) {
	if g.workflowSchedules == nil {
		return nil, errWorkflowSchedulesUnimplemented
	}
	return g.workflowSchedules.TriggerWorkflowSchedule(ctx, req)
}
