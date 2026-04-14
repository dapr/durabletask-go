/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package client

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
)

// dispatchWorkflow runs a workflow work item through the executor,
// and returns the WorkflowResponse ready to be completed via the caller's transport (gRPC or backend).
// The response always has InstanceId set,
//
//	and either Actions from the executor or a single CompleteWorkflow failure action.
func dispatchWorkflow(
	ctx context.Context,
	executor backend.Executor,
	req *protos.WorkflowRequest,
) *protos.WorkflowResponse {
	results, err := executor.ExecuteWorkflow(
		ctx,
		api.InstanceID(req.InstanceId),
		req.PastEvents,
		req.NewEvents,
	)

	resp := &protos.WorkflowResponse{InstanceId: req.InstanceId}
	if err != nil {
		resp.Actions = []*protos.WorkflowAction{
			{
				Id: -1,
				WorkflowActionType: &protos.WorkflowAction_CompleteWorkflow{
					CompleteWorkflow: &protos.CompleteWorkflowAction{
						WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
						Result:         wrapperspb.String("An internal error occurred while executing the orchestration."),
						FailureDetails: &protos.TaskFailureDetails{
							ErrorType:    fmt.Sprintf("%T", err),
							ErrorMessage: err.Error(),
						},
					},
				},
			},
		}
	} else {
		resp.Actions = results.Actions
		resp.CustomStatus = results.GetCustomStatus()
		resp.Version = results.GetVersion()
	}
	return resp
}

// dispatchActivity runs an activity work item through the executor,
// and returns the ActivityResponse ready to be completed via the caller's transport.
// Trace-context propagation is handled internally.
func dispatchActivity(
	ctx context.Context,
	executor backend.Executor,
	logger backend.Logger,
	req *protos.ActivityRequest,
) *protos.ActivityResponse {
	ptc := req.ParentTraceContext
	ctx, err := helpers.ContextFromTraceContext(ctx, ptc)
	if err != nil {
		logger.Warnf("%v: failed to parse trace context: %v", req.Name, err)
	}

	event := &protos.HistoryEvent{
		EventId:   req.TaskId,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_TaskScheduled{
			TaskScheduled: &protos.TaskScheduledEvent{
				Name:               req.Name,
				Version:            req.Version,
				Input:              req.Input,
				TaskExecutionId:    req.TaskExecutionId,
				ParentTraceContext: ptc,
			},
		},
	}
	result, err := executor.ExecuteActivity(
		ctx,
		api.InstanceID(req.WorkflowInstance.InstanceId),
		event,
	)

	resp := &protos.ActivityResponse{
		InstanceId: req.WorkflowInstance.InstanceId,
		TaskId:     req.TaskId,
	}
	if err != nil {
		resp.FailureDetails = &protos.TaskFailureDetails{
			ErrorType:    fmt.Sprintf("%T", err),
			ErrorMessage: err.Error(),
		}
	} else if tc := result.GetTaskCompleted(); tc != nil {
		resp.Result = tc.Result
	} else if tf := result.GetTaskFailed(); tf != nil {
		resp.FailureDetails = tf.FailureDetails
	} else {
		resp.FailureDetails = &protos.TaskFailureDetails{
			ErrorType:    "UnknownTaskResult",
			ErrorMessage: "Unknown task result",
		}
	}
	return resp
}
