package tests

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/task"
)

const (
	raiseErrorWorkflowName = "RaiseErrorWorkflow"
	raiseErrorEventName    = "Approval"
	raiseErrorType         = "example.ApprovalRejected"
)

func raisedErrorWorkflowResult(ctx *task.WorkflowContext) (any, error) {
	err := ctx.WaitForSingleEvent(raiseErrorEventName, -1).Await(nil)

	var taskError *task.TaskFailedError
	if !errors.As(err, &taskError) {
		if err == nil {
			return nil, fmt.Errorf("expected the external event to fail, but it completed successfully")
		}
		return nil, err
	}

return map[string]string{
		"errorType":         taskError.ErrorType(),
		"errorMessage":      taskError.ErrorMessage(),
		"innerErrorMessage": taskError.FailureDetails.GetInnerFailure().GetErrorMessage(),
		"hasInnerError":     fmt.Sprint(taskError.IsCausedBy("*errors.errorString")),
	}, nil
}

func Test_RaiseError_FailsPendingExternalEventWait(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN(raiseErrorWorkflowName, func(ctx *task.WorkflowContext) (any, error) {
		ctx.SetCustomStatus("waiting")
		return raisedErrorWorkflowResult(ctx)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, raiseErrorWorkflowName)
	require.NoError(t, err)

	_, err = client.WaitForWorkflowStart(ctx, id)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		metadata, err := client.FetchWorkflowMetadata(ctx, id, api.WithFetchPayloads(true))
		return err == nil && metadata.GetCustomStatus().GetValue() == "waiting"
	}, 3*time.Second, 10*time.Millisecond, "workflow did not register its external-event wait")

	failure := fmt.Errorf("approval rejected: %w", errors.New("risk service unavailable"))
	require.NoError(t, client.RaiseError(ctx, id, raiseErrorEventName, api.WithError(failure)))

	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.GetRuntimeStatus())
	assert.JSONEq(t, `{"errorType":"*fmt.wrapError","errorMessage":"approval rejected: risk service unavailable","innerErrorMessage":"risk service unavailable","hasInnerError":"true"}`, metadata.GetOutput().GetValue())
}

func Test_RaiseError_BuffersFailureUntilExternalEventWaitIsRegistered(t *testing.T) {
	activityStarted := make(chan struct{}, 1)
	releaseActivity := make(chan struct{})
	var releaseOnce sync.Once
	releaseGate := func() {
		releaseOnce.Do(func() { close(releaseActivity) })
	}
	defer releaseGate()

	r := task.NewTaskRegistry()
	r.AddActivityN("WaitAtGate", func(task.ActivityContext) (any, error) {
		select {
		case activityStarted <- struct{}{}:
		default:
		}
		<-releaseActivity
		return nil, nil
	})
	r.AddWorkflowN(raiseErrorWorkflowName, func(ctx *task.WorkflowContext) (any, error) {
		if err := ctx.CallActivity("WaitAtGate").Await(nil); err != nil {
			return nil, err
		}
		return raisedErrorWorkflowResult(ctx)
	})

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, raiseErrorWorkflowName)
	require.NoError(t, err)

	select {
	case <-activityStarted:
	case <-ctx.Done():
		t.Fatal("workflow did not reach its activity gate")
	}

	require.NoError(t, client.RaiseError(ctx, id, raiseErrorEventName, api.WithError(errors.New("buffered external error"))))
	releaseGate()

	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.GetRuntimeStatus())
	assert.JSONEq(t, `{"errorType":"*errors.errorString","errorMessage":"buffered external error","innerErrorMessage":"","hasInnerError":"false"}`, metadata.GetOutput().GetValue())
}
