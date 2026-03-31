package tests

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
	"github.com/dapr/durabletask-go/task"
	"github.com/dapr/durabletask-go/tests/utils"
)

var tracer = otel.Tracer("workflow-test")

func Test_EmptyWorkflow(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("EmptyWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "EmptyWorkflow")
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("EmptyWorkflow", id),
		utils.AssertWorkflowExecuted("EmptyWorkflow", id, "COMPLETED"),
	)
}

func Test_SingleTimer(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SingleTimer", func(ctx *task.WorkflowContext) (any, error) {
		err := ctx.CreateTimer(time.Duration(0)).Await(nil)
		return nil, err
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "SingleTimer")
	if assert.NoError(t, err) {
		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.GreaterOrEqual(t, metadata.LastUpdatedAt.AsTime(), metadata.CreatedAt.AsTime())
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("SingleTimer", id),
		utils.AssertTimer(id),
		utils.AssertWorkflowExecuted("SingleTimer", id, "COMPLETED"),
	)
}

func Test_ConcurrentTimers(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("TimerFanOut", func(ctx *task.WorkflowContext) (any, error) {
		tasks := []task.Task{}
		for i := 0; i < 3; i++ {
			tasks = append(tasks, ctx.CreateTimer(1*time.Second))
		}
		for _, t := range tasks {
			if err := t.Await(nil); err != nil {
				return nil, err
			}
		}
		return nil, nil
	})

	// Initialization
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "TimerFanOut")
	if assert.NoError(t, err) {
		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.GreaterOrEqual(t, metadata.LastUpdatedAt.AsTime(), metadata.CreatedAt.AsTime())
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("TimerFanOut", id),
		utils.AssertTimer(id),
		utils.AssertTimer(id),
		utils.AssertTimer(id),
		utils.AssertWorkflowExecuted("TimerFanOut", id, "COMPLETED"),
	)
}

func Test_IsReplaying(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("IsReplayingOrch", func(ctx *task.WorkflowContext) (any, error) {
		values := []bool{ctx.IsReplaying}
		ctx.CreateTimer(time.Duration(0)).Await(nil)
		values = append(values, ctx.IsReplaying)
		ctx.CreateTimer(time.Duration(0)).Await(nil)
		values = append(values, ctx.IsReplaying)
		return values, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "IsReplayingOrch")
	if assert.NoError(t, err) {
		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `[true,true,false]`, metadata.Output.Value)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("IsReplayingOrch", id),
		utils.AssertTimer(id),
		utils.AssertTimer(id),
		utils.AssertWorkflowExecuted("IsReplayingOrch", id, "COMPLETED"),
	)
}

func Test_SingleActivity(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SingleActivity", func(ctx *task.WorkflowContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		err := ctx.CallActivity("SayHello", task.WithActivityInput(input)).Await(&output)
		return output, err
	})
	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		var name string
		if err := ctx.GetInput(&name); err != nil {
			return nil, err
		}
		return fmt.Sprintf("Hello, %s!", name), nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "SingleActivity", api.WithInput("世界"))
	if assert.NoError(t, err) {
		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `"Hello, 世界!"`, metadata.Output.Value)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("SingleActivity", id),
		utils.AssertActivity("SayHello", id, 0),
		utils.AssertWorkflowExecuted("SingleActivity", id, "COMPLETED"),
	)
}

func Test_SingleActivity_TaskSpan(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SingleActivity", func(ctx *task.WorkflowContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		err := ctx.CallActivity("SayHello", task.WithActivityInput(input)).Await(&output)
		return output, err
	})
	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		var name string
		if err := ctx.GetInput(&name); err != nil {
			return nil, err
		}
		ctx.GetTraceContext()
		_, childSpan := tracer.Start(ctx.Context(), "activityChild")
		childSpan.End()
		return fmt.Sprintf("Hello, %s!", name), nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()

	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "SingleActivity", api.WithInput("世界"))
	if assert.NoError(t, err) {
		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `"Hello, 世界!"`, metadata.Output.Value)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("SingleActivity", id),
		utils.AssertSpan("activityChild"),
		utils.AssertActivity("SayHello", id, 0),
		utils.AssertWorkflowExecuted("SingleActivity", id, "COMPLETED"),
	)
	// assert child-parent relationship
	assert.Equal(t, spans[1].Parent().SpanID(), spans[2].SpanContext().SpanID())
}

func Test_ActivityChain(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("ActivityChain", func(ctx *task.WorkflowContext) (any, error) {
		val := 0
		for i := 0; i < 10; i++ {
			if err := ctx.CallActivity("PlusOne", task.WithActivityInput(val)).Await(&val); err != nil {
				return nil, err
			}
		}
		return val, nil
	})
	r.AddActivityN("PlusOne", func(ctx task.ActivityContext) (any, error) {
		var input int
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		return input + 1, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "ActivityChain")
	if assert.NoError(t, err) {
		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `10`, metadata.Output.Value)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("ActivityChain", id),
		utils.AssertActivity("PlusOne", id, 0), utils.AssertActivity("PlusOne", id, 1), utils.AssertActivity("PlusOne", id, 2),
		utils.AssertActivity("PlusOne", id, 3), utils.AssertActivity("PlusOne", id, 4), utils.AssertActivity("PlusOne", id, 5),
		utils.AssertActivity("PlusOne", id, 6), utils.AssertActivity("PlusOne", id, 7), utils.AssertActivity("PlusOne", id, 8),
		utils.AssertActivity("PlusOne", id, 9), utils.AssertWorkflowExecuted("ActivityChain", id, "COMPLETED"),
	)
}

func Test_ActivityRetries(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("ActivityRetries", func(ctx *task.WorkflowContext) (any, error) {
		if err := ctx.CallActivity("FailActivity", task.WithActivityRetryPolicy(&task.RetryPolicy{
			MaxAttempts:          3,
			InitialRetryInterval: 10 * time.Millisecond,
		})).Await(nil); err != nil {
			return nil, err
		}
		return nil, nil
	})
	r.AddActivityN("FailActivity", func(ctx task.ActivityContext) (any, error) {
		return nil, errors.New("activity failure")
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "ActivityRetries")
	if assert.NoError(t, err) {
		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
			// With 3 max attempts there will be two retries with 10 millis delay before each
			require.GreaterOrEqual(t, metadata.LastUpdatedAt.AsTime(), metadata.CreatedAt.AsTime().Add(2*10*time.Millisecond))
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("ActivityRetries", id),
		utils.AssertActivity("FailActivity", id, 0),
		utils.AssertTimer(id, utils.AssertTaskID(1)),
		utils.AssertActivity("FailActivity", id, 2),
		utils.AssertTimer(id, utils.AssertTaskID(3)),
		utils.AssertActivity("FailActivity", id, 4),
		utils.AssertWorkflowExecuted("ActivityRetries", id, "FAILED"),
	)
}

func Test_ActivityFanOut(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("ActivityFanOut", func(ctx *task.WorkflowContext) (any, error) {
		tasks := []task.Task{}
		for i := 0; i < 10; i++ {
			tasks = append(tasks, ctx.CallActivity("ToString", task.WithActivityInput(i)))
		}
		results := []string{}
		for _, t := range tasks {
			var result string
			if err := t.Await(&result); err != nil {
				return nil, err
			}
			results = append(results, result)
		}
		sort.Sort(sort.Reverse(sort.StringSlice(results)))
		return results, nil
	})
	r.AddActivityN("ToString", func(ctx task.ActivityContext) (any, error) {
		var input int
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		time.Sleep(1 * time.Second)
		return fmt.Sprintf("%d", input), nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r, backend.WithMaxParallelism(10))
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "ActivityFanOut")
	if assert.NoError(t, err) {
		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `["9","8","7","6","5","4","3","2","1","0"]`, metadata.Output.Value)

			// Because all the activities run in parallel, they should complete very quickly
			assert.Less(t, metadata.LastUpdatedAt.AsTime().Sub(metadata.CreatedAt.AsTime()), 3*time.Second)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("ActivityFanOut", id),
		// TODO: Find a way to assert an unordered sequence of traces since the order of activity traces is non-deterministic.
	)
}

func Test_SingleChildWorkflow_Completed(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Parent", func(ctx *task.WorkflowContext) (any, error) {
		var input any
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output any
		err := ctx.CallChildWorkflow(
			"Child",
			task.WithChildWorkflowInstanceID(string(ctx.ID)+"_child"),
			task.WithChildWorkflowInput(input)).Await(&output)
		return output, err
	})
	r.AddWorkflowN("Child", func(ctx *task.WorkflowContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return "", err
		}
		return input, nil
	})

	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "Parent", api.WithInput("Hello, world!"))
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"Hello, world!"`, metadata.Output.Value)

	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("Parent", id),
		utils.AssertWorkflowExecuted("Child", id+"_child", "COMPLETED"),
		utils.AssertWorkflowExecuted("Parent", id, "COMPLETED"),
	)
}

func Test_SingleChildWorkflow_Failed(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Parent", func(ctx *task.WorkflowContext) (any, error) {
		err := ctx.CallChildWorkflow(
			"Child",
			task.WithChildWorkflowInstanceID(string(ctx.ID)+"_child")).Await(nil)
		return nil, err
	})
	r.AddWorkflowN("Child", func(ctx *task.WorkflowContext) (any, error) {
		return nil, errors.New("Child failed")
	})

	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "Parent")
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
	if assert.NotNil(t, metadata.FailureDetails) {
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "Child failed")
	}

	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("Parent", id),
		utils.AssertWorkflowExecuted("Child", id+"_child", "FAILED"),
		utils.AssertWorkflowExecuted("Parent", id, "FAILED"),
	)
}

func Test_SingleChildWorkflow_Failed_Retries(t *testing.T) {
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Parent", func(ctx *task.WorkflowContext) (any, error) {
		err := ctx.CallChildWorkflow(
			"Child",
			task.WithChildWorkflowInstanceID(string(ctx.ID)+"_child"),
			task.WithChildWorkflowRetryPolicy(&task.RetryPolicy{
				MaxAttempts:          3,
				InitialRetryInterval: 10 * time.Millisecond,
				BackoffCoefficient:   2,
			})).Await(nil)
		return nil, err
	})
	r.AddWorkflowN("Child", func(ctx *task.WorkflowContext) (any, error) {
		return nil, errors.New("Child failed")
	})

	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "Parent")
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
	if assert.NotNil(t, metadata.FailureDetails) {
		assert.Contains(t, metadata.FailureDetails.ErrorMessage, "Child failed")
	}

	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("Parent", id),
		utils.AssertWorkflowExecuted("Child", id+"_child", "FAILED"),
		utils.AssertTimer(id, utils.AssertTaskID(1)),
		utils.AssertWorkflowExecuted("Child", id+"_child", "FAILED"),
		utils.AssertTimer(id, utils.AssertTaskID(3)),
		utils.AssertWorkflowExecuted("Child", id+"_child", "FAILED"),
		utils.AssertWorkflowExecuted("Parent", id, "FAILED"),
	)
}

func Test_ContinueAsNew(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("ContinueAsNewTest", func(ctx *task.WorkflowContext) (any, error) {
		var input int32
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}

		if input < 10 {
			if err := ctx.CreateTimer(0).Await(nil); err != nil {
				return nil, err
			}
			ctx.ContinueAsNew(input + 1)
		}
		return input, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "ContinueAsNewTest", api.WithInput(0))
	if assert.NoError(t, err) {
		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		if assert.NoError(t, err) {
			assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			assert.Equal(t, `10`, metadata.Output.Value)
		}
	}

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("ContinueAsNewTest", id),
		utils.AssertTimer(id), utils.AssertWorkflowExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		utils.AssertTimer(id), utils.AssertWorkflowExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		utils.AssertTimer(id), utils.AssertWorkflowExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		utils.AssertTimer(id), utils.AssertWorkflowExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		utils.AssertTimer(id), utils.AssertWorkflowExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		utils.AssertTimer(id), utils.AssertWorkflowExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		utils.AssertTimer(id), utils.AssertWorkflowExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		utils.AssertTimer(id), utils.AssertWorkflowExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		utils.AssertTimer(id), utils.AssertWorkflowExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		utils.AssertTimer(id), utils.AssertWorkflowExecuted("ContinueAsNewTest", id, "CONTINUED_AS_NEW"),
		utils.AssertWorkflowExecuted("ContinueAsNewTest", id, "COMPLETED"),
	)
}

func Test_ContinueAsNew_Events(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("ContinueAsNewTest", func(ctx *task.WorkflowContext) (any, error) {
		var input int32
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var complete bool
		if err := ctx.WaitForSingleEvent("MyEvent", -1).Await(&complete); err != nil {
			return nil, err
		}
		if complete {
			return input, nil
		}
		ctx.ContinueAsNew(input+1, task.WithKeepUnprocessedEvents())
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "ContinueAsNewTest", api.WithInput(0))
	require.NoError(t, err)
	for i := 0; i < 10; i++ {
		require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent", api.WithEventPayload(false)))
	}
	require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent", api.WithEventPayload(true)))
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `10`, metadata.Output.Value)
}

func Test_ExternalEventContention(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("ContinueAsNewTest", func(ctx *task.WorkflowContext) (any, error) {
		var data int32
		if err := ctx.WaitForSingleEvent("MyEventData", 1*time.Second).Await(&data); err != nil && !errors.Is(err, task.ErrTaskCanceled) {
			return nil, err
		}

		var complete bool
		if err := ctx.WaitForSingleEvent("MyEventSignal", -1).Await(&complete); err != nil {
			return nil, err
		}

		if complete {
			return data, nil
		}

		ctx.ContinueAsNew(nil, task.WithKeepUnprocessedEvents())
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "ContinueAsNewTest")
	require.NoError(t, err)

	// Wait for the timer to elapse
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.ErrorIs(t, err, timeoutCtx.Err())

	// Now raise the event, which should queue correctly for the next time
	// around
	require.NoError(t, client.RaiseEvent(ctx, id, "MyEventData", api.WithEventPayload(42)))
	require.NoError(t, client.RaiseEvent(ctx, id, "MyEventSignal", api.WithEventPayload(false)))
	require.NoError(t, client.RaiseEvent(ctx, id, "MyEventSignal", api.WithEventPayload(true)))

	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `42`, metadata.Output.Value)
}

func Test_ExternalEventWorkflow(t *testing.T) {
	const eventCount = 10

	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("ExternalEventWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		for i := 0; i < eventCount; i++ {
			var value int
			ctx.WaitForSingleEvent("MyEvent", 5*time.Second).Await(&value)
			if value != i {
				return false, errors.New("Unexpected value")
			}
		}
		return true, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "ExternalEventWorkflow", api.WithInput(0))
	if assert.NoError(t, err) {
		for i := 0; i < eventCount; i++ {
			opts := api.WithEventPayload(i)
			require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent", opts))
		}

		timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()

		metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
		require.NoError(t, err)
		assert.True(t, api.WorkflowMetadataIsComplete(metadata))
		require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	}

	// Validate the exported OTel traces
	eventSizeInBytes := 1
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("ExternalEventWorkflow", id),
		utils.AssertWorkflowExecuted("ExternalEventWorkflow", id, "COMPLETED", utils.AssertSpanEvents(
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
		)),
	)
}

func Test_ExternalEventTimeout(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("ExternalEventWorkflowWithTimeout", func(ctx *task.WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("MyEvent", 2*time.Second).Await(nil); err != nil {
			return nil, err
		}
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run two variations, one where we raise the external event and one where we don't (timeout)
	for _, raiseEvent := range []bool{true, false} {
		t.Run(fmt.Sprintf("RaiseEvent:%v", raiseEvent), func(t *testing.T) {
			// Run the workflow
			id, err := client.ScheduleNewWorkflow(ctx, "ExternalEventWorkflowWithTimeout")
			require.NoError(t, err)
			if raiseEvent {
				require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent"))
			}

			timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			defer cancel()

			metadata, err := client.WaitForWorkflowCompletion(timeoutCtx, id)
			require.NoError(t, err)

			// Validate the exported OTel traces
			spans := exporter.GetSpans().Snapshots()
			if raiseEvent {
				assert.True(t, api.WorkflowMetadataIsComplete(metadata))
				assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

				utils.AssertSpanSequence(t, spans,
					utils.AssertWorkflowCreated("ExternalEventWorkflowWithTimeout", id),
					utils.AssertWorkflowExecuted("ExternalEventWorkflowWithTimeout", id, "COMPLETED", utils.AssertSpanEvents(
						utils.AssertExternalEvent("MyEvent", 0),
					)),
				)
			} else {
				assert.True(t, api.WorkflowMetadataIsComplete(metadata))
				require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED, metadata.RuntimeStatus)
				if assert.NotNil(t, metadata.FailureDetails) {
					// The exact message is not important - just make sure it's something clear
					// NOTE: In a future version, we might have a specifc ErrorType contract. For now, the
					//       caller shouldn't make any assumptions about this.
					assert.Equal(t, "the task was canceled", metadata.FailureDetails.ErrorMessage)
				}

				utils.AssertSpanSequence(t, spans,
					utils.AssertWorkflowCreated("ExternalEventWorkflowWithTimeout", id),
					// A timer is used to implement the event timeout
					utils.AssertTimer(id),
					utils.AssertWorkflowExecuted("ExternalEventWorkflowWithTimeout", id, "FAILED", utils.AssertSpanEvents()),
				)
			}
		})
		exporter.Reset()
	}
}

func Test_SuspendResumeWorkflow(t *testing.T) {
	const eventCount = 10

	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SuspendResumeWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		for i := 0; i < eventCount; i++ {
			var value int
			ctx.WaitForSingleEvent("MyEvent", 5*time.Second).Await(&value)
			if value != i {
				return false, errors.New("Unexpected value")
			}
		}
		return true, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow, which will block waiting for external events
	id, err := client.ScheduleNewWorkflow(ctx, "SuspendResumeWorkflow", api.WithInput(0))
	require.NoError(t, err)

	// Wait for the workflow to finish starting
	_, err = client.WaitForWorkflowStart(ctx, id)
	require.NoError(t, err)

	// Suspend the workflow
	require.NoError(t, client.SuspendWorkflow(ctx, id, ""))

	// Raise a bunch of events to the workflow (they should get buffered but not consumed)
	for i := 0; i < eventCount; i++ {
		opts := api.WithEventPayload(i)
		require.NoError(t, client.RaiseEvent(ctx, id, "MyEvent", opts))
	}

	// Make sure the workflow *doesn't* complete
	timeoutCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.ErrorIs(t, err, timeoutCtx.Err())

	var metadata *backend.WorkflowMetadata
	metadata, err = client.FetchWorkflowMetadata(ctx, id)
	if assert.NoError(t, err) {
		assert.True(t, api.WorkflowMetadataIsRunning(metadata))
		assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED, metadata.RuntimeStatus)
	}

	// Resume the workflow and wait for it to complete
	require.NoError(t, client.ResumeWorkflow(ctx, id, ""))
	timeoutCtx, cancel = context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = client.WaitForWorkflowCompletion(timeoutCtx, id)
	require.NoError(t, err)

	// Validate the exported OTel traces
	eventSizeInBytes := 1
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("SuspendResumeWorkflow", id),
		utils.AssertWorkflowExecuted("SuspendResumeWorkflow", id, "COMPLETED", utils.AssertSpanEvents(
			utils.AssertSuspendedEvent(),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertExternalEvent("MyEvent", eventSizeInBytes),
			utils.AssertResumedEvent(),
		)),
	)
}

func Test_TerminateWorkflow(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("MyWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		ctx.CreateTimer(3 * time.Second).Await(nil)
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow, which will block waiting for external events
	id, err := client.ScheduleNewWorkflow(ctx, "MyWorkflow")
	require.NoError(t, err)

	// Terminate the workflow before the timer expires
	require.NoError(t, client.TerminateWorkflow(ctx, id, api.WithOutput("You got terminated!")))

	// Wait for the workflow to complete
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	assert.True(t, api.WorkflowMetadataIsComplete(metadata))
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, metadata.RuntimeStatus)
	require.Equal(t, `"You got terminated!"`, metadata.Output.Value)

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("MyWorkflow", id),
		utils.AssertWorkflowExecuted("MyWorkflow", id, "TERMINATED"),
	)
}

func Test_TerminateWorkflow_Recursive(t *testing.T) {
	delayTime := 4 * time.Second
	executedActivity := false
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Root", func(ctx *task.WorkflowContext) (any, error) {
		tasks := []task.Task{}
		for i := 0; i < 5; i++ {
			task := ctx.CallChildWorkflow("L1", task.WithChildWorkflowInstanceID(string(ctx.ID)+"_L1_"+strconv.Itoa(i)))
			tasks = append(tasks, task)
		}
		for _, task := range tasks {
			task.Await(nil)
		}
		return nil, nil
	})
	r.AddWorkflowN("L1", func(ctx *task.WorkflowContext) (any, error) {
		ctx.CallChildWorkflow("L2", task.WithChildWorkflowInstanceID(string(ctx.ID)+"_L2")).Await(nil)
		return nil, nil
	})
	r.AddWorkflowN("L2", func(ctx *task.WorkflowContext) (any, error) {
		ctx.CreateTimer(delayTime).Await(nil)
		ctx.CallActivity("Fail").Await(nil)
		return nil, nil
	})
	r.AddActivityN("Fail", func(ctx task.ActivityContext) (any, error) {
		executedActivity = true
		return nil, errors.New("Failed: Should not have executed the activity")
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Test terminating with and without recursion
	for _, recurse := range []bool{true, false} {
		t.Run(fmt.Sprintf("Recurse = %v", recurse), func(t *testing.T) {
			// Run the workflow, which will block waiting for external events
			id, err := client.ScheduleNewWorkflow(ctx, "Root")
			require.NoError(t, err)

			// Wait long enough to ensure all workflows have started (but not longer than the timer delay)
			assert.Eventually(t, func() bool {
				// List of all workflows created
				workflowIDs := []string{string(id)}
				for i := 0; i < 5; i++ {
					workflowIDs = append(workflowIDs, string(id)+"_L1_"+strconv.Itoa(i), string(id)+"_L1_"+strconv.Itoa(i)+"_L2")
				}
				for _, orchID := range workflowIDs {
					metadata, err := client.FetchWorkflowMetadata(ctx, api.InstanceID(orchID))
					require.NoError(t, err)
					// All workflows should be running
					if metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING {
						return false
					}
				}
				return true
			}, 2*time.Second, 100*time.Millisecond)

			// Terminate the root workflow and mark whether a recursive termination
			output := fmt.Sprintf("Recursive termination = %v", recurse)
			opts := []api.TerminateOptions{api.WithOutput(output), api.WithRecursiveTerminate(recurse)}
			require.NoError(t, client.TerminateWorkflow(ctx, id, opts...))

			// Wait for the root workflow to complete and verify its terminated status
			metadata, err := client.WaitForWorkflowCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, metadata.RuntimeStatus)
			require.Equal(t, fmt.Sprintf("\"%s\"", output), metadata.Output.Value)

			// Wait for all L2 child workflows to complete
			orchIDs := []string{}
			for i := 0; i < 5; i++ {
				orchIDs = append(orchIDs, string(id)+"_L1_"+strconv.Itoa(i)+"_L2")
			}
			for _, orchID := range orchIDs {
				_, err := client.WaitForWorkflowCompletion(ctx, api.InstanceID(orchID))
				require.NoError(t, err)
			}
			// Verify tht none of the L2 child workflows executed the activity in case of recursive termination
			assert.NotEqual(t, recurse, executedActivity)
		})
	}
}

func Test_TerminateWorkflow_Recursive_TerminateCompletedChildWorkflow(t *testing.T) {
	delayTime := 4 * time.Second
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Root", func(ctx *task.WorkflowContext) (any, error) {
		// Create L1 child workflow and wait for it to complete
		ctx.CallChildWorkflow("L1", task.WithChildWorkflowInstanceID(string(ctx.ID)+"_L1")).Await(nil)
		ctx.CreateTimer(delayTime).Await(nil)
		return nil, nil
	})
	r.AddWorkflowN("L1", func(ctx *task.WorkflowContext) (any, error) {
		// Create L2 child workflow but don't wait for it to complete
		ctx.CallChildWorkflow("L2", task.WithChildWorkflowInstanceID(string(ctx.ID)+"_L2"))
		return nil, nil
	})
	r.AddWorkflowN("L2", func(ctx *task.WorkflowContext) (any, error) {
		// Wait for `delayTime`
		ctx.CreateTimer(delayTime).Await(nil)
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Test terminating with and without recursion
	for _, recurse := range []bool{true, false} {
		t.Run(fmt.Sprintf("Recurse = %v", recurse), func(t *testing.T) {
			// Run the workflow, which will block waiting for external events
			id, err := client.ScheduleNewWorkflow(ctx, "Root")
			require.NoError(t, err)

			// Wait long enough to ensure that all L1 workflows have completed but Root and L2 are still running
			assert.Eventually(t, func() bool {
				// List of all workflows created
				workflowIDs := []string{string(id), string(id) + "_L1", string(id) + "_L1_L2"}
				for _, orchID := range workflowIDs {
					// Fetch workflow metadata
					metadata, err := client.FetchWorkflowMetadata(ctx, api.InstanceID(orchID))
					require.NoError(t, err)
					if orchID == string(id)+"_L1" {
						// L1 workflow should have completed
						if metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED {
							return false
						}
					} else {
						// Root and L2 workflows should still be running
						if metadata.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING {
							return false
						}
					}
				}
				return true
			}, 2*time.Second, 100*time.Millisecond)

			// Terminate the root workflow and mark whether a recursive termination
			output := fmt.Sprintf("Recursive termination = %v", recurse)
			opts := []api.TerminateOptions{api.WithOutput(output), api.WithRecursiveTerminate(recurse)}
			require.NoError(t, client.TerminateWorkflow(ctx, id, opts...))

			// Wait for the root workflow to complete and verify its terminated status
			metadata, err := client.WaitForWorkflowCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED, metadata.RuntimeStatus)
			require.Equal(t, fmt.Sprintf("\"%s\"", output), metadata.Output.Value)

			// Verify that the L1 and L2 workflows have completed with the appropriate status
			L1_WorkflowStatus := protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED
			L2_WorkflowStatus := protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED
			L2_Output := ""
			L1_Output := ""
			if recurse {
				L2_WorkflowStatus = protos.OrchestrationStatus_ORCHESTRATION_STATUS_TERMINATED
				L2_Output = fmt.Sprintf("\"%s\"", output)
			}
			// In recursive case, L1 workflow is not terminated because it was already completed when the root workflow was terminated
			metadata, err = client.WaitForWorkflowCompletion(ctx, id+"_L1")
			require.NoError(t, err)
			require.Equal(t, L1_WorkflowStatus, metadata.RuntimeStatus)
			require.Equal(t, L1_Output, metadata.Output.Value)

			// In recursive case, L2 is terminated because it was still running when the root workflow was terminated
			metadata, err = client.WaitForWorkflowCompletion(ctx, id+"_L1_L2")
			require.NoError(t, err)
			require.Equal(t, L2_WorkflowStatus, metadata.RuntimeStatus)
			require.Equal(t, L2_Output, metadata.Output.Value)
		})
	}
}

func Test_PurgeCompletedWorkflow(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("ExternalEventWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("MyEvent", 30*time.Second).Await(nil); err != nil {
			return nil, err
		}
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "ExternalEventWorkflow")
	if !assert.NoError(t, err) {
		return
	}
	if _, err = client.WaitForWorkflowStart(ctx, id); !assert.NoError(t, err) {
		return
	}

	// Try to purge the workflow state before it completes and verify that it fails with ErrNotCompleted
	if err = client.PurgeWorkflowState(ctx, id); !assert.ErrorIs(t, err, api.ErrNotCompleted) {
		return
	}

	// Raise an event to the workflow so that it can complete
	if err = client.RaiseEvent(ctx, id, "MyEvent"); !assert.NoError(t, err) {
		return
	}
	if _, err = client.WaitForWorkflowCompletion(ctx, id); !assert.NoError(t, err) {
		return
	}

	// Try to purge the workflow state again and verify that it succeeds
	if err = client.PurgeWorkflowState(ctx, id); !assert.NoError(t, err) {
		return
	}

	// Try to fetch the workflow metadata and verify that it fails with ErrInstanceNotFound
	if _, err = client.FetchWorkflowMetadata(ctx, id); !assert.ErrorIs(t, err, api.ErrInstanceNotFound) {
		return
	}

	// Try to purge again and verify that it also fails with ErrInstanceNotFound
	if err = client.PurgeWorkflowState(ctx, id); !assert.ErrorIs(t, err, api.ErrInstanceNotFound) {
		return
	}
}

func Test_PurgeWorkflow_Recursive(t *testing.T) {
	delayTime := 4 * time.Second
	r := task.NewTaskRegistry()
	r.AddWorkflowN("Root", func(ctx *task.WorkflowContext) (any, error) {
		ctx.CallChildWorkflow("L1", task.WithChildWorkflowInstanceID(string(ctx.ID)+"_L1")).Await(nil)
		return nil, nil
	})
	r.AddWorkflowN("L1", func(ctx *task.WorkflowContext) (any, error) {
		ctx.CallChildWorkflow("L2", task.WithChildWorkflowInstanceID(string(ctx.ID)+"_L2")).Await(nil)
		return nil, nil
	})
	r.AddWorkflowN("L2", func(ctx *task.WorkflowContext) (any, error) {
		ctx.CreateTimer(delayTime).Await(nil)
		return nil, nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Test terminating with and without recursion
	for _, recurse := range []bool{true, false} {
		t.Run(fmt.Sprintf("Recurse = %v", recurse), func(t *testing.T) {
			// Run the workflow, which will block waiting for external events
			id, err := client.ScheduleNewWorkflow(ctx, "Root")
			require.NoError(t, err)

			metadata, err := client.WaitForWorkflowCompletion(ctx, id)
			require.NoError(t, err)
			require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

			// Purge the root workflow
			opts := []api.PurgeOptions{api.WithRecursivePurge(recurse)}
			err = client.PurgeWorkflowState(ctx, id, opts...)
			assert.NoError(t, err)

			// Verify that root Workflow has been purged
			_, err = client.FetchWorkflowMetadata(ctx, id)
			assert.ErrorIs(t, err, api.ErrInstanceNotFound)

			if recurse {
				// Verify that L1 and L2 workflows have been purged
				_, err = client.FetchWorkflowMetadata(ctx, id+"_L1")
				assert.ErrorIs(t, err, api.ErrInstanceNotFound)

				_, err = client.FetchWorkflowMetadata(ctx, id+"_L1_L2")
				assert.ErrorIs(t, err, api.ErrInstanceNotFound)
			} else {
				// Verify that L1 and L2 workflows are not purged
				metadata, err = client.FetchWorkflowMetadata(ctx, id+"_L1")
				assert.NoError(t, err)
				assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

				_, err = client.FetchWorkflowMetadata(ctx, id+"_L1_L2")
				assert.NoError(t, err)
				assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
			}
		})
	}
}

func Test_RecreateCompletedWorkflow(t *testing.T) {
	t.Skip("Not yet supported. Needs https://github.com/dapr/durabletask-go/issues/42")

	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("HelloWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		err := ctx.CallActivity("SayHello", task.WithActivityInput(input)).Await(&output)
		return output, err
	})
	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		var name string
		if err := ctx.GetInput(&name); err != nil {
			return nil, err
		}
		return fmt.Sprintf("Hello, %s!", name), nil
	})

	// Initialization
	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the first workflow
	id, err := client.ScheduleNewWorkflow(ctx, "HelloWorkflow", api.WithInput("世界"))
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"Hello, 世界!"`, metadata.Output.Value)

	// Run the second workflow with the same ID as the first
	var newID api.InstanceID
	newID, err = client.ScheduleNewWorkflow(ctx, "HelloWorkflow", api.WithInstanceID(id), api.WithInput("World"))
	require.NoError(t, err)
	require.Equal(t, id, newID)
	metadata, err = client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, `"Hello, World!"`, metadata.Output.Value)

	// Validate the exported OTel traces
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("SingleActivity", id),
		utils.AssertActivity("SayHello", id, 0),
		utils.AssertWorkflowExecuted("SingleActivity", id, "COMPLETED"),
		utils.AssertWorkflowCreated("SingleActivity", id),
		utils.AssertActivity("SayHello", id, 0),
		utils.AssertWorkflowExecuted("SingleActivity", id, "COMPLETED"),
	)
}

func Test_SingleActivity_ReuseInstanceIDError(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	r.AddWorkflowN("SingleActivity", func(ctx *task.WorkflowContext) (any, error) {
		var input string
		if err := ctx.GetInput(&input); err != nil {
			return nil, err
		}
		var output string
		err := ctx.CallActivity("SayHello", task.WithActivityInput(input)).Await(&output)
		return output, err
	})
	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		var name string
		if err := ctx.GetInput(&name); err != nil {
			return nil, err
		}
		return fmt.Sprintf("Hello, %s!", name), nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	instanceID := api.InstanceID("ERROR_IF_RUNNING_OR_COMPLETED")

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "SingleActivity", api.WithInput("世界"), api.WithInstanceID(instanceID))
	require.NoError(t, err)
	id, err = client.ScheduleNewWorkflow(ctx, "SingleActivity", api.WithInput("World"), api.WithInstanceID(id))
	if assert.Error(t, err) {
		assert.Contains(t, err.Error(), "workflow instance already exists")
	}
}

func Test_TaskExecutionId(t *testing.T) {
	t.Run("SingleActivityWithRetry", func(t *testing.T) {
		// Registration
		r := task.NewTaskRegistry()
		require.NoError(t, r.AddWorkflowN("TaskExecutionID", func(ctx *task.WorkflowContext) (any, error) {
			if err := ctx.CallActivity("FailActivity", task.WithActivityRetryPolicy(&task.RetryPolicy{
				MaxAttempts:          3,
				InitialRetryInterval: 10 * time.Millisecond,
			})).Await(nil); err != nil {
				return nil, err
			}
			return nil, nil
		}))

		executionMap := make(map[string]int)
		var executionId string
		require.NoError(t, r.AddActivityN("FailActivity", func(ctx task.ActivityContext) (any, error) {
			executionId = ctx.GetTaskExecutionID()
			executionMap[executionId]++
			if executionMap[executionId] == 3 {
				return nil, nil
			}
			return nil, errors.New("activity failure")
		}))

		// Initialization
		ctx := context.Background()

		client, worker := initTaskHubWorker(ctx, r)
		defer worker.Shutdown(ctx)

		// Run the workflow
		id, err := client.ScheduleNewWorkflow(ctx, "TaskExecutionID")
		require.NoError(t, err)

		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		require.NoError(t, err)

		assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
		// With 3 max attempts there will be two retries with 10 millis delay before each
		require.GreaterOrEqual(t, metadata.LastUpdatedAt.AsTime(), metadata.CreatedAt.AsTime().Add(2*10*time.Millisecond))
		assert.NotEmpty(t, executionId)
		assert.Equal(t, 1, len(executionMap))
		assert.Equal(t, 3, executionMap[executionId])
	})

	t.Run("ParallelActivityWithRetry", func(t *testing.T) {
		// Registration
		r := task.NewTaskRegistry()
		require.NoError(t, r.AddWorkflowN("TaskExecutionID", func(ctx *task.WorkflowContext) (any, error) {
			t1 := ctx.CallActivity("FailActivity", task.WithActivityRetryPolicy(&task.RetryPolicy{
				MaxAttempts:          3,
				InitialRetryInterval: 10 * time.Millisecond,
			}))

			t2 := ctx.CallActivity("FailActivity", task.WithActivityRetryPolicy(&task.RetryPolicy{
				MaxAttempts:          3,
				InitialRetryInterval: 10 * time.Millisecond,
			}))

			err := t1.Await(nil)
			if err != nil {
				return nil, err
			}

			err = t2.Await(nil)
			if err != nil {
				return nil, err
			}

			return nil, nil
		}))

		executionMap := make(map[string]int)

		lock := sync.Mutex{}
		require.NoError(t, r.AddActivityN("FailActivity", func(ctx task.ActivityContext) (any, error) {
			lock.Lock()
			defer lock.Unlock()
			executionMap[ctx.GetTaskExecutionID()] = executionMap[ctx.GetTaskExecutionID()] + 1
			if executionMap[ctx.GetTaskExecutionID()] == 3 {
				return nil, nil
			}
			return nil, errors.New("activity failure")
		}))

		// Initialization
		ctx := context.Background()

		client, worker := initTaskHubWorker(ctx, r)
		defer worker.Shutdown(ctx)

		// Run the workflow
		id, err := client.ScheduleNewWorkflow(ctx, "TaskExecutionID")
		require.NoError(t, err)

		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		require.NoError(t, err)

		assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
		// With 3 max attempts there will be two retries with 10 millis delay before each
		require.GreaterOrEqual(t, metadata.LastUpdatedAt.AsTime(), metadata.CreatedAt.AsTime().Add(2*10*time.Millisecond))

		assert.Equal(t, 2, len(executionMap))
		for k, v := range executionMap {
			assert.NotEmpty(t, k)
			assert.Equal(t, 3, v)
		}

	})

	t.Run("SingleActivityWithNoRetry", func(t *testing.T) {
		// Registration
		r := task.NewTaskRegistry()
		require.NoError(t, r.AddWorkflowN("TaskExecutionID", func(ctx *task.WorkflowContext) (any, error) {
			if err := ctx.CallActivity("Activity").Await(nil); err != nil {
				return nil, err
			}
			return nil, nil
		}))

		var executionId string
		require.NoError(t, r.AddActivityN("Activity", func(ctx task.ActivityContext) (any, error) {
			executionId = ctx.GetTaskExecutionID()
			return nil, nil
		}))

		// Initialization
		ctx := context.Background()

		client, worker := initTaskHubWorker(ctx, r)
		defer worker.Shutdown(ctx)

		// Run the workflow
		id, err := client.ScheduleNewWorkflow(ctx, "TaskExecutionID")
		require.NoError(t, err)

		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		require.NoError(t, err)

		assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
		assert.NotEmpty(t, executionId)
		uuid.MustParse(executionId)
	})
}

func Test_ActivityTraceContext(t *testing.T) {
	t.Run("TraceContext is propagated in the activity context", func(t *testing.T) {
		// Registration
		r := task.NewTaskRegistry()
		require.NoError(t, r.AddWorkflowN("TraceContextWorkflow", func(ctx *task.WorkflowContext) (any, error) {
			if err := ctx.CallActivity("ActivityWithContext", task.WithActivityRetryPolicy(&task.RetryPolicy{
				MaxAttempts:          3,
				InitialRetryInterval: 10 * time.Millisecond,
			})).Await(nil); err != nil {
				return nil, err
			}
			return nil, nil
		}))

		traceParentMap := make(map[string]string)
		var executionId string
		require.NoError(t, r.AddActivityN("ActivityWithContext", func(ctx task.ActivityContext) (any, error) {
			executionId = ctx.GetTaskExecutionID()
			tp := ctx.GetTraceContext().GetTraceParent()
			traceParentMap[executionId] = tp

			// Create a new context
			newCtx := context.Background()

			// Create a TextMapCarrier with the traceparent
			carrier := propagation.MapCarrier{}
			carrier.Set("traceparent", tp)

			// Use the TraceContext propagator to extract the trace context
			propagator := propagation.TraceContext{}
			newCtx = propagator.Extract(newCtx, carrier)

			_, childSpan := tracer.Start(context.Background(), "ActivityWith1Context")
			childSpan.End()
			return nil, nil
		}))

		// Initialization
		ctx := context.Background()
		exporter := utils.InitTracing()

		client, worker := initTaskHubWorker(ctx, r)
		defer worker.Shutdown(ctx)

		// Run the workflow
		id, err := client.ScheduleNewWorkflow(ctx, "TraceContextWorkflow")
		require.NoError(t, err)

		metadata, err := client.WaitForWorkflowCompletion(ctx, id)
		require.NoError(t, err)

		assert.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
		assert.NotEmpty(t, executionId)
		assert.NotEmpty(t, traceParentMap[executionId])

		// Validate the exported OTel traces include patch spans
		spans := exporter.GetSpans().Snapshots()
		utils.AssertSpanSequence(t, spans,
			utils.AssertWorkflowCreated("TraceContextWorkflow", id),
			utils.AssertSpan("ActivityWith1Context"),
			utils.AssertActivity("ActivityWithContext", id, 0),
			utils.AssertWorkflowExecuted("TraceContextWorkflow", id, "COMPLETED"),
		)
	})
}

func Test_WorkflowPatching_DefaultToPatched(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	patchesFound := []bool{}
	require.NoError(t, r.AddWorkflowN("Workflow", func(ctx *task.WorkflowContext) (any, error) {
		patchesFound = append(patchesFound, ctx.IsPatched("patch1"))
		return nil, nil
	}))

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "Workflow")
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, []bool{true}, patchesFound)
}

func Test_WorkflowPatching_RunUnpatchedVersion(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	runNumber := atomic.Uint32{}
	patchesFound := []bool{}
	require.NoError(t, r.AddWorkflowN("Workflow", func(ctx *task.WorkflowContext) (any, error) {
		currentRun := runNumber.Add(1)
		// Simulate a version upgrade across runs, first run supports version 1 and 2, following runs support version 1, 2 and 3
		// It's expected to respect first run, and receive version 2 in the second run.
		if currentRun > 1 {
			patchesFound = append(patchesFound, ctx.IsPatched("patch1"))
		}
		ctx.CallActivity("SayHello").Await(nil)

		return nil, nil
	}))

	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		return "Hello", nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "Workflow")
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, uint32(2), runNumber.Load())
	assert.Equal(t, []bool{false}, patchesFound)
}

func Test_WorkflowPatching_MultiplePatches(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	runNumber := atomic.Uint32{}
	patches1Found := []bool{}
	patches2Found := []bool{}
	require.NoError(t, r.AddWorkflowN("Workflow", func(ctx *task.WorkflowContext) (any, error) {
		currentRun := runNumber.Add(1)
		if currentRun > 1 {
			patches1Found = append(patches1Found, ctx.IsPatched("patch1"))
		}
		ctx.CallActivity("SayHello").Await(nil)
		patches2Found = append(patches2Found, ctx.IsPatched("patch2"))
		ctx.CallActivity("SayHello").Await(nil)

		return nil, nil
	}))

	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		return "Hello", nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "Workflow")
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, uint32(3), runNumber.Load())
	assert.Equal(t, []bool{false, false}, patches1Found)
	assert.Equal(t, []bool{true, true}, patches2Found)
}

func Test_WorkflowPatching_ContinueAsNewDoNotCarryOverChoices(t *testing.T) {
	// Registration
	r := task.NewTaskRegistry()
	patchesFound := []bool{}
	ranContinueAsNew := false
	runNumber := atomic.Uint32{}
	require.NoError(t, r.AddWorkflowN("Workflow", func(ctx *task.WorkflowContext) (any, error) {
		currentRun := runNumber.Add(1)
		// The patch is checked from the 2nd rerun, so it should be false for the in-flight run, but true for the continue-as-new run.
		if currentRun > 1 {
			patchesFound = append(patchesFound, ctx.IsPatched("patch1"))
		}
		ctx.CallActivity("SayHello").Await(nil)
		if !ranContinueAsNew {
			ranContinueAsNew = true
			ctx.ContinueAsNew(nil)
		}
		return nil, nil
	}))

	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		return "Hello", nil
	})

	// Initialization
	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	// Run the workflow
	id, err := client.ScheduleNewWorkflow(ctx, "Workflow")
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)
	assert.Equal(t, []bool{false, true, true}, patchesFound)
}

func Test_WorkflowPatching_PatchPersistsAcrossReplays(t *testing.T) {
	// This test verifies that once a patch is enabled, it remains enabled
	// across multiple activity completions (replays).
	r := task.NewTaskRegistry()
	runNumber := atomic.Uint32{}
	patchResults := []bool{}

	require.NoError(t, r.AddWorkflowN("Workflow", func(ctx *task.WorkflowContext) (any, error) {
		runNumber.Add(1)

		// First activity - patch should be enabled (at end of history)
		patchResults = append(patchResults, ctx.IsPatched("patch1"))
		ctx.CallActivity("SayHello").Await(nil)

		// Second activity - patch should still be enabled (from history)
		patchResults = append(patchResults, ctx.IsPatched("patch1"))
		ctx.CallActivity("SayHello").Await(nil)

		// Third activity - patch should still be enabled (from history)
		patchResults = append(patchResults, ctx.IsPatched("patch1"))
		ctx.CallActivity("SayHello").Await(nil)

		return nil, nil
	}))

	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		return "Hello", nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "Workflow")
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

	// Workflow runs 4 times: initial + 3 activity completions
	assert.Equal(t, uint32(4), runNumber.Load())
	// All patch checks should return true (first time enabled, subsequent times from history)
	// Run 1: [true] - first check at end of history
	// Run 2: [true, true] - replay + second check at end of history
	// Run 3: [true, true, true] - replay + third check at end of history
	// Run 4: [true, true, true] - final replay
	// Total: 9 checks, all true
	assert.Len(t, patchResults, 9)
	for i, result := range patchResults {
		assert.True(t, result, "patch check %d should be true", i)
	}
}

func Test_WorkflowPatching_PatchRemembersToStayFalse(t *testing.T) {
	r := task.NewTaskRegistry()
	runNumber := atomic.Uint32{}
	patchResults := []bool{}

	require.NoError(t, r.AddWorkflowN("Workflow", func(ctx *task.WorkflowContext) (any, error) {
		currentRun := runNumber.Add(1)

		// Simulate code upgrade: patch check is only present from run 2 onwards
		// At run 2, we're replaying history (activity 1 already completed),
		// so this patch should return false
		if currentRun >= 2 {
			ctx.IsPatched("patch1")
		}

		ctx.CallActivity("SayHello").Await(nil)

		// Call the patch again. It should return false because it was already seen as false earlier in this turn.
		patchResults = append(patchResults, ctx.IsPatched("patch1"))

		ctx.CallActivity("SayHello").Await(nil)

		return nil, nil
	}))

	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		return "Hello", nil
	})

	ctx := context.Background()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "Workflow")
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

	assert.Equal(t, uint32(3), runNumber.Load())

	assert.Len(t, patchResults, 2)
	assert.False(t, patchResults[0])
	assert.False(t, patchResults[1])
}

func Test_WorkflowPatching_TracingSpans(t *testing.T) {
	// This test verifies that patch checks generate distributed tracing spans.
	r := task.NewTaskRegistry()
	require.NoError(t, r.AddWorkflowN("PatchTracingWorkflow", func(ctx *task.WorkflowContext) (any, error) {
		ctx.IsPatched("patch1")
		if err := ctx.CallActivity("SayHello").Await(nil); err != nil {
			return nil, err
		}
		ctx.IsPatched("patch2")
		if err := ctx.CallActivity("SayHello").Await(nil); err != nil {
			return nil, err
		}
		ctx.IsPatched("patch3")
		return nil, nil
	}))

	r.AddActivityN("SayHello", func(ctx task.ActivityContext) (any, error) {
		return "Hello", nil
	})

	ctx := context.Background()
	exporter := utils.InitTracing()
	client, worker := initTaskHubWorker(ctx, r)
	defer worker.Shutdown(ctx)

	id, err := client.ScheduleNewWorkflow(ctx, "PatchTracingWorkflow")
	require.NoError(t, err)
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	require.NoError(t, err)
	require.Equal(t, protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED, metadata.RuntimeStatus)

	// Validate the exported OTel traces include patch spans
	spans := exporter.GetSpans().Snapshots()
	utils.AssertSpanSequence(t, spans,
		utils.AssertWorkflowCreated("PatchTracingWorkflow", id),
		utils.AssertActivity("SayHello", id, 0),
		utils.AssertActivity("SayHello", id, 1),
		utils.AssertWorkflowExecuted("PatchTracingWorkflow", id, "COMPLETED",
			utils.AssertSpanStringSliceAttribute("applied_patches", []string{"patch1", "patch2", "patch3"}),
		),
	)
}

func initTaskHubWorker(ctx context.Context, r *task.TaskRegistry, opts ...backend.NewTaskWorkerOptions) (backend.TaskHubClient, backend.TaskHubWorker) {
	// TODO: Switch to options pattern
	logger := backend.DefaultLogger()
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	executor := task.NewTaskExecutor(r)
	workflowWorker := backend.NewWorkflowWorker(backend.WorkflowWorkerOptions{
		Backend:  be,
		Executor: executor,
		Logger:   logger,
		AppID:    "testapp",
	}, opts...)
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger, opts...)
	taskHubWorker := backend.NewTaskHubWorker(be, workflowWorker, activityWorker, logger)
	if err := taskHubWorker.Start(ctx); err != nil {
		panic(err)
	}
	taskHubClient := backend.NewTaskHubClient(be)
	return taskHubClient, taskHubWorker
}
