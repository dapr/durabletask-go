package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"time"

	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
	"github.com/dapr/durabletask-go/task"
)

func main() {
	// Create a new task registry and add the workflow and activities
	r := task.NewTaskRegistry()
	r.AddWorkflow(RetryActivityWorkflow)
	r.AddActivity(RandomFailActivity)

	// Init the client
	ctx := context.Background()
	client, worker, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize the client: %v", err)
	}
	defer worker.Shutdown(ctx)

	// Start a new workflow
	id, err := client.ScheduleNewWorkflow(ctx, RetryActivityWorkflow)
	if err != nil {
		log.Fatalf("Failed to schedule new workflow: %v", err)
	}

	// Wait for the workflow to complete
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	if err != nil {
		log.Fatalf("Failed to wait for workflow to complete: %v", err)
	}

	// Print the results
	metadataEnc, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		log.Fatalf("Failed to encode result to JSON: %v", err)
	}
	log.Printf("Workflow completed: %v", string(metadataEnc))
}

// Init creates and initializes an in-memory client and worker pair with default configuration.
func Init(ctx context.Context, r *task.TaskRegistry) (backend.TaskHubClient, backend.TaskHubWorker, error) {
	logger := backend.DefaultLogger()

	// Create an executor
	executor := task.NewTaskExecutor(r)

	// Create a new backend
	// Use the in-memory sqlite provider by specifying ""
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	workflowWorker := backend.NewWorkflowWorker(backend.WorkflowWorkerOptions{
		Backend:  be,
		Executor: executor,
		Logger:   logger,
	})
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	taskHubWorker := backend.NewTaskHubWorker(be, workflowWorker, activityWorker, logger)

	// Start the worker
	err := taskHubWorker.Start(ctx)
	if err != nil {
		return nil, nil, err
	}

	// Get the client to the backend
	taskHubClient := backend.NewTaskHubClient(be)

	return taskHubClient, taskHubWorker, nil
}

func RetryActivityWorkflow(ctx *task.WorkflowContext) (any, error) {
	if err := ctx.CallActivity(RandomFailActivity, task.WithActivityRetryPolicy(&task.RetryPolicy{
		MaxAttempts:          10,
		InitialRetryInterval: 100 * time.Millisecond,
		BackoffCoefficient:   2,
		MaxRetryInterval:     3 * time.Second,
	})).Await(nil); err != nil {
		return nil, err
	}
	return nil, nil
}

func RandomFailActivity(ctx task.ActivityContext) (any, error) {
	// 70% possibility for activity failure
	if rand.Intn(100) <= 70 {
		log.Println("random activity failure")
		return "", errors.New("random activity failure")
	}
	return "ok", nil
}
