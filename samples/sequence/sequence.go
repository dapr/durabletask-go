package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
	"github.com/dapr/durabletask-go/task"
)

func main() {
	// Create a new task registry and add the workflow and activities
	r := task.NewTaskRegistry()
	r.AddWorkflow(ActivitySequenceWorkflow)
	r.AddActivity(SayHelloActivity)

	// Init the client
	ctx := context.Background()
	client, worker, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize the client: %v", err)
	}
	defer worker.Shutdown(ctx)

	// Start a new workflow
	id, err := client.ScheduleNewWorkflow(ctx, ActivitySequenceWorkflow)
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

// ActivitySequenceWorkflow makes three activity calls in sequence and results the results
// as an array.
func ActivitySequenceWorkflow(ctx *task.WorkflowContext) (any, error) {
	var helloTokyo string
	if err := ctx.CallActivity(SayHelloActivity, task.WithActivityInput("Tokyo")).Await(&helloTokyo); err != nil {
		return nil, err
	}
	var helloLondon string
	if err := ctx.CallActivity(SayHelloActivity, task.WithActivityInput("London")).Await(&helloLondon); err != nil {
		return nil, err
	}
	var helloSeattle string
	if err := ctx.CallActivity(SayHelloActivity, task.WithActivityInput("Seattle")).Await(&helloSeattle); err != nil {
		return nil, err
	}
	return []string{helloTokyo, helloLondon, helloSeattle}, nil
}

// SayHelloActivity can be called by an workflow function and will return a friendly greeting.
func SayHelloActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello, %s!", input), nil
}
