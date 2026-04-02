package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
	"github.com/dapr/durabletask-go/task"
)

func main() {
	// Create a new task registry and add the workflow and activities
	r := task.NewTaskRegistry()
	r.AddWorkflow(ExternalEventWorkflow)

	// Init the client
	ctx := context.Background()
	client, worker, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize the client: %v", err)
	}
	defer worker.Shutdown(ctx)

	// Start a new workflow
	id, err := client.ScheduleNewWorkflow(ctx, "ExternalEventWorkflow")
	if err != nil {
		log.Fatalf("Failed to schedule new workflow: %v", err)
	}
	metadata, err := client.WaitForWorkflowStart(ctx, id)
	if err != nil {
		log.Fatalf("Failed to wait for workflow to start: %v", err)
	}

	// Prompt the user for their name and send that to the workflow
	go func() {
		fmt.Println("Enter your first name: ")
		var nameInput string
		fmt.Scanln(&nameInput)
		if err = client.RaiseEvent(ctx, id, "Name", api.WithEventPayload(nameInput)); err != nil {
			log.Fatalf("Failed to raise event: %v", err)
		}
	}()

	// After the workflow receives the event, it should complete on its own
	metadata, err = client.WaitForWorkflowCompletion(ctx, id)
	if err != nil {
		log.Fatalf("Failed to wait for workflow to complete: %v", err)
	}
	if metadata.FailureDetails != nil {
		log.Println("workflow failed:", metadata.FailureDetails.ErrorMessage)
	} else {
		log.Println("workflow completed:", metadata.Output)
	}
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

// ExternalEventWorkflow is a workflow function that blocks for 30 seconds or
// until a "Name" event is sent to it.
func ExternalEventWorkflow(ctx *task.WorkflowContext) (any, error) {
	var nameInput string
	if err := ctx.WaitForSingleEvent("Name", 30*time.Second).Await(&nameInput); err != nil {
		// Timeout expired
		return nil, err
	}

	return fmt.Sprintf("Hello, %s!", nameInput), nil
}
