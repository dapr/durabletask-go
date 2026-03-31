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
	// Create a new task registry and add the orchestrator and activities
	r := task.NewTaskRegistry()
	r.AddOrchestrator(ExternalEventOrchestrator)

	// Init the client
	ctx := context.Background()
	client, shutdown, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize the client: %v", err)
	}
	defer shutdown()

	// Start a new orchestration
	id, err := client.ScheduleNewOrchestration(ctx, "ExternalEventOrchestrator")
	if err != nil {
		log.Fatalf("Failed to schedule new orchestration: %v", err)
	}
	metadata, err := client.WaitForOrchestrationStart(ctx, id)
	if err != nil {
		log.Fatalf("Failed to wait for orchestration to start: %v", err)
	}

	// Prompt the user for their name and send that to the orchestrator
	go func() {
		fmt.Println("Enter your first name: ")
		var nameInput string
		fmt.Scanln(&nameInput)
		if err = client.RaiseEvent(ctx, id, "Name", api.WithEventPayload(nameInput)); err != nil {
			log.Fatalf("Failed to raise event: %v", err)
		}
	}()

	// After the orchestration receives the event, it should complete on its own
	metadata, err = client.WaitForOrchestrationCompletion(ctx, id)
	if err != nil {
		log.Fatalf("Failed to wait for orchestration to complete: %v", err)
	}
	if metadata.FailureDetails != nil {
		log.Println("orchestration failed:", metadata.FailureDetails.ErrorMessage)
	} else {
		log.Println("orchestration completed:", metadata.Output)
	}
}

// Init creates and initializes an in-memory client and worker pair with default configuration.
func Init(ctx context.Context, r *task.TaskRegistry) (backend.TaskHubClient, context.CancelFunc, error) {
	logger := backend.DefaultLogger()

	// Create an executor
	executor := task.NewTaskExecutor(r)

	// Create a new backend
	// Use the in-memory sqlite provider by specifying ""
	be := sqlite.NewSqliteBackend(sqlite.NewSqliteOptions(""), logger)
	orchestrationWorker := backend.NewOrchestrationWorker(backend.OrchestratorOptions{
		Backend:  be,
		Executor: executor,
		Logger:   logger,
		AppID:    "sample",
	})
	activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
	taskHubWorker := backend.NewTaskHubWorker(be, orchestrationWorker, activityWorker, logger)

	ctx, cancel := context.WithCancel(ctx)
	go taskHubWorker.Start(ctx)

	taskHubClient := backend.NewTaskHubClient(be)

	return taskHubClient, cancel, nil
}

// ExternalEventOrchestrator is an orchestrator function that blocks for 30 seconds or
// until a "Name" event is sent to it.
func ExternalEventOrchestrator(ctx *task.OrchestrationContext) (any, error) {
	var nameInput string
	if err := ctx.WaitForSingleEvent("Name", 30*time.Second).Await(&nameInput); err != nil {
		// Timeout expired
		return nil, err
	}

	return fmt.Sprintf("Hello, %s!", nameInput), nil
}
