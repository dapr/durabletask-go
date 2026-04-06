package main

import (
	"context"
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/google/uuid"

	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/sqlite"
	"github.com/dapr/durabletask-go/task"
)

func main() {
	// Create a new task registry and add the workflow and activities
	r := task.NewTaskRegistry()
	r.AddWorkflow(UpdateDevicesWorkflow)
	r.AddActivity(GetDevicesToUpdate)
	r.AddActivity(UpdateDevice)

	// Init the client
	ctx := context.Background()
	client, worker, err := Init(ctx, r)
	if err != nil {
		log.Fatalf("Failed to initialize the client: %v", err)
	}
	defer worker.Shutdown(ctx)

	// Start a new workflow
	id, err := client.ScheduleNewWorkflow(ctx, UpdateDevicesWorkflow)
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

// UpdateDevicesWorkflow is a workflow that runs activities in parallel
func UpdateDevicesWorkflow(ctx *task.WorkflowContext) (any, error) {
	// Get a dynamic list of devices to perform updates on
	var devices []string
	if err := ctx.CallActivity(GetDevicesToUpdate).Await(&devices); err != nil {
		return nil, err
	}

	// Start a dynamic number of tasks in parallel, not waiting for any to complete (yet)
	tasks := make([]task.Task, len(devices))
	for i, id := range devices {
		tasks[i] = ctx.CallActivity(UpdateDevice, task.WithActivityInput(id))
	}

	// Now that all are started, wait for them to complete and then return the success rate
	successCount := 0
	for _, task := range tasks {
		var succeeded bool
		if err := task.Await(&succeeded); err == nil && succeeded {
			successCount++
		}
	}

	return float32(successCount) / float32(len(devices)), nil
}

// GetDevicesToUpdate is an activity that returns a list of random device IDs to a workflow.
func GetDevicesToUpdate(task.ActivityContext) (any, error) {
	// Return a fake list of device IDs
	const deviceCount = 10
	deviceIDs := make([]string, deviceCount)
	for i := 0; i < deviceCount; i++ {
		deviceIDs[i] = uuid.NewString()
	}
	return deviceIDs, nil
}

// UpdateDevice is an activity that takes a device ID (string) and pretends to perform an update
// on the corresponding device, with a random 67% success rate.
func UpdateDevice(ctx task.ActivityContext) (any, error) {
	var deviceID string
	if err := ctx.GetInput(&deviceID); err != nil {
		return nil, err
	}
	log.Printf("updating device: %s", deviceID)

	// Delay and success results are randomly generated
	delay := time.Duration(rand.Int31n(500)) * time.Millisecond
	select {
	case <-ctx.Context().Done():
		return nil, ctx.Context().Err()
	case <-time.After(delay):
		// All good, continue
	}

	// Simulate random failures
	success := rand.Intn(3) > 0

	return success, nil
}
