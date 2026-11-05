# Durable Task for Go

[![Build](https://github.com/dapr/durabletask-go/actions/workflows/pr-validation.yml/badge.svg)](https://github.com/dapr/durabletask-go/actions/workflows/pr-validation.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/dapr/durabletask-go.svg)](https://pkg.go.dev/github.com/dapr/durabletask-go)

This repository holds the durable execution engine behind [Dapr Workflow](https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-overview/) and the Go workflow SDK.

- **The engine.** The Dapr sidecar embeds it to run workflows. It stores workflow state through a pluggable backend, replays workflow code after crashes and restarts, and serves the Durable Task gRPC protocol that every Dapr SDK uses to run workflows and activities.
- **The Go workflow SDK.** The [`workflow`](https://pkg.go.dev/github.com/dapr/durabletask-go/workflow) package is the API for writing workflows and activities in Go. The [Dapr Go SDK](https://github.com/dapr/go-sdk) builds on it, so Dapr Go applications import it directly.

The engine started as a Go port of the [.NET Durable Task Framework](https://github.com/Azure/durabletask) and also draws on [Go Workflows](https://github.com/cschleiden/go-workflows). It is designed to run as a sidecar next to the application.

## Writing a workflow in Go

A workflow is an ordinary Go function that calls activities and awaits their results. The engine records each step, so a workflow that is interrupted resumes where it left off.

```go
import (
	"context"
	"fmt"
	"log"

	"github.com/dapr/durabletask-go/workflow"
)

// GreetingWorkflow calls the SayHello activity once per city, in sequence,
// and returns the greetings.
func GreetingWorkflow(ctx *workflow.WorkflowContext) (any, error) {
	var greetings []string
	for _, city := range []string{"Tokyo", "London", "Seattle"} {
		var greeting string
		if err := ctx.CallActivity(SayHello, workflow.WithActivityInput(city)).Await(&greeting); err != nil {
			return nil, err
		}
		greetings = append(greetings, greeting)
	}
	return greetings, nil
}

// SayHello is an activity that returns a greeting for its input.
func SayHello(ctx workflow.ActivityContext) (any, error) {
	var name string
	if err := ctx.GetInput(&name); err != nil {
		return nil, err
	}
	return fmt.Sprintf("Hello, %s!", name), nil
}

func run(ctx context.Context, client *workflow.Client) {
	r := workflow.NewRegistry()
	r.AddWorkflow(GreetingWorkflow)
	r.AddActivity(SayHello)

	// Connect to the engine and start processing workflows and activities.
	if err := client.StartWorker(ctx, r); err != nil {
		log.Fatal(err)
	}

	id, err := client.ScheduleWorkflow(ctx, "GreetingWorkflow")
	if err != nil {
		log.Fatal(err)
	}
	metadata, err := client.WaitForWorkflowCompletion(ctx, id)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(metadata.Output.GetValue()) // ["Hello, Tokyo!","Hello, London!","Hello, Seattle!"]
}
```

In a Dapr application, [`dapr.NewWorkflowClient`](https://pkg.go.dev/github.com/dapr/go-sdk/client#NewWorkflowClient) from the Dapr Go SDK returns a `*workflow.Client` that is connected to the Dapr sidecar. Outside Dapr, [`workflow.NewClient`](https://pkg.go.dev/github.com/dapr/durabletask-go/workflow#NewClient) takes any gRPC connection to a Durable Task engine. The [package example](https://pkg.go.dev/github.com/dapr/durabletask-go/workflow#example-package) runs this workflow end to end against an in-process engine.

The `workflow` package also covers child workflows, durable timers, external events, `Select` for racing tasks, retry policies and `ContinueAsNew`. To learn the concepts, see the [Dapr Workflow documentation](https://docs.dapr.io/developing-applications/building-blocks/workflow/), including [how to author a workflow](https://docs.dapr.io/developing-applications/building-blocks/workflow/howto-author-workflow/) and [workflow patterns](https://docs.dapr.io/developing-applications/building-blocks/workflow/workflow-patterns/).

## Language SDKs

Each Dapr SDK speaks this engine's gRPC protocol, which is defined in [dapr/durabletask-protobuf](https://github.com/dapr/durabletask-protobuf).

| Language | Package | Source |
| - | - | - |
| Go | [`github.com/dapr/go-sdk`](https://pkg.go.dev/github.com/dapr/go-sdk/client), with workflows written using [`github.com/dapr/durabletask-go/workflow`](https://pkg.go.dev/github.com/dapr/durabletask-go/workflow) | [dapr/go-sdk](https://github.com/dapr/go-sdk) |
| Python | [`dapr-ext-workflow`](https://pypi.org/project/dapr-ext-workflow/), built on [`durabletask-dapr`](https://pypi.org/project/durabletask-dapr/) | [dapr/python-sdk](https://github.com/dapr/python-sdk), [dapr/durabletask-python](https://github.com/dapr/durabletask-python) |
| .NET | [`Dapr.Workflow`](https://www.nuget.org/packages/Dapr.Workflow) | [dapr/dotnet-sdk](https://github.com/dapr/dotnet-sdk) |
| Java | [`io.dapr:dapr-sdk-workflows`](https://central.sonatype.com/artifact/io.dapr/dapr-sdk-workflows) | [dapr/java-sdk](https://github.com/dapr/java-sdk) |
| JavaScript | [`@dapr/dapr`](https://www.npmjs.com/package/@dapr/dapr) | [dapr/js-sdk](https://github.com/dapr/js-sdk) |

The [Dapr Workflow documentation](https://docs.dapr.io/developing-applications/building-blocks/workflow/) has quickstarts and examples for each language.

## Embedding the engine

The rest of this README covers running the engine yourself, outside Dapr. This is useful for tests and local experiments, and for building a different host around it.

### Storage providers

The engine persists workflow state through the [`backend.Backend`](https://pkg.go.dev/github.com/dapr/durabletask-go/backend#Backend) interface. This repository includes [sqlite](https://pkg.go.dev/github.com/dapr/durabletask-go/backend/sqlite) and [PostgreSQL](https://pkg.go.dev/github.com/dapr/durabletask-go/backend/postgres) implementations.

```go
// Persists state to a file named test.sqlite3. Use "" for in-memory storage.
options := sqlite.NewSqliteOptions("test.sqlite3")
be := sqlite.NewSqliteBackend(options, backend.DefaultLogger())
```

You can add other storage providers by implementing `backend.Backend`.

### Creating a standalone gRPC sidecar

[`main.go`](https://github.com/dapr/durabletask-go/blob/main/main.go) shows how to build a standalone gRPC sidecar that embeds the engine. It needs a `Backend` for storage and an `Executor` that runs user code, and it hosts both in a `TaskHubWorker`.

The following code creates a task hub worker with a sqlite `Backend` and a gRPC `Executor`:

```go
// Use the default logger or provide your own
logger := backend.DefaultLogger()

// Configure the sqlite backend that will store the runtime state
sqliteOptions := sqlite.NewSqliteOptions(sqliteFilePath)
be := sqlite.NewSqliteBackend(sqliteOptions, logger)

// Create a gRPC server that the language SDKs will connect to
grpcServer := grpc.NewServer()
executor, registerFn := backend.NewGrpcExecutor(be, logger)
registerFn(grpcServer)

// Construct and start the task hub worker, which polls the backend for new work
workflowWorker := backend.NewWorkflowWorker(backend.WorkflowWorkerOptions{
	Backend:  be,
	Executor: executor,
	Logger:   logger,
	AppID:    "example",
})
activityWorker := backend.NewActivityTaskWorker(be, executor, logger)
taskHubWorker := backend.NewTaskHubWorker(be, workflowWorker, activityWorker, logger)
if err := taskHubWorker.Start(context.Background()); err != nil {
	log.Fatalf("failed to start worker: %v", err)
}

// Start listening
lis, err := net.Listen("tcp", "localhost:4001")
if err != nil {
	log.Fatalf("failed to listen: %v", err)
}
fmt.Printf("server listening at %v\n", lis.Addr())
grpcServer.Serve(lis)
```

The gRPC service serves one application at a time, as in any sidecar architecture. To scale out, add replicas that each contain the application and its sidecar, all connected to a shared database.

### Running locally

Run `go run main.go` to start a local Durable Task gRPC server that listens on port 4001:

```bash
go run main.go --port 4001 --db ./test.sqlite3
```

The output looks like this:

```
INFO: 2026/10/28 16:57:49 worker started with backend sqlite::./test.sqlite3
server listening at 127.0.0.1:4001
```

Any of the [language SDKs](#language-sdks) can now connect to port `4001` from a separate process to run workflows.

## Running workflows in-process

You can also run workflows in the same process as the engine, without gRPC, by using the lower-level [`task`](https://pkg.go.dev/github.com/dapr/durabletask-go/task) package with [`task.NewTaskExecutor`](https://pkg.go.dev/github.com/dapr/durabletask-go/task#NewTaskExecutor). The [samples](https://github.com/dapr/durabletask-go/tree/main/samples) directory uses this approach. To run a sample, go to its folder and run `go run .`.

### Activity sequence example

An activity sequence is the simplest and most common workflow pattern.

```go
// ActivitySequenceWorkflow makes three activity calls in sequence and returns the results
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

// SayHelloActivity can be called by a workflow function and will return a friendly greeting.
func SayHelloActivity(ctx task.ActivityContext) (any, error) {
	var input string
	if err := ctx.GetInput(&input); err != nil {
		return "", err
	}
	return fmt.Sprintf("Hello, %s!", input), nil
}
```

The full sample is in [samples/sequence](https://github.com/dapr/durabletask-go/tree/main/samples/sequence).

### Fan-out / fan-in example

In the fan-out / fan-in pattern, a workflow runs several activities in parallel and then waits for all of them. The snippet below leaves out the `GetDevicesToUpdate` and `UpdateDevice` activity definitions.

```go
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
	for _, t := range tasks {
		var succeeded bool
		if err := t.Await(&succeeded); err == nil && succeeded {
			successCount++
		}
	}

	return float32(successCount) / float32(len(devices)), nil
}
```

The full sample is in [samples/parallel](https://github.com/dapr/durabletask-go/tree/main/samples/parallel).

### External events example

Sometimes a workflow needs input from an external system. For example, an approval workflow may wait for a person to approve a request, or a workflow may pause until an operator sends a command. `WaitForSingleEvent` pauses the workflow until a named event arrives or a timeout expires. Pass `-1` as the timeout to wait indefinitely.

```go
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
```

To send an event to a workflow, call the task hub client's `RaiseEvent` method. The workflow state buffers the event durably. The workflow consumes it when it calls `WaitForSingleEvent` with a matching event name.

```go
id, _ := client.ScheduleNewWorkflow(ctx, "ExternalEventWorkflow")

// Prompt the user for their name and send that to the workflow
go func() {
	fmt.Println("Enter your first name: ")
	var nameInput string
	fmt.Scanln(&nameInput)

	client.RaiseEvent(ctx, id, "Name", api.WithEventPayload(nameInput))
}()
```

The full sample is in [samples/externalevents](https://github.com/dapr/durabletask-go/tree/main/samples/externalevents).

### Managing in-process workflows

Register workflow and activity functions with a `TaskRegistry`. The `TaskHubClient` starts, queries, terminates, suspends, resumes and waits for workflows.

The following snippet registers `ActivitySequenceWorkflow`, starts an instance and waits for it to complete. It leaves out how the client and worker are created. Each sample contains an `Init` function that does this.

```go
r := task.NewTaskRegistry()
r.AddWorkflow(ActivitySequenceWorkflow)
r.AddActivity(SayHelloActivity)

ctx := context.Background()
client, worker, err := Init(ctx, r)
if err != nil {
	panic(err)
}
defer worker.Shutdown(ctx)

id, err := client.ScheduleNewWorkflow(ctx, ActivitySequenceWorkflow)
if err != nil {
	panic(err)
}

metadata, err := client.WaitForWorkflowCompletion(ctx, id)
if err != nil {
	panic(err)
}

fmt.Printf("workflow completed: %v\n", metadata)
```

## Distributed tracing

The engine publishes distributed traces to any configured [OpenTelemetry](https://opentelemetry.io/) exporter. Register a global `TracerProvider` at application startup with [`otel.SetTracerProvider(tp)`](https://pkg.go.dev/go.opentelemetry.io/otel#SetTracerProvider), and the task hub worker uses it to emit OTLP trace spans.

The following example sends traces to [Zipkin](https://zipkin.io/) running locally:

```go
func ConfigureZipkinTracing() (*trace.TracerProvider, error) {
	// Inspired by this sample: https://github.com/open-telemetry/opentelemetry-go/blob/main/example/zipkin/main.go
	exp, err := zipkin.New("http://localhost:9411/api/v2/spans")
	if err != nil {
		return nil, err
	}

	// NOTE: The simple span processor is not recommended for production.
	//       Instead, the batch span processor should be used for production.
	processor := trace.NewSimpleSpanProcessor(exp)
	// processor := trace.NewBatchSpanProcessor(exp)

	tp := trace.NewTracerProvider(
		trace.WithSpanProcessor(processor),
		trace.WithSampler(trace.AlwaysSample()),
		trace.WithResource(resource.NewWithAttributes(
			"durabletask.io",
			attribute.KeyValue{Key: "service.name", Value: attribute.StringValue("sample-app")},
		)),
	)
	otel.SetTracerProvider(tp)
	return tp, nil
}
```

This code is in the [distributedtracing](https://github.com/dapr/durabletask-go/tree/main/samples/distributedtracing) sample. The screenshot below shows the trace for the sample's workflow. The workflow calls an activity, creates a 2-second durable timer, and then calls another activity that makes an HTTP request:

![Trace of the distributed tracing sample in Zipkin](https://user-images.githubusercontent.com/2704139/205171291-8d12d6fe-5d4f-40c7-9a48-2586a4c4af49.png)

Each workflow appears as a single span, with its activities, timers and child workflows as child spans. Span attributes include the workflow instance ID, task name and task ID.

## Contributing

Contributions are welcome. [CONTRIBUTING.md](https://github.com/dapr/durabletask-go/blob/main/CONTRIBUTING.md) explains how to clone the repository with its submodules, generate the protobuf code, and run the tests.

As in other Dapr projects, contributions to this repository use the [Developer Certificate of Origin](https://developercertificate.org/) (DCO) instead of a Contributor License Agreement. Sign off each commit to certify that you wrote it or otherwise have the right to submit it. Use `git commit -s` to add a `Signed-off-by` line with your name and email:

```
Signed-off-by: Random J Developer <random@developer.example.org>
```

A DCO check runs on every pull request. A pull request cannot be merged until all of its commits are signed off.

## Code of Conduct

Please refer to the [Dapr Community Code of Conduct](https://github.com/dapr/community/blob/master/CODE-OF-CONDUCT.md).

## License

This project is licensed under the [Apache License 2.0](https://github.com/dapr/durabletask-go/blob/main/LICENSE).
