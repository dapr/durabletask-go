package backend

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/loops"
	loopexecutor "github.com/dapr/durabletask-go/backend/loops/executor"
	"github.com/dapr/kit/concurrency"
	"github.com/dapr/kit/events/loop"
)

var emptyCompleteTaskResponse = &protos.CompleteTaskResponse{}

type Executor interface {
	Start(context.Context) error
	ExecuteOrchestrator(ctx context.Context, iid api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*protos.OrchestratorResponse, error)
	ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent) (*protos.HistoryEvent, error)
	Shutdown(ctx context.Context) error
}

type grpcExecutor struct {
	protos.UnimplementedTaskHubSidecarServiceServer

	executorLoop             loop.Interface[loops.EventExecutor]
	backend                  Backend
	logger                   Logger
	onWorkItemConnection     func(context.Context) error
	onWorkItemDisconnect     func(context.Context) error
	streamShutdownChan       <-chan any
	streamSendTimeout        *time.Duration
	skipWaitForInstanceStart bool
}

type grpcExecutorOptions func(g *grpcExecutor)

// IsDurableTaskGrpcRequest returns true if the specified gRPC method name represents an operation
// that is compatible with the gRPC executor.
func IsDurableTaskGrpcRequest(fullMethodName string) bool {
	return strings.HasPrefix(fullMethodName, "/TaskHubSidecarService/")
}

// WithOnGetWorkItemsConnectionCallback allows the caller to get a notification when an external process
// connects over gRPC and invokes the GetWorkItems operation. This can be useful for doing things like
// lazily auto-starting the task hub worker only when necessary.
func WithOnGetWorkItemsConnectionCallback(callback func(context.Context) error) grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.onWorkItemConnection = callback
	}
}

// WithOnGetWorkItemsDisconnectCallback allows the caller to get a notification when an external process
// disconnects from the GetWorkItems operation. This can be useful for doing things like shutting down
// the task hub worker when the client disconnects.
func WithOnGetWorkItemsDisconnectCallback(callback func(context.Context) error) grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.onWorkItemDisconnect = callback
	}
}

func WithStreamShutdownChannel(c <-chan any) grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.streamShutdownChan = c
	}
}

func WithSkipWaitForInstanceStart() grpcExecutorOptions {
	return func(g *grpcExecutor) {
		g.skipWaitForInstanceStart = true
	}
}

// NewGrpcExecutor returns the Executor object and a method to invoke to register the gRPC server in the executor.
func NewGrpcExecutor(be Backend, logger Logger, opts ...grpcExecutorOptions) (executor Executor, registerServerFn func(grpcServer grpc.ServiceRegistrar)) {
	grpcExec := &grpcExecutor{
		backend: be,
		logger:  logger,
	}

	for _, opt := range opts {
		opt(grpcExec)
	}

	grpcExec.executorLoop = loopexecutor.New(loopexecutor.Options{
		Backend: be,
		Logger:  logger,
	})

	return grpcExec, func(grpcServer grpc.ServiceRegistrar) {
		protos.RegisterTaskHubSidecarServiceServer(grpcServer, grpcExec)
	}
}

// Start starts the executor loop and blocks until the context is cancelled.
func (g *grpcExecutor) Start(ctx context.Context) error {
	manager := concurrency.NewRunnerManager(g.executorLoop.Run)
	if g.streamShutdownChan != nil {
		manager.Add(func(ctx context.Context) error {
			select {
			case <-g.streamShutdownChan:
				g.executorLoop.Enqueue(new(loops.StreamShutdown))
			case <-ctx.Done():
			}
			return nil
		})
	}
	// When context is cancelled, close the executor loop so Run unblocks.
	manager.Add(func(ctx context.Context) error {
		<-ctx.Done()
		g.executorLoop.Close(new(loops.ShutdownExecutor))
		return nil
	})
	return manager.Run(ctx)
}

// ExecuteOrchestrator implements Executor
func (g *grpcExecutor) ExecuteOrchestrator(ctx context.Context, iid api.InstanceID, oldEvents []*protos.HistoryEvent, newEvents []*protos.HistoryEvent) (*protos.OrchestratorResponse, error) {
	req := &protos.OrchestratorRequest{
		InstanceId:  string(iid),
		ExecutionId: nil,
		PastEvents:  oldEvents,
		NewEvents:   newEvents,
	}

	workItem := &protos.WorkItem{
		Request: &protos.WorkItem_OrchestratorRequest{
			OrchestratorRequest: req,
		},
	}

	wait := g.backend.WaitForOrchestratorCompletion(req)

	dispatched := make(chan error, 1)
	g.executorLoop.Enqueue(&loops.ExecuteOrchestrator{
		InstanceID: string(iid),
		WorkItem:   workItem,
		Dispatched: dispatched,
	})

	// Block until dispatched to stream (preserves back-pressure).
	select {
	case <-ctx.Done():
		g.logger.Warnf("%s: context canceled before dispatching orchestrator work item", iid)
		return nil, fmt.Errorf("context canceled before dispatching orchestrator work item: %w", ctx.Err())
	case err := <-dispatched:
		if err != nil {
			return nil, fmt.Errorf("failed to dispatch orchestrator work item: %w", err)
		}
	}

	resp, err := wait(ctx)
	if err != nil {
		if errors.Is(err, api.ErrTaskCancelled) {
			return nil, errors.New("operation aborted")
		}
		g.logger.Warnf("%s: failed before receiving orchestration result", iid)
		return nil, err
	}

	return resp, nil
}

// ExecuteActivity implements Executor
func (g *grpcExecutor) ExecuteActivity(ctx context.Context, iid api.InstanceID, e *protos.HistoryEvent) (*protos.HistoryEvent, error) {
	key := GetActivityExecutionKey(string(iid), e.EventId)
	task := e.GetTaskScheduled()

	req := &protos.ActivityRequest{
		Name:                  task.Name,
		Version:               task.Version,
		Input:                 task.Input,
		OrchestrationInstance: &protos.OrchestrationInstance{InstanceId: string(iid)},
		TaskId:                e.EventId,
		TaskExecutionId:       task.TaskExecutionId,
		ParentTraceContext:    task.ParentTraceContext,
	}
	workItem := &protos.WorkItem{
		Request: &protos.WorkItem_ActivityRequest{
			ActivityRequest: req,
		},
	}

	wait := g.backend.WaitForActivityCompletion(req)

	dispatched := make(chan error, 1)
	g.executorLoop.Enqueue(&loops.ExecuteActivity{
		Key:        key,
		InstanceID: string(iid),
		TaskID:     e.EventId,
		WorkItem:   workItem,
		Dispatched: dispatched,
	})

	// Block until dispatched to stream (preserves back-pressure).
	select {
	case <-ctx.Done():
		g.logger.Warnf("%s/%s#%d: context canceled before dispatching activity work item", iid, task.Name, e.EventId)
		return nil, fmt.Errorf("context canceled before dispatching activity work item: %w", ctx.Err())
	case err := <-dispatched:
		if err != nil {
			return nil, fmt.Errorf("failed to dispatch activity work item: %w", err)
		}
	}

	resp, err := wait(ctx)
	if err != nil {
		if errors.Is(err, api.ErrTaskCancelled) {
			return nil, errors.New("operation aborted")
		}
		g.logger.Warnf("%s/%s#%d: failed before receiving activity result", iid, task.Name, e.EventId)
		return nil, err
	}

	var responseEvent *protos.HistoryEvent
	if failureDetails := resp.GetFailureDetails(); failureDetails != nil {
		responseEvent = &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.Now(),
			EventType: &protos.HistoryEvent_TaskFailed{
				TaskFailed: &protos.TaskFailedEvent{
					TaskScheduledId: resp.TaskId,
					TaskExecutionId: task.TaskExecutionId,
					FailureDetails:  failureDetails,
				},
			},
			Router: e.Router,
		}
	} else {
		responseEvent = &protos.HistoryEvent{
			EventId:   -1,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_TaskCompleted{
				TaskCompleted: &protos.TaskCompletedEvent{
					TaskScheduledId: resp.TaskId,
					Result:          resp.Result,
					TaskExecutionId: task.TaskExecutionId,
				},
			},
			Router: e.Router,
		}
	}

	return responseEvent, nil
}

// Shutdown implements Executor
func (g *grpcExecutor) Shutdown(ctx context.Context) error {
	g.executorLoop.Close(new(loops.ShutdownExecutor))
	return nil
}

// Hello implements protos.TaskHubSidecarServiceServer
func (grpcExecutor) Hello(ctx context.Context, empty *emptypb.Empty) (*emptypb.Empty, error) {
	return empty, nil
}

// GetWorkItems implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) GetWorkItems(req *protos.GetWorkItemsRequest, stream protos.TaskHubSidecarService_GetWorkItemsServer) error {
	if md, ok := metadata.FromIncomingContext(stream.Context()); ok {
		g.logger.Infof("work item stream established by user-agent: %v", md.Get("user-agent"))
	}

	streamID := uuid.NewString()

	// There are some cases where the app may need to be notified when a client connects to fetch work items, like
	// for auto-starting the worker. The app also has an opportunity to set itself as unavailable by returning an error.
	if err := g.executeOnWorkItemConnection(stream.Context()); err != nil {
		message := "unable to establish work item stream at this time: " + err.Error()
		g.logger.Warn(message)

		if derr := g.executeOnWorkItemDisconnect(stream.Context()); derr != nil {
			g.logger.Warnf("error while disconnecting work item stream: %v", derr)
		}

		return status.Errorf(codes.Unavailable, "%s", message)
	}

	defer func() {
		g.executorLoop.Enqueue(&loops.DisconnectStream{StreamID: streamID})
		if err := g.executeOnWorkItemDisconnect(stream.Context()); err != nil {
			g.logger.Warnf("error while disconnecting work item stream: %v", err)
		}
	}()

	errCh := make(chan error, 1)
	g.executorLoop.Enqueue(&loops.ConnectStream{
		StreamID: streamID,
		Stream:   stream,
		ErrCh:    errCh,
	})

	// Wait for either the stream context to be done or the loop to signal an error.
	select {
	case <-stream.Context().Done():
		g.logger.Info("work item stream closed")
		return nil
	case err := <-errCh:
		return err
	}
}

func (g *grpcExecutor) executeOnWorkItemConnection(ctx context.Context) error {
	if callback := g.onWorkItemConnection; callback != nil {
		if err := callback(ctx); err != nil {
			return err
		}
	}
	return nil
}

func (g *grpcExecutor) executeOnWorkItemDisconnect(ctx context.Context) error {
	if callback := g.onWorkItemDisconnect; callback != nil {
		if err := callback(ctx); err != nil {
			return err
		}
	}
	return nil
}

// CompleteOrchestratorTask implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) CompleteOrchestratorTask(ctx context.Context, res *protos.OrchestratorResponse) (*protos.CompleteTaskResponse, error) {
	return emptyCompleteTaskResponse, g.backend.CompleteOrchestratorTask(ctx, res)
}

// CompleteActivityTask implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) CompleteActivityTask(ctx context.Context, res *protos.ActivityResponse) (*protos.CompleteTaskResponse, error) {
	return emptyCompleteTaskResponse, g.backend.CompleteActivityTask(ctx, res)
}

func GetActivityExecutionKey(iid string, taskID int32) string {
	return iid + "/" + strconv.FormatInt(int64(taskID), 10)
}

// GetInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) GetInstance(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	metadata, err := g.backend.GetOrchestrationMetadata(ctx, api.InstanceID(req.InstanceId))
	if err != nil {
		if errors.Is(err, api.ErrInstanceNotFound) {
			return &protos.GetInstanceResponse{Exists: false}, nil
		}
		return nil, err
	}

	if metadata == nil {
		return &protos.GetInstanceResponse{Exists: false}, nil
	}

	return createGetInstanceResponse(req, metadata), nil
}

// PurgeInstances implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) PurgeInstances(ctx context.Context, req *protos.PurgeInstancesRequest) (*protos.PurgeInstancesResponse, error) {
	if req.GetPurgeInstanceFilter() != nil {
		return nil, errors.New("multi-instance purge is not unimplemented")
	}
	count, err := purgeOrchestrationState(ctx, g.backend, api.InstanceID(req.GetInstanceId()), req.Recursive, req.GetForce())
	resp := &protos.PurgeInstancesResponse{DeletedInstanceCount: int32(count)}
	if err != nil {
		return resp, fmt.Errorf("failed to purge orchestration state: %w", err)
	}

	return resp, nil
}

// RaiseEvent implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) RaiseEvent(ctx context.Context, req *protos.RaiseEventRequest) (*protos.RaiseEventResponse, error) {
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_EventRaised{
			EventRaised: &protos.EventRaisedEvent{Name: req.Name, Input: req.Input},
		},
	}
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	return &protos.RaiseEventResponse{}, nil
}

// StartInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) StartInstance(ctx context.Context, req *protos.CreateInstanceRequest) (*protos.CreateInstanceResponse, error) {
	if req.ParentTraceContext != nil {
		var err error
		ctx, err = helpers.ContextFromTraceContext(ctx, req.ParentTraceContext)
		if err != nil {
			return nil, err
		}
	}

	instanceID := req.InstanceId
	ctx, span := helpers.StartNewCreateOrchestrationSpan(ctx, req.Name, req.Version.GetValue(), instanceID)
	defer span.End()

	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name:  req.Name,
				Input: req.Input,
				OrchestrationInstance: &protos.OrchestrationInstance{
					InstanceId:  instanceID,
					ExecutionId: wrapperspb.String(uuid.New().String()),
				},
				ParentTraceContext:      helpers.TraceContextFromSpan(span),
				ScheduledStartTimestamp: req.ScheduledStartTimestamp,
			},
		},
	}
	if err := g.backend.CreateOrchestrationInstance(ctx, e, WithOrchestrationIdReusePolicy(req.OrchestrationIdReusePolicy)); err != nil {
		return nil, fmt.Errorf("failed to create orchestration instance: %w", err)
	}

	if req.ScheduledStartTimestamp == nil && !g.skipWaitForInstanceStart {
		_, err := g.WaitForInstanceStart(ctx, &protos.GetInstanceRequest{InstanceId: instanceID})
		if err != nil {
			return nil, err
		}
	}

	return &protos.CreateInstanceResponse{InstanceId: instanceID}, nil
}

// RerunWorkflowFromEvent reruns a workflow from a specific event ID of some
// source instance ID. If not given, a random new instance ID will be
// generated and returned. Can optionally give a new input to the target
// event ID to rerun from.
func (g *grpcExecutor) RerunWorkflowFromEvent(ctx context.Context, req *protos.RerunWorkflowFromEventRequest) (*protos.RerunWorkflowFromEventResponse, error) {
	newInstanceID, err := g.backend.RerunWorkflowFromEvent(ctx, req)
	if err != nil {
		return nil, err
	}

	_, err = g.WaitForInstanceStart(ctx, &protos.GetInstanceRequest{InstanceId: newInstanceID.String()})
	if err != nil {
		return nil, err
	}

	return &protos.RerunWorkflowFromEventResponse{NewInstanceID: newInstanceID.String()}, nil
}

func (g *grpcExecutor) ListInstanceIDs(ctx context.Context, req *protos.ListInstanceIDsRequest) (*protos.ListInstanceIDsResponse, error) {
	return g.backend.ListInstanceIDs(ctx, req)
}

func (g *grpcExecutor) GetInstanceHistory(ctx context.Context, req *protos.GetInstanceHistoryRequest) (*protos.GetInstanceHistoryResponse, error) {
	return g.backend.GetInstanceHistory(ctx, req)
}

// TerminateInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) TerminateInstance(ctx context.Context, req *protos.TerminateRequest) (*protos.TerminateResponse, error) {
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ExecutionTerminated{
			ExecutionTerminated: &protos.ExecutionTerminatedEvent{
				Input:   req.Output,
				Recurse: req.Recursive,
			},
		},
	}
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, fmt.Errorf("failed to submit termination request: %w", err)
	}

	_, err := g.WaitForInstanceCompletion(ctx, &protos.GetInstanceRequest{InstanceId: req.InstanceId})

	return &protos.TerminateResponse{}, err
}

// SuspendInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) SuspendInstance(ctx context.Context, req *protos.SuspendRequest) (*protos.SuspendResponse, error) {
	var input *wrapperspb.StringValue
	if req.Reason.GetValue() != "" {
		input = wrapperspb.String(req.Reason.GetValue())
	}
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionSuspended{
			ExecutionSuspended: &protos.ExecutionSuspendedEvent{
				Input: input,
			},
		},
	}
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	_, err := g.waitForInstance(ctx, &protos.GetInstanceRequest{
		InstanceId: req.InstanceId,
	}, func(metadata *OrchestrationMetadata) bool {
		return metadata.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_SUSPENDED ||
			api.OrchestrationMetadataIsComplete(metadata)
	})

	return &protos.SuspendResponse{}, err
}

// ResumeInstance implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) ResumeInstance(ctx context.Context, req *protos.ResumeRequest) (*protos.ResumeResponse, error) {
	var input *wrapperspb.StringValue
	if req.Reason.GetValue() != "" {
		input = wrapperspb.String(req.Reason.GetValue())
	}
	e := &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionResumed{
			ExecutionResumed: &protos.ExecutionResumedEvent{
				Input: input,
			},
		},
	}
	if err := g.backend.AddNewOrchestrationEvent(ctx, api.InstanceID(req.InstanceId), e); err != nil {
		return nil, err
	}

	_, err := g.waitForInstance(ctx, &protos.GetInstanceRequest{
		InstanceId: req.InstanceId,
	}, func(metadata *OrchestrationMetadata) bool {
		return metadata.RuntimeStatus == protos.OrchestrationStatus_ORCHESTRATION_STATUS_RUNNING ||
			api.OrchestrationMetadataIsComplete(metadata)
	})

	return &protos.ResumeResponse{}, err
}

// WaitForInstanceCompletion implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) WaitForInstanceCompletion(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return g.waitForInstance(ctx, req, api.OrchestrationMetadataIsComplete)
}

// WaitForInstanceStart implements protos.TaskHubSidecarServiceServer
func (g *grpcExecutor) WaitForInstanceStart(ctx context.Context, req *protos.GetInstanceRequest) (*protos.GetInstanceResponse, error) {
	return g.waitForInstance(ctx, req, func(m *OrchestrationMetadata) bool {
		return m.RuntimeStatus != protos.OrchestrationStatus_ORCHESTRATION_STATUS_PENDING
	})
}

func (g *grpcExecutor) waitForInstance(ctx context.Context, req *protos.GetInstanceRequest, condition func(*OrchestrationMetadata) bool) (*protos.GetInstanceResponse, error) {
	iid := api.InstanceID(req.InstanceId)

	var metadata *protos.OrchestrationMetadata
	err := g.backend.WatchOrchestrationRuntimeStatus(ctx, iid, func(m *OrchestrationMetadata) bool {
		metadata = m
		return condition(m)
	})
	if err != nil {
		return nil, err
	}

	if metadata == nil {
		return &protos.GetInstanceResponse{Exists: false}, nil
	}

	return createGetInstanceResponse(req, metadata), nil
}

func createGetInstanceResponse(req *protos.GetInstanceRequest, metadata *OrchestrationMetadata) *protos.GetInstanceResponse {
	state := &protos.OrchestrationState{
		InstanceId:           req.InstanceId,
		Name:                 metadata.Name,
		OrchestrationStatus:  metadata.RuntimeStatus,
		CreatedTimestamp:     metadata.CreatedAt,
		LastUpdatedTimestamp: metadata.LastUpdatedAt,
	}

	if req.GetInputsAndOutputs {
		state.Input = metadata.Input
		state.CustomStatus = metadata.CustomStatus
		state.Output = metadata.Output
		state.FailureDetails = metadata.FailureDetails
	}

	return &protos.GetInstanceResponse{Exists: true, OrchestrationState: state}
}
