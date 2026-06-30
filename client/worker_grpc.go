package client

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cenkalti/backoff/v4"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/helpers"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/task"
)

type workItemsStream interface {
	Recv() (*protos.WorkItem, error)
}

const keepaliveInterval = 30 * time.Second
const keepaliveTimeout = 5 * time.Second

func (c *TaskHubGrpcClient) startKeepaliveLoop(ctx context.Context) context.CancelFunc {
	ctx, cancel := context.WithCancel(ctx)
	go func() {
		ticker := time.NewTicker(keepaliveInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				callCtx, callCancel := context.WithTimeout(ctx, keepaliveTimeout)
				_, err := c.client.Hello(callCtx, &emptypb.Empty{})
				callCancel()
				if err != nil && ctx.Err() == nil {
					c.logger.Debugf("keepalive failed: %v", err)
				}
			}
		}
	}()
	return cancel
}

func (c *TaskHubGrpcClient) StartWorkItemListener(ctx context.Context, r *task.TaskRegistry) error {
	executor := task.NewTaskExecutor(r)

	var stream workItemsStream
	// streamCancel cancels the context of the current GetWorkItems stream. It lets
	// a work item that cannot reconstruct its history tear down its own stream to
	// force a reconnect (and the sidecar's prompt CancelWorkflowTask) instead of
	// waiting for the backend's work-item lease to expire.
	var streamCancel context.CancelFunc

	// historyCache retains each instance's committed history between turns so the
	// sidecar can send only the delta (see WORKER_CAPABILITY_STATEFUL_HISTORY).
	// Its lifetime is the stream: on reconnect the sidecar drops the old stream's
	// warm state, so we start cold to stay in sync (a stale entry would be safely
	// overwritten by the next full send anyway). A janitor reclaims idle entries
	// for the lifetime of this listener.
	historyCache := newWorkflowHistoryCache(c.historyCacheConfig)
	if !c.statefulHistoryDisabled {
		go historyCache.runJanitor(ctx)
	}

	initStream := func() error {
		_, err := c.client.Hello(ctx, &emptypb.Empty{})
		if err != nil {
			return fmt.Errorf("failed to connect to task hub service: %w", err)
		}

		req := protos.GetWorkItemsRequest{}
		if !c.statefulHistoryDisabled {
			req.Capabilities = []protos.WorkerCapability{
				protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY,
			}
		}

		// Give this stream its own cancelable context (a child of the listener's
		// ctx) so an individual work item can tear it down without stopping the
		// listener. Release any prior stream's context first.
		if streamCancel != nil {
			streamCancel()
		}
		streamCtx, cancel := context.WithCancel(ctx)
		streamCancel = cancel

		stream, err = c.client.GetWorkItems(streamCtx, &req)
		if err != nil {
			cancel()
			return fmt.Errorf("failed to get work item stream: %w", err)
		}
		historyCache.reset()
		return nil
	}

	c.logger.Infof("connecting work item listener stream")
	err := initStream()
	if err != nil {
		return err
	}

	go func() {
		c.logger.Info("starting background processor")
		cancelKeepalive := c.startKeepaliveLoop(ctx)
		defer func() {
			cancelKeepalive()
			c.logger.Info("stopping background processor")
			// We must use a background context here as the stream's context is likely canceled
			shutdownErr := executor.Shutdown(context.Background())
			if shutdownErr != nil {
				c.logger.Warnf("error while shutting down background processor: %v", shutdownErr)
			}
		}()
		for {
			workItem, err := stream.Recv()

			if err != nil {
				// user wants to stop the listener
				if ctx.Err() != nil {
					c.logger.Infof("stopping background processor: %v", err)
					return
				}

				retriable := false

				c.logger.Errorf("background processor received stream error: %v", err)

				if errors.Is(err, io.EOF) {
					retriable = true
				} else if grpcStatus, ok := status.FromError(err); ok {
					c.logger.Warnf("received grpc error code %v", grpcStatus.Code().String())
					switch grpcStatus.Code() {
					case codes.Unavailable, codes.Canceled:
						retriable = true
					default:
						retriable = true
					}
				}

				if !retriable {
					c.logger.Infof("stopping background processor, non retriable error: %v", err)
					return
				}

				err = backoff.Retry(
					func() error {
						// user wants to stop the listener
						if ctx.Err() != nil {
							return backoff.Permanent(ctx.Err())
						}

						c.logger.Infof("reconnecting work item listener stream")
						streamErr := initStream()
						if streamErr != nil {
							c.logger.Errorf("error initializing work item listener stream %v", streamErr)
							return streamErr
						}
						return nil
					},
					// retry forever since we don't have a way of asynchronously return errors to the user
					newInfiniteRetries(),
				)
				if err != nil {
					c.logger.Infof("stopping background processor, unable to reconnect stream: %v", err)
					return
				}
				c.logger.Infof("successfully reconnected work item listener stream...")
				cancelKeepalive()
				cancelKeepalive = c.startKeepaliveLoop(ctx)
				// continue iterating
				continue
			}

			if orchReq := workItem.GetWorkflowRequest(); orchReq != nil {
				// Capture this stream's cancel so the handler tears down the
				// stream it arrived on, not a later reconnected one.
				teardownStream := streamCancel
				go c.processWorkflowWorkItem(ctx, executor, historyCache, orchReq, teardownStream)
			} else if actReq := workItem.GetActivityRequest(); actReq != nil {
				go c.processActivityWorkItem(ctx, executor, actReq)
			} else {
				c.logger.Warnf("received unknown work item type: %v", workItem)
			}
		}
	}()
	return nil
}

func (c *TaskHubGrpcClient) processWorkflowWorkItem(
	ctx context.Context,
	executor backend.Executor,
	historyCache *workflowHistoryCache,
	workItem *protos.WorkflowRequest,
	teardownStream context.CancelFunc,
) {
	iid := api.InstanceID(workItem.InstanceId)

	// Reconstruct the full committed history. When the sidecar sent a delta
	// (cachedHistory), prepend the prefix we cached from the previous turn; on a
	// cache miss, recover the full history via the GetInstanceHistory RPC.
	pastEvents, err := c.resolveWorkflowHistory(ctx, historyCache, workItem)
	if err != nil {
		// The cache-miss fallback fetch failed. There is no per-item NACK in the
		// protocol, so tear down this stream to force a reconnect: the sidecar's
		// disconnect handler cancels this still-pending task (CancelWorkflowTask)
		// and the backend redelivers it promptly, rather than waiting for the
		// work item's lease to expire. The retried turn is a full-history send on
		// the fresh (cold) stream, which needs no cache. This also redelivers the
		// stream's other in-flight tasks, which is acceptable for this rare path
		// (the fetch almost only fails when the connection is already gone).
		c.logger.Errorf("%s: failed to resolve workflow history, resetting stream: %v", iid, err)
		if teardownStream != nil {
			teardownStream()
		}
		return
	}

	opts := backend.ExecuteOptions{
		PropagatedHistory: workItem.GetPropagatedHistory(),
	}
	results, err := executor.ExecuteWorkflow(ctx, iid, pastEvents, workItem.NewEvents, opts)

	// Refresh the cache so the next turn can be served as a delta. The cached
	// prefix is exactly the committed history we just replayed (never the
	// not-yet-committed NewEvents). Drop it once the instance completes or
	// continues-as-new, since its history no longer extends this prefix. Skipped
	// entirely when the stateful-history optimization is disabled.
	if err == nil && !c.statefulHistoryDisabled {
		if workflowHistoryReset(results) {
			historyCache.delete(iid)
		} else {
			historyCache.put(iid, pastEvents)
		}
	}

	resp := protos.WorkflowResponse{InstanceId: workItem.InstanceId}
	if err != nil {
		// NOTE: At the time of writing, there's no known case where this error is returned.
		//       We add error handling here anyways, just in case.
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

	if _, err = c.client.CompleteWorkflowTask(ctx, &resp); err != nil {
		if ctx.Err() != nil {
			c.logger.Warn("failed to complete workflow task: context canceled")
		} else {
			c.logger.Errorf("failed to complete workflow task: %v", err)
		}
	}
}

func (c *TaskHubGrpcClient) processActivityWorkItem(
	ctx context.Context,
	executor backend.Executor,
	req *protos.ActivityRequest,
) {
	opts := backend.ExecuteOptions{
		PropagatedHistory: req.GetPropagatedHistory(),
	}
	var ptc *protos.TraceContext = req.ParentTraceContext
	ctx, err := helpers.ContextFromTraceContext(ctx, ptc)
	if err != nil {
		c.logger.Warnf("%s: failed to parse trace context: %v", req.Name, err)
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
	result, err := executor.ExecuteActivity(ctx, api.InstanceID(req.WorkflowInstance.InstanceId), event, opts)

	resp := protos.ActivityResponse{InstanceId: req.WorkflowInstance.InstanceId, TaskId: req.TaskId}
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

	if _, err = c.client.CompleteActivityTask(ctx, &resp); err != nil {
		if ctx.Err() != nil {
			c.logger.Warn("failed to complete activity task: context canceled")
		} else {
			c.logger.Errorf("failed to complete activity task: %v", err)
		}
	}
}

func newInfiniteRetries() *backoff.ExponentialBackOff {
	b := backoff.NewExponentialBackOff()
	// max wait of 15 seconds between retries
	b.MaxInterval = 15 * time.Second
	// retry forever
	b.MaxElapsedTime = 0
	b.Reset()
	return b
}
