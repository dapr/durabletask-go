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

	initStream := func() error {
		_, err := c.client.Hello(ctx, &emptypb.Empty{})
		if err != nil {
			return fmt.Errorf("failed to connect to task hub service: %w", err)
		}

		req := protos.GetWorkItemsRequest{}
		stream, err = c.client.GetWorkItems(ctx, &req)
		if err != nil {
			return fmt.Errorf("failed to get work item stream: %w", err)
		}
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
				go c.processWorkflowWorkItem(ctx, executor, orchReq)
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
	workItem *protos.WorkflowRequest,
) {
	resp := dispatchWorkflow(ctx, executor, workItem)
	if _, err := c.client.CompleteWorkflowTask(ctx, resp); err != nil {
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
	resp := dispatchActivity(ctx, executor, c.logger, req)
	if _, err := c.client.CompleteActivityTask(ctx, resp); err != nil {
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
