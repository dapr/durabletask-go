package executor

import (
	"context"
	"fmt"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/loops"
	"github.com/dapr/kit/events/loop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errShuttingDown error = status.Error(codes.Canceled, "shutting down")

// Logger is the logging interface needed by the executor handler.
type Logger interface {
	Debugf(format string, v ...any)
	Infof(format string, v ...any)
	Warnf(format string, v ...any)
	Errorf(format string, v ...any)
}

// Backend is the subset of the backend interface needed for cancellation.
type Backend interface {
	CancelOrchestratorTask(context.Context, api.InstanceID) error
	CancelActivityTask(context.Context, api.InstanceID, int32) error
}

type pendingOrchestrator struct {
	instanceID api.InstanceID
	streamID   string
}

type pendingActivity struct {
	instanceID api.InstanceID
	taskID     int32
	streamID   string
}

type streamState struct {
	streamID string
	stream   protos.TaskHubSidecarService_GetWorkItemsServer
	errCh    chan<- error
}

type pendingDispatchItem struct {
	workItem       *protos.WorkItem
	dispatched     chan<- error
	orchInstanceID *api.InstanceID
	activityKey    *string
	actInstanceID  *api.InstanceID
	actTaskID      *int32
}

// Options configures the executor loop handler.
type Options struct {
	Backend Backend
	Logger  Logger
}

type executor struct {
	backend Backend
	logger  Logger

	stream          *streamState
	pendingOrch     map[api.InstanceID]*pendingOrchestrator
	pendingAct      map[string]*pendingActivity
	pendingDispatch []pendingDispatchItem

	loop loop.Interface[loops.EventExecutor]
}

// New creates a new executor loop.
func New(opts Options) loop.Interface[loops.EventExecutor] {
	e := &executor{
		backend:     opts.Backend,
		logger:      opts.Logger,
		pendingOrch: make(map[api.InstanceID]*pendingOrchestrator),
		pendingAct:  make(map[string]*pendingActivity),
	}
	e.loop = loop.New[loops.EventExecutor](64).NewLoop(e)
	return e.loop
}

// Handle implements loop.Handler[loops.EventExecutor].
func (e *executor) Handle(ctx context.Context, event loops.EventExecutor) error {
	switch ev := event.(type) {
	case *loops.ExecuteOrchestrator:
		e.handleExecuteOrchestrator(ev)
	case *loops.ExecuteActivity:
		e.handleExecuteActivity(ev)
	case *loops.ConnectStream:
		e.handleConnectStream(ev)
	case *loops.DisconnectStream:
		e.handleDisconnectStream(ev)
	case *loops.StreamShutdown:
		e.handleStreamShutdown()
	case *loops.ShutdownExecutor:
		e.handleShutdown(ctx)
	}
	return nil
}

func (e *executor) handleExecuteOrchestrator(ev *loops.ExecuteOrchestrator) {
	iid := api.InstanceID(ev.InstanceID)
	e.pendingOrch[iid] = &pendingOrchestrator{instanceID: iid}

	if e.stream == nil {
		iidCopy := iid
		e.pendingDispatch = append(e.pendingDispatch, pendingDispatchItem{
			workItem:       ev.WorkItem,
			dispatched:     ev.Dispatched,
			orchInstanceID: &iidCopy,
		})
		return
	}

	e.pendingOrch[iid].streamID = e.stream.streamID

	if err := e.sendWorkItem(ev.WorkItem); err != nil {
		ev.Dispatched <- err
		return
	}
	ev.Dispatched <- nil
}

func (e *executor) handleExecuteActivity(ev *loops.ExecuteActivity) {
	iid := api.InstanceID(ev.InstanceID)
	e.pendingAct[ev.Key] = &pendingActivity{
		instanceID: iid,
		taskID:     ev.TaskID,
	}

	if e.stream == nil {
		key := ev.Key
		iidCopy := iid
		taskID := ev.TaskID
		e.pendingDispatch = append(e.pendingDispatch, pendingDispatchItem{
			workItem:      ev.WorkItem,
			dispatched:    ev.Dispatched,
			activityKey:   &key,
			actInstanceID: &iidCopy,
			actTaskID:     &taskID,
		})
		return
	}

	e.pendingAct[ev.Key].streamID = e.stream.streamID

	if err := e.sendWorkItem(ev.WorkItem); err != nil {
		ev.Dispatched <- err
		return
	}
	ev.Dispatched <- nil
}

func (e *executor) handleConnectStream(ev *loops.ConnectStream) {
	if e.stream != nil {
		e.logger.Warnf("rejecting stream %s: another stream %s is already connected", ev.StreamID, e.stream.streamID)
		ev.ErrCh <- fmt.Errorf("another stream is already connected")
		return
	}

	e.stream = &streamState{
		streamID: ev.StreamID,
		stream:   ev.Stream,
		errCh:    ev.ErrCh,
	}

	// Flush any buffered work items.
	for i, item := range e.pendingDispatch {
		if item.orchInstanceID != nil {
			if p, ok := e.pendingOrch[*item.orchInstanceID]; ok {
				p.streamID = ev.StreamID
			}
		}
		if item.activityKey != nil {
			if p, ok := e.pendingAct[*item.activityKey]; ok {
				p.streamID = ev.StreamID
			}
		}

		if err := e.sendWorkItem(item.workItem); err != nil {
			item.dispatched <- err
			// Stream failed; keep remaining items for next stream.
			e.pendingDispatch = e.pendingDispatch[i+1:]
			e.stream = nil
			return
		}
		item.dispatched <- nil
	}
	e.pendingDispatch = nil
}

func (e *executor) handleDisconnectStream(ev *loops.DisconnectStream) {
	if e.stream == nil || e.stream.streamID != ev.StreamID {
		return
	}

	e.logger.Infof("stream %s disconnected, cleaning up", ev.StreamID)

	for iid, p := range e.pendingOrch {
		if p.streamID == ev.StreamID {
			e.logger.Debugf("cleaning up pending orchestrator: %s", iid)
			if err := e.backend.CancelOrchestratorTask(context.Background(), p.instanceID); err != nil {
				e.logger.Warnf("failed to cancel orchestrator task: %v", err)
			}
			delete(e.pendingOrch, iid)
		}
	}
	for key, p := range e.pendingAct {
		if p.streamID == ev.StreamID {
			e.logger.Debugf("cleaning up pending activity: %s", key)
			if err := e.backend.CancelActivityTask(context.Background(), p.instanceID, p.taskID); err != nil {
				e.logger.Warnf("failed to cancel activity task: %v", err)
			}
			delete(e.pendingAct, key)
		}
	}

	e.stream = nil
}

func (e *executor) handleStreamShutdown() {
	if e.stream != nil {
		e.stream.errCh <- errShuttingDown
		e.stream = nil
	}
}

func (e *executor) handleShutdown(ctx context.Context) {
	if e.stream != nil {
		e.stream.errCh <- errShuttingDown
		e.stream = nil
	}

	for _, item := range e.pendingDispatch {
		item.dispatched <- errShuttingDown
	}
	e.pendingDispatch = nil

	for iid, p := range e.pendingOrch {
		if err := e.backend.CancelOrchestratorTask(ctx, p.instanceID); err != nil {
			e.logger.Warnf("failed to cancel orchestrator task: %v", err)
		}
		delete(e.pendingOrch, iid)
	}
	for key, p := range e.pendingAct {
		if err := e.backend.CancelActivityTask(ctx, p.instanceID, p.taskID); err != nil {
			e.logger.Warnf("failed to cancel activity task: %v", err)
		}
		delete(e.pendingAct, key)
	}
}

func (e *executor) sendWorkItem(wi *protos.WorkItem) error {
	if e.stream == nil {
		return fmt.Errorf("no stream connected")
	}

	stream := e.stream.stream
	return stream.Send(wi)
}
