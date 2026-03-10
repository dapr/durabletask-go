package task

import (
	"context"
	"fmt"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/local/loops"
	"github.com/dapr/kit/events/loop"
)

type pendingOrchestrator struct {
	response chan<- *protos.OrchestratorResponse
}

type pendingActivity struct {
	response chan<- *protos.ActivityResponse
}

type task struct {
	pendingOrchestrators map[string]*pendingOrchestrator
	pendingActivities    map[string]*pendingActivity
}

// New creates a new task backend loop.
func New() loop.Interface[loops.EventTask] {
	return loop.New[loops.EventTask](64).NewLoop(&task{
		pendingOrchestrators: make(map[string]*pendingOrchestrator),
		pendingActivities:    make(map[string]*pendingActivity),
	})
}

// Handle implements loop.Handler[loops.EventTask].
func (t *task) Handle(_ context.Context, event loops.EventTask) error {
	switch e := event.(type) {
	case *loops.RegisterPendingOrchestrator:
		t.pendingOrchestrators[e.InstanceID] = &pendingOrchestrator{
			response: e.Response,
		}
	case *loops.CompleteOrchestrator:
		e.ErrCh <- t.completeOrchestrator(e.InstanceID, e.Response)
	case *loops.CancelOrchestrator:
		e.ErrCh <- t.completeOrchestrator(string(e.InstanceID), nil)
	case *loops.RegisterPendingActivity:
		t.pendingActivities[e.Key] = &pendingActivity{
			response: e.Response,
		}
	case *loops.CompleteActivity:
		key := backend.GetActivityExecutionKey(e.InstanceID, e.TaskID)
		e.ErrCh <- t.completeActivity(key, e.Response)
	case *loops.CancelActivity:
		key := backend.GetActivityExecutionKey(string(e.InstanceID), e.TaskID)
		e.ErrCh <- t.completeActivity(key, nil)
	case *loops.Shutdown:
		t.shutdown()
	default:
		return fmt.Errorf("unexpected event type %T", event)
	}

	return nil
}

func (t *task) completeOrchestrator(instanceID string, res *protos.OrchestratorResponse) error {
	pending, ok := t.pendingOrchestrators[instanceID]
	if !ok {
		return api.NewUnknownInstanceIDError(instanceID)
	}
	delete(t.pendingOrchestrators, instanceID)

	// Send response (nil means cancelled) and close the channel.
	pending.response <- res
	close(pending.response)
	return nil
}

func (t *task) completeActivity(key string, res *protos.ActivityResponse) error {
	pending, ok := t.pendingActivities[key]
	if !ok {
		return api.NewUnknownTaskIDError(key, 0)
	}
	delete(t.pendingActivities, key)

	// Send response (nil means cancelled) and close the channel.
	pending.response <- res
	close(pending.response)
	return nil
}

func (t *task) shutdown() {
	for id, pending := range t.pendingOrchestrators {
		close(pending.response)
		delete(t.pendingOrchestrators, id)
	}
	for key, pending := range t.pendingActivities {
		close(pending.response)
		delete(t.pendingActivities, key)
	}
}
