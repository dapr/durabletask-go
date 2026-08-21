package backend

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

type fakeCallbackBackend struct {
	Backend
}

func (f *fakeCallbackBackend) OnActivityCompletion(*protos.ActivityRequest, func(*protos.ActivityResponse, error)) func() {
	return func() {}
}

func (f *fakeCallbackBackend) OnWorkflowTaskCompletion(*protos.WorkflowRequest, func(*protos.WorkflowResponse, error)) func() {
	return func() {}
}

// The dispatch must derive the WorkflowRequest's ExecutionId from the
// history's ExecutionStarted event: SDKs seed deterministic TaskExecutionId
// derivation with it, and a nil value makes a recreated instance's
// activities collide with the prior run's.
func Test_executeWorkflowAsync_carriesExecutionID(t *testing.T) {
	fb := &fakeCallbackBackend{}
	g := &grpcExecutor{
		workItemQueue:     make(chan *protos.WorkItem, 1),
		pendingWorkflows:  &sync.Map{},
		pendingActivities: &sync.Map{},
		streams:           &sync.Map{},
		backend:           fb,
		logger:            DefaultLogger(),
	}

	execID := "9e2ab534-9f13-4d9a-a558-a68912ab2a68"
	newEvents := []*protos.HistoryEvent{{
		EventId:   -1,
		Timestamp: timestamppb.New(time.Now()),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name: "MyOrch",
				WorkflowInstance: &protos.WorkflowInstance{
					InstanceId:  "wf1",
					ExecutionId: wrapperspb.String(execID),
				},
			},
		},
	}}

	g.executeWorkflowAsync(context.Background(), api.InstanceID("wf1"), nil, newEvents, ExecuteOptions{}, func(*protos.WorkflowResponse, error) {})

	select {
	case wi := <-g.workItemQueue:
		require.Equal(t, execID, wi.GetWorkflowRequest().GetExecutionId().GetValue())
	default:
		t.Fatal("the work item must have been queued")
	}
}
