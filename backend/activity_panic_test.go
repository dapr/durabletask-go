package backend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

type panickingActivityExecutor struct{}

func (panickingActivityExecutor) ExecuteActivity(context.Context, api.InstanceID, *protos.HistoryEvent, ExecuteOptions) (*protos.HistoryEvent, error) {
	panic("activity exploded")
}

// An executor panic in the inline branch must surface as an explicit error
// through done, never crash the worker goroutine or complete the item.
func Test_activityProcessor_inlineExecutorPanic(t *testing.T) {
	p := &activityProcessor{executor: panickingActivityExecutor{}}

	awi := &ActivityWorkItem{
		InstanceID: "wf1",
		NewEvent: &protos.HistoryEvent{
			EventId:   0,
			Timestamp: timestamppb.New(time.Now()),
			EventType: &protos.HistoryEvent_TaskScheduled{
				TaskScheduled: &protos.TaskScheduledEvent{Name: "Boom"},
			},
		},
	}

	var doneErr error
	p.ProcessWorkItemAsync(context.Background(), awi, func(err error) { doneErr = err })

	require.Error(t, doneErr)
	assert.Contains(t, doneErr.Error(), "activity executor panicked")
	assert.Nil(t, awi.Result, "a panicked execution must not produce a result")
}
