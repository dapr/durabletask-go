package backend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/runtimestate"
)

type panickingWorkflowExecutor struct{}

func (panickingWorkflowExecutor) ExecuteWorkflow(context.Context, api.InstanceID, []*protos.HistoryEvent, []*protos.HistoryEvent, ExecuteOptions) (*protos.WorkflowResponse, error) {
	panic("workflow exploded")
}

// An inline executor panic must surface as an explicit error through the
// turn's completion, never crash the delivering goroutine.
func Test_workflowTurn_execute_inlineExecutorPanic(t *testing.T) {
	var doneErr error
	turn := &workflowTurn{
		processor: &workflowProcessor{
			logger:   DefaultLogger(),
			executor: panickingWorkflowExecutor{},
			applier:  runtimestate.NewApplier("testapp", ""),
		},
		wi: &WorkflowWorkItem{
			InstanceID: "wf1",
			State:      runtimestate.NewWorkflowRuntimeState("wf1", nil, nil),
		},
		ctx:  context.Background(),
		span: trace.SpanFromContext(context.Background()),
		done: func(err error) { doneErr = err },
	}

	turn.execute()

	require.Error(t, doneErr)
	assert.Contains(t, doneErr.Error(), "workflow executor panicked")
}
