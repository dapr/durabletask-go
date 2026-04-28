package task

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api/protos"
)

func newTestContext(t *testing.T) *WorkflowContext {
	t.Helper()
	ctx := NewWorkflowContext(NewTaskRegistry(), "test-id", nil, nil)
	ctx.pendingActions = make(map[int32]*protos.WorkflowAction)
	ctx.pendingTasks = make(map[int32]*completableTask)
	return ctx
}

func dummyActivity(ActivityContext) (any, error) { return nil, nil }
func dummyWorkflow(*WorkflowContext) (any, error) { return nil, nil }

func TestCallActivity_RouterAppIDOnly(t *testing.T) {
	ctx := newTestContext(t)
	ctx.CallActivity(dummyActivity, WithActivityAppID("target-app"))

	require.Len(t, ctx.pendingActions, 1)
	for _, a := range ctx.pendingActions {
		require.NotNil(t, a.Router)
		assert.Equal(t, "target-app", a.Router.GetTargetAppID())
		assert.Equal(t, "", a.Router.GetTargetAppNamespace())
	}
}

func TestCallActivity_RouterAppIDAndNamespace(t *testing.T) {
	ctx := newTestContext(t)
	ctx.CallActivity(dummyActivity,
		WithActivityAppID("target-app"),
		WithActivityAppNamespace("target-ns"),
	)

	require.Len(t, ctx.pendingActions, 1)
	for _, a := range ctx.pendingActions {
		require.NotNil(t, a.Router)
		assert.Equal(t, "target-app", a.Router.GetTargetAppID())
		assert.Equal(t, "target-ns", a.Router.GetTargetAppNamespace())
	}
}

// Namespace without appID is invalid: setting it must produce a failed task
// and no scheduled action — namespace-only routing has no defined target.
func TestCallActivity_NamespaceWithoutAppIDFails(t *testing.T) {
	ctx := newTestContext(t)
	task := ctx.CallActivity(dummyActivity, WithActivityAppNamespace("target-ns"))

	assert.Empty(t, ctx.pendingActions, "no action should be scheduled when options are invalid")
	ct, ok := task.(*completableTask)
	require.True(t, ok)
	require.True(t, ct.isCompleted)
	require.NotNil(t, ct.failureDetails)
	assert.Equal(t, "InvalidActivityOptions", ct.failureDetails.GetErrorType())
}

func TestCallActivity_NoRouterByDefault(t *testing.T) {
	ctx := newTestContext(t)
	ctx.CallActivity(dummyActivity)

	require.Len(t, ctx.pendingActions, 1)
	for _, a := range ctx.pendingActions {
		assert.Nil(t, a.Router, "no router envelope should be emitted when no app/namespace target is set")
	}
}

func TestCallChildWorkflow_RouterAppIDOnly(t *testing.T) {
	ctx := newTestContext(t)
	ctx.CallChildWorkflow(dummyWorkflow, WithChildWorkflowAppID("target-app"))

	require.Len(t, ctx.pendingActions, 1)
	for _, a := range ctx.pendingActions {
		require.NotNil(t, a.Router)
		assert.Equal(t, "target-app", a.Router.GetTargetAppID())
		assert.Equal(t, "", a.Router.GetTargetAppNamespace())
	}
}

func TestCallChildWorkflow_RouterAppIDAndNamespace(t *testing.T) {
	ctx := newTestContext(t)
	ctx.CallChildWorkflow(dummyWorkflow,
		WithChildWorkflowAppID("target-app"),
		WithChildWorkflowAppNamespace("target-ns"),
	)

	require.Len(t, ctx.pendingActions, 1)
	for _, a := range ctx.pendingActions {
		require.NotNil(t, a.Router)
		assert.Equal(t, "target-app", a.Router.GetTargetAppID())
		assert.Equal(t, "target-ns", a.Router.GetTargetAppNamespace())
	}
}

func TestCallChildWorkflow_NamespaceWithoutAppIDFails(t *testing.T) {
	ctx := newTestContext(t)
	task := ctx.CallChildWorkflow(dummyWorkflow, WithChildWorkflowAppNamespace("target-ns"))

	assert.Empty(t, ctx.pendingActions, "no action should be scheduled when options are invalid")
	ct, ok := task.(*completableTask)
	require.True(t, ok)
	require.True(t, ct.isCompleted)
	require.NotNil(t, ct.failureDetails)
	assert.Equal(t, "InvalidChildWorkflowOptions", ct.failureDetails.GetErrorType())
}

func TestCallChildWorkflow_NoRouterByDefault(t *testing.T) {
	ctx := newTestContext(t)
	ctx.CallChildWorkflow(dummyWorkflow)

	require.Len(t, ctx.pendingActions, 1)
	for _, a := range ctx.pendingActions {
		assert.Nil(t, a.Router)
	}
}
