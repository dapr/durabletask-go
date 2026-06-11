/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package task

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

func TestScheduleNewWorkflow_EmitsAction(t *testing.T) {
	ctx := newTestContext(t)
	startTime := time.Date(2030, 1, 2, 3, 4, 5, 0, time.UTC)

	id, err := ctx.ScheduleNewWorkflow(dummyWorkflow,
		WithDetachedWorkflowInstanceID("spawned-1"),
		WithDetachedWorkflowInput(map[string]string{"hello": "world"}),
		WithDetachedWorkflowStartTime(startTime),
	)
	require.NoError(t, err)
	assert.Equal(t, api.InstanceID("spawned-1"), id)

	require.Len(t, ctx.pendingActions, 1)
	assert.Empty(t, ctx.pendingTasks, "ScheduleNewWorkflow is fire-and-forget; no Task should be registered")

	for _, a := range ctx.pendingActions {
		dw := a.GetCreateDetachedWorkflow()
		require.NotNil(t, dw)
		assert.Equal(t, "spawned-1", dw.InstanceId)
		assert.Equal(t, "dummyWorkflow", dw.Name)
		assert.Equal(t, `{"hello":"world"}`, dw.GetInput().GetValue())
		assert.Equal(t, startTime, dw.GetScheduledStartTimestamp().AsTime())
		assert.Nil(t, dw.GetExecutionId(), "execution IDs are runtime-minted; the SDK never sets one")
		assert.Empty(t, dw.GetTags(), "tags are not settable from the workflow context")
		assert.Nil(t, a.Router, "no router envelope should be emitted when no app/namespace target is set")
	}
}

func TestScheduleNewWorkflow_RawInput(t *testing.T) {
	ctx := newTestContext(t)
	raw := wrapperspb.String("raw-bytes")

	_, err := ctx.ScheduleNewWorkflow(dummyWorkflow,
		WithDetachedWorkflowInstanceID("spawned-raw"),
		WithRawDetachedWorkflowInput(raw),
	)
	require.NoError(t, err)

	require.Len(t, ctx.pendingActions, 1)
	for _, a := range ctx.pendingActions {
		dw := a.GetCreateDetachedWorkflow()
		require.NotNil(t, dw)
		assert.Equal(t, "raw-bytes", dw.GetInput().GetValue())
	}
}

func TestScheduleNewWorkflow_ExplicitEmptyInstanceID_Errors(t *testing.T) {
	ctx := newTestContext(t)

	id, err := ctx.ScheduleNewWorkflow(dummyWorkflow,
		WithDetachedWorkflowInstanceID(""),
	)
	require.Error(t, err)
	assert.Equal(t, api.EmptyInstanceID, id)
	assert.Contains(t, err.Error(), "empty string")
	assert.Empty(t, ctx.pendingActions, "no action should be scheduled when instanceID is set to an empty string")
	assert.Equal(t, int32(0), ctx.defaultDetachedWorkflowCounter,
		"a rejected call must not advance the default-ID counter")
}

func TestScheduleNewWorkflow_DefaultsInstanceID(t *testing.T) {
	ctx := newTestContext(t)

	// First default-ID spawn → "<caller>-0".
	id0, err := ctx.ScheduleNewWorkflow(dummyWorkflow)
	require.NoError(t, err)
	assert.Equal(t, api.InstanceID("test-id-0"), id0)

	// An intervening action with a different shape (e.g., a timer) must
	// NOT advance the default-ID counter — the counter only tracks
	// default-ID spawns so the suffix is stable across replays even if
	// the user reorders unrelated calls.
	ctx.CreateTimer(time.Second)

	id1, err := ctx.ScheduleNewWorkflow(dummyWorkflow)
	require.NoError(t, err)
	assert.Equal(t, api.InstanceID("test-id-1"), id1)

	// An explicit ID must not be overridden, and must not bump the
	// default counter.
	idExplicit, err := ctx.ScheduleNewWorkflow(dummyWorkflow,
		WithDetachedWorkflowInstanceID("custom-id"),
	)
	require.NoError(t, err)
	assert.Equal(t, api.InstanceID("custom-id"), idExplicit)

	id2, err := ctx.ScheduleNewWorkflow(dummyWorkflow)
	require.NoError(t, err)
	assert.Equal(t, api.InstanceID("test-id-2"), id2,
		"explicit-ID spawns must not advance the default-ID counter")
}

func TestScheduleNewWorkflow_NamespaceWithoutAppIDFails(t *testing.T) {
	ctx := newTestContext(t)

	id, err := ctx.ScheduleNewWorkflow(dummyWorkflow,
		WithDetachedWorkflowInstanceID("spawned-ns"),
		WithDetachedWorkflowAppNamespace("target-ns"),
	)
	require.Error(t, err)
	assert.Equal(t, api.EmptyInstanceID, id)
	assert.Contains(t, err.Error(), "WithDetachedWorkflowAppID")
	assert.Empty(t, ctx.pendingActions)
}

func TestScheduleNewWorkflow_RouterAppIDOnly(t *testing.T) {
	ctx := newTestContext(t)

	_, err := ctx.ScheduleNewWorkflow(dummyWorkflow,
		WithDetachedWorkflowInstanceID("spawned-app"),
		WithDetachedWorkflowAppID("target-app"),
	)
	require.NoError(t, err)

	require.Len(t, ctx.pendingActions, 1)
	for _, a := range ctx.pendingActions {
		require.NotNil(t, a.Router)
		assert.Equal(t, "target-app", a.Router.GetTargetAppID())
		assert.Equal(t, "", a.Router.GetTargetAppNamespace())
	}
}

func TestScheduleNewWorkflow_RouterAppIDAndNamespace(t *testing.T) {
	ctx := newTestContext(t)

	_, err := ctx.ScheduleNewWorkflow(dummyWorkflow,
		WithDetachedWorkflowInstanceID("spawned-cross"),
		WithDetachedWorkflowAppID("target-app"),
		WithDetachedWorkflowAppNamespace("target-ns"),
	)
	require.NoError(t, err)

	require.Len(t, ctx.pendingActions, 1)
	for _, a := range ctx.pendingActions {
		require.NotNil(t, a.Router)
		assert.Equal(t, "target-app", a.Router.GetTargetAppID())
		assert.Equal(t, "target-ns", a.Router.GetTargetAppNamespace())
	}
}

func TestOnDetachedWorkflowCreated_RetiresPendingAction(t *testing.T) {
	ctx := newTestContext(t)

	_, err := ctx.ScheduleNewWorkflow(dummyWorkflow,
		WithDetachedWorkflowInstanceID("spawned-replay"),
	)
	require.NoError(t, err)
	require.Len(t, ctx.pendingActions, 1)

	// Find the emitted action's id so we can simulate the matching history event.
	var actionID int32
	for id := range ctx.pendingActions {
		actionID = id
	}

	err = ctx.processEvent(&protos.HistoryEvent{
		EventId:   actionID,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_DetachedWorkflowInstanceCreated{
			DetachedWorkflowInstanceCreated: &protos.DetachedWorkflowInstanceCreatedEvent{
				InstanceId: "spawned-replay",
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, ctx.pendingActions, "matching event should retire the pending action")
}

func TestOnDetachedWorkflowCreated_InstanceIDMismatch_Errors(t *testing.T) {
	ctx := newTestContext(t)

	// Current execution schedules "spawned-new" at sequence 0...
	_, err := ctx.ScheduleNewWorkflow(dummyWorkflow,
		WithDetachedWorkflowInstanceID("spawned-new"),
	)
	require.NoError(t, err)

	var actionID int32
	for id := range ctx.pendingActions {
		actionID = id
	}

	// ...but history records that the previous execution spawned
	// "spawned-old" at that position. The current execution's code has
	// already received "spawned-new" from ScheduleNewWorkflow, so
	// accepting this history would leave the workflow referencing an
	// instance that was never started.
	err = ctx.processEvent(&protos.HistoryEvent{
		EventId:   actionID,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_DetachedWorkflowInstanceCreated{
			DetachedWorkflowInstanceCreated: &protos.DetachedWorkflowInstanceCreatedEvent{
				InstanceId: "spawned-old",
			},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "spawned-old")
	assert.Contains(t, err.Error(), "spawned-new")
	assert.Len(t, ctx.pendingActions, 1, "mismatched action must not be retired")
}

func TestOnDetachedWorkflowCreated_NondeterministicReplay_Errors(t *testing.T) {
	ctx := newTestContext(t)

	// No pending action staged for this id — simulating a history event
	// produced by a workflow version whose code path no longer schedules a
	// detached workflow at that sequence position.
	err := ctx.processEvent(&protos.HistoryEvent{
		EventId:   42,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_DetachedWorkflowInstanceCreated{
			DetachedWorkflowInstanceCreated: &protos.DetachedWorkflowInstanceCreatedEvent{
				InstanceId: "spawned-mismatch",
			},
		},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ScheduleNewWorkflow")
	assert.Contains(t, err.Error(), "spawned-mismatch")
	assert.Contains(t, err.Error(), "42")
}
