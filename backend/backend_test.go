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

package backend

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/ptr"
)

// childCreatedEvent returns a HistoryEvent for ChildWorkflowInstanceCreated
// with an optional cross-namespace router.
func childCreatedEvent(instanceID string, targetAppNamespace *string) *HistoryEvent {
	router := &protos.TaskRouter{}
	if targetAppNamespace != nil {
		router.TargetAppNamespace = targetAppNamespace
	}
	return &HistoryEvent{
		EventId:   1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{
			ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{
				InstanceId: instanceID,
			},
		},
		Router: router,
	}
}

// TestGetChildWorkflowInstances_PreservesCrossNamespaceRouter asserts that
// cross-namespace children are enumerated and that the router recorded on
// their ChildWorkflowInstanceCreated event is preserved on the returned ref.
// The caller relies on the router to dispatch the recursive terminate to the
// correct sidecar.
func TestGetChildWorkflowInstances_PreservesCrossNamespaceRouter(t *testing.T) {
	old := []*HistoryEvent{
		childCreatedEvent("local-old", nil),
		childCreatedEvent("xns-old", ptr.Of("other-ns")),
	}
	newEvts := []*HistoryEvent{
		childCreatedEvent("local-new", nil),
		childCreatedEvent("xns-new", ptr.Of("other-ns")),
	}

	got := getChildWorkflowInstances(old, newEvts)
	gotMap := make(map[api.InstanceID]*protos.TaskRouter, len(got))
	for _, child := range got {
		gotMap[child.InstanceID] = child.Router
	}

	require.Len(t, got, 4, "all children should be enumerated regardless of router")
	assert.Contains(t, gotMap, api.InstanceID("local-old"))
	assert.Contains(t, gotMap, api.InstanceID("local-new"))
	assert.Contains(t, gotMap, api.InstanceID("xns-old"))
	assert.Contains(t, gotMap, api.InstanceID("xns-new"))

	require.NotNil(t, gotMap[api.InstanceID("xns-old")])
	assert.Equal(t, "other-ns", gotMap[api.InstanceID("xns-old")].GetTargetAppNamespace())
	require.NotNil(t, gotMap[api.InstanceID("xns-new")])
	assert.Equal(t, "other-ns", gotMap[api.InstanceID("xns-new")].GetTargetAppNamespace())
}

// TestGetChildWorkflowInstances_NoRouterTreatedAsLocal confirms that a
// ChildWorkflowInstanceCreated event with no router (i.e. legacy/same-app
// scheduling) is enumerated as a local child with a nil router.
func TestGetChildWorkflowInstances_NoRouterTreatedAsLocal(t *testing.T) {
	e := &HistoryEvent{
		EventId:   1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{
			ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{
				InstanceId: "no-router-child",
			},
		},
		// Router intentionally nil.
	}
	got := getChildWorkflowInstances(nil, []*HistoryEvent{e})
	require.Len(t, got, 1)
	assert.Equal(t, api.InstanceID("no-router-child"), got[0].InstanceID)
	assert.Nil(t, got[0].Router)
}

// TestGetChildWorkflowInstances_RouterWithoutNamespaceIsLocal asserts that a
// router carrying a target appID but no target namespace (cross-app
// invocation, same-namespace) is enumerated and its router preserved.
func TestGetChildWorkflowInstances_RouterWithoutNamespaceIsLocal(t *testing.T) {
	e := &HistoryEvent{
		EventId:   1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{
			ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{
				InstanceId: "cross-app-child",
			},
		},
		Router: &protos.TaskRouter{TargetAppID: ptr.Of("other-app")},
	}
	got := getChildWorkflowInstances(nil, []*HistoryEvent{e})
	require.Len(t, got, 1)
	assert.Equal(t, api.InstanceID("cross-app-child"), got[0].InstanceID)
	require.NotNil(t, got[0].Router)
	assert.Equal(t, "other-app", got[0].Router.GetTargetAppID())
}

// TestGetChildWorkflowInstances_Deduplicates sanity-checks that a child
// instance ID appearing in both old and new events isn't double-counted.
func TestGetChildWorkflowInstances_Deduplicates(t *testing.T) {
	dup := childCreatedEvent("dup", nil)
	got := getChildWorkflowInstances([]*HistoryEvent{dup}, []*HistoryEvent{dup})
	require.Len(t, got, 1)
	assert.Equal(t, api.InstanceID("dup"), got[0].InstanceID)
}

// fakePurgeBackend is a partial Backend used by purgeWorkflowState tests.
// Methods other than GetWorkflowRuntimeState/PurgeWorkflowState panic via the
// nil embedded interface so an accidental new dependency surfaces loudly.
type fakePurgeBackend struct {
	Backend

	// states keyed by instance ID. A nil entry models "purged out-of-band":
	// runtime state with no events, which is how the recursive driver
	// detects [api.ErrInstanceNotFound] for same-app children.
	states map[api.InstanceID]*protos.WorkflowRuntimeState

	// purgeResults keyed by instance ID maps to (count, error). Records what
	// be.PurgeWorkflowState should return for each instance. Used by the
	// recursive driver for both cross-app children (via the router branch)
	// and the root.
	purgeResults map[api.InstanceID]struct {
		count int
		err   error
	}

	// purgeCalls records the order of be.PurgeWorkflowState calls for
	// assertions on dispatch order.
	purgeCalls []api.InstanceID
}

func (f *fakePurgeBackend) GetWorkflowRuntimeState(_ context.Context, wi *WorkflowWorkItem) (*protos.WorkflowRuntimeState, error) {
	s, ok := f.states[wi.InstanceID]
	if !ok || s == nil {
		// Empty state mirrors what real backends return for a purged
		// instance: no events, no completed event.
		return &protos.WorkflowRuntimeState{InstanceId: string(wi.InstanceID)}, nil
	}
	return s, nil
}

func (f *fakePurgeBackend) PurgeWorkflowState(_ context.Context, id api.InstanceID, _ *protos.TaskRouter, _ bool) (int, error) {
	f.purgeCalls = append(f.purgeCalls, id)
	r, ok := f.purgeResults[id]
	if !ok {
		return 1, nil
	}
	return r.count, r.err
}

// completedStateWithChildren builds a runtime state that
// [runtimestate.IsCompleted] returns true for and that exposes the given
// children via [getChildWorkflowInstances].
func completedStateWithChildren(instanceID string, children ...*HistoryEvent) *protos.WorkflowRuntimeState {
	s := &protos.WorkflowRuntimeState{
		InstanceId: instanceID,
		NewEvents:  children,
		CompletedEvent: &protos.ExecutionCompletedEvent{
			WorkflowStatus: protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		},
	}
	return s
}

// TestPurgeWorkflowState_Recursive_SkipsMissingSameAppChild asserts that a
// recursive purge does not abort when a same-app child has already been
// purged out-of-band. The driver must continue and still purge the parent.
func TestPurgeWorkflowState_Recursive_SkipsMissingSameAppChild(t *testing.T) {
	const (
		parentID = api.InstanceID("parent")
		childID  = api.InstanceID("missing-child")
	)

	be := &fakePurgeBackend{
		states: map[api.InstanceID]*protos.WorkflowRuntimeState{
			parentID: completedStateWithChildren(string(parentID), childCreatedEvent(string(childID), nil)),
			// childID intentionally absent: GetWorkflowRuntimeState returns a
			// state with no events so the recursive driver hits the
			// ErrInstanceNotFound branch for the child.
		},
		purgeResults: map[api.InstanceID]struct {
			count int
			err   error
		}{
			parentID: {count: 1, err: nil},
		},
	}

	count, err := purgeWorkflowState(t.Context(), be, parentID, true, false)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "parent must still be purged when child is already gone")
	assert.Equal(t, []api.InstanceID{parentID}, be.purgeCalls,
		"only the parent should reach be.PurgeWorkflowState; the missing child short-circuits before the cross-app/local backend call")
}

// TestPurgeWorkflowState_Recursive_SkipsMissingCrossAppChild asserts that the
// driver swallows api.ErrInstanceNotFound returned from the backend's
// cross-app dispatch and continues to purge the parent. This is the case the
// dapr backend hits when a remote app's recursive purge handler reports the
// child as already purged.
func TestPurgeWorkflowState_Recursive_SkipsMissingCrossAppChild(t *testing.T) {
	const (
		parentID = api.InstanceID("parent")
		childID  = api.InstanceID("xapp-child")
	)

	xappChild := &HistoryEvent{
		EventId:   1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{
			ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{
				InstanceId: string(childID),
			},
		},
		Router: &protos.TaskRouter{TargetAppID: ptr.Of("other-app")},
	}

	be := &fakePurgeBackend{
		states: map[api.InstanceID]*protos.WorkflowRuntimeState{
			parentID: completedStateWithChildren(string(parentID), xappChild),
		},
		purgeResults: map[api.InstanceID]struct {
			count int
			err   error
		}{
			childID:  {count: 0, err: api.ErrInstanceNotFound},
			parentID: {count: 1, err: nil},
		},
	}

	count, err := purgeWorkflowState(t.Context(), be, parentID, true, false)
	require.NoError(t, err)
	assert.Equal(t, 1, count, "deleted count should reflect only the parent; the missing child contributes 0")
	assert.Equal(t, []api.InstanceID{childID, parentID}, be.purgeCalls,
		"driver should attempt the cross-app child purge before purging the parent")
}

// TestPurgeWorkflowState_Recursive_PropagatesNonNotFoundChildError ensures
// the idempotency carve-out only covers ErrInstanceNotFound. Any other error
// from a child purge must abort the recursion so callers learn about real
// failures.
func TestPurgeWorkflowState_Recursive_PropagatesNonNotFoundChildError(t *testing.T) {
	const (
		parentID = api.InstanceID("parent")
		childID  = api.InstanceID("xapp-child")
	)

	xappChild := &HistoryEvent{
		EventId:   1,
		Timestamp: timestamppb.Now(),
		EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{
			ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{
				InstanceId: string(childID),
			},
		},
		Router: &protos.TaskRouter{TargetAppID: ptr.Of("other-app")},
	}

	boom := errors.New("backend exploded")
	be := &fakePurgeBackend{
		states: map[api.InstanceID]*protos.WorkflowRuntimeState{
			parentID: completedStateWithChildren(string(parentID), xappChild),
		},
		purgeResults: map[api.InstanceID]struct {
			count int
			err   error
		}{
			childID: {count: 0, err: boom},
		},
	}

	count, err := purgeWorkflowState(t.Context(), be, parentID, true, false)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom)
	assert.Equal(t, 0, count)
	assert.Equal(t, []api.InstanceID{childID}, be.purgeCalls,
		"driver must abort before attempting to purge the parent on a non-NotFound child error")
}
