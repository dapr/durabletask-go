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
