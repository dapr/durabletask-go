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

// TestGetChildWorkflowInstances_SkipsCrossNamespace asserts that recursive
// terminate enumeration excludes children scheduled with a router that
// targets another namespace. Their state lives on a different sidecar; the
// local backend can't terminate them and the host (dapr) handles
// cross-namespace terminate dispatch separately.
func TestGetChildWorkflowInstances_SkipsCrossNamespace(t *testing.T) {
	old := []*HistoryEvent{
		childCreatedEvent("local-old", nil),
		childCreatedEvent("xns-old", ptr.Of("other-ns")),
	}
	newEvts := []*HistoryEvent{
		childCreatedEvent("local-new", nil),
		childCreatedEvent("xns-new", ptr.Of("other-ns")),
	}

	got := getChildWorkflowInstances(old, newEvts)
	gotSet := make(map[api.InstanceID]struct{}, len(got))
	for _, id := range got {
		gotSet[id] = struct{}{}
	}

	require.Len(t, got, 2, "only local children should be enumerated")
	assert.Contains(t, gotSet, api.InstanceID("local-old"))
	assert.Contains(t, gotSet, api.InstanceID("local-new"))
	assert.NotContains(t, gotSet, api.InstanceID("xns-old"))
	assert.NotContains(t, gotSet, api.InstanceID("xns-new"))
}

// TestGetChildWorkflowInstances_NoRouterTreatedAsLocal confirms that a
// ChildWorkflowInstanceCreated event with no router (i.e. legacy/same-app
// scheduling) is enumerated as a local child.
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
	assert.Equal(t, api.InstanceID("no-router-child"), got[0])
}

// TestGetChildWorkflowInstances_RouterWithoutNamespaceIsLocal asserts that a
// router carrying a target appID but no target namespace (cross-app
// invocation, same-namespace) is still enumerated as a local child for
// recursive terminate.
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
	assert.Equal(t, api.InstanceID("cross-app-child"), got[0])
}

// TestGetChildWorkflowInstances_Deduplicates sanity-checks that a child
// instance ID appearing in both old and new events isn't double-counted.
func TestGetChildWorkflowInstances_Deduplicates(t *testing.T) {
	dup := childCreatedEvent("dup", nil)
	got := getChildWorkflowInstances([]*HistoryEvent{dup}, []*HistoryEvent{dup})
	require.Len(t, got, 1)
	assert.Equal(t, api.InstanceID("dup"), got[0])
}
