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

package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func makeTestHistory() *PropagatedHistory {
	return &PropagatedHistory{
		events: []*protos.HistoryEvent{
			// Chunk 0: MerchantCheckout (appA, wf-001), events[0..3]
			{EventId: 0, EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{Name: "MerchantCheckout"},
			}},
			{EventId: 1, EventType: &protos.HistoryEvent_TaskScheduled{
				TaskScheduled: &protos.TaskScheduledEvent{
					Name: "ValidateMerchant", TaskExecutionId: "exec-1",
					Input: wrapperspb.String(`{"merchant":"abc"}`),
				},
			}},
			{EventId: -1, EventType: &protos.HistoryEvent_TaskCompleted{
				TaskCompleted: &protos.TaskCompletedEvent{
					TaskScheduledId: 1,
					TaskExecutionId: "exec-1",
					Result:          wrapperspb.String(`true`),
				},
			}},
			{EventId: 2, EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{
				ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{
					Name: "ProcessPayment", InstanceId: "wf-002",
				},
			}},
			// Chunk 1: ProcessPayment (appB, wf-002), events[4..9]
			{EventId: 0, EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{Name: "ProcessPayment"},
			}},
			{EventId: 1, EventType: &protos.HistoryEvent_TaskScheduled{
				TaskScheduled: &protos.TaskScheduledEvent{
					Name: "ValidateCard", TaskExecutionId: "exec-2",
					Input: wrapperspb.String(`{"card":"4242"}`),
				},
			}},
			{EventId: -1, EventType: &protos.HistoryEvent_TaskCompleted{
				TaskCompleted: &protos.TaskCompletedEvent{
					TaskScheduledId: 1,
					TaskExecutionId: "exec-2",
					Result:          wrapperspb.String(`true`),
				},
			}},
			{EventId: 2, EventType: &protos.HistoryEvent_TaskScheduled{
				TaskScheduled: &protos.TaskScheduledEvent{
					Name: "ValidateCard", TaskExecutionId: "exec-2",
					Input: wrapperspb.String(`{"card":"4242","retry":true}`),
				},
			}},
			{EventId: -1, EventType: &protos.HistoryEvent_TaskFailed{
				TaskFailed: &protos.TaskFailedEvent{
					TaskScheduledId: 2,
					TaskExecutionId: "exec-2",
					FailureDetails:  &protos.TaskFailureDetails{ErrorMessage: "card declined"},
				},
			}},
			{EventId: 3, EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{
				ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{
					Name: "FraudDetection", InstanceId: "wf-003",
				},
			}},
		},
		scope: protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		chunks: []historyChunk{
			{appID: "appA", startEventIndex: 0, eventCount: 4, instanceID: "wf-001", workflowName: "MerchantCheckout"},
			{appID: "appB", startEventIndex: 4, eventCount: 6, instanceID: "wf-002", workflowName: "ProcessPayment"},
		},
	}
}

func TestGetWorkflows(t *testing.T) {
	ph := makeTestHistory()
	wfs := ph.GetWorkflows()

	require.Len(t, wfs, 2)
	assert.Equal(t, "MerchantCheckout", wfs[0].Name)
	assert.Equal(t, "appA", wfs[0].AppID)
	assert.Equal(t, "wf-001", wfs[0].InstanceID)
	assert.True(t, wfs[0].Found)

	assert.Equal(t, "ProcessPayment", wfs[1].Name)
	assert.Equal(t, "appB", wfs[1].AppID)
	assert.Equal(t, "wf-002", wfs[1].InstanceID)
	assert.True(t, wfs[1].Found)
}

func TestGetWorkflowByName(t *testing.T) {
	ph := makeTestHistory()

	wf := ph.GetWorkflowByName("ProcessPayment")
	assert.True(t, wf.Found)
	assert.Equal(t, "appB", wf.AppID)
	assert.Equal(t, "wf-002", wf.InstanceID)

	notFound := ph.GetWorkflowByName("NonExistent")
	assert.False(t, notFound.Found)
}

func TestGetWorkflowsByName(t *testing.T) {
	ph := makeTestHistory()

	wfs := ph.GetWorkflowsByName("ProcessPayment")
	require.Len(t, wfs, 1)
	assert.Equal(t, "wf-002", wfs[0].InstanceID)

	none := ph.GetWorkflowsByName("NonExistent")
	assert.Nil(t, none)
}

func TestGetWorkflowsByName_Duplicates(t *testing.T) {
	// Two instances of the same workflow name
	ph := &PropagatedHistory{
		events: []*protos.HistoryEvent{
			{EventId: 0, EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{Name: "Worker"},
			}},
			{EventId: 0, EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{Name: "Worker"},
			}},
		},
		chunks: []historyChunk{
			{appID: "app1", startEventIndex: 0, eventCount: 1, instanceID: "w-1", workflowName: "Worker"},
			{appID: "app2", startEventIndex: 1, eventCount: 1, instanceID: "w-2", workflowName: "Worker"},
		},
	}

	// Singular returns last (most recent)
	wf := ph.GetWorkflowByName("Worker")
	assert.Equal(t, "w-2", wf.InstanceID)
	assert.Equal(t, "app2", wf.AppID)

	// Plural returns all in order
	wfs := ph.GetWorkflowsByName("Worker")
	require.Len(t, wfs, 2)
	assert.Equal(t, "w-1", wfs[0].InstanceID)
	assert.Equal(t, "w-2", wfs[1].InstanceID)
}

func TestGetActivityByName(t *testing.T) {
	ph := makeTestHistory()
	wf := ph.GetWorkflowByName("MerchantCheckout")

	// Activity that completed
	act := wf.GetActivityByName("ValidateMerchant")
	assert.True(t, act.Started)
	assert.True(t, act.Completed)
	assert.False(t, act.Failed)
	assert.Equal(t, `{"merchant":"abc"}`, act.Input.GetValue())
	assert.Equal(t, `true`, act.Output.GetValue())
	assert.Nil(t, act.Error)

	// Activity not found
	notFound := wf.GetActivityByName("NonExistent")
	assert.False(t, notFound.Started)
	assert.False(t, notFound.Completed)
}

func TestGetActivityByName_ReturnsLast(t *testing.T) {
	ph := makeTestHistory()
	wf := ph.GetWorkflowByName("ProcessPayment")

	// ValidateCard was called twice — singular returns the LAST (failed retry)
	act := wf.GetActivityByName("ValidateCard")
	assert.True(t, act.Started)
	assert.False(t, act.Completed)
	assert.True(t, act.Failed)
	assert.Equal(t, `{"card":"4242","retry":true}`, act.Input.GetValue())
	assert.Equal(t, "card declined", act.Error.GetErrorMessage())
}

func TestGetActivitiesByName_Retries(t *testing.T) {
	ph := makeTestHistory()
	wf := ph.GetWorkflowByName("ProcessPayment")

	// ValidateCard was called twice (retry scenario)
	acts := wf.GetActivitiesByName("ValidateCard")
	require.Len(t, acts, 2, "should find both invocations of ValidateCard")

	// First invocation completed
	assert.True(t, acts[0].Started)
	assert.True(t, acts[0].Completed)
	assert.False(t, acts[0].Failed)
	assert.Equal(t, `{"card":"4242"}`, acts[0].Input.GetValue())
	assert.Equal(t, `true`, acts[0].Output.GetValue())

	// Second invocation failed
	assert.True(t, acts[1].Started)
	assert.False(t, acts[1].Completed)
	assert.True(t, acts[1].Failed)
	assert.Equal(t, `{"card":"4242","retry":true}`, acts[1].Input.GetValue())
	assert.Equal(t, "card declined", acts[1].Error.GetErrorMessage())
}

func TestGetActivitiesByName_NotFound(t *testing.T) {
	ph := makeTestHistory()
	wf := ph.GetWorkflowByName("ProcessPayment")

	acts := wf.GetActivitiesByName("NonExistent")
	assert.Nil(t, acts)
}

func TestGetActivitiesByName_WorkflowNotFound(t *testing.T) {
	ph := makeTestHistory()
	wf := ph.GetWorkflowByName("NonExistent")

	acts := wf.GetActivitiesByName("ValidateCard")
	assert.Nil(t, acts)
}

// TestGetWorkflowByName_EqualsPluralLast verifies the contract that
// GetWorkflowByName(name) returns the same result as
// GetWorkflowsByName(name)[len-1] when multiple matches exist.
func TestGetWorkflowByName_EqualsPluralLast(t *testing.T) {
	ph := &PropagatedHistory{
		events: []*protos.HistoryEvent{
			{EventId: 0, EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{Name: "Worker"},
			}},
			{EventId: 0, EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{Name: "Worker"},
			}},
			{EventId: 0, EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{Name: "Worker"},
			}},
		},
		chunks: []historyChunk{
			{appID: "app1", startEventIndex: 0, eventCount: 1, instanceID: "w-1", workflowName: "Worker"},
			{appID: "app2", startEventIndex: 1, eventCount: 1, instanceID: "w-2", workflowName: "Worker"},
			{appID: "app3", startEventIndex: 2, eventCount: 1, instanceID: "w-3", workflowName: "Worker"},
		},
	}

	singular := ph.GetWorkflowByName("Worker")
	plural := ph.GetWorkflowsByName("Worker")

	require.Len(t, plural, 3)
	last := plural[len(plural)-1]

	// Singular must match the last entry of plural on all observable fields.
	assert.Equal(t, last.Found, singular.Found)
	assert.Equal(t, last.InstanceID, singular.InstanceID)
	assert.Equal(t, last.AppID, singular.AppID)
	assert.Equal(t, last.Name, singular.Name)

	// last differs from first
	assert.NotEqual(t, plural[0].InstanceID, singular.InstanceID,
		"singular must differ from plural[0]")
}

// TestGetActivityByName_EqualsPluralLast verifies the contract that
// GetActivityByName(name) returns the same result as
// GetActivitiesByName(name)[len-1] when multiple matches exist.
func TestGetActivityByName_EqualsPluralLast(t *testing.T) {
	// ValidateCard appears twice in the ProcessPayment chunk: once
	// completed (first), once failed (retry).
	ph := makeTestHistory()
	wf := ph.GetWorkflowByName("ProcessPayment")

	singular := wf.GetActivityByName("ValidateCard")
	plural := wf.GetActivitiesByName("ValidateCard")

	require.Len(t, plural, 2)
	last := plural[len(plural)-1]

	// Singular must match the last entry of plural on all observable fields.
	assert.Equal(t, last.Name, singular.Name)
	assert.Equal(t, last.Started, singular.Started)
	assert.Equal(t, last.Completed, singular.Completed)
	assert.Equal(t, last.Failed, singular.Failed)
	assert.Equal(t, last.Input.GetValue(), singular.Input.GetValue())
	assert.Equal(t, last.Output.GetValue(), singular.Output.GetValue())
	assert.Equal(t, last.Error.GetErrorMessage(), singular.Error.GetErrorMessage())

	// last differs from first (failed retry vs completed first call)
	assert.NotEqual(t, plural[0].Failed, singular.Failed,
		"singular must differ from plural[0]")
}

func TestGetChildWorkflowByName(t *testing.T) {
	ph := makeTestHistory()
	wf := ph.GetWorkflowByName("MerchantCheckout")

	child := wf.GetChildWorkflowByName("ProcessPayment")
	assert.True(t, child.Started)
	assert.Equal(t, "ProcessPayment", child.Name)
	// Not completed in our test data (no ChildWorkflowInstanceCompleted event)
	assert.False(t, child.Completed)

	notFound := wf.GetChildWorkflowByName("NonExistent")
	assert.False(t, notFound.Started)
}

func TestGetChildWorkflowsByName(t *testing.T) {
	ph := makeTestHistory()
	wf := ph.GetWorkflowByName("ProcessPayment")

	children := wf.GetChildWorkflowsByName("FraudDetection")
	require.Len(t, children, 1)
	assert.True(t, children[0].Started)
	assert.Equal(t, "FraudDetection", children[0].Name)

	none := wf.GetChildWorkflowsByName("NonExistent")
	assert.Nil(t, none)
}

func TestGetAppIDs(t *testing.T) {
	ph := makeTestHistory()
	ids := ph.GetAppIDs()
	require.Len(t, ids, 2)
	assert.Equal(t, "appA", ids[0])
	assert.Equal(t, "appB", ids[1])
}

func TestGetAppIDs_Deduplicated(t *testing.T) {
	ph := &PropagatedHistory{
		chunks: []historyChunk{
			{appID: "app1"},
			{appID: "app1"},
			{appID: "app2"},
		},
	}
	ids := ph.GetAppIDs()
	require.Len(t, ids, 2)
	assert.Equal(t, "app1", ids[0])
	assert.Equal(t, "app2", ids[1])
}

func TestGetEventsByAppID(t *testing.T) {
	ph := makeTestHistory()

	appAEvents := ph.GetEventsByAppID("appA")
	assert.Len(t, appAEvents, 4)

	appBEvents := ph.GetEventsByAppID("appB")
	assert.Len(t, appBEvents, 6)

	noneEvents := ph.GetEventsByAppID("nonExistent")
	assert.Nil(t, noneEvents)
}

func TestGetEventsByInstanceID(t *testing.T) {
	ph := makeTestHistory()

	events := ph.GetEventsByInstanceID("wf-002")
	assert.Len(t, events, 6)

	none := ph.GetEventsByInstanceID("nonExistent")
	assert.Nil(t, none)
}

func TestGetEventsByWorkflowName(t *testing.T) {
	ph := makeTestHistory()

	events := ph.GetEventsByWorkflowName("MerchantCheckout")
	assert.Len(t, events, 4)

	none := ph.GetEventsByWorkflowName("nonExistent")
	assert.Nil(t, none)
}

func TestPropagatedHistoryFromProto(t *testing.T) {
	proto := &protos.PropagatedHistory{
		Events: []*protos.HistoryEvent{
			{EventId: 1},
			{EventId: 2},
		},
		Scope: protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		Chunks: []*protos.PropagatedHistoryChunk{
			{AppId: "app1", StartEventIndex: 0, EventCount: 2, InstanceId: "wf-1", WorkflowName: "MyWf"},
		},
	}

	ph := PropagatedHistoryFromProto(proto)
	require.NotNil(t, ph)
	assert.Len(t, ph.Events(), 2)
	assert.Equal(t, protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE, ph.Scope())

	wfs := ph.GetWorkflows()
	require.Len(t, wfs, 1)
	assert.Equal(t, "app1", wfs[0].AppID)
	assert.Equal(t, "wf-1", wfs[0].InstanceID)
	assert.Equal(t, "MyWf", wfs[0].Name)
}

func TestPropagatedHistoryFromProto_Nil(t *testing.T) {
	ph := PropagatedHistoryFromProto(nil)
	assert.Nil(t, ph)
}

func TestNewHistoryPropagationScope_Nil(t *testing.T) {
	assert.NotPanics(t, func() {
		scope := NewHistoryPropagationScope(nil)
		assert.Equal(t, protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_NONE, scope)
	})
}

// TestNewHistoryPropagationScope_Options verifies the option constructors
// produce the expected scope values.
func TestNewHistoryPropagationScope_Options(t *testing.T) {
	assert.Equal(t,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY,
		NewHistoryPropagationScope(PropagateOwnHistory()))
	assert.Equal(t,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		NewHistoryPropagationScope(PropagateLineage()))
}

// TestChunkEvents_Bounds verifies bounds-checking against malformed or
// forged propagated history. chunkEvents must return nil (not panic) when
// a chunk's indices are out of range.
func TestChunkEvents_Bounds(t *testing.T) {
	ph := &PropagatedHistory{
		events: []*protos.HistoryEvent{
			{EventId: 0},
			{EventId: 1},
			{EventId: 2},
		},
	}

	// Happy path: valid range.
	assert.Len(t, ph.chunkEvents(historyChunk{startEventIndex: 0, eventCount: 2}), 2)
	assert.Len(t, ph.chunkEvents(historyChunk{startEventIndex: 1, eventCount: 2}), 2)

	// Negative start — reject.
	assert.Nil(t, ph.chunkEvents(historyChunk{startEventIndex: -1, eventCount: 2}))

	// Start beyond the events slice — reject.
	assert.Nil(t, ph.chunkEvents(historyChunk{startEventIndex: 10, eventCount: 1}))

	// Start == len is borderline: empty slice, not out-of-bounds.
	assert.NotPanics(t, func() {
		_ = ph.chunkEvents(historyChunk{startEventIndex: len(ph.events), eventCount: 0})
	})

	// Negative count — end < start, reject.
	assert.Nil(t, ph.chunkEvents(historyChunk{startEventIndex: 1, eventCount: -5}))

	// Count overflows the slice — clamp to the end, no panic.
	result := ph.chunkEvents(historyChunk{startEventIndex: 1, eventCount: 100})
	assert.Len(t, result, 2, "should clamp to len(events)")
}
