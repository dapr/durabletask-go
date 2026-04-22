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

package runtimestate

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api/protos"
)

func makeTaskScheduled(id int32, name string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId: id,
		EventType: &protos.HistoryEvent_TaskScheduled{
			TaskScheduled: &protos.TaskScheduledEvent{Name: name},
		},
	}
}

func makeExecutionStarted(id int32, name string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId: id,
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{Name: name},
		},
	}
}

func TestAssembleProtoPropagatedHistory_OwnHistory_SingleApp(t *testing.T) {
	state := &protos.WorkflowRuntimeState{
		InstanceId: "wf-001",
		OldEvents: []*protos.HistoryEvent{
			makeExecutionStarted(0, "MyWorkflow"),
			makeTaskScheduled(1, "act1"),
		},
		NewEvents: []*protos.HistoryEvent{
			makeTaskScheduled(2, "act2"),
		},
	}

	ph := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY,
		nil,    // no ancestor
		"appA", // appID
	)

	require.NotNil(t, ph)
	assert.Len(t, ph.Events, 3, "should have 3 events")
	assert.Equal(t, protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY, ph.Scope)

	// Single chunk for appA with instance ID and workflow name
	require.Len(t, ph.Chunks, 1)
	assert.Equal(t, "appA", ph.Chunks[0].AppId)
	assert.Equal(t, int32(0), ph.Chunks[0].StartEventIndex)
	assert.Equal(t, int32(3), ph.Chunks[0].EventCount)
	assert.Equal(t, "wf-001", ph.Chunks[0].InstanceId)
	assert.Equal(t, "MyWorkflow", ph.Chunks[0].WorkflowName)
}

func TestAssembleProtoPropagatedHistory_Lineage_NoAncestor(t *testing.T) {
	state := &protos.WorkflowRuntimeState{
		OldEvents: []*protos.HistoryEvent{
			makeTaskScheduled(0, "act1"),
		},
	}

	ph := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		nil, // no ancestor
		"appA",
	)

	require.NotNil(t, ph)
	assert.Len(t, ph.Events, 1)

	// Single chunk — no ancestor to prepend
	require.Len(t, ph.Chunks, 1)
	assert.Equal(t, "appA", ph.Chunks[0].AppId)
	assert.Equal(t, int32(0), ph.Chunks[0].StartEventIndex)
	assert.Equal(t, int32(1), ph.Chunks[0].EventCount)
}

func TestAssembleProtoPropagatedHistory_Lineage_WithAncestor(t *testing.T) {
	// Ancestor history from appA with 2 events and 1 chunk
	ancestor := &protos.PropagatedHistory{
		Events: []*protos.HistoryEvent{
			makeTaskScheduled(0, "ancestorAct1"),
			makeTaskScheduled(1, "ancestorAct2"),
		},
		Chunks: []*protos.PropagatedHistoryChunk{
			{AppId: "appA", StartEventIndex: 0, EventCount: 2, InstanceId: "wf-parent", WorkflowName: "ParentWf"},
		},
	}

	// Current app (appB) has 3 events
	state := &protos.WorkflowRuntimeState{
		InstanceId: "wf-child",
		OldEvents: []*protos.HistoryEvent{
			makeExecutionStarted(0, "ChildWf"),
			makeTaskScheduled(1, "ownAct1"),
		},
		NewEvents: []*protos.HistoryEvent{
			makeTaskScheduled(2, "ownAct2"),
		},
	}

	ph := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		ancestor,
		"appB",
	)

	require.NotNil(t, ph)
	assert.Len(t, ph.Events, 5, "should have 5 events: 2 ancestor + 3 own")

	// Verify event order: ancestor events first, then own
	assert.Equal(t, "ancestorAct1", ph.Events[0].GetTaskScheduled().GetName())
	assert.Equal(t, "ancestorAct2", ph.Events[1].GetTaskScheduled().GetName())
	assert.Equal(t, "ChildWf", ph.Events[2].GetExecutionStarted().GetName())
	assert.Equal(t, "ownAct1", ph.Events[3].GetTaskScheduled().GetName())
	assert.Equal(t, "ownAct2", ph.Events[4].GetTaskScheduled().GetName())

	// 2 chunks: appA's ancestor chunk + appB's own chunk
	require.Len(t, ph.Chunks, 2)

	// Ancestor chunk preserves its metadata
	assert.Equal(t, "appA", ph.Chunks[0].AppId)
	assert.Equal(t, int32(0), ph.Chunks[0].StartEventIndex)
	assert.Equal(t, int32(2), ph.Chunks[0].EventCount)
	assert.Equal(t, "wf-parent", ph.Chunks[0].InstanceId)
	assert.Equal(t, "ParentWf", ph.Chunks[0].WorkflowName)

	// Own chunk has appB's metadata
	assert.Equal(t, "appB", ph.Chunks[1].AppId)
	assert.Equal(t, int32(2), ph.Chunks[1].StartEventIndex)
	assert.Equal(t, int32(3), ph.Chunks[1].EventCount)
	assert.Equal(t, "wf-child", ph.Chunks[1].InstanceId)
	assert.Equal(t, "ChildWf", ph.Chunks[1].WorkflowName)
}

func TestAssembleProtoPropagatedHistory_OwnHistory_IgnoresAncestor(t *testing.T) {
	ancestor := &protos.PropagatedHistory{
		Events: []*protos.HistoryEvent{
			makeTaskScheduled(0, "ancestorAct"),
		},
		Chunks: []*protos.PropagatedHistoryChunk{
			{AppId: "appA", StartEventIndex: 0, EventCount: 1},
		},
	}

	state := &protos.WorkflowRuntimeState{
		OldEvents: []*protos.HistoryEvent{
			makeTaskScheduled(0, "ownAct"),
		},
	}

	ph := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY,
		ancestor, // passed but should be ignored
		"appB",
	)

	require.NotNil(t, ph)

	// Ancestor should be excluded
	assert.Len(t, ph.Events, 1, "should have 1 event (own only)")
	assert.Equal(t, "ownAct", ph.Events[0].GetTaskScheduled().GetName())

	// Only appB's chunk
	require.Len(t, ph.Chunks, 1)
	assert.Equal(t, "appB", ph.Chunks[0].AppId)
	assert.Equal(t, int32(0), ph.Chunks[0].StartEventIndex)
	assert.Equal(t, int32(1), ph.Chunks[0].EventCount)
}

func TestAssembleProtoPropagatedHistory_Lineage_ThreeHopChain(t *testing.T) {
	// Simulate A -> B -> C chain where C assembles history

	// Ancestor history already contains A's and B's chunks (built by B)
	ancestor := &protos.PropagatedHistory{
		Events: []*protos.HistoryEvent{
			makeTaskScheduled(0, "actA"),
			makeTaskScheduled(0, "actB"),
		},
		Chunks: []*protos.PropagatedHistoryChunk{
			{AppId: "appA", StartEventIndex: 0, EventCount: 1},
			{AppId: "appB", StartEventIndex: 1, EventCount: 1},
		},
	}

	// Current app (appC) has 1 event
	state := &protos.WorkflowRuntimeState{
		OldEvents: []*protos.HistoryEvent{
			makeTaskScheduled(0, "actC"),
		},
	}

	ph := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		ancestor,
		"appC",
	)

	require.NotNil(t, ph)
	assert.Len(t, ph.Events, 3, "should have 3 events: A + B + C")

	// 3 chunks, one per app
	require.Len(t, ph.Chunks, 3)

	assert.Equal(t, "appA", ph.Chunks[0].AppId)
	assert.Equal(t, int32(0), ph.Chunks[0].StartEventIndex)
	assert.Equal(t, int32(1), ph.Chunks[0].EventCount)

	assert.Equal(t, "appB", ph.Chunks[1].AppId)
	assert.Equal(t, int32(1), ph.Chunks[1].StartEventIndex)
	assert.Equal(t, int32(1), ph.Chunks[1].EventCount)

	assert.Equal(t, "appC", ph.Chunks[2].AppId)
	assert.Equal(t, int32(2), ph.Chunks[2].StartEventIndex)
	assert.Equal(t, int32(1), ph.Chunks[2].EventCount)
}

func TestAssembleProtoPropagatedHistory_EmptyState(t *testing.T) {
	state := &protos.WorkflowRuntimeState{}

	ph := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY,
		nil,
		"appA",
	)

	require.NotNil(t, ph)
	assert.Empty(t, ph.Events, "should have 0 events")
	assert.Empty(t, ph.Chunks, "should have 0 chunks when no events")
}
