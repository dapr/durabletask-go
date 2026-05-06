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
	"google.golang.org/protobuf/proto"

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

// decodeChunkEvents decodes a chunk's rawEvents into typed HistoryEvents
// for assertion.
func decodeChunkEvents(t *testing.T, c *protos.PropagatedHistoryChunk) []*protos.HistoryEvent {
	t.Helper()
	out := make([]*protos.HistoryEvent, len(c.GetRawEvents()))
	for i, raw := range c.GetRawEvents() {
		var e protos.HistoryEvent
		require.NoError(t, proto.Unmarshal(raw, &e))
		out[i] = &e
	}
	return out
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

	ph, err := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY,
		nil,
		"appA",
	)
	require.NoError(t, err)
	require.NotNil(t, ph)
	require.Len(t, ph.GetChunks(), 1)
	chunk := ph.GetChunks()[0]
	assert.Equal(t, "appA", chunk.GetAppId())
	assert.Equal(t, "wf-001", chunk.GetInstanceId())
	assert.Equal(t, "MyWorkflow", chunk.GetWorkflowName())
	assert.Len(t, chunk.GetRawEvents(), 3, "chunk should carry all 3 events as rawEvents")
}

func TestAssembleProtoPropagatedHistory_Lineage_NoAncestor(t *testing.T) {
	state := &protos.WorkflowRuntimeState{
		InstanceId: "wf-001",
		OldEvents: []*protos.HistoryEvent{
			makeExecutionStarted(0, "MyWorkflow"),
		},
	}

	ph, err := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		nil,
		"appA",
	)
	require.NoError(t, err)
	require.NotNil(t, ph)
	require.Len(t, ph.GetChunks(), 1, "lineage with no ancestor should have one own chunk")
	assert.Equal(t, "appA", ph.GetChunks()[0].GetAppId())
}

func TestAssembleProtoPropagatedHistory_Lineage_WithAncestor(t *testing.T) {
	parentEvents := []*protos.HistoryEvent{
		makeExecutionStarted(0, "ParentWf"),
		makeTaskScheduled(1, "parentAct"),
	}
	parentRaw := make([][]byte, len(parentEvents))
	for i, e := range parentEvents {
		b, err := proto.MarshalOptions{Deterministic: true}.Marshal(e)
		require.NoError(t, err)
		parentRaw[i] = b
	}
	receivedHistory := &protos.PropagatedHistory{
		Scope: protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		Chunks: []*protos.PropagatedHistoryChunk{
			{AppId: "appA", InstanceId: "wf-parent", WorkflowName: "ParentWf", RawEvents: parentRaw},
		},
	}

	state := &protos.WorkflowRuntimeState{
		InstanceId: "wf-child",
		OldEvents: []*protos.HistoryEvent{
			makeExecutionStarted(0, "ChildWf"),
		},
		NewEvents: []*protos.HistoryEvent{
			makeTaskScheduled(1, "childAct1"),
			makeTaskScheduled(2, "childAct2"),
		},
	}

	ph, err := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		receivedHistory,
		"appB",
	)
	require.NoError(t, err)
	require.NotNil(t, ph)
	require.Len(t, ph.GetChunks(), 2, "lineage with ancestor should produce ancestor + own chunks")

	// Ancestor chunk passes through verbatim.
	assert.Equal(t, "appA", ph.GetChunks()[0].GetAppId())
	assert.Equal(t, "wf-parent", ph.GetChunks()[0].GetInstanceId())
	assert.Len(t, ph.GetChunks()[0].GetRawEvents(), 2)

	// Own chunk has appB events.
	assert.Equal(t, "appB", ph.GetChunks()[1].GetAppId())
	assert.Equal(t, "wf-child", ph.GetChunks()[1].GetInstanceId())
	assert.Equal(t, "ChildWf", ph.GetChunks()[1].GetWorkflowName())
	ownDecoded := decodeChunkEvents(t, ph.GetChunks()[1])
	require.Len(t, ownDecoded, 3)
	assert.NotNil(t, ownDecoded[0].GetExecutionStarted())
	assert.Equal(t, "childAct1", ownDecoded[1].GetTaskScheduled().GetName())
	assert.Equal(t, "childAct2", ownDecoded[2].GetTaskScheduled().GetName())
}

func TestAssembleProtoPropagatedHistory_OwnHistory_IgnoresAncestor(t *testing.T) {
	receivedHistory := &protos.PropagatedHistory{
		Scope: protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		Chunks: []*protos.PropagatedHistoryChunk{
			{AppId: "appA", RawEvents: [][]byte{[]byte("ancestor-bytes")}},
		},
	}

	state := &protos.WorkflowRuntimeState{
		InstanceId: "wf-001",
		OldEvents: []*protos.HistoryEvent{
			makeExecutionStarted(0, "MyWorkflow"),
		},
	}

	ph, err := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY,
		receivedHistory,
		"appB",
	)
	require.NoError(t, err)
	require.NotNil(t, ph)
	require.Len(t, ph.GetChunks(), 1, "OWN_HISTORY must drop the ancestor chunk")
	assert.Equal(t, "appB", ph.GetChunks()[0].GetAppId())
}

func TestAssembleProtoPropagatedHistory_EmptyState(t *testing.T) {
	state := &protos.WorkflowRuntimeState{}

	ph, err := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY,
		nil,
		"appA",
	)
	require.NoError(t, err)
	require.NotNil(t, ph)
	assert.Empty(t, ph.GetChunks(), "no events => no chunks")
}

// TestCanForwardScope_NoDefaults asserts the CAN-boundary scope chooser only
// forwards what the wf already received from a parent. There is no
// implicit default. A root workflow that schedules propagating tasks but has
// no IncomingHistory does NOT get a chunk seeded for its next generation.
func TestCanForwardScope_NoDefaults(t *testing.T) {
	t.Run("no incoming history returns SCOPE_NONE", func(t *testing.T) {
		assert.Equal(t,
			protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_NONE,
			canForwardScope(nil),
			"a root workflow with no IncomingHistory must not auto-seed a chunk")
	})

	t.Run("incoming history with SCOPE_NONE returns SCOPE_NONE", func(t *testing.T) {
		assert.Equal(t,
			protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_NONE,
			canForwardScope(&protos.PropagatedHistory{
				Scope: protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_NONE,
			}),
			"a NONE-scoped incoming chunk must not be promoted")
	})

	t.Run("incoming history with OWN_HISTORY is inherited", func(t *testing.T) {
		assert.Equal(t,
			protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY,
			canForwardScope(&protos.PropagatedHistory{
				Scope: protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY,
			}),
			"OWN_HISTORY incoming must pass through unchanged")
	})

	t.Run("incoming history with LINEAGE is inherited", func(t *testing.T) {
		assert.Equal(t,
			protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
			canForwardScope(&protos.PropagatedHistory{
				Scope: protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
			}),
			"LINEAGE incoming must pass through unchanged")
	})
}
