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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/spiffe/go-spiffe/v2/spiffeid"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/historysigning"
	"github.com/dapr/kit/crypto/spiffe/signer"
	"github.com/dapr/kit/crypto/spiffe/trustanchors/fake"
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

	t.Run("incoming history with START_EVENT_ONLY is inherited", func(t *testing.T) {
		assert.Equal(t,
			protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_START_EVENT_ONLY,
			canForwardScope(&protos.PropagatedHistory{
				Scope: protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_START_EVENT_ONLY,
			}),
			"START_EVENT_ONLY incoming must pass through unchanged")
	})
}

// makeTaggedExecutionStarted returns a run's start event carrying input and
// tags, stamped at the given time.
func makeTaggedExecutionStarted(name string, tags map[string]string, at time.Time) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   -1,
		Timestamp: timestamppb.New(at),
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name:  name,
				Input: wrapperspb.String(`{"q":"hello"}`),
				WorkflowInstance: &protos.WorkflowInstance{
					InstanceId:  "wf-001",
					ExecutionId: wrapperspb.String("exec-1"),
				},
				Tags: tags,
			},
		},
	}
}

// TestAssembleProtoPropagatedHistory_StartEventOnly asserts START_EVENT_ONLY
// sends one chunk holding the run's ExecutionStarted event and nothing else,
// whether that event is already saved (OldEvents) or arrived in this turn
// (NewEvents), and drops any ancestor chunk.
func TestAssembleProtoPropagatedHistory_StartEventOnly(t *testing.T) {
	tags := map[string]string{"owner": "user-1", "team": "a"}
	start := makeTaggedExecutionStarted("MyWorkflow", tags, time.Now())
	workflowStarted := &protos.HistoryEvent{
		EventId:   -1,
		EventType: &protos.HistoryEvent_WorkflowStarted{WorkflowStarted: &protos.WorkflowStartedEvent{}},
	}
	receivedHistory := &protos.PropagatedHistory{
		Scope: protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		Chunks: []*protos.PropagatedHistoryChunk{
			{AppId: "appA", RawEvents: [][]byte{[]byte("ancestor-bytes")}},
		},
	}

	tests := []struct {
		name  string
		state *protos.WorkflowRuntimeState
	}{
		{
			name: "start event already saved",
			state: &protos.WorkflowRuntimeState{
				InstanceId: "wf-001",
				OldEvents:  []*protos.HistoryEvent{workflowStarted, start, makeTaskScheduled(0, "act1")},
				NewEvents:  []*protos.HistoryEvent{makeTaskScheduled(1, "act2")},
			},
		},
		{
			name: "start event in this turn",
			state: &protos.WorkflowRuntimeState{
				InstanceId: "wf-001",
				NewEvents:  []*protos.HistoryEvent{workflowStarted, start, makeTaskScheduled(0, "act1")},
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ph, err := AssembleProtoPropagatedHistory(
				tc.state,
				protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_START_EVENT_ONLY,
				receivedHistory,
				"appB",
			)
			require.NoError(t, err)
			require.NotNil(t, ph)
			assert.Equal(t, protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_START_EVENT_ONLY, ph.GetScope())
			require.Len(t, ph.GetChunks(), 1, "START_EVENT_ONLY must drop the ancestor chunk")

			chunk := ph.GetChunks()[0]
			assert.Equal(t, "appB", chunk.GetAppId())
			assert.Equal(t, "wf-001", chunk.GetInstanceId())
			assert.Equal(t, "MyWorkflow", chunk.GetWorkflowName())
			require.Len(t, chunk.GetRawEvents(), 1, "the chunk must hold the ExecutionStarted event only")

			want, err := historysigning.MarshalEvent(start)
			require.NoError(t, err)
			assert.Equal(t, want, chunk.GetRawEvents()[0], "the event must be marshaled with historysigning.MarshalEvent")

			es := decodeChunkEvents(t, chunk)[0].GetExecutionStarted()
			require.NotNil(t, es)
			assert.Equal(t, tags, es.GetTags())
			assert.Equal(t, `{"q":"hello"}`, es.GetInput().GetValue())
		})
	}
}

// TestAssembleProtoPropagatedHistory_StartEventOnly_NoStartEvent asserts a
// state with no ExecutionStarted event yields no chunk rather than an empty
// one, which VerifyPropagatedHistory would reject.
func TestAssembleProtoPropagatedHistory_StartEventOnly_NoStartEvent(t *testing.T) {
	state := &protos.WorkflowRuntimeState{
		InstanceId: "wf-001",
		OldEvents:  []*protos.HistoryEvent{makeTaskScheduled(1, "act1")},
	}

	ph, err := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_START_EVENT_ONLY,
		nil,
		"appA",
	)
	require.NoError(t, err)
	require.NotNil(t, ph)
	assert.Empty(t, ph.GetChunks(), "no ExecutionStarted event => no chunk")
}

// newChunkSigner returns a signer whose SVID is
// spiffe://example.org/ns/<namespace>/<appID>: a self-signed Ed25519
// certificate valid an hour either side of at, trusted as its own anchor.
func newChunkSigner(t *testing.T, namespace, appID string, at time.Time) *signer.Signer {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	id, err := spiffeid.FromSegments(spiffeid.RequireTrustDomainFromString("example.org"), "ns", namespace, appID)
	require.NoError(t, err)
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: appID},
		NotBefore:    at.Add(-time.Hour),
		NotAfter:     at.Add(time.Hour),
		URIs:         []*url.URL{id.URL()},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	require.NoError(t, err)
	cert, err := x509.ParseCertificate(der)
	require.NoError(t, err)
	svid := &x509svid.SVID{ID: id, Certificates: []*x509.Certificate{cert}, PrivateKey: priv}
	return signer.New(svid, fake.New(cert))
}

// TestAssembleProtoPropagatedHistory_StartEventOnly_SignsAndVerifies signs
// the START_EVENT_ONLY chunk the way a producer does and checks that
// historysigning.VerifyPropagatedHistory accepts a chunk of one event.
func TestAssembleProtoPropagatedHistory_StartEventOnly_SignsAndVerifies(t *testing.T) {
	now := time.Now()
	s := newChunkSigner(t, "default", "appB", now)
	state := &protos.WorkflowRuntimeState{
		InstanceId: "wf-001",
		OldEvents: []*protos.HistoryEvent{
			makeTaggedExecutionStarted("MyWorkflow", map[string]string{"owner": "user-1"}, now),
		},
		NewEvents: []*protos.HistoryEvent{makeTaskScheduled(0, "act1")},
	}

	ph, err := AssembleProtoPropagatedHistory(
		state,
		protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_START_EVENT_ONLY,
		nil,
		"appB",
	)
	require.NoError(t, err)
	require.Len(t, ph.GetChunks(), 1)
	chunk := ph.GetChunks()[0]
	require.Len(t, chunk.GetRawEvents(), 1)

	res, err := historysigning.Sign(s, historysigning.SignOptions{
		RawEvents: chunk.GetRawEvents(),
		PropagationContext: &historysigning.PropagationContext{
			InstanceID:   chunk.GetInstanceId(),
			WorkflowName: chunk.GetWorkflowName(),
		},
	})
	require.NoError(t, err)
	require.NotNil(t, res.NewCert)
	require.NoError(t, historysigning.BuildSignedChunk(chunk,
		[][]byte{res.RawSignature}, [][]byte{res.NewCert.GetCertificate()}))

	verified, err := historysigning.VerifyPropagatedHistory(historysigning.VerifyPropagationOptions{
		History:           ph,
		Signer:            s,
		ExpectedNamespace: "default",
	})
	require.NoError(t, err)
	assert.Len(t, verified.VerifiedCerts, 1)
}

// TestAssembleProtoPropagatedHistory_UnknownScope_IsOwnHistory asserts a
// scope value this build does not know propagates the caller's own history,
// exactly as OWN_HISTORY does. That is what an engine that predates a scope
// does when a newer SDK asks for it.
func TestAssembleProtoPropagatedHistory_UnknownScope_IsOwnHistory(t *testing.T) {
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
			makeTaskScheduled(1, "act1"),
		},
		NewEvents: []*protos.HistoryEvent{
			makeTaskScheduled(2, "act2"),
		},
	}

	ph, err := AssembleProtoPropagatedHistory(state, protos.HistoryPropagationScope(99), receivedHistory, "appB")
	require.NoError(t, err)
	require.NotNil(t, ph)
	require.Len(t, ph.GetChunks(), 1, "an unknown scope must drop the ancestor chunk, as OWN_HISTORY does")
	chunk := ph.GetChunks()[0]
	assert.Equal(t, "appB", chunk.GetAppId())
	assert.Equal(t, "MyWorkflow", chunk.GetWorkflowName())
	assert.Len(t, chunk.GetRawEvents(), 3, "an unknown scope must carry every own event")
}
