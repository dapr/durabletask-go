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

	"github.com/dapr/durabletask-go/api/protos"
)

func events(n int) []*protos.HistoryEvent {
	e := make([]*protos.HistoryEvent, n)
	for i := range e {
		e[i] = &protos.HistoryEvent{EventId: int32(i)}
	}
	return e
}

func workflowReq(iid string, past, new int) *protos.WorkflowRequest {
	return &protos.WorkflowRequest{
		InstanceId: iid,
		PastEvents: events(past),
		NewEvents:  events(new),
	}
}

func TestNewStreamState_Capabilities(t *testing.T) {
	t.Run("no capabilities", func(t *testing.T) {
		ss := newStreamState(&protos.GetWorkItemsRequest{})
		assert.False(t, ss.statefulHistory)
	})

	t.Run("stateful history advertised alongside an unknown capability", func(t *testing.T) {
		ss := newStreamState(&protos.GetWorkItemsRequest{
			Capabilities: []protos.WorkerCapability{
				protos.WorkerCapability(99), // an unknown/future capability is ignored
				protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY,
			},
		})
		assert.True(t, ss.statefulHistory)
	})
}

func TestApplyStatefulHistory_NonCapableStreamUnchanged(t *testing.T) {
	ss := newStreamState(&protos.GetWorkItemsRequest{})
	req := workflowReq("a", 5, 2)

	ss.applyStatefulHistory(req)

	assert.Len(t, req.PastEvents, 5, "full history must be retained for non-capable workers")
	assert.Nil(t, req.CachedHistory)
	assert.Empty(t, ss.warm)
}

func TestApplyStatefulHistory_FirstTurnSendsFullThenWarms(t *testing.T) {
	ss := newStreamState(&protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	})
	req := workflowReq("a", 5, 2)

	ss.applyStatefulHistory(req)

	// First turn: no warm entry yet, so the full history is sent...
	assert.Len(t, req.PastEvents, 5)
	assert.Nil(t, req.CachedHistory)
	// ...but the instance is now warm up to the committed-history length.
	assert.Equal(t, 5, ss.warm["a"])
}

func TestApplyStatefulHistory_SubsequentTurnSendsDelta(t *testing.T) {
	ss := newStreamState(&protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	})

	// Turn 1: 5 committed events, full send, warm -> 5.
	ss.applyStatefulHistory(workflowReq("a", 5, 2))
	assert.Equal(t, 5, ss.warm["a"])

	// Turn 2: history has grown to 8 committed events. The worker already holds
	// the first 5, so only the 3-event delta should be sent.
	req2 := workflowReq("a", 8, 1)
	ss.applyStatefulHistory(req2)

	require.NotNil(t, req2.CachedHistory)
	assert.Equal(t, int32(5), req2.CachedHistory.GetEventCount())
	assert.Len(t, req2.PastEvents, 3, "only events 5..8 should be sent as the delta")
	assert.Len(t, req2.NewEvents, 1, "new events are always sent in full")
	assert.Equal(t, 8, ss.warm["a"])
}

func TestApplyStatefulHistory_PerInstanceIsolation(t *testing.T) {
	ss := newStreamState(&protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	})

	ss.applyStatefulHistory(workflowReq("a", 3, 0))
	// "b" has never been seen on this stream: it must get a full send.
	reqB := workflowReq("b", 4, 0)
	ss.applyStatefulHistory(reqB)

	assert.Nil(t, reqB.CachedHistory)
	assert.Len(t, reqB.PastEvents, 4)
	assert.Equal(t, 3, ss.warm["a"])
	assert.Equal(t, 4, ss.warm["b"])
}

func TestApplyStatefulHistory_ShrinkingHistoryFallsBackToFull(t *testing.T) {
	// A continue-as-new resets the committed history to a shorter list. The warm
	// count is now larger than the current history, so we must not send a delta.
	ss := newStreamState(&protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	})

	ss.applyStatefulHistory(workflowReq("a", 10, 0))
	assert.Equal(t, 10, ss.warm["a"])

	req2 := workflowReq("a", 2, 1)
	ss.applyStatefulHistory(req2)

	assert.Nil(t, req2.CachedHistory, "must not send a delta when history shrank")
	assert.Len(t, req2.PastEvents, 2)
	assert.Equal(t, 2, ss.warm["a"], "warm count is re-based to the new history length")
}

func TestApplyStatefulHistory_BoundsWarmMap(t *testing.T) {
	ss := newStreamState(&protos.GetWorkItemsRequest{
		Capabilities: []protos.WorkerCapability{protos.WorkerCapability_WORKER_CAPABILITY_STATEFUL_HISTORY},
	})

	for i := 0; i < maxWarmInstancesPerStream+10; i++ {
		ss.applyStatefulHistory(workflowReq("inst-"+string(rune(i)), 1, 0))
	}

	assert.LessOrEqual(t, len(ss.warm), maxWarmInstancesPerStream+1,
		"warm map must stay bounded as new instances are dispatched")
}
