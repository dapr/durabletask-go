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
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/historysigning"
)

// Only forward what the wf already received from its parent, no default. If the
// workflow had no incoming chunk (a root workflow), the new generation
// starts clean.
func canForwardScope(receivedHistory *protos.PropagatedHistory) protos.HistoryPropagationScope {
	if receivedHistory != nil {
		return receivedHistory.GetScope()
	}
	return protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_NONE
}

// AssembleProtoPropagatedHistory builds a proto PropagatedHistory from the
// current workflow's runtime state, using the given propagation scope.
// receivedHistory is the propagated history this workflow received from its
// own parent — used when scope is LINEAGE. appID is the current app's Dapr
// app ID, used to tag the chunk of events produced by this workflow.
//
// Each chunk owns its own rawEvents bytes. For the current workflow's own
// chunk, events are marshaled deterministically here (same encoding as
// historysigning.MarshalEvent) so the bytes the producer signs match the
// bytes receivers digest. Lineage chunks pass through verbatim - their
// rawEvents and signatures were attached at the upstream producer's
// dispatch and travel with the chain.
func AssembleProtoPropagatedHistory(
	state *protos.WorkflowRuntimeState,
	scope protos.HistoryPropagationScope,
	receivedHistory *protos.PropagatedHistory,
	appID string,
) (*protos.PropagatedHistory, error) {
	if scope == protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_NONE {
		return nil, nil
	}

	var chunks []*protos.PropagatedHistoryChunk

	// If lineage, prepend the chunks we received from our parent.
	// They already carry their own rawEvents/signatures/certs.
	if scope == protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE {
		if receivedHistory != nil {
			chunks = append(chunks, receivedHistory.GetChunks()...)
		}
	}

	// Build this workflow's own chunk.
	ownEvents := make([]*protos.HistoryEvent, 0, len(state.GetOldEvents())+len(state.GetNewEvents()))
	ownEvents = append(ownEvents, state.GetOldEvents()...)
	ownEvents = append(ownEvents, state.GetNewEvents()...)

	if len(ownEvents) > 0 {
		var workflowName string
		for _, e := range ownEvents {
			if es := e.GetExecutionStarted(); es != nil {
				workflowName = es.GetName()
				break
			}
		}

		rawEvents := make([][]byte, len(ownEvents))
		for i, e := range ownEvents {
			b, err := historysigning.MarshalEvent(e)
			if err != nil {
				return nil, err
			}
			rawEvents[i] = b
		}

		chunks = append(chunks, &protos.PropagatedHistoryChunk{
			AppId:        appID,
			InstanceId:   state.GetInstanceId(),
			WorkflowName: workflowName,
			RawEvents:    rawEvents,
		})
	}

	return &protos.PropagatedHistory{
		Scope:  scope,
		Chunks: chunks,
	}, nil
}
