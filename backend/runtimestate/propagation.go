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
)

// AssembleProtoPropagatedHistory builds a proto PropagatedHistory from the
// current workflow's runtime state, using the given propagation scope.
// receivedHistory is the propagated history this workflow received from its
// own parent — used when scope is LINEAGE.
// appID is the current app's Dapr app ID, used to tag the chunk of events
// produced by this workflow.
func AssembleProtoPropagatedHistory(
	state *protos.WorkflowRuntimeState,
	scope protos.HistoryPropagationScope,
	receivedHistory *protos.PropagatedHistory,
	appID string,
) *protos.PropagatedHistory {
	if scope == protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_UNSPECIFIED {
		return nil
	}

	var events []*protos.HistoryEvent
	var chunks []*protos.PropagatedHistoryChunk

	// If lineage, prepend the propagated history we received from our parent
	if scope == protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE {
		if receivedHistory != nil {
			events = append(events, receivedHistory.GetEvents()...)
			chunks = append(chunks, receivedHistory.GetChunks()...)
		}
	}

	// Always include this workflow's own events
	ownEvents := make([]*protos.HistoryEvent, 0, len(state.GetOldEvents())+len(state.GetNewEvents()))
	ownEvents = append(ownEvents, state.GetOldEvents()...)
	ownEvents = append(ownEvents, state.GetNewEvents()...)

	// Add a chunk for the current app's events
	if len(ownEvents) > 0 {
		var workflowName string
		for _, e := range ownEvents {
			if es := e.GetExecutionStarted(); es != nil {
				workflowName = es.GetName()
				break
			}
		}

		chunks = append(chunks, &protos.PropagatedHistoryChunk{
			AppId:           appID,
			StartEventIndex: int32(len(events)),
			EventCount:      int32(len(ownEvents)),
			InstanceId:      state.GetInstanceId(),
			WorkflowName:    workflowName,
		})
	}

	events = append(events, ownEvents...)

	return &protos.PropagatedHistory{
		Events: events,
		Scope:  scope,
		Chunks: chunks,
	}
}
