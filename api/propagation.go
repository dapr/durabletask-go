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
	"github.com/dapr/durabletask-go/api/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// PropagationOption is a functional option for configuring history propagation.
// Pass one of PropagateOwnHistory() or PropagateLineage().
type PropagationOption func(*protos.HistoryPropagationScope)

// PropagateOwnHistory configures propagation to include the caller's own
// history events (activities, child workflows, timers, etc.). The child only
// sees events from the immediate parent, not the ancestor chain.
func PropagateOwnHistory() PropagationOption {
	return func(s *protos.HistoryPropagationScope) {
		*s = protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY
	}
}

// PropagateLineage configures propagation to include the caller's own history
// events AND the full ancestor chain. Any propagated history this workflow
// received from its own parent is forwarded to the child, preserving the full
// chain of custody.
func PropagateLineage() PropagationOption {
	return func(s *protos.HistoryPropagationScope) {
		*s = protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE
	}
}

// NewHistoryPropagationScope creates a HistoryPropagationScope from the given option.
func NewHistoryPropagationScope(opt PropagationOption) protos.HistoryPropagationScope {
	var scope protos.HistoryPropagationScope
	opt(&scope)
	return scope
}

// historyChunk represents a contiguous range of events produced by a single
// workflow instance
type historyChunk struct {
	appID           string
	startEventIndex int
	eventCount      int
	instanceID      string
	workflowName    string
}

// PropagatedHistory is the history propagated from a parent workflow to a
// child wf or activity
type PropagatedHistory struct {
	events []*protos.HistoryEvent
	scope  protos.HistoryPropagationScope
	chunks []historyChunk
}

// Events returns all propagated history events.
func (ph *PropagatedHistory) Events() []*protos.HistoryEvent {
	return ph.events
}

// Scope returns the propagation scope used to produce this history.
func (ph *PropagatedHistory) Scope() protos.HistoryPropagationScope {
	return ph.scope
}

// GetAppIDs returns the ordered, deduplicated list of app IDs in the history chain.
func (ph *PropagatedHistory) GetAppIDs() []string {
	seen := make(map[string]bool)
	var ids []string
	for _, chunk := range ph.chunks {
		if !seen[chunk.appID] {
			seen[chunk.appID] = true
			ids = append(ids, chunk.appID)
		}
	}
	return ids
}

// GetEventsByAppID returns only the events produced by the given app.
func (ph *PropagatedHistory) GetEventsByAppID(appID string) []*protos.HistoryEvent {
	var events []*protos.HistoryEvent
	for _, chunk := range ph.chunks {
		if chunk.appID == appID {
			events = append(events, ph.chunkEvents(chunk)...)
		}
	}
	return events
}

// GetEventsByInstanceID returns only the events produced by the given workflow instance.
func (ph *PropagatedHistory) GetEventsByInstanceID(instanceID string) []*protos.HistoryEvent {
	var events []*protos.HistoryEvent
	for _, chunk := range ph.chunks {
		if chunk.instanceID == instanceID {
			events = append(events, ph.chunkEvents(chunk)...)
		}
	}
	return events
}

// GetEventsByWorkflowName returns only the events produced by workflows with the given name.
func (ph *PropagatedHistory) GetEventsByWorkflowName(name string) []*protos.HistoryEvent {
	var events []*protos.HistoryEvent
	for _, chunk := range ph.chunks {
		if chunk.workflowName == name {
			events = append(events, ph.chunkEvents(chunk)...)
		}
	}
	return events
}

// chunkEvents returns the slice of events for a given chunk, with bounds checking.
func (ph *PropagatedHistory) chunkEvents(chunk historyChunk) []*protos.HistoryEvent {
	end := chunk.startEventIndex + chunk.eventCount
	if end > len(ph.events) {
		end = len(ph.events)
	}
	return ph.events[chunk.startEventIndex:end]
}

// WorkflowResult is a scoped view of a single workflow's chunk in propagated history.
// Use GetActivityByName or GetChildWorkflowByName to query specific items.
type WorkflowResult struct {
	Found      bool
	InstanceID string
	AppID      string
	Name       string // wf name
	events     []*protos.HistoryEvent
}

// ActivityResult holds the status and data of a named activity.
type ActivityResult struct {
	Name      string // activity name
	Started   bool
	Completed bool
	Failed    bool
	Input     *wrapperspb.StringValue
	Output    *wrapperspb.StringValue
	Error     *protos.TaskFailureDetails
}

// ChildWorkflowResult holds the status and data of a named child workflow.
type ChildWorkflowResult struct {
	Name      string // child wf name
	Started   bool
	Completed bool
	Failed    bool
	Output    *wrapperspb.StringValue
	Error     *protos.TaskFailureDetails
}

// makeWorkflowResult creates a WorkflowResult from a chunk.
func (ph *PropagatedHistory) makeWorkflowResult(chunk historyChunk) WorkflowResult {
	return WorkflowResult{
		Found:      true,
		InstanceID: chunk.instanceID,
		AppID:      chunk.appID,
		Name:       chunk.workflowName,
		events:     ph.chunkEvents(chunk),
	}
}

// GetWorkflows returns all workflow results in the propagated history chain,
// in execution order (ancestor first, then own)
func (ph *PropagatedHistory) GetWorkflows() []WorkflowResult {
	results := make([]WorkflowResult, 0, len(ph.chunks))
	for _, chunk := range ph.chunks {
		results = append(results, ph.makeWorkflowResult(chunk))
	}
	return results
}

// GetWorkflowByName returns the last workflow result matching the given name.
// Returns the most recent instance, which is the final outcome after retries.
// Returns a zero WorkflowResult if not found
func (ph *PropagatedHistory) GetWorkflowByName(name string) WorkflowResult {
	all := ph.GetWorkflowsByName(name)
	if len(all) == 0 {
		return WorkflowResult{}
	}
	return all[len(all)-1]
}

// GetWorkflowsByName returns all workflow results matching the given name,
// in execution order. Returns nil if none found.
func (ph *PropagatedHistory) GetWorkflowsByName(name string) []WorkflowResult {
	var results []WorkflowResult
	for _, chunk := range ph.chunks {
		if chunk.workflowName == name {
			results = append(results, ph.makeWorkflowResult(chunk))
		}
	}
	return results
}

// resolveActivity builds an ActivityResult for a single TaskScheduled history
// event and its matching TaskCompleted/TaskFailed. Matching is done by the
// scheduling event's ID (TaskCompletedEvent.taskScheduledId /
// TaskFailedEvent.taskScheduledId), NOT by taskExecutionId, because SDK
// retries reuse the same taskExecutionId
func resolveActivity(events []*protos.HistoryEvent, scheduleEvent *protos.HistoryEvent) ActivityResult {
	ts := scheduleEvent.GetTaskScheduled()
	result := ActivityResult{
		Name:    ts.GetName(),
		Started: true,
		Input:   ts.GetInput(),
	}
	scheduleID := scheduleEvent.GetEventId()
	for _, e := range events {
		if tc := e.GetTaskCompleted(); tc != nil && tc.GetTaskScheduledId() == scheduleID {
			result.Completed = true
			result.Output = tc.GetResult()
		}
		if tf := e.GetTaskFailed(); tf != nil && tf.GetTaskScheduledId() == scheduleID {
			result.Failed = true
			result.Error = tf.GetFailureDetails()
		}
	}
	return result
}

// GetActivityByName returns the last activity result matching the given name.
// Returns the most recent invocation, which is the final outcome after retries.
// Returns a zero ActivityResult if not found or if the wf was not found.
func (wr WorkflowResult) GetActivityByName(name string) ActivityResult {
	all := wr.GetActivitiesByName(name)
	if len(all) == 0 {
		return ActivityResult{Name: name}
	}
	return all[len(all)-1]
}

// GetActivitiesByName returns all activity results matching the given name,
// in execution order. Useful when the same activity is called multiple times
// (ex retries/loops). Returns nil if none found.
func (wr WorkflowResult) GetActivitiesByName(name string) []ActivityResult {
	if !wr.Found {
		return nil
	}
	var results []ActivityResult
	for _, e := range wr.events {
		if ts := e.GetTaskScheduled(); ts != nil && ts.GetName() == name {
			results = append(results, resolveActivity(wr.events, e))
		}
	}
	return results
}

// resolveChildWorkflow builds a ChildWorkflowResult for a single
// ChildWorkflowInstanceCreated event and its matching Completed/Failed.
func resolveChildWorkflow(events []*protos.HistoryEvent, eventID int32) ChildWorkflowResult {
	result := ChildWorkflowResult{Started: true}
	for _, e := range events {
		if cw := e.GetChildWorkflowInstanceCreated(); cw != nil && e.GetEventId() == eventID {
			result.Name = cw.GetName()
		}
		if cc := e.GetChildWorkflowInstanceCompleted(); cc != nil && cc.GetTaskScheduledId() == eventID {
			result.Completed = true
			result.Output = cc.GetResult()
		}
		if cf := e.GetChildWorkflowInstanceFailed(); cf != nil && cf.GetTaskScheduledId() == eventID {
			result.Failed = true
			result.Error = cf.GetFailureDetails()
		}
	}
	return result
}

// GetChildWorkflowByName returns the last child workflow result matching the given name.
// Returns the most recent invocation, which is the final outcome after retries.
// Returns a zero ChildWorkflowResult if not found or if the wf was not found.
func (wr WorkflowResult) GetChildWorkflowByName(name string) ChildWorkflowResult {
	all := wr.GetChildWorkflowsByName(name)
	if len(all) == 0 {
		return ChildWorkflowResult{Name: name}
	}
	return all[len(all)-1]
}

// GetChildWorkflowsByName returns all child workflow results matching the given name,
// in execution order. Returns nil if none found.
func (wr WorkflowResult) GetChildWorkflowsByName(name string) []ChildWorkflowResult {
	if !wr.Found {
		return nil
	}
	var results []ChildWorkflowResult
	for _, e := range wr.events {
		if cw := e.GetChildWorkflowInstanceCreated(); cw != nil && cw.GetName() == name {
			results = append(results, resolveChildWorkflow(wr.events, e.GetEventId()))
		}
	}
	return results
}

// PropagatedHistoryFromProto converts a proto PropagatedHistory to the Go type.
func PropagatedHistoryFromProto(ph *protos.PropagatedHistory) *PropagatedHistory {
	if ph == nil {
		return nil
	}
	chunks := make([]historyChunk, len(ph.GetChunks()))
	for i, c := range ph.GetChunks() {
		chunks[i] = historyChunk{
			appID:           c.GetAppId(),
			startEventIndex: int(c.GetStartEventIndex()),
			eventCount:      int(c.GetEventCount()),
			instanceID:      c.GetInstanceId(),
			workflowName:    c.GetWorkflowName(),
		}
	}
	return &PropagatedHistory{
		events: ph.GetEvents(),
		scope:  ph.GetScope(),
		chunks: chunks,
	}
}
