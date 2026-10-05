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

package task

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
)

type warnLogger struct{ warns []string }

func (l *warnLogger) Debug(...any)             {}
func (l *warnLogger) Debugf(string, ...any)    {}
func (l *warnLogger) Info(...any)              {}
func (l *warnLogger) Infof(string, ...any)     {}
func (l *warnLogger) Warn(...any)              {}
func (l *warnLogger) Warnf(f string, v ...any) { l.warns = append(l.warns, fmt.Sprintf(f, v...)) }
func (l *warnLogger) Error(...any)             {}
func (l *warnLogger) Errorf(string, ...any)    {}

// A resolution replayed before the workflow has scheduled its step is
// ignored, like the other SDKs do, and the replay carries on. The backend
// (daprd) is responsible for handing such a resolution to the worker after
// the events that lead the workflow to schedule the step.
func Test_UnmatchedResolutionIsIgnored(t *testing.T) {
	r := NewTaskRegistry()
	require.NoError(t, r.AddActivityN("a", func(ActivityContext) (any, error) { return "x", nil }))
	require.NoError(t, r.AddWorkflowN("gated", func(ctx *WorkflowContext) (any, error) {
		if err := ctx.WaitForSingleEvent("go", time.Hour).Await(nil); err != nil {
			return nil, err
		}
		var out string
		if err := ctx.CallActivity("a").Await(&out); err != nil {
			return nil, err
		}
		return out, nil
	}))
	started := &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ExecutionStarted{
		ExecutionStarted: &protos.ExecutionStartedEvent{Name: "gated", WorkflowInstance: &protos.WorkflowInstance{InstanceId: "id"}},
	}}
	goEvent := &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_EventRaised{
		EventRaised: &protos.EventRaisedEvent{Name: "go"},
	}}
	// The synthetic external event timer is id 0, so the activity is id 1.
	early := &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_TaskCompleted{
		TaskCompleted: &protos.TaskCompletedEvent{TaskScheduledId: 1, Result: wrapperspb.String(`"early"`)},
	}}
	scheduledEvent := &protos.HistoryEvent{EventId: 1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_TaskScheduled{
		TaskScheduled: &protos.TaskScheduledEvent{Name: "a"},
	}}
	replay := func(oldEvents, newEvents []*protos.HistoryEvent) ([]*protos.WorkflowAction, *warnLogger) {
		l := new(warnLogger)
		ctx := NewWorkflowContext(r, "id", oldEvents, newEvents)
		ctx.SetLogger(l)
		return ctx.start(), l
	}
	run := func(newEvents ...*protos.HistoryEvent) ([]*protos.WorkflowAction, *warnLogger) {
		return replay([]*protos.HistoryEvent{started}, newEvents)
	}
	scheduled := func(actions []*protos.WorkflowAction) (n int) {
		for _, a := range actions {
			if a.GetScheduleTask() != nil {
				n++
			}
		}
		return n
	}
	completed := func(actions []*protos.WorkflowAction) *protos.CompleteWorkflowAction {
		for _, a := range actions {
			if c := a.GetCompleteWorkflow(); c != nil {
				return c
			}
		}
		return nil
	}

	t.Run("before the gating event it is ignored", func(t *testing.T) {
		actions, l := run(early, goEvent)
		assert.Equal(t, 1, scheduled(actions), "the activity is scheduled")
		assert.Nil(t, completed(actions), "its result was not used")
		require.Len(t, l.warns, 1)
		assert.Contains(t, l.warns[0], "ignoring TaskCompleted for id 1")
	})

	t.Run("after the gating event it resolves the step", func(t *testing.T) {
		actions, l := run(goEvent, early)
		assert.Equal(t, 1, scheduled(actions), "the schedule is still emitted for the backend to record")
		require.NotNil(t, completed(actions))
		assert.Equal(t, `"early"`, completed(actions).GetResult().GetValue())
		assert.Empty(t, l.warns)
	})

	t.Run("persisted after its scheduling it resolves the step on replay", func(t *testing.T) {
		nudge := &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_EventRaised{
			EventRaised: &protos.EventRaisedEvent{Name: "nudge"},
		}}
		actions, l := replay([]*protos.HistoryEvent{started, goEvent, scheduledEvent, early}, []*protos.HistoryEvent{nudge})
		assert.Equal(t, 0, scheduled(actions), "the schedule is already in history")
		require.NotNil(t, completed(actions))
		assert.Equal(t, `"early"`, completed(actions).GetResult().GetValue())
		assert.Empty(t, l.warns)
	})
}
