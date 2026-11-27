package backend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
)

func taskScheduledEvent(taskExecutionID string, stamp *timestamppb.Timestamp) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   3,
		Timestamp: stamp,
		EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{
			Name:            "act",
			TaskExecutionId: taskExecutionID,
		}},
	}
}

func Test_ActivitySchedulingID(t *testing.T) {
	stamp := timestamppb.New(time.Unix(1700000000, 123))

	tests := []struct {
		name  string
		event *protos.HistoryEvent
		want  string
	}{
		{name: "TaskExecutionId and timestamp", event: taskScheduledEvent("exec-1", stamp), want: "exec-1@1700000000.123"},
		{name: "timestamp only (SDK without TaskExecutionId)", event: taskScheduledEvent("", stamp), want: "@1700000000.123"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, ActivitySchedulingID(tt.event))
			assert.Equal(t, tt.want, ActivitySchedulingID(tt.event), "the token is a function of the scheduling, so a re-dispatch gets the same token")
		})
	}

	// A rerun copies the TaskExecutionId, so without a timestamp it does not
	// identify a run, and the token falls back to a fresh UUID.
	noTimestamp := []struct {
		name  string
		event *protos.HistoryEvent
	}{
		{name: "no TaskExecutionId, no timestamp", event: taskScheduledEvent("", nil)},
		{name: "TaskExecutionId, no timestamp", event: taskScheduledEvent("exec-1", nil)},
	}
	for _, tt := range noTimestamp {
		t.Run(tt.name+": a fresh token per dispatch", func(t *testing.T) {
			first, second := ActivitySchedulingID(tt.event), ActivitySchedulingID(tt.event)
			require.Len(t, first, 36)
			assert.NotEqual(t, first, second)
		})
	}
}

// Two schedulings that re-use one task ID must never share a token, or a
// response of the earlier one would settle the later one.
func Test_ActivitySchedulingIDDistinguishesSchedulings(t *testing.T) {
	base := timestamppb.New(time.Unix(1700000000, 123))

	tests := []struct {
		name string
		a, b *protos.HistoryEvent
	}{
		{name: "other TaskExecutionId", a: taskScheduledEvent("exec-1", base), b: taskScheduledEvent("exec-2", base)},
		{name: "same TaskExecutionId, other nanoseconds", a: taskScheduledEvent("exec-1", base), b: taskScheduledEvent("exec-1", timestamppb.New(time.Unix(1700000000, 124)))},
		{name: "same TaskExecutionId, other seconds", a: taskScheduledEvent("exec-1", base), b: taskScheduledEvent("exec-1", timestamppb.New(time.Unix(1700000001, 123)))},
		{name: "same TaskExecutionId, timestamp and none", a: taskScheduledEvent("exec-1", base), b: taskScheduledEvent("exec-1", nil)},
		{name: "no TaskExecutionId, other timestamp", a: taskScheduledEvent("", base), b: taskScheduledEvent("", timestamppb.New(time.Unix(1700000000, 124)))},
		{name: "no TaskExecutionId and TaskExecutionId", a: taskScheduledEvent("", base), b: taskScheduledEvent("exec-1", base)},
		{name: "seconds and nanoseconds do not run together", a: taskScheduledEvent("exec-1", timestamppb.New(time.Unix(1, 23))), b: taskScheduledEvent("exec-1", timestamppb.New(time.Unix(12, 3)))},
		{name: "zero nanoseconds and a timestamp of zero", a: taskScheduledEvent("exec-1", timestamppb.New(time.Unix(5, 0))), b: taskScheduledEvent("exec-1", timestamppb.New(time.Unix(0, 5)))},
		{name: "TaskExecutionId containing the separator", a: taskScheduledEvent("x@1.5", nil), b: taskScheduledEvent("x", timestamppb.New(time.Unix(1, 5)))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.NotEqual(t, ActivitySchedulingID(tt.a), ActivitySchedulingID(tt.b))
		})
	}
}

// The token is a function of the scheduling only. The activity processor
// rewrites the event's trace context before it dispatches, and other fields
// can differ between a copy read from a reminder and one read from history.
func Test_ActivitySchedulingIDIgnoresOtherEventFields(t *testing.T) {
	stamp := timestamppb.New(time.Unix(1700000000, 123))
	a := taskScheduledEvent("exec-1", stamp)
	b := taskScheduledEvent("exec-1", stamp)
	b.GetTaskScheduled().ParentTraceContext = &protos.TraceContext{TraceParent: "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}
	b.GetTaskScheduled().Input = wrapperspb.String("input")
	b.GetTaskScheduled().Name = "other"

	assert.Equal(t, ActivitySchedulingID(a), ActivitySchedulingID(b))
}

func Test_ActivitySchedulingIDWithoutTaskScheduled(t *testing.T) {
	for _, event := range []*protos.HistoryEvent{nil, {}} {
		token := ActivitySchedulingID(event)
		require.Len(t, token, 36, "an event with no scheduling identity gets a fresh UUID")
	}
}
