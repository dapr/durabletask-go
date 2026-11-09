package local_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend"
	"github.com/dapr/durabletask-go/backend/local"
	"github.com/dapr/durabletask-go/backend/runtimestate"
)

// host models one sidecar process: its own tasks backend, executor and work
// item stream. Two hosts model a sidecar that died and the one that took
// its work over.
type host struct {
	be     *tasksOnlyBackend
	exec   backend.Executor
	stream *workItemsStream
}

func newHost(t *testing.T) *host {
	t.Helper()
	be := &tasksOnlyBackend{tasks: local.NewTasksBackend()}
	exec, _ := backend.NewGrpcExecutor(be, backend.DefaultLogger())
	server := exec.(protos.TaskHubSidecarServiceServer)

	ctx, cancel := context.WithCancel(t.Context())
	t.Cleanup(cancel)
	stream := &workItemsStream{ctx: ctx, items: make(chan *protos.WorkItem, 4)}
	go func() { _ = server.GetWorkItems(&protos.GetWorkItemsRequest{}, stream) }()
	return &host{be: be, exec: exec, stream: stream}
}

type activityOutcome struct {
	event *protos.HistoryEvent
	err   error
}

// dispatchEvent runs ExecuteActivity for event on the host and returns the
// token of the work item sent to the worker and the channel its outcome
// lands on.
func (h *host) dispatchEvent(t *testing.T, ctx context.Context, event *protos.HistoryEvent) (token string, outcomes <-chan activityOutcome) {
	t.Helper()
	out := make(chan activityOutcome, 1)
	go func() {
		ev, err := h.exec.ExecuteActivity(ctx, api.InstanceID("wf1"), event, backend.ExecuteOptions{})
		out <- activityOutcome{event: ev, err: err}
	}()
	select {
	case wi := <-h.stream.items:
		return wi.GetCompletionToken(), out
	case <-time.After(5 * time.Second):
		require.FailNow(t, "work item was not dispatched")
		return "", out
	}
}

// dispatch is dispatchEvent with the result reduced to the activity's result
// string, or "error: <message>".
func (h *host) dispatch(t *testing.T, ctx context.Context, event *protos.HistoryEvent) (token string, results <-chan string) {
	t.Helper()
	token, outcomes := h.dispatchEvent(t, ctx, event)
	out := make(chan string, 1)
	go func() {
		o := <-outcomes
		if o.err != nil {
			out <- "error: " + o.err.Error()
			return
		}
		out <- o.event.GetTaskCompleted().GetResult().GetValue()
	}()
	return token, out
}

func scheduled(taskExecutionID string, stamp *timestamppb.Timestamp) *protos.HistoryEvent {
	return scheduledTask(7, taskExecutionID, stamp)
}

func scheduledTask(taskID int32, taskExecutionID string, stamp *timestamppb.Timestamp) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId:   taskID,
		Timestamp: stamp,
		EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{
			Name:            "ship",
			TaskExecutionId: taskExecutionID,
		}},
	}
}

func response(token, result string) *protos.ActivityResponse {
	return responseForTask(7, token, result)
}

func responseForTask(taskID int32, token, result string) *protos.ActivityResponse {
	return &protos.ActivityResponse{
		InstanceId:      "wf1",
		TaskId:          taskID,
		Result:          wrapperspb.String(result),
		CompletionToken: token,
	}
}

// A sidecar dies while the worker runs an activity. The next sidecar
// re-dispatches the same TaskScheduled event when the worker reconnects, and
// the worker then completes its first execution with the first sidecar's
// token. The re-dispatch must settle on that response: it is the result of
// the same scheduling. Waiting for the second execution instead costs one
// activity duration on every such recovery.
func Test_redispatchOfSameSchedulingAcceptsEarlierAttemptsResponse(t *testing.T) {
	event := scheduled("exec-1", timestamppb.New(time.Unix(1700000000, 123)))

	hostA := newHost(t)
	ctxA, cancelA := context.WithCancel(t.Context())
	tokenA, _ := hostA.dispatch(t, ctxA, event)
	cancelA()

	hostB := newHost(t)
	tokenB, results := hostB.dispatch(t, t.Context(), event)

	require.NoError(t, hostB.be.CompleteActivityTask(t.Context(), response(tokenA, "first execution")))
	select {
	case got := <-results:
		assert.Equal(t, "first execution", got)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "the re-dispatch did not settle on the earlier attempt's response; it waits for a second execution of the activity")
	}
	assert.Equal(t, tokenA, tokenB, "every dispatch of one scheduling carries the same token")
}

// A response for another scheduling that re-uses the task ID (an instance
// recreated under the same ID, or a continued-as-new generation) is still
// discarded, and the execution settles on its own response. This is the
// protection #121 added.
func Test_redispatchDiscardsResponseOfAnotherScheduling(t *testing.T) {
	stamp1 := timestamppb.New(time.Unix(1700000000, 123))
	stamp2 := timestamppb.New(time.Unix(1700000000, 124))
	stamp3 := timestamppb.New(time.Unix(1700000001, 123))
	tests := []struct {
		name     string
		previous *protos.HistoryEvent
		current  *protos.HistoryEvent
	}{
		{name: "other TaskExecutionId", previous: scheduled("exec-1", stamp1), current: scheduled("exec-2", stamp1)},
		{name: "other timestamp, SDK without TaskExecutionId", previous: scheduled("", stamp1), current: scheduled("", stamp2)},
		{name: "other TaskExecutionId, no timestamp", previous: scheduled("exec-1", nil), current: scheduled("exec-2", nil)},
		{name: "same TaskExecutionId, other nanoseconds", previous: scheduled("exec-1", stamp1), current: scheduled("exec-1", stamp2)},
		{name: "same TaskExecutionId, other seconds", previous: scheduled("exec-1", stamp1), current: scheduled("exec-1", stamp3)},
		{name: "same TaskExecutionId, no timestamp on the previous one", previous: scheduled("exec-1", nil), current: scheduled("exec-1", stamp1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hostA := newHost(t)
			ctxA, cancelA := context.WithCancel(t.Context())
			tokenA, _ := hostA.dispatch(t, ctxA, tt.previous)
			cancelA()

			hostB := newHost(t)
			tokenB, results := hostB.dispatch(t, t.Context(), tt.current)
			require.NotEqual(t, tokenA, tokenB)

			require.NoError(t, hostB.be.CompleteActivityTask(t.Context(), response(tokenA, "previous scheduling")))
			select {
			case got := <-results:
				require.FailNow(t, "settled on another scheduling's response", "got %q", got)
			case <-time.After(300 * time.Millisecond):
			}

			require.NoError(t, hostB.be.CompleteActivityTask(t.Context(), response(tokenB, "current scheduling")))
			select {
			case got := <-results:
				assert.Equal(t, "current scheduling", got)
			case <-time.After(2 * time.Second):
				require.FailNow(t, "the execution did not settle on its own response")
			}
		})
	}
}

// A TaskScheduled event with neither a TaskExecutionId nor a timestamp has
// no scheduling identity, so each dispatch keeps a token of its own, as
// before.
func Test_dispatchWithoutSchedulingIdentityKeepsPerDispatchToken(t *testing.T) {
	event := scheduled("", nil)

	hostA := newHost(t)
	ctxA, cancelA := context.WithCancel(t.Context())
	tokenA, _ := hostA.dispatch(t, ctxA, event)
	cancelA()

	hostB := newHost(t)
	tokenB, _ := hostB.dispatch(t, t.Context(), event)
	require.NotEqual(t, tokenA, tokenB)
}

// Two executions of one scheduling can be pending on the same host, for
// example a re-dispatch while the first dispatch is still waiting. They
// carry one token, so one response settles both. Each yields a completion
// event for the same task, and the runtime state drops the second as a
// duplicate.
func Test_concurrentDispatchesOfSameSchedulingSettleOnOneResponse(t *testing.T) {
	event := scheduled("exec-1", timestamppb.New(time.Unix(1700000000, 123)))

	h := newHost(t)
	token1, results1 := h.dispatch(t, t.Context(), event)
	token2, results2 := h.dispatch(t, t.Context(), event)
	require.Equal(t, token1, token2)

	require.NoError(t, h.be.CompleteActivityTask(t.Context(), response(token1, "result")))
	for _, results := range []<-chan string{results1, results2} {
		select {
		case got := <-results:
			assert.Equal(t, "result", got)
		case <-time.After(2 * time.Second):
			require.FailNow(t, "an execution of the scheduling did not settle on the response")
		}
	}
}

// Each retry attempt of an activity is a scheduling of its own: it carries
// the TaskExecutionId of the retry chain but has its own task ID and its own
// timestamp. The response of an earlier attempt must never settle a later
// attempt, whether it arrives with the earlier attempt's task ID or with the
// later attempt's task ID.
func Test_responseOfEarlierRetryAttemptIsRejected(t *testing.T) {
	attempt1 := scheduledTask(7, "retry-chain", timestamppb.New(time.Unix(1700000000, 123)))
	attempt2 := scheduledTask(9, "retry-chain", timestamppb.New(time.Unix(1700000005, 456)))

	h := newHost(t)
	token1, results1 := h.dispatch(t, t.Context(), attempt1)
	token2, results2 := h.dispatch(t, t.Context(), attempt2)
	require.NotEqual(t, token1, token2, "retry attempts of one chain are different schedulings")

	expectNothing := func(results <-chan string, what string) {
		t.Helper()
		select {
		case got := <-results:
			require.FailNow(t, what, "got %q", got)
		case <-time.After(300 * time.Millisecond):
		}
	}

	// The earlier attempt's token on the later attempt's task ID.
	require.NoError(t, h.be.CompleteActivityTask(t.Context(), responseForTask(9, token1, "attempt 1")))
	expectNothing(results2, "attempt 2 settled on the response of attempt 1")
	expectNothing(results1, "attempt 1 settled on a response for another task ID")

	// The earlier attempt's own response settles only the earlier attempt.
	require.NoError(t, h.be.CompleteActivityTask(t.Context(), responseForTask(7, token1, "attempt 1")))
	select {
	case got := <-results1:
		assert.Equal(t, "attempt 1", got)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "attempt 1 did not settle on its own response")
	}
	expectNothing(results2, "attempt 2 settled on the response of attempt 1")

	require.NoError(t, h.be.CompleteActivityTask(t.Context(), responseForTask(9, token2, "attempt 2")))
	select {
	case got := <-results2:
		assert.Equal(t, "attempt 2", got)
	case <-time.After(2 * time.Second):
		require.FailNow(t, "attempt 2 did not settle on its own response")
	}
}

// A late response of an earlier retry attempt arrives on the host that now
// runs a later attempt. The two attempts have different task IDs, so nothing
// is registered for the earlier attempt's task and the response is refused.
func Test_lateResponseOfEarlierRetryAttemptFindsNoRegistration(t *testing.T) {
	attempt1 := scheduledTask(7, "retry-chain", timestamppb.New(time.Unix(1700000000, 123)))
	attempt2 := scheduledTask(9, "retry-chain", timestamppb.New(time.Unix(1700000005, 456)))

	hostA := newHost(t)
	ctxA, cancelA := context.WithCancel(t.Context())
	token1, _ := hostA.dispatch(t, ctxA, attempt1)
	cancelA()

	hostB := newHost(t)
	_, results := hostB.dispatch(t, t.Context(), attempt2)

	err := hostB.be.CompleteActivityTask(t.Context(), responseForTask(7, token1, "attempt 1"))
	require.Error(t, err)
	assert.True(t, api.IsUnknownTaskIDError(err), "got %v", err)

	select {
	case got := <-results:
		require.FailNow(t, "attempt 2 settled on the response of attempt 1", "got %q", got)
	case <-time.After(300 * time.Millisecond):
	}
}

// Two schedulings that re-use one task ID are pending on one host, as when an
// instance is recreated under the same ID while the previous run's activity
// still runs. Each response settles the scheduling it belongs to and no other.
func Test_concurrentSchedulingsOfOneTaskIDSettleOnTheirOwnResponses(t *testing.T) {
	stamp := timestamppb.New(time.Unix(1700000000, 123))
	tests := []struct {
		name         string
		first, other *protos.HistoryEvent
	}{
		{name: "other TaskExecutionId", first: scheduled("exec-1", stamp), other: scheduled("exec-2", stamp)},
		{name: "same TaskExecutionId, other timestamp", first: scheduled("exec-1", stamp), other: scheduled("exec-1", timestamppb.New(time.Unix(1700000000, 124)))},
		{name: "no TaskExecutionId, other timestamp", first: scheduled("", stamp), other: scheduled("", timestamppb.New(time.Unix(1700000000, 124)))},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := newHost(t)
			tokenFirst, resultsFirst := h.dispatch(t, t.Context(), tt.first)
			tokenOther, resultsOther := h.dispatch(t, t.Context(), tt.other)
			require.NotEqual(t, tokenFirst, tokenOther)

			require.NoError(t, h.be.CompleteActivityTask(t.Context(), response(tokenFirst, "first")))
			select {
			case got := <-resultsFirst:
				assert.Equal(t, "first", got)
			case <-time.After(2 * time.Second):
				require.FailNow(t, "the first scheduling did not settle on its own response")
			}
			select {
			case got := <-resultsOther:
				require.FailNow(t, "the other scheduling settled on the first one's response", "got %q", got)
			case <-time.After(300 * time.Millisecond):
			}

			require.NoError(t, h.be.CompleteActivityTask(t.Context(), response(tokenOther, "other")))
			select {
			case got := <-resultsOther:
				assert.Equal(t, "other", got)
			case <-time.After(2 * time.Second):
				require.FailNow(t, "the other scheduling did not settle on its own response")
			}
		})
	}
}

// Two executions of one scheduling settle on the same response and each
// yields a TaskCompleted event. The workflow state must apply exactly one of
// them. A second response for the settled scheduling, such as the result of
// the second execution of the activity, finds nothing registered.
func Test_oneSchedulingIsAppliedOnceWhenSeveralExecutionsSettle(t *testing.T) {
	event := scheduled("exec-1", timestamppb.New(time.Unix(1700000000, 123)))

	h := newHost(t)
	token, outcomes1 := h.dispatchEvent(t, t.Context(), event)
	_, outcomes2 := h.dispatchEvent(t, t.Context(), event)

	require.NoError(t, h.be.CompleteActivityTask(t.Context(), response(token, "result")))

	state := runtimestate.NewWorkflowRuntimeState("wf1", nil, nil)
	require.NoError(t, runtimestate.AddEvent(state, &protos.HistoryEvent{
		EventId:   -1,
		EventType: &protos.HistoryEvent_ExecutionStarted{ExecutionStarted: &protos.ExecutionStartedEvent{Name: "wf"}},
	}))
	require.NoError(t, runtimestate.AddEvent(state, event))

	var applied, duplicates int
	for _, outcomes := range []<-chan activityOutcome{outcomes1, outcomes2} {
		select {
		case o := <-outcomes:
			require.NoError(t, o.err)
			assert.Equal(t, "result", o.event.GetTaskCompleted().GetResult().GetValue())
			switch err := runtimestate.AddEvent(state, o.event); {
			case err == nil:
				applied++
			case errors.Is(err, runtimestate.ErrDuplicateEvent):
				duplicates++
			default:
				require.NoError(t, err)
			}
		case <-time.After(2 * time.Second):
			require.FailNow(t, "an execution of the scheduling did not settle on the response")
		}
	}
	assert.Equal(t, 1, applied)
	assert.Equal(t, 1, duplicates)

	completed := 0
	for _, e := range state.GetNewEvents() {
		if e.GetTaskCompleted() != nil {
			completed++
		}
	}
	assert.Equal(t, 1, completed, "exactly one TaskCompleted event is in the state")

	err := h.be.CompleteActivityTask(t.Context(), response(token, "second execution"))
	require.Error(t, err)
	assert.True(t, api.IsUnknownTaskIDError(err), "got %v", err)
}
