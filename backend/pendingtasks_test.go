package backend

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

func Test_pendingTasksTracksEachExecution(t *testing.T) {
	p := newPendingTasks()
	var cancelled []string
	older, newer := &protos.WorkItem{}, &protos.WorkItem{}
	doneOlder := p.add("k", older, api.InstanceID("wf1"), 3, func() { cancelled = append(cancelled, "older") })
	doneNewer := p.add("k", newer, api.InstanceID("wf1"), 3, func() { cancelled = append(cancelled, "newer") })
	p.dispatched(older, "s1")
	p.dispatched(newer, "s2")
	assert.Equal(t, []pendingTask{{instanceID: "wf1", taskID: 3}}, p.all())
	assert.False(t, p.isLatest(older))
	assert.True(t, p.isLatest(newer))

	for _, cancel := range p.onStream("s1") {
		cancel()
	}
	assert.Equal(t, []string{"older"}, cancelled)

	doneOlder()
	assert.Empty(t, p.onStream("s1"))
	assert.Len(t, p.onStream("s2"), 1)

	doneNewer()
	assert.Empty(t, p.all())
	assert.Empty(t, p.onStream("s2"))
	assert.False(t, p.isLatest(newer))
}

func Test_pendingTasksLatestEndingFirstLeavesNoLatest(t *testing.T) {
	p := newPendingTasks()
	older, newer := &protos.WorkItem{}, &protos.WorkItem{}
	p.add("k", older, api.InstanceID("wf1"), 0, func() {})
	p.add("k", newer, api.InstanceID("wf1"), 0, func() {})()
	assert.False(t, p.isLatest(older))
	assert.Len(t, p.all(), 1)
}

func Test_pendingTasksConcurrentUse(t *testing.T) {
	p := newPendingTasks()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			stream := strconv.Itoa(i % 2)
			for range 200 {
				wi := &protos.WorkItem{}
				done := p.add("k", wi, api.InstanceID("wf1"), 0, func() {})
				p.dispatched(wi, stream)
				p.isLatest(wi)
				p.onStream(stream)
				p.all()
				done()
			}
		})
	}
	wg.Wait()
	assert.Empty(t, p.all())
}

type ctxStream struct {
	protos.TaskHubSidecarService_GetWorkItemsServer
	ctx context.Context
}

func (s ctxStream) Context() context.Context { return s.ctx }

// sendOn dispatches wi on streamID the way the dispatch loop does.
func sendOn(t *testing.T, g *grpcExecutor, streamID string, wi *protos.WorkItem) {
	t.Helper()
	outCh := make(chan *protos.WorkItem, 1)
	require.NoError(t, g.dispatchToStream(ctxStream{ctx: t.Context()}, streamID, nil, wi, outCh, make(chan error)))
}

// startActivity runs one execution of wf1's task 0, reporting how it ended
// on the returned channel, and returns the work item it dispatched.
func startActivity(t *testing.T, ctx context.Context, g *grpcExecutor) (*protos.WorkItem, <-chan error) {
	t.Helper()
	ended := make(chan error, 1)
	g.executeActivityAsync(ctx, api.InstanceID("wf1"), &protos.HistoryEvent{
		EventType: &protos.HistoryEvent_TaskScheduled{TaskScheduled: &protos.TaskScheduledEvent{Name: "act"}},
	}, ExecuteOptions{}, func(_ *protos.HistoryEvent, err error) { ended <- err })
	select {
	case wi := <-g.workItemQueue:
		return wi, ended
	case <-time.After(5 * time.Second):
		require.FailNow(t, "work item was not dispatched")
		return nil, nil
	}
}

// Two executions of the same task sent on different streams: the older one's
// stream disconnecting cancels it and leaves the newer one running.
func Test_streamDisconnectCancelsOnlyItsExecution(t *testing.T) {
	exec, _ := NewGrpcExecutor(&fakeCallbackBackend{}, DefaultLogger())
	g := exec.(*grpcExecutor)

	older, olderEnded := startActivity(t, t.Context(), g)
	newer, newerEnded := startActivity(t, t.Context(), g)
	sendOn(t, g, "s1", older)
	sendOn(t, g, "s2", newer)

	g.cancelStreamTasks("s1")
	select {
	case err := <-olderEnded:
		require.EqualError(t, err, "operation aborted")
	case <-time.After(5 * time.Second):
		require.FailNow(t, "older execution was not cancelled")
	}
	assert.Empty(t, newerEnded, "newer execution on a live stream was cancelled")

	g.cancelStreamTasks("s2")
	select {
	case <-newerEnded:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "newer execution was not cancelled")
	}
}

// An execution that has ended must not tie its stream to a later execution of
// the same task on another stream.
func Test_endedExecutionStreamDoesNotCancelNewer(t *testing.T) {
	exec, _ := NewGrpcExecutor(&fakeCallbackBackend{}, DefaultLogger())
	g := exec.(*grpcExecutor)

	olderCtx, cancelOlder := context.WithCancel(t.Context())
	older, olderEnded := startActivity(t, olderCtx, g)
	newer, newerEnded := startActivity(t, t.Context(), g)
	sendOn(t, g, "s1", older)
	sendOn(t, g, "s2", newer)

	cancelOlder()
	select {
	case <-olderEnded:
	case <-time.After(5 * time.Second):
		require.FailNow(t, "older execution did not end")
	}

	g.cancelStreamTasks("s1")
	assert.Empty(t, newerEnded)
}
