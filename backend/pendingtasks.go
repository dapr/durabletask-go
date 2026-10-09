package backend

import (
	"sync"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

// pendingTasks tracks every pending execution by the work item it sends, so
// several executions of the same task are each tracked on their own stream.
type pendingTasks struct {
	lock       sync.Mutex
	executions map[*protos.WorkItem]*execution
	latest     map[string]*protos.WorkItem
}

type pendingTask struct {
	instanceID api.InstanceID
	taskID     int32
}

type execution struct {
	key      string
	task     pendingTask
	streamID string
	cancel   func()
}

func newPendingTasks() *pendingTasks {
	return &pendingTasks{
		executions: make(map[*protos.WorkItem]*execution),
		latest:     make(map[string]*protos.WorkItem),
	}
}

func (p *pendingTasks) add(key string, wi *protos.WorkItem, iid api.InstanceID, taskID int32, cancel func()) func() {
	p.lock.Lock()
	p.executions[wi] = &execution{key: key, task: pendingTask{instanceID: iid, taskID: taskID}, cancel: cancel}
	p.latest[key] = wi
	p.lock.Unlock()

	return func() {
		p.lock.Lock()
		defer p.lock.Unlock()
		delete(p.executions, wi)
		if p.latest[key] == wi {
			delete(p.latest, key)
		}
	}
}

func (p *pendingTasks) dispatched(wi *protos.WorkItem, streamID string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if e, ok := p.executions[wi]; ok {
		e.streamID = streamID
	}
}

// isLatest reports whether wi is still pending and is the most recent
// execution of its task.
func (p *pendingTasks) isLatest(wi *protos.WorkItem) bool {
	p.lock.Lock()
	defer p.lock.Unlock()
	e, ok := p.executions[wi]
	return ok && p.latest[e.key] == wi
}

// onStream returns the cancel funcs of the executions sent on streamID.
func (p *pendingTasks) onStream(streamID string) []func() {
	p.lock.Lock()
	defer p.lock.Unlock()
	var cancels []func()
	for _, e := range p.executions {
		if e.streamID == streamID {
			cancels = append(cancels, e.cancel)
		}
	}
	return cancels
}

func (p *pendingTasks) all() []pendingTask {
	p.lock.Lock()
	defer p.lock.Unlock()
	tasks := make(map[pendingTask]struct{})
	for _, e := range p.executions {
		tasks[e.task] = struct{}{}
	}
	out := make([]pendingTask, 0, len(tasks))
	for t := range tasks {
		out = append(out, t)
	}
	return out
}
