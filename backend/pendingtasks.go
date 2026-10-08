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
	"sync"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

// pendingTasks tracks every pending execution by the work item it sends, so
// several executions of the same task are each tracked on their own stream.
type pendingTasks struct {
	lock       sync.Mutex
	executions map[*protos.WorkItem]*execution
}

type pendingTask struct {
	instanceID api.InstanceID
	taskID     int32
}

type execution struct {
	task     pendingTask
	streamID string
	cancel   func()
}

func newPendingTasks() *pendingTasks {
	return &pendingTasks{executions: make(map[*protos.WorkItem]*execution)}
}

func (p *pendingTasks) add(wi *protos.WorkItem, iid api.InstanceID, taskID int32, cancel func()) func() {
	p.lock.Lock()
	p.executions[wi] = &execution{task: pendingTask{instanceID: iid, taskID: taskID}, cancel: cancel}
	p.lock.Unlock()

	return func() {
		p.lock.Lock()
		delete(p.executions, wi)
		p.lock.Unlock()
	}
}

func (p *pendingTasks) dispatched(wi *protos.WorkItem, streamID string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if e, ok := p.executions[wi]; ok {
		e.streamID = streamID
	}
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
