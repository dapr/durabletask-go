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
)

// pendingTasks tracks the executions pending per task key. Cancellation is
// per key and reaches every execution of the task, so a key stays tracked
// until its last execution ends.
type pendingTasks struct {
	lock  sync.Mutex
	byKey map[string]*pendingTask
}

type pendingTask struct {
	instanceID api.InstanceID
	taskID     int32
	executions int
	streams    map[string]struct{}
}

func newPendingTasks() *pendingTasks {
	return &pendingTasks{byKey: make(map[string]*pendingTask)}
}

func (p *pendingTasks) add(key string, iid api.InstanceID, taskID int32) func() {
	p.lock.Lock()
	t, ok := p.byKey[key]
	if !ok {
		t = &pendingTask{instanceID: iid, taskID: taskID, streams: make(map[string]struct{})}
		p.byKey[key] = t
	}
	t.executions++
	p.lock.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			p.lock.Lock()
			defer p.lock.Unlock()
			if t.executions--; t.executions == 0 && p.byKey[key] == t {
				delete(p.byKey, key)
			}
		})
	}
}

func (p *pendingTasks) dispatched(key, streamID string) {
	p.lock.Lock()
	defer p.lock.Unlock()
	if t, ok := p.byKey[key]; ok {
		t.streams[streamID] = struct{}{}
	}
}

// onStream returns the tasks with a work item sent on streamID and forgets
// that stream for them.
func (p *pendingTasks) onStream(streamID string) []pendingTask {
	p.lock.Lock()
	defer p.lock.Unlock()
	var tasks []pendingTask
	for _, t := range p.byKey {
		if _, ok := t.streams[streamID]; ok {
			delete(t.streams, streamID)
			tasks = append(tasks, pendingTask{instanceID: t.instanceID, taskID: t.taskID})
		}
	}
	return tasks
}

func (p *pendingTasks) all() []pendingTask {
	p.lock.Lock()
	defer p.lock.Unlock()
	tasks := make([]pendingTask, 0, len(p.byKey))
	for _, t := range p.byKey {
		tasks = append(tasks, pendingTask{instanceID: t.instanceID, taskID: t.taskID})
	}
	return tasks
}
