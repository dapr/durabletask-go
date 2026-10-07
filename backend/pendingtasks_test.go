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
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

func Test_pendingTasksTracksEachExecution(t *testing.T) {
	p := newPendingTasks()
	older, newer := &protos.WorkItem{}, &protos.WorkItem{}
	doneOlder := p.add(older, api.InstanceID("wf1"), 3)
	doneNewer := p.add(newer, api.InstanceID("wf1"), 3)
	p.dispatched(older, "s1")
	p.dispatched(newer, "s2")
	assert.Equal(t, []pendingTask{{instanceID: "wf1", taskID: 3}}, p.all())

	doneOlder()
	assert.Empty(t, p.onStream("s1"))
	assert.Equal(t, []pendingTask{{instanceID: "wf1", taskID: 3}}, p.onStream("s2"))
	assert.Empty(t, p.onStream("s2"))
	assert.Len(t, p.all(), 1)

	doneNewer()
	assert.Empty(t, p.all())
	p.dispatched(newer, "s2")
	assert.Empty(t, p.onStream("s2"))
}

func Test_pendingTasksConcurrentUse(t *testing.T) {
	p := newPendingTasks()
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stream := strconv.Itoa(i % 2)
			for range 200 {
				wi := &protos.WorkItem{}
				done := p.add(wi, api.InstanceID("wf1"), 0)
				p.dispatched(wi, stream)
				p.onStream(stream)
				p.all()
				done()
			}
		}()
	}
	wg.Wait()
	assert.Empty(t, p.all())
}
