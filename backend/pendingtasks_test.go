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
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api"
)

func Test_pendingTasksKeepsKeyUntilLastExecutionEnds(t *testing.T) {
	p := newPendingTasks()
	doneOlder := p.add("k", api.InstanceID("wf1"), 3)
	doneNewer := p.add("k", api.InstanceID("wf1"), 3)
	p.dispatched("k", "s1")

	doneNewer()
	doneNewer()
	require.Equal(t, []pendingTask{{instanceID: "wf1", taskID: 3}}, p.all())

	require.Len(t, p.onStream("s1"), 1)
	assert.Empty(t, p.onStream("s1"))
	require.Len(t, p.all(), 1)

	doneOlder()
	assert.Empty(t, p.all())
	p.dispatched("k", "s1")
	assert.Empty(t, p.onStream("s1"))
}

func Test_pendingTasksOnStreamMatchesOnlyThatStream(t *testing.T) {
	p := newPendingTasks()
	defer p.add("a", api.InstanceID("a"), 0)()
	defer p.add("b", api.InstanceID("b"), 0)()
	p.dispatched("a", "s1")
	p.dispatched("b", "s2")

	assert.Equal(t, []pendingTask{{instanceID: "a"}}, p.onStream("s1"))
	assert.Equal(t, []pendingTask{{instanceID: "b"}}, p.onStream("s2"))
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
				done := p.add("k", api.InstanceID("wf1"), 0)
				p.dispatched("k", stream)
				p.onStream(stream)
				p.all()
				done()
			}
		}()
	}
	wg.Wait()
	assert.Empty(t, p.all())
}
