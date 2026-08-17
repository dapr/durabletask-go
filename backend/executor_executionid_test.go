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
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
)

func executionStarted(execID string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventType: &protos.HistoryEvent_ExecutionStarted{
			ExecutionStarted: &protos.ExecutionStartedEvent{
				Name: "test",
				WorkflowInstance: &protos.WorkflowInstance{
					InstanceId:  "instance",
					ExecutionId: wrapperspb.String(execID),
				},
			},
		},
	}
}

func Test_executionID(t *testing.T) {
	t.Parallel()

	other := &protos.HistoryEvent{
		EventType: &protos.HistoryEvent_TaskScheduled{
			TaskScheduled: &protos.TaskScheduledEvent{Name: "act"},
		},
	}

	t.Run("from past events", func(t *testing.T) {
		t.Parallel()
		got := executionID([]*protos.HistoryEvent{executionStarted("exec-1"), other}, []*protos.HistoryEvent{other})
		assert.Equal(t, "exec-1", got.GetValue())
	})

	t.Run("from new events", func(t *testing.T) {
		t.Parallel()
		got := executionID(nil, []*protos.HistoryEvent{executionStarted("exec-2")})
		assert.Equal(t, "exec-2", got.GetValue())
	})

	t.Run("new events win over past so a recreated run reports its own execution", func(t *testing.T) {
		t.Parallel()
		got := executionID(
			[]*protos.HistoryEvent{executionStarted("exec-old")},
			[]*protos.HistoryEvent{executionStarted("exec-new")},
		)
		assert.Equal(t, "exec-new", got.GetValue())
	})

	t.Run("no execution started", func(t *testing.T) {
		t.Parallel()
		assert.Nil(t, executionID([]*protos.HistoryEvent{other}, []*protos.HistoryEvent{other}))
	})

	t.Run("execution started without an execution id", func(t *testing.T) {
		t.Parallel()
		e := executionStarted("ignored")
		e.GetExecutionStarted().GetWorkflowInstance().ExecutionId = nil
		assert.Nil(t, executionID(nil, []*protos.HistoryEvent{e}))
	})
}
