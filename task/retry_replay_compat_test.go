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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
)

// Test_RetryPolicy_ReplayCompatWithAwaitDrivenHistories replays histories recorded by the
// Await-driven retry design (retries decided inside taskWrapper.Await) against the pollable retry
// chain. Each history was captured from that design by running the same workflow function and
// feeding one completion per turn; the action sequence numbers embedded in it must line up with
// what this implementation emits, or replay fails with a nondeterminism error instead of reaching
// the expected CompleteWorkflow action. This is what lets a workflow mid-retry at upgrade time
// finish under the new code.
//
// The helpers below are self-contained so this file can be dropped into a checkout of either
// design and run unchanged.
func Test_RetryPolicy_ReplayCompatWithAwaitDrivenHistories(t *testing.T) {
	policy := func(maxAttempts int) *RetryPolicy {
		return &RetryPolicy{MaxAttempts: maxAttempts, InitialRetryInterval: time.Second, BackoffCoefficient: 2}
	}
	awaitAll := func(tasks ...Task) error {
		for _, tk := range tasks {
			if err := tk.Await(nil); err != nil {
				return err
			}
		}
		return nil
	}

	for name, tc := range map[string]struct {
		wf         func(ctx *WorkflowContext) (any, error)
		history    []*protos.HistoryEvent
		completeID int32
		status     protos.OrchestrationStatus
	}{
		"single activity retried twice": {
			wf: func(ctx *WorkflowContext) (any, error) {
				return nil, ctx.CallActivity("A", WithActivityRetryPolicy(policy(5))).Await(nil)
			},
			history: []*protos.HistoryEvent{
				cEvTaskScheduled(0, "A"), cEvTaskFailed(0), cEvRetryTimerCreated(1), cEvTimerFired(1),
				cEvTaskScheduled(2, "A"), cEvTaskFailed(2), cEvRetryTimerCreated(3), cEvTimerFired(3),
				cEvTaskScheduled(4, "A"), cEvTaskCompleted(4),
			},
			completeID: 5,
			status:     protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		},
		"retries exhausted": {
			wf: func(ctx *WorkflowContext) (any, error) {
				return nil, ctx.CallActivity("A", WithActivityRetryPolicy(policy(3))).Await(nil)
			},
			history: []*protos.HistoryEvent{
				cEvTaskScheduled(0, "A"), cEvTaskFailed(0), cEvRetryTimerCreated(1), cEvTimerFired(1),
				cEvTaskScheduled(2, "A"), cEvTaskFailed(2), cEvRetryTimerCreated(3), cEvTimerFired(3),
				cEvTaskScheduled(4, "A"), cEvTaskFailed(4),
			},
			completeID: 5,
			status:     protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED,
		},
		"fan-out awaited out of declaration order": {
			// A fails once, B twice, C once; awaited as C, A, B. Every failure lands before
			// anything is awaited, so each retry timer's id is decided by Await order alone.
			wf: func(ctx *WorkflowContext) (any, error) {
				a := ctx.CallActivity("A", WithActivityRetryPolicy(policy(5)))
				b := ctx.CallActivity("B", WithActivityRetryPolicy(policy(5)))
				c := ctx.CallActivity("C", WithActivityRetryPolicy(policy(5)))
				return nil, awaitAll(c, a, b)
			},
			history: []*protos.HistoryEvent{
				cEvTaskScheduled(0, "A"), cEvTaskScheduled(1, "B"), cEvTaskScheduled(2, "C"),
				cEvTaskFailed(0), cEvTaskFailed(1), cEvTaskFailed(2),
				cEvRetryTimerCreated(3), cEvTimerFired(3), cEvTaskScheduled(4, "C"), cEvTaskCompleted(4),
				cEvRetryTimerCreated(5), cEvTimerFired(5), cEvTaskScheduled(6, "A"), cEvTaskCompleted(6),
				cEvRetryTimerCreated(7), cEvTimerFired(7), cEvTaskScheduled(8, "B"), cEvTaskFailed(8),
				cEvRetryTimerCreated(9), cEvTimerFired(9), cEvTaskScheduled(10, "B"), cEvTaskCompleted(10),
			},
			completeID: 11,
			status:     protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		},
		"failure lands before a plain task is awaited": {
			// A fails while the workflow is awaiting B; C is scheduled before A's failure is
			// observed, so C takes id 2 and A's first retry timer id 3.
			wf: func(ctx *WorkflowContext) (any, error) {
				a := ctx.CallActivity("A", WithActivityRetryPolicy(policy(5)))
				b := ctx.CallActivity("B")
				if err := b.Await(nil); err != nil {
					return nil, err
				}
				c := ctx.CallActivity("C")
				return nil, awaitAll(a, c)
			},
			history: []*protos.HistoryEvent{
				cEvTaskScheduled(0, "A"), cEvTaskScheduled(1, "B"), cEvTaskFailed(0), cEvTaskCompleted(1),
				cEvTaskScheduled(2, "C"), cEvRetryTimerCreated(3), cEvTaskCompleted(2), cEvTimerFired(3),
				cEvTaskScheduled(4, "A"), cEvTaskFailed(4), cEvRetryTimerCreated(5), cEvTimerFired(5),
				cEvTaskScheduled(6, "A"), cEvTaskCompleted(6),
			},
			completeID: 7,
			status:     protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		},
		"retry interleaved with a plain timer": {
			wf: func(ctx *WorkflowContext) (any, error) {
				a := ctx.CallActivity("A", WithActivityRetryPolicy(policy(5)))
				if err := ctx.CreateTimer(time.Second).Await(nil); err != nil {
					return nil, err
				}
				b := ctx.CallActivity("B")
				return nil, awaitAll(a, b)
			},
			history: []*protos.HistoryEvent{
				cEvTaskScheduled(0, "A"), cEvPlainTimerCreated(1), cEvTaskFailed(0), cEvTimerFired(1),
				cEvTaskScheduled(2, "B"), cEvRetryTimerCreated(3), cEvTaskCompleted(2), cEvTimerFired(3),
				cEvTaskScheduled(4, "A"), cEvTaskFailed(4), cEvRetryTimerCreated(5), cEvTimerFired(5),
				cEvTaskScheduled(6, "A"), cEvTaskCompleted(6),
			},
			completeID: 7,
			status:     protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		},
		"child workflow retried twice": {
			wf: func(ctx *WorkflowContext) (any, error) {
				return nil, ctx.CallChildWorkflow("Child", WithChildWorkflowRetryPolicy(policy(4))).Await(nil)
			},
			history: []*protos.HistoryEvent{
				cEvChildCreated(0, "Child"), cEvChildFailed(0), cEvChildRetryTimerCreated(1), cEvTimerFired(1),
				cEvChildCreated(2, "Child"), cEvChildFailed(2), cEvChildRetryTimerCreated(3), cEvTimerFired(3),
				cEvChildCreated(4, "Child"), cEvChildCompleted(4),
			},
			completeID: 5,
			status:     protos.OrchestrationStatus_ORCHESTRATION_STATUS_COMPLETED,
		},
	} {
		t.Run(name, func(t *testing.T) {
			r := NewTaskRegistry()
			require.NoError(t, r.AddWorkflowN("wf", tc.wf))
			for _, a := range []string{"A", "B", "C"} {
				require.NoError(t, r.AddActivityN(a, func(ActivityContext) (any, error) { return nil, nil }))
			}
			require.NoError(t, r.AddWorkflowN("Child", func(*WorkflowContext) (any, error) { return nil, nil }))

			ctx := NewWorkflowContext(r, "compat", append([]*protos.HistoryEvent{cEvExecutionStarted("wf")}, tc.history...), nil)
			actions := ctx.start()

			require.Len(t, actions, 1, "a fully recorded history must leave only the completion to emit, got %v", actions)
			co := actions[0].GetCompleteWorkflow()
			require.NotNil(t, co, "expected CompleteWorkflow, got %v", actions[0])
			assert.Equal(t, tc.completeID, actions[0].Id)
			assert.Equal(t, tc.status, co.WorkflowStatus, co.GetFailureDetails().GetErrorMessage())
			if tc.status == protos.OrchestrationStatus_ORCHESTRATION_STATUS_FAILED {
				assert.NotContains(t, co.GetFailureDetails().GetErrorMessage(), "a previous execution called")
			}
		})
	}
}

func cEvExecutionStarted(name string) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ExecutionStarted{
		ExecutionStarted: &protos.ExecutionStartedEvent{Name: name, WorkflowInstance: &protos.WorkflowInstance{InstanceId: "compat"}},
	}}
}

func cEvTaskScheduled(id int32, name string) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventId: id, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_TaskScheduled{
		TaskScheduled: &protos.TaskScheduledEvent{Name: name},
	}}
}

func cEvTaskCompleted(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_TaskCompleted{
		TaskCompleted: &protos.TaskCompletedEvent{TaskScheduledId: id, Result: wrapperspb.String(`null`)},
	}}
}

func cEvTaskFailed(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_TaskFailed{
		TaskFailed: &protos.TaskFailedEvent{TaskScheduledId: id, TaskExecutionId: "exec", FailureDetails: &protos.TaskFailureDetails{ErrorType: "TestError", ErrorMessage: "injected"}},
	}}
}

func cEvTimerFired(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_TimerFired{
		TimerFired: &protos.TimerFiredEvent{TimerId: id, FireAt: timestamppb.Now()},
	}}
}

func cEvTimerCreated(id int32, tc *protos.TimerCreatedEvent) *protos.HistoryEvent {
	tc.FireAt = timestamppb.Now()
	return &protos.HistoryEvent{EventId: id, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_TimerCreated{TimerCreated: tc}}
}

func cEvPlainTimerCreated(id int32) *protos.HistoryEvent {
	return cEvTimerCreated(id, &protos.TimerCreatedEvent{Origin: &protos.TimerCreatedEvent_CreateTimer{CreateTimer: &protos.TimerOriginCreateTimer{}}})
}

func cEvRetryTimerCreated(id int32) *protos.HistoryEvent {
	return cEvTimerCreated(id, &protos.TimerCreatedEvent{Origin: &protos.TimerCreatedEvent_ActivityRetry{ActivityRetry: &protos.TimerOriginActivityRetry{TaskExecutionId: "exec"}}})
}

func cEvChildRetryTimerCreated(id int32) *protos.HistoryEvent {
	return cEvTimerCreated(id, &protos.TimerCreatedEvent{Origin: &protos.TimerCreatedEvent_ChildWorkflowRetry{ChildWorkflowRetry: &protos.TimerOriginChildWorkflowRetry{}}})
}

func cEvChildCreated(id int32, name string) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventId: id, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ChildWorkflowInstanceCreated{
		ChildWorkflowInstanceCreated: &protos.ChildWorkflowInstanceCreatedEvent{Name: name},
	}}
}

func cEvChildCompleted(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ChildWorkflowInstanceCompleted{
		ChildWorkflowInstanceCompleted: &protos.ChildWorkflowInstanceCompletedEvent{TaskScheduledId: id, Result: wrapperspb.String(`null`)},
	}}
}

func cEvChildFailed(id int32) *protos.HistoryEvent {
	return &protos.HistoryEvent{EventId: -1, Timestamp: timestamppb.Now(), EventType: &protos.HistoryEvent_ChildWorkflowInstanceFailed{
		ChildWorkflowInstanceFailed: &protos.ChildWorkflowInstanceFailedEvent{TaskScheduledId: id, FailureDetails: &protos.TaskFailureDetails{ErrorType: "TestError", ErrorMessage: "injected"}},
	}}
}
