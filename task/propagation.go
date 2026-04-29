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
	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/api/protos"
)

// HistoryPropagation implements both CallActivityOption and ChildWorkflowOption,
// allowing WithHistoryPropagation to be used with both CallActivity and
// CallChildWorkflow.
type HistoryPropagation struct {
	scope protos.HistoryPropagationScope
}

// WithHistoryPropagation creates an option that configures history propagation
// for a child workflow or activity call. It can be passed to both CallActivity
// and CallChildWorkflow. Pass one of PropagateOwnHistory() or PropagateLineage().
//
// Usage:
//
//	ctx.CallChildWorkflow("child",
//	    task.WithChildWorkflowInput(input),
//	    task.WithHistoryPropagation(task.PropagateLineage()),
//	)
//
//	ctx.CallActivity("activity",
//	    task.WithActivityInput(input),
//	    task.WithHistoryPropagation(task.PropagateOwnHistory()),
//	)
func WithHistoryPropagation(opt api.PropagationOption) HistoryPropagation {
	return HistoryPropagation{
		scope: api.NewHistoryPropagationScope(opt),
	}
}

// applyActivityOption implements CallActivityOption.
func (h HistoryPropagation) applyActivityOption(opts *callActivityOptions) error {
	opts.propagationScope = &h.scope
	return nil
}

// applyChildWorkflowOption implements ChildWorkflowOption.
func (h HistoryPropagation) applyChildWorkflowOption(opts *callChildWorkflowOptions) error {
	opts.propagationScope = &h.scope
	return nil
}
