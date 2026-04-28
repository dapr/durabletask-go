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

package workflow

import (
	"github.com/dapr/durabletask-go/api"
	"github.com/dapr/durabletask-go/task"
)

// PropagationOption configures how history is propagated to child
// workflows & activities.
type PropagationOption = api.PropagationOption

// PropagatedHistory is the history propagated from a parent workflow to a
// child workflow or activity.
type PropagatedHistory = api.PropagatedHistory

// PropagateOwnHistory configures propagation to include the caller's own
// history events (activities, child workflows, timers, etc.). The child only
// sees events from the immediate parent, not the ancestor chain.
func PropagateOwnHistory() PropagationOption {
	return api.PropagateOwnHistory()
}

// PropagateLineage configures propagation to include the caller's own history
// events AND the full ancestor chain. Any propagated history this workflow
// received from its own parent is forwarded to the child, preserving the full
// chain of custody.
func PropagateLineage() PropagationOption {
	return api.PropagateLineage()
}

// WithHistoryPropagation creates an option that configures history propagation
// for a child workflow or activity call. It can be passed to both CallActivity
// and CallChildWorkflow. Pass one of PropagateOwnHistory() or PropagateLineage().
//
// Usage:
//
//	ctx.CallChildWorkflow("child",
//	    workflow.WithChildWorkflowInput(input),
//	    workflow.WithHistoryPropagation(workflow.PropagateLineage()),
//	)
//
//	ctx.CallActivity("activity",
//	    workflow.WithActivityInput(input),
//	    workflow.WithHistoryPropagation(workflow.PropagateOwnHistory()),
//	)
func WithHistoryPropagation(opt PropagationOption) task.HistoryPropagation {
	return task.WithHistoryPropagation(opt)
}
