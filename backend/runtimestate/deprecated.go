package runtimestate

import (
	"github.com/dapr/durabletask-go/api/protos"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TODO: rm in a future release

// Deprecated: Use NewWorkflowRuntimeState instead.
func NewOrchestrationRuntimeState(instanceID string, customStatus *wrapperspb.StringValue, existingHistory []*protos.HistoryEvent) *protos.WorkflowRuntimeState {
	return NewWorkflowRuntimeState(instanceID, customStatus, existingHistory)
}
