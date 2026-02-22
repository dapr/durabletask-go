package helpers

import (
	"reflect"
	"strconv"
	"strings"

	"github.com/dapr/durabletask-go/api/protos"
)

func HistoryListSummary(list []*protos.HistoryEvent) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, e := range list {
		if i > 0 {
			sb.WriteString(", ")
		}
		if i >= 10 {
			sb.WriteString("...")
			break
		}
		name := getHistoryEventTypeName(e)
		sb.WriteString(name)
		taskID := GetTaskId(e)
		if taskID > -0 {
			sb.WriteRune('#')
			sb.WriteString(strconv.FormatInt(int64(taskID), 10))
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func ActionListSummary(actions []*protos.WorkflowAction) string {
	var sb strings.Builder
	sb.WriteString("[")
	for i, a := range actions {
		if i > 0 {
			sb.WriteString(", ")
		}
		if i >= 10 {
			sb.WriteString("...")
			break
		}
		name := getActionTypeName(a)
		sb.WriteString(name)
		if a.Id >= 0 {
			sb.WriteRune('#')
			sb.WriteString(strconv.FormatInt(int64(a.Id), 10))
		}
	}
	sb.WriteString("]")
	return sb.String()
}

func GetTaskId(e *protos.HistoryEvent) int32 {
	if e.EventId >= 0 {
		return e.EventId
	} else if x := e.GetTaskCompleted(); x != nil {
		return x.TaskScheduledId
	} else if x := e.GetTaskFailed(); x != nil {
		return x.TaskScheduledId
	} else if x := e.GetSubWorkflowInstanceCompleted(); x != nil {
		return x.TaskScheduledId
	} else if x := e.GetSubWorkflowInstanceFailed(); x != nil {
		return x.TaskScheduledId
	} else if x := e.GetTimerFired(); x != nil {
		return x.TimerId
	} else if x := e.GetExecutionStarted().GetParentInstance(); x != nil {
		return x.TaskScheduledId
	} else {
		return -1
	}
}

func ToRuntimeStatusString(status protos.WorkflowStatus) string {
	name := protos.WorkflowStatus_name[int32(status)]
	return name[len("WORKFLOW_STATUS_"):]
}

func FromRuntimeStatusString(status string) protos.WorkflowStatus {
	runtimeStatus := "WORKFLOW_STATUS_" + status
	return protos.WorkflowStatus(protos.WorkflowStatus_value[runtimeStatus])
}

func getHistoryEventTypeName(e *protos.HistoryEvent) string {
	// PERFORMANCE: Replace this with a switch statement or a map lookup to avoid this use of reflection
	return reflect.TypeOf(e.EventType).Elem().Name()[len("HistoryEvent_"):]
}

func getActionTypeName(a *protos.WorkflowAction) string {
	// PERFORMANCE: Replace this with a switch statement or a map lookup to avoid this use of reflection
	return reflect.TypeOf(a.WorkflowActionType).Elem().Name()[len("WorkflowAction_"):]
}
