package helpers

import (
	"fmt"
	"reflect"
	"runtime"
	"strings"
)

// GenerateChildWorkflowInstanceID generates a deterministic instance ID for a
// child workflow based on the parent instance ID and the action's sequence number.
func GenerateChildWorkflowInstanceID(parentInstanceID string, actionID int32) string {
	return fmt.Sprintf("%s:%04x", parentInstanceID, actionID)
}

func GetTaskFunctionName(f any) string {
	if name, ok := f.(string); ok {
		return name
	} else {
		// this gets the full module path (github.com/org/module/package.function)
		name = runtime.FuncForPC(reflect.ValueOf(f).Pointer()).Name()
		startIndex := strings.LastIndexByte(name, '.')
		if startIndex > 0 {
			name = name[startIndex+1:]
		}
		return name
	}
}
