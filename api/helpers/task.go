package helpers

import (
	"reflect"
	"runtime"
	"strconv"
	"strings"
)

// GenerateChildWorkflowInstanceID generates a deterministic instance ID for a
// child workflow based on the parent instance ID and the action's sequence number.
func GenerateChildWorkflowInstanceID(parentInstanceID string, actionID int32) string {
	// Adding 0x10000 guarantees at least 5 hex digits (e.g. 0 → "10000"),
	// so taking the last 4 gives a zero-padded result (e.g. "0000").
	hex := strconv.FormatInt(0x10000+int64(actionID), 16)
	hex = hex[len(hex)-4:]
	return parentInstanceID + ":" + hex
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
