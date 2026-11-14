package helpers

import (
	"fmt"
	"strings"

	"google.golang.org/protobuf/encoding/protojson"

	"github.com/dapr/durabletask-go/api/protos"
)

// ExternalEventErrorPrefix identifies a failed external-event notification in
// the existing EventRaised history event's input. It is deliberately versioned
// so future encodings can coexist with histories already persisted by a task hub.
const ExternalEventErrorPrefix = "\x00dapr.workflow.error.v1:"

// EncodeExternalEventError encodes failure details for storage in an EventRaised
// event. Normal external-event payloads are left untouched by this encoding.
func EncodeExternalEventError(failureDetails *protos.TaskFailureDetails) (string, error) {
	if failureDetails == nil {
		return "", fmt.Errorf("workflow error details are required")
	}
	if failureDetails.GetErrorType() == "" {
		return "", fmt.Errorf("workflow error type is required")
	}
	if failureDetails.GetErrorMessage() == "" {
		return "", fmt.Errorf("workflow error message is required")
	}

	data, err := protojson.Marshal(failureDetails)
	if err != nil {
		return "", fmt.Errorf("encode workflow error details: %w", err)
	}

	return ExternalEventErrorPrefix + string(data), nil
}

// DecodeExternalEventError decodes a failed external-event notification. The
// boolean is false for ordinary event payloads. A payload with the reserved
// prefix but invalid details is an error, never silently treated as user data.
func DecodeExternalEventError(input string) (*protos.TaskFailureDetails, bool, error) {
	if !strings.HasPrefix(input, ExternalEventErrorPrefix) {
		return nil, false, nil
	}

	failureDetails := new(protos.TaskFailureDetails)
	data := strings.TrimPrefix(input, ExternalEventErrorPrefix)
	if err := protojson.Unmarshal([]byte(data), failureDetails); err != nil {
		return nil, true, fmt.Errorf("decode workflow error details: %w", err)
	}
	if failureDetails.GetErrorType() == "" {
		return nil, true, fmt.Errorf("workflow error type is required")
	}
	if failureDetails.GetErrorMessage() == "" {
		return nil, true, fmt.Errorf("workflow error message is required")
	}

	return failureDetails, true, nil
}
