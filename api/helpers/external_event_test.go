package helpers

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/dapr/durabletask-go/api/protos"
)

func TestExternalEventErrorRoundTrip(t *testing.T) {
	want := &protos.TaskFailureDetails{
		ErrorType:    "example.FooBarException",
		ErrorMessage: "approval was rejected",
		StackTrace:   nil,
		InnerFailure: &protos.TaskFailureDetails{
			ErrorType:    "example.InnerException",
			ErrorMessage: "inner failure",
		},
		IsNonRetriable: true,
	}

	encoded, err := EncodeExternalEventError(want)
	require.NoError(t, err)
	require.Contains(t, encoded, ExternalEventErrorPrefix)

	got, isError, err := DecodeExternalEventError(encoded)
	require.NoError(t, err)
	require.True(t, isError)
	require.True(t, proto.Equal(want, got))
}

func TestDecodeExternalEventError_LeavesNormalPayloadUntouched(t *testing.T) {
	const payload = `{"status":"approved"}`

	got, isError, err := DecodeExternalEventError(payload)
	require.NoError(t, err)
	require.False(t, isError)
	require.Nil(t, got)
}

func TestDecodeExternalEventError_RejectsMalformedReservedPayload(t *testing.T) {
	_, isError, err := DecodeExternalEventError(ExternalEventErrorPrefix + "{")
	require.Error(t, err)
	require.True(t, isError)
}

func TestDecodeExternalEventError_RejectsWhitespaceRequiredFields(t *testing.T) {
	_, isError, err := DecodeExternalEventError(ExternalEventErrorPrefix + `{"errorType":" ","errorMessage":"failed"}`)
	require.Error(t, err)
	require.True(t, isError)

	_, isError, err = DecodeExternalEventError(ExternalEventErrorPrefix + `{"errorType":"example.Failure","errorMessage":" "}`)
	require.Error(t, err)
	require.True(t, isError)
}

func TestEncodeExternalEventError_RequiresDetails(t *testing.T) {
	_, err := EncodeExternalEventError(nil)
	require.Error(t, err)

	_, err = EncodeExternalEventError(&protos.TaskFailureDetails{ErrorMessage: "failed"})
	require.Error(t, err)

	_, err = EncodeExternalEventError(&protos.TaskFailureDetails{ErrorType: "example.Failure"})
	require.Error(t, err)

	_, err = EncodeExternalEventError(&protos.TaskFailureDetails{ErrorType: " ", ErrorMessage: "failed"})
	require.Error(t, err)

	_, err = EncodeExternalEventError(&protos.TaskFailureDetails{ErrorType: "example.Failure", ErrorMessage: " "})
	require.Error(t, err)
}
