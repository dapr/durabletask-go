package api

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api/protos"
)

const (
	unspecified = protos.PriorityLevel_PRIORITY_LEVEL_UNSPECIFIED
	low         = protos.PriorityLevel_PRIORITY_LEVEL_LOW
	critical    = protos.PriorityLevel_PRIORITY_LEVEL_CRITICAL
)

func TestWithPriority(t *testing.T) {
	req := new(protos.CreateInstanceRequest)
	require.NoError(t, WithPriority(critical, 300)(req))
	assert.Equal(t, critical, req.GetPriority().GetPriorityLevel())
	assert.Equal(t, uint32(300), req.GetPriority().GetFairnessWeight())

	req = new(protos.CreateInstanceRequest)
	require.NoError(t, WithPriority(unspecified, 5)(req))
	assert.Nil(t, req.GetPriority().PriorityLevel, "an unspecified level is left unset")
	assert.Equal(t, uint32(5), req.GetPriority().GetFairnessWeight())

	req = new(protos.CreateInstanceRequest)
	require.NoError(t, WithPriority(low, 0)(req))
	assert.Nil(t, req.GetPriority().FairnessWeight, "a zero weight is left unset")

	req = new(protos.CreateInstanceRequest)
	require.NoError(t, WithPriority(unspecified, 0)(req))
	assert.Nil(t, req.GetPriority(), "nothing given sets nothing")
}

func TestValidatePriority(t *testing.T) {
	require.NoError(t, ValidatePriority(nil))
	require.NoError(t, ValidatePriority(&protos.Priority{}))
	for _, w := range []uint32{0, 1, 500, 1000} {
		require.NoError(t, ValidatePriority(&protos.Priority{FairnessWeight: &w}), w)
	}
	for _, w := range []uint32{1001, 4294967295} {
		require.Error(t, ValidatePriority(&protos.Priority{FairnessWeight: &w}), w)
	}
	for level, name := range protos.PriorityLevel_name {
		require.NoError(t, ValidatePriority(&protos.Priority{PriorityLevel: protos.PriorityLevel(level).Enum()}), name)
	}
	require.Error(t, ValidatePriority(&protos.Priority{PriorityLevel: protos.PriorityLevel(42).Enum()}), "a value the enum does not have")
}
