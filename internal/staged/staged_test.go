package staged

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api/protos"
)

type option func(*protos.TerminateRequest) error

func register(dst *bool) option {
	return func(req *protos.TerminateRequest) error {
		Register(req, dst)
		return nil
	}
}

func entries() int {
	n := 0
	sinks.Range(func(any, any) bool { n++; return true })
	return n
}

func TestConfigure(t *testing.T) {
	t.Run("reports to the registered destination", func(t *testing.T) {
		dst := true
		sink, err := Configure(new(protos.TerminateRequest), []option{register(&dst)})
		require.NoError(t, err)
		assert.False(t, dst, "Register resets the destination")
		sink.Report(true)
		assert.True(t, dst)
		assert.Zero(t, entries())
	})

	t.Run("no destination registered", func(t *testing.T) {
		sink, err := Configure(new(protos.TerminateRequest), []option{register(nil)})
		require.NoError(t, err)
		sink.Report(true)
		assert.Zero(t, entries())
	})

	t.Run("an option error still removes the entry", func(t *testing.T) {
		dst := true
		failing := option(func(*protos.TerminateRequest) error { return errors.New("bad") })
		_, err := Configure(new(protos.TerminateRequest), []option{register(&dst), failing})
		require.Error(t, err)
		assert.False(t, dst)
		assert.Zero(t, entries())
	})
}
