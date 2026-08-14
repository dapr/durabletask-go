package backend

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	kitlogger "github.com/dapr/kit/logger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestSlogFromLoggerKitFastPath pins that a logger from dapr/kit converts in
// place: records keep the dapr log schema and the scope from the logger's
// name. This is what daprd relies on for its workflow logs.
func TestSlogFromLoggerKitFastPath(t *testing.T) {
	var buf bytes.Buffer

	kl := kitlogger.NewLogger("dapr.wfengine.durabletask.backend.test")
	kl.EnableJSONOutput(true)
	kl.SetOutputLevel(kitlogger.DebugLevel)
	kl.SetOutput(&buf)

	sl := SlogFromLogger(kl)
	sl.Info("worker started", "worker", "workflow-processor")

	var o map[string]any
	require.NoError(t, json.Unmarshal(buf.Bytes(), &o))

	assert.Equal(t, "dapr.wfengine.durabletask.backend.test", o["scope"])
	assert.Equal(t, "worker started", o["msg"])
	assert.Equal(t, "workflow-processor", o["worker"])
	assert.Equal(t, "info", o["level"])
}

// TestSlogFromLoggerPrintfBridge pins that a non-kit Logger implementation
// still receives output, with attributes rendered into the message.
func TestSlogFromLoggerPrintfBridge(t *testing.T) {
	rec := &recordingLogger{}

	sl := SlogFromLogger(rec)
	sl.Warn("dropping event", "instance_id", "abc")

	require.Len(t, rec.warns, 1)
	assert.Equal(t, "dropping event instance_id=abc", rec.warns[0])
}

// TestSlogFromLoggerNil pins that a nil Logger falls back to the default
// rather than panicking.
func TestSlogFromLoggerNil(t *testing.T) {
	assert.NotPanics(t, func() {
		SlogFromLogger(nil).Debug("no sink")
	})
}

type recordingLogger struct {
	warns []string
}

func (r *recordingLogger) Debug(v ...any)            {}
func (r *recordingLogger) Debugf(f string, v ...any) {}
func (r *recordingLogger) Info(v ...any)             {}
func (r *recordingLogger) Infof(f string, v ...any)  {}
func (r *recordingLogger) Warn(v ...any)             { r.warns = append(r.warns, join(v)) }
func (r *recordingLogger) Warnf(f string, v ...any)  {}
func (r *recordingLogger) Error(v ...any)            {}
func (r *recordingLogger) Errorf(f string, v ...any) {}

func join(v []any) string {
	parts := make([]string, len(v))
	for i, x := range v {
		parts[i], _ = x.(string)
	}

	return strings.Join(parts, "")
}
