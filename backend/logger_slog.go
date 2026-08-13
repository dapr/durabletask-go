package backend

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"strings"

	kitlogger "github.com/dapr/kit/logger"
)

// SlogFromLogger adapts a [Logger] to a *slog.Logger.
//
// The public constructors keep accepting Logger, which callers such as dapr
// satisfy structurally, so nothing outside this repo has to change; internally
// everything logs through slog with structured attributes. Backend
// implementations outside this package can use this for the same purpose.
func SlogFromLogger(l Logger) *slog.Logger {
	if l == nil {
		l = DefaultLogger()
	}

	// A logger from dapr/kit converts in place: records flow through kit's
	// own handler, keeping the dapr log schema, the scope from the logger's
	// name, and any runtime configuration applied to it.
	if kl, ok := l.(kitlogger.Logger); ok {
		return kitlogger.FromLogger(kl).Logger
	}

	return slog.New(&printfHandler{target: l})
}

// printfHandler renders slog records through a printf-style [Logger], for
// implementations that did not come from dapr/kit, including the stdlib-log
// DefaultLogger. Attributes are appended to the message as key=value pairs,
// which is the best a printf sink can represent, so nothing is dropped.
type printfHandler struct {
	target Logger
	attrs  []slog.Attr
	groups []string
}

var _ slog.Handler = (*printfHandler)(nil)

// Enabled always reports true: Logger has no level-query method, so filtering
// is left to the target implementation.
func (h *printfHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *printfHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	if len(attrs) == 0 {
		return h
	}

	n := *h
	n.attrs = slices.Clip(h.attrs)

	for _, a := range attrs {
		n.attrs = append(n.attrs, h.qualify(a))
	}

	return &n
}

func (h *printfHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}

	n := *h
	n.groups = append(slices.Clip(h.groups), name)

	return &n
}

func (h *printfHandler) qualify(a slog.Attr) slog.Attr {
	if len(h.groups) == 0 {
		return a
	}

	a.Key = strings.Join(h.groups, ".") + "." + a.Key

	return a
}

func (h *printfHandler) Handle(_ context.Context, r slog.Record) error {
	var sb strings.Builder
	sb.WriteString(r.Message)

	appendAttr := func(a slog.Attr) {
		a.Value = a.Value.Resolve()

		if a.Equal(slog.Attr{}) {
			return
		}

		sb.WriteByte(' ')
		sb.WriteString(a.Key)
		sb.WriteByte('=')
		sb.WriteString(a.Value.String())
	}

	for _, a := range h.attrs {
		appendAttr(a)
	}

	r.Attrs(func(a slog.Attr) bool {
		appendAttr(h.qualify(a))
		return true
	})

	msg := sb.String()

	switch {
	case r.Level < slog.LevelInfo:
		h.target.Debug(msg)
	case r.Level < slog.LevelWarn:
		h.target.Info(msg)
	case r.Level < slog.LevelError:
		h.target.Warn(msg)
	default:
		h.target.Error(msg)
	}

	return nil
}

// stringer defers rendering a fmt.Stringer until a record is actually
// emitted, and renders it the same way in both text and JSON encodings. Use it
// for values whose String method is the curated log representation, such as
// work items, where letting the JSON encoder marshal the raw struct would dump
// entire workflow histories into a log line.
type stringer struct{ fmt.Stringer }

func (s stringer) LogValue() slog.Value {
	return slog.StringValue(s.String())
}

// lazyString defers building a string until a record is actually emitted, so
// expensive summaries cost nothing when the level is disabled.
type lazyString func() string

func (f lazyString) LogValue() slog.Value {
	return slog.StringValue(f())
}
