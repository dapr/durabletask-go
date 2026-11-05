// Package staged carries a caller's "was this operation staged" destination
// from a request option to the client call that sends the request.
//
// Request options only see the request message, not the response, so an
// option such as api.WithStartStaged registers its destination against the
// request here, and the client takes it back after applying the options.
package staged

import (
	"sync"

	"google.golang.org/protobuf/proto"
)

// sinks maps a request message to the destination registered for it.
var sinks sync.Map

// Register records dst as the destination for whether req was staged, and
// sets it to false until the client reports the result. A nil dst is ignored.
func Register(req proto.Message, dst *bool) {
	if dst == nil {
		return
	}
	*dst = false
	sinks.Store(req, dst)
}

// Sink reports whether an operation was staged to the destination the
// caller registered, if any.
type Sink struct {
	dst *bool
}

// Report sets the registered destination to staged. It does nothing when no
// destination was registered.
func (s Sink) Report(staged bool) {
	if s.dst != nil {
		*s.dst = staged
	}
}

// Configure applies opts to req in order, stopping at the first error, and
// returns the sink registered by any of them. It always removes req's entry,
// so an option error does not leave it behind.
func Configure[R proto.Message, O ~func(R) error](req R, opts []O) (Sink, error) {
	var err error
	for _, configure := range opts {
		if err = configure(req); err != nil {
			break
		}
	}
	var sink Sink
	if v, ok := sinks.LoadAndDelete(req); ok {
		sink.dst = v.(*bool)
	}
	return sink, err
}
