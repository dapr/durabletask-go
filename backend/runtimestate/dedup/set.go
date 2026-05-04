package dedup

import "github.com/dapr/durabletask-go/api/protos"

// setKey packs (kind, id) into a single comparable so Set's underlying map
// uses a trivially-hashable key rather than a struct.
type setKey int64

func packKey(kind Kind, id int32) setKey {
	return setKey(uint64(kind)<<32 | uint64(uint32(id)))
}

// Set is a constant-time duplicate detector for resolution-key events. The
// nil Set is a sentinel: methods on it return false so callers can fall
// back to a linear walk via IsPresent.
type Set map[setKey]struct{}

// New returns an empty Set sized for hint expected entries.
func New(hint int) Set {
	return make(Set, hint)
}

// NewForState returns a Set pre-populated with every resolution key in
// s.OldEvents and s.NewEvents.
func NewForState(s *protos.OrchestrationRuntimeState) Set {
	rs := New(len(s.OldEvents) + len(s.NewEvents))
	rs.populate(s.OldEvents)
	rs.populate(s.NewEvents)
	return rs
}

// Has reports whether (kind, id) is already in the set.
func (s Set) Has(kind Kind, id int32) bool {
	if s == nil {
		return false
	}
	_, ok := s[packKey(kind, id)]
	return ok
}

// Add records (kind, id). Returns true if the key was already present.
func (s Set) Add(kind Kind, id int32) bool {
	if s == nil {
		return false
	}
	k := packKey(kind, id)
	if _, dup := s[k]; dup {
		return true
	}
	s[k] = struct{}{}
	return false
}

// Observe extracts the resolution key from e and records it. Returns true
// if the key was already present. Returns false for events without
// resolution semantics or when called on a nil Set.
func (s Set) Observe(e *protos.HistoryEvent) (duplicate bool) {
	if s == nil {
		return false
	}
	k, id, ok := Of(e)
	if !ok {
		return false
	}
	return s.Add(k, id)
}

func (s Set) populate(events []*protos.HistoryEvent) {
	for _, e := range events {
		if k, id, ok := Of(e); ok {
			s[packKey(k, id)] = struct{}{}
		}
	}
}
