/*
Copyright 2026 The Dapr Authors
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
    http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
func NewForState(s *protos.WorkflowRuntimeState) Set {
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

func (s Set) populate(events []*protos.HistoryEvent) {
	for _, e := range events {
		if k, id, ok := Of(e); ok {
			s[packKey(k, id)] = struct{}{}
		}
	}
}
