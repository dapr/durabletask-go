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

package payloadstore

import (
	"context"
	"crypto/sha256"
	"fmt"

	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/dapr/durabletask-go/api/protos"
)

// maxConcurrentGets bounds the store reads issued in parallel for one
// dereference pass. A long history replayed cold can carry many
// references; the bound keeps the fan-out to the store reasonable.
const maxConcurrentGets = 8

// Dereference returns events with every payload-store reference resolved
// back to its payload, ready to hand to workflow or activity code. The
// input events are never mutated: they alias the backend's cached,
// persisted (and possibly signed) state, so reference-carrying events are
// replaced by clones with the payload inlined while all other events pass
// through by pointer. When no event carries a reference the input slice
// is returned as-is.
//
// Values that carry the reference magic but fail a strict decode are user
// data the offload pass persisted verbatim, and pass through unchanged.
//
// The payload bytes returned by the store are re-verified against the
// reference's checksum and size here, regardless of the store's own
// mandatory verification, so a misbehaving store cannot feed the workflow
// payload bytes that do not match the (typically signed) reference.
func Dereference(ctx context.Context, store Store, instanceID string, events []*protos.HistoryEvent) ([]*protos.HistoryEvent, error) {
	// Find the events to resolve sequentially - the scan is cheap - then
	// fan out the store reads, which may be remote I/O.
	type job struct {
		idx int
		ref Reference
	}
	var jobs []job
	for i, e := range events {
		p := Payload(e)
		if p == nil {
			continue
		}
		ref, err := DecodeReference(p.GetValue())
		if err != nil {
			continue
		}
		jobs = append(jobs, job{idx: i, ref: ref})
	}
	if len(jobs) == 0 {
		return events, nil
	}

	out := make([]*protos.HistoryEvent, len(events))
	copy(out, events)

	eg, egCtx := errgroup.WithContext(ctx)
	eg.SetLimit(maxConcurrentGets)
	for _, j := range jobs {
		eg.Go(func() error {
			data, err := fetchVerified(egCtx, store, instanceID, j.ref)
			if err != nil {
				return fmt.Errorf("failed to resolve payload of event %d: %w", events[j.idx].GetEventId(), err)
			}

			clone := proto.CloneOf(events[j.idx])
			Payload(clone).Value = string(data)
			out[j.idx] = clone
			return nil
		})
	}
	if err := eg.Wait(); err != nil {
		return nil, err
	}

	return out, nil
}

// Resolve returns the payload for s when it encodes a payload-store
// reference, reporting whether a resolution happened. Ordinary user data
// (including values that carry the reference magic but fail a strict
// decode, which the offload pass persists verbatim) is returned
// unchanged. Use it for single payload values outside of history events,
// such as the input/output fields of workflow metadata.
func Resolve(ctx context.Context, store Store, instanceID, s string) (string, bool, error) {
	ref, err := DecodeReference(s)
	if err != nil {
		return s, false, nil
	}

	data, err := fetchVerified(ctx, store, instanceID, ref)
	if err != nil {
		return "", false, err
	}
	return string(data), true, nil
}

// fetchVerified reads a payload from the store and re-verifies it against
// the reference's checksum and size, regardless of the store's own
// mandatory verification, so a misbehaving store cannot return payload
// bytes that do not match the (typically signed) reference.
func fetchVerified(ctx context.Context, store Store, instanceID string, ref Reference) ([]byte, error) {
	data, err := store.Get(ctx, instanceID, ref)
	if err != nil {
		return nil, err
	}
	if sha256.Sum256(data) != ref.Checksum || uint64(len(data)) != ref.Size {
		return nil, fmt.Errorf("payload for key '%s' failed checksum verification against its reference", ref.Key)
	}
	return data, nil
}
