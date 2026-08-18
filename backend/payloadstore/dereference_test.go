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

package payloadstore_test

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/backend/payloadstore"
	"github.com/dapr/durabletask-go/backend/payloadstore/fake"
)

func resultEvent(id int32, result string) *protos.HistoryEvent {
	return &protos.HistoryEvent{
		EventId: id,
		EventType: &protos.HistoryEvent_TaskCompleted{
			TaskCompleted: &protos.TaskCompletedEvent{
				TaskScheduledId: id,
				Result:          wrapperspb.String(result),
			},
		},
	}
}

// offloaded stores payload and returns an event whose payload field
// carries the resulting reference.
func offloaded(t *testing.T, store payloadstore.Store, id int32, payload string) *protos.HistoryEvent {
	t.Helper()
	ref, err := store.Put(t.Context(), "wf1", []byte(payload))
	require.NoError(t, err)
	return resultEvent(id, payloadstore.EncodeReference(ref))
}

func TestDereferenceRestoresPayloads(t *testing.T) {
	t.Parallel()

	store := fake.New()
	const big = "a large offloaded payload"
	const small = "inline"

	events := []*protos.HistoryEvent{
		offloaded(t, store, 0, big),
		resultEvent(1, small),
		{EventId: 2, EventType: &protos.HistoryEvent_TimerFired{TimerFired: &protos.TimerFiredEvent{}}},
	}

	out, err := payloadstore.Dereference(t.Context(), store, "wf1", events)
	require.NoError(t, err)
	require.Len(t, out, len(events))

	assert.Equal(t, big, out[0].GetTaskCompleted().GetResult().GetValue())
	assert.Equal(t, small, out[1].GetTaskCompleted().GetResult().GetValue())
	assert.Same(t, events[2], out[2], "events without payloads pass through untouched")
	assert.Same(t, events[1], out[1], "inline events pass through untouched")
}

// TestDereferenceDoesNotMutateOriginals pins the copy-on-write contract:
// the input events alias the backend's cached, persisted (and possibly
// signed) state, so the reference-carrying originals must stay intact.
func TestDereferenceDoesNotMutateOriginals(t *testing.T) {
	t.Parallel()

	store := fake.New()
	events := []*protos.HistoryEvent{offloaded(t, store, 0, "payload")}
	encoded := events[0].GetTaskCompleted().GetResult().GetValue()

	out, err := payloadstore.Dereference(t.Context(), store, "wf1", events)
	require.NoError(t, err)

	assert.NotSame(t, events[0], out[0], "reference-carrying events must be cloned")
	assert.Equal(t, encoded, events[0].GetTaskCompleted().GetResult().GetValue(),
		"the original event must keep its reference")
	assert.Equal(t, "payload", out[0].GetTaskCompleted().GetResult().GetValue())
}

func TestDereferenceNoReferencesReturnsInput(t *testing.T) {
	t.Parallel()

	events := []*protos.HistoryEvent{resultEvent(0, "inline")}
	out, err := payloadstore.Dereference(t.Context(), fake.New(), "wf1", events)
	require.NoError(t, err)
	assert.Same(t, &events[0], &out[0], "no references: the input slice is returned as-is")
}

// A value carrying the magic prefix but failing a strict decode is user
// data persisted verbatim by the offload pass, and passes through as-is.
func TestDereferenceMalformedMagicPassesThrough(t *testing.T) {
	t.Parallel()

	store := fake.New()
	crafted := payloadstore.EncodeReference(payloadstore.Reference{Key: "x"})
	corrupt := crafted[:len(crafted)-2] + "garbage"
	events := []*protos.HistoryEvent{resultEvent(0, corrupt)}

	out, err := payloadstore.Dereference(t.Context(), store, "wf1", events)
	require.NoError(t, err)
	assert.Equal(t, corrupt, out[0].GetTaskCompleted().GetResult().GetValue())
}

func TestDereferenceGetFailure(t *testing.T) {
	t.Parallel()

	errGet := errors.New("store unavailable")
	store := fake.New()
	events := []*protos.HistoryEvent{offloaded(t, store, 7, "payload")}

	failing := fake.New().WithGetFn(func(context.Context, string, payloadstore.Reference) ([]byte, error) {
		return nil, errGet
	})

	_, err := payloadstore.Dereference(t.Context(), failing, "wf1", events)
	require.ErrorIs(t, err, errGet)
}

// TestDereferenceVerifiesChecksum pins the engine-side defense in depth:
// even if a store implementation skips the mandatory checksum check, the
// engine refuses payload bytes that do not match the signed reference.
func TestDereferenceVerifiesChecksum(t *testing.T) {
	t.Parallel()

	store := fake.New()
	events := []*protos.HistoryEvent{offloaded(t, store, 0, "original")}

	sloppy := fake.New().WithGetFn(func(context.Context, string, payloadstore.Reference) ([]byte, error) {
		return []byte("tampered"), nil
	})

	_, err := payloadstore.Dereference(t.Context(), sloppy, "wf1", events)
	require.ErrorContains(t, err, "checksum")
}

func TestDereferenceManyEvents(t *testing.T) {
	t.Parallel()

	store := fake.New()
	const n = 24
	events := make([]*protos.HistoryEvent, n)
	payloads := make([]string, n)
	for i := range events {
		payloads[i] = fmt.Sprintf("payload-%02d-", i) + strings.Repeat("p", 64)
		events[i] = offloaded(t, store, int32(i), payloads[i]) //nolint:gosec
	}

	out, err := payloadstore.Dereference(t.Context(), store, "wf1", events)
	require.NoError(t, err)
	for i := range out {
		assert.Equal(t, payloads[i], out[i].GetTaskCompleted().GetResult().GetValue())
	}
}

// Checksums in references are computed over the payload bytes; sanity
// check the fixture helper produces resolvable references.
func TestDereferenceFixtureSanity(t *testing.T) {
	t.Parallel()

	store := fake.New()
	e := offloaded(t, store, 0, "abc")
	ref, err := payloadstore.DecodeReference(e.GetTaskCompleted().GetResult().GetValue())
	require.NoError(t, err)
	assert.Equal(t, sha256.Sum256([]byte("abc")), ref.Checksum)
}

func TestResolve(t *testing.T) {
	t.Parallel()

	store := fake.New()
	ref, err := store.Put(t.Context(), "wf1", []byte("payload"))
	require.NoError(t, err)
	encoded := payloadstore.EncodeReference(ref)

	t.Run("resolves a reference", func(t *testing.T) {
		t.Parallel()
		got, resolved, err := payloadstore.Resolve(t.Context(), store, "wf1", encoded)
		require.NoError(t, err)
		assert.True(t, resolved)
		assert.Equal(t, "payload", got)
	})

	t.Run("passes user data through", func(t *testing.T) {
		t.Parallel()
		got, resolved, err := payloadstore.Resolve(t.Context(), store, "wf1", "plain user data")
		require.NoError(t, err)
		assert.False(t, resolved)
		assert.Equal(t, "plain user data", got)
	})

	t.Run("passes malformed magic-prefixed data through", func(t *testing.T) {
		t.Parallel()
		corrupt := encoded[:len(encoded)-2] + "garbage"
		got, resolved, err := payloadstore.Resolve(t.Context(), store, "wf1", corrupt)
		require.NoError(t, err)
		assert.False(t, resolved)
		assert.Equal(t, corrupt, got)
	})

	t.Run("get failure surfaces", func(t *testing.T) {
		t.Parallel()
		failing := fake.New().WithGetFn(func(context.Context, string, payloadstore.Reference) ([]byte, error) {
			return nil, errors.New("store unavailable")
		})
		_, _, err := payloadstore.Resolve(t.Context(), failing, "wf1", encoded)
		require.ErrorContains(t, err, "store unavailable")
	})

	t.Run("checksum mismatch surfaces", func(t *testing.T) {
		t.Parallel()
		sloppy := fake.New().WithGetFn(func(context.Context, string, payloadstore.Reference) ([]byte, error) {
			return []byte("tampered"), nil
		})
		_, _, err := payloadstore.Resolve(t.Context(), sloppy, "wf1", encoded)
		require.ErrorContains(t, err, "checksum")
	})
}

// A nil store disables the feature entirely: references pass through
// unresolved instead of panicking, per the package contract.
func TestDereferenceNilStore(t *testing.T) {
	t.Parallel()

	backing := fake.New()
	events := []*protos.HistoryEvent{offloaded(t, backing, 0, "payload")}
	encoded := events[0].GetTaskCompleted().GetResult().GetValue()

	out, err := payloadstore.Dereference(t.Context(), nil, "wf1", events)
	require.NoError(t, err)
	assert.Equal(t, encoded, out[0].GetTaskCompleted().GetResult().GetValue())
}

func TestResolveNilStore(t *testing.T) {
	t.Parallel()

	backing := fake.New()
	ref, err := backing.Put(t.Context(), "wf1", []byte("payload"))
	require.NoError(t, err)
	encoded := payloadstore.EncodeReference(ref)

	got, resolved, err := payloadstore.Resolve(t.Context(), nil, "wf1", encoded)
	require.NoError(t, err)
	assert.False(t, resolved)
	assert.Equal(t, encoded, got)
}
