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

package historysigning

import (
	"crypto/sha256"
	"encoding/binary"

	"google.golang.org/protobuf/proto"

	"github.com/dapr/durabletask-go/api/protos"
)

// MarshalEvent deterministically marshals a HistoryEvent to bytes.
// The output is stable for a given message within the same binary, making it
// suitable for signing. Events should be marshaled once and the resulting
// bytes used for both signing and persistence.
func MarshalEvent(event *protos.HistoryEvent) ([]byte, error) {
	return proto.MarshalOptions{Deterministic: true}.Marshal(event)
}

// EventsDigest computes the SHA-256 digest of pre-marshaled history event
// bytes. Each event is length-prefixed (big-endian uint64) before being
// written to the hash, preventing ambiguity from concatenation.
// The raw bytes should come from MarshalEvent (at sign time) or directly
// from the state store (at verification time).
func EventsDigest(rawEvents [][]byte) []byte {
	h := sha256.New()
	var lenBuf [8]byte
	for _, b := range rawEvents {
		binary.BigEndian.PutUint64(lenBuf[:], uint64(len(b)))
		h.Write(lenBuf[:])
		h.Write(b)
	}
	return h.Sum(nil)
}

// SignatureDigest computes the SHA-256 digest of a raw serialized
// HistorySignature message. This is the value used in
// previousSignatureDigest chaining. The rawSig bytes must be the exact
// bytes as persisted to the state store — never re-marshal a Go struct,
// as protobuf deterministic marshaling is not stable across binary versions.
func SignatureDigest(rawSig []byte) []byte {
	d := sha256.Sum256(rawSig)
	return d[:]
}

// SignatureInput computes the input to the cryptographic signing operation:
// SHA-256(previousSignatureDigest || eventsDigest).
func SignatureInput(previousSignatureDigest, eventsDigest []byte) []byte {
	h := sha256.New()
	h.Write(previousSignatureDigest)
	h.Write(eventsDigest)
	return h.Sum(nil)
}
