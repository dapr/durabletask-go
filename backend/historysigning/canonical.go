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
	"fmt"

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
// bytes. The raw bytes should come from MarshalEvent (at sign time) or
// directly from the state store (at verification time).
func EventsDigest(rawEvents [][]byte) []byte {
	h := sha256.New()
	for _, b := range rawEvents {
		h.Write(b)
	}
	return h.Sum(nil)
}

// SignatureDigest computes the canonical SHA-256 digest of a HistorySignature
// message. This is the value used in previousSignatureDigest chaining.
func SignatureDigest(sig *protos.HistorySignature) ([]byte, error) {
	b, err := proto.MarshalOptions{Deterministic: true}.Marshal(sig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal HistorySignature: %v", err)
	}
	d := sha256.Sum256(b)
	return d[:], nil
}

// SignatureInput computes the input to the cryptographic signing operation:
// SHA-256(previousSignatureDigest || eventsDigest).
func SignatureInput(previousSignatureDigest, eventsDigest []byte) []byte {
	h := sha256.New()
	h.Write(previousSignatureDigest)
	h.Write(eventsDigest)
	return h.Sum(nil)
}
