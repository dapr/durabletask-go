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
	"bytes"
	"crypto/x509"
	"encoding/asn1"
	"errors"
	"fmt"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/crypto/spiffe/signer"
)

// VerifySignature verifies a single HistorySignature against the raw event
// bytes and certificate table. The allRawEvents slice must contain the exact
// raw bytes as stored in the state store for every history event, in order.
// Never re-marshal events from Go structs for verification, as protobuf
// deterministic marshaling is not stable across binary versions.
func VerifySignature(s *signer.Signer, sig *protos.HistorySignature, certs []*protos.SigningCertificate, allRawEvents [][]byte) error {
	_, err := verifySignature(s, sig, certs, allRawEvents)
	return err
}

// verifySignature is the internal implementation that also returns the event
// time of the last event in the signed range, for use in chain-of-trust
// verification.
func verifySignature(s *signer.Signer, sig *protos.HistorySignature, certs []*protos.SigningCertificate, allRawEvents [][]byte) (time.Time, error) {
	var zero time.Time

	if s == nil {
		return zero, errors.New("signer is required")
	}

	if sig.GetEventCount() == 0 {
		return zero, errors.New("signature has zero event count")
	}

	if sig.GetCertificateIndex() >= uint64(len(certs)) {
		return zero, fmt.Errorf("certificate index %d out of range [0, %d)", sig.GetCertificateIndex(), len(certs))
	}

	certEntry := certs[sig.GetCertificateIndex()]
	certDER := certEntry.GetCertificate()

	// Parse the leaf certificate for the validity window check.
	leaf, err := parseCertificateChainDER(certDER)
	if err != nil {
		return zero, fmt.Errorf("failed to parse certificate: %w", err)
	}

	start := sig.GetStartEventIndex()
	count := sig.GetEventCount()
	total := uint64(len(allRawEvents))

	if start > total {
		return zero, fmt.Errorf("start event index %d exceeds events length %d", start, len(allRawEvents))
	}
	if count > total-start {
		return zero, fmt.Errorf("signature event range [%d, %d) exceeds events length %d",
			start, start+count, len(allRawEvents))
	}

	end := start + count

	rawSlice := allRawEvents[start:end]

	// Verify the certificate was valid at the time of the last event in the
	// signed range. We unmarshal only the last event to extract its timestamp.
	lastRaw := rawSlice[len(rawSlice)-1]
	var lastEvent protos.HistoryEvent
	if err := proto.Unmarshal(lastRaw, &lastEvent); err != nil {
		return zero, fmt.Errorf("failed to unmarshal last event in range: %w", err)
	}

	if lastEvent.GetTimestamp() == nil {
		return zero, fmt.Errorf("last event in range [%d, %d) has no timestamp", start, end)
	}

	eventTime := lastEvent.GetTimestamp().AsTime()
	if eventTime.Before(leaf.NotBefore) || eventTime.After(leaf.NotAfter) {
		return zero, fmt.Errorf("certificate not valid at event time %v (valid %v to %v)",
			eventTime, leaf.NotBefore, leaf.NotAfter)
	}

	// Recompute events digest
	eventsDigest := EventsDigest(rawSlice)

	if !bytes.Equal(eventsDigest, sig.GetEventsDigest()) {
		return zero, fmt.Errorf("events digest mismatch for range [%d, %d)",
			sig.GetStartEventIndex(), end)
	}

	// Verify the cryptographic signature
	sigInput := SignatureInput(sig.GetPreviousSignatureDigest(), sig.GetEventsDigest())

	if err := s.VerifySignature(sigInput, sig.GetSignature(), certDER); err != nil {
		return zero, fmt.Errorf("signature verification failed: %w", err)
	}

	return eventTime, nil
}

// VerifyChainOptions are the parameters for chain verification.
type VerifyChainOptions struct {
	// RawSignatures is the raw serialized bytes of each HistorySignature,
	// as stored in the state store or from SignResult.RawSignature. These
	// are the single source of truth — they are both parsed into
	// HistorySignature structs and used for digest computation in chain
	// linking.
	RawSignatures [][]byte
	// Certs is the certificate table (sigcert entries).
	Certs []*protos.SigningCertificate
	// AllRawEvents is the raw marshaled bytes of all history events, in order.
	AllRawEvents [][]byte
	// Signer provides cryptographic verification and certificate chain-of-trust
	// checking.
	Signer *signer.Signer
}

// VerifyChain walks the full signature chain and verifies each signature,
// including chain linkage via previousSignatureDigest, contiguity of event
// ranges, and certificate chain-of-trust against trust anchors.
// The allRawEvents slice must contain the raw marshaled bytes as stored in the
// state store.
func VerifyChain(opts VerifyChainOptions) error {
	if len(opts.RawSignatures) == 0 {
		if len(opts.AllRawEvents) == 0 {
			return nil
		}
		return fmt.Errorf("no signatures but %d events exist", len(opts.AllRawEvents))
	}

	if opts.Signer == nil {
		return errors.New("signer is required")
	}

	// Parse all signatures from raw bytes up front.
	sigs := make([]*protos.HistorySignature, len(opts.RawSignatures))
	for i, raw := range opts.RawSignatures {
		var sig protos.HistorySignature
		if err := proto.Unmarshal(raw, &sig); err != nil {
			return fmt.Errorf("failed to unmarshal signature %d: %w", i, err)
		}
		sigs[i] = &sig
	}

	// certTrustVerified caches the verified time window [min, max] for each
	// certificate index. Since X.509 validity is a continuous interval, if the
	// chain-of-trust has been verified at times T1 and T2, it is also valid
	// for any time T where T1 <= T <= T2. We only re-verify when an event
	// time falls outside the cached window.
	type verifiedWindow struct{ min, max time.Time }
	certTrustVerified := make(map[uint64]verifiedWindow)

	var expectedStart uint64
	for i, sig := range sigs {
		// Verify chain linkage using raw bytes for digest computation.
		if i == 0 {
			if sig.GetPreviousSignatureDigest() != nil {
				return fmt.Errorf("root signature (index 0) must have nil previousSignatureDigest")
			}
		} else {
			expectedPrevDigest := SignatureDigest(opts.RawSignatures[i-1])
			if !bytes.Equal(sig.GetPreviousSignatureDigest(), expectedPrevDigest) {
				return fmt.Errorf("signature %d: previousSignatureDigest does not match digest of signature %d", i, i-1)
			}
		}

		// Verify contiguity
		if sig.GetStartEventIndex() != expectedStart {
			return fmt.Errorf("signature %d: expected start event index %d, got %d", i, expectedStart, sig.GetStartEventIndex())
		}

		eventTime, err := verifySignature(opts.Signer, sig, opts.Certs, opts.AllRawEvents)
		if err != nil {
			return fmt.Errorf("signature %d: %w", i, err)
		}

		expectedStart = sig.GetStartEventIndex() + sig.GetEventCount()

		// Verify certificate chain-of-trust against trust bundle at event
		// time. Skip if the event time falls within the already-verified
		// [min, max] window for this cert.
		certIdx := sig.GetCertificateIndex()
		w, ok := certTrustVerified[certIdx]
		if !ok || eventTime.Before(w.min) || eventTime.After(w.max) {
			if err := opts.Signer.VerifyCertChainOfTrust(opts.Certs[certIdx].GetCertificate(), eventTime); err != nil {
				return fmt.Errorf("signature %d: chain-of-trust verification failed for certificate index %d: %w", i, certIdx, err)
			}
			if !ok {
				certTrustVerified[certIdx] = verifiedWindow{min: eventTime, max: eventTime}
			} else {
				if eventTime.Before(w.min) {
					w.min = eventTime
				}
				if eventTime.After(w.max) {
					w.max = eventTime
				}
				certTrustVerified[certIdx] = w
			}
		}
	}

	// Verify full coverage
	if expectedStart != uint64(len(opts.AllRawEvents)) {
		return fmt.Errorf("signatures cover events [0, %d) but %d events exist", expectedStart, len(opts.AllRawEvents))
	}

	return nil
}

// parseCertificateChainDER parses the leaf (first) certificate from a
// DER-encoded X.509 certificate chain without parsing the remaining
// intermediates/root, since only the leaf is needed for the validity
// window check.
func parseCertificateChainDER(chainDER []byte) (*x509.Certificate, error) {
	if len(chainDER) == 0 {
		return nil, errors.New("certificate chain is empty")
	}

	// Each certificate in the chain is a self-delimiting ASN.1 SEQUENCE.
	// Read only the first structure to extract the leaf.
	var raw asn1.RawValue
	if _, err := asn1.Unmarshal(chainDER, &raw); err != nil {
		return nil, fmt.Errorf("failed to read leaf certificate ASN.1: %w", err)
	}

	return x509.ParseCertificate(raw.FullBytes)
}
