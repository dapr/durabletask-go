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
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/crypto/spiffe/signer"
)

// VerifySignature verifies a single HistorySignature against the raw event
// bytes and certificate table. The allRawEvents slice must contain the
// deterministically marshaled bytes of every history event, in order.
func VerifySignature(s *signer.Signer, sig *protos.HistorySignature, certs []*protos.SigningCertificate, allRawEvents [][]byte) error {
	if s == nil {
		return errors.New("signer is required")
	}

	if sig.GetEventCount() == 0 {
		return errors.New("signature has zero event count")
	}

	if sig.GetCertificateIndex() >= uint64(len(certs)) {
		return fmt.Errorf("certificate index %d out of range [0, %d)", sig.GetCertificateIndex(), len(certs))
	}

	certEntry := certs[sig.GetCertificateIndex()]
	certDER := certEntry.GetCertificate()

	// Parse the leaf certificate for the validity window check.
	leaf, err := parseCertificateChainDER(certDER)
	if err != nil {
		return fmt.Errorf("failed to parse certificate: %w", err)
	}

	start := sig.GetStartEventIndex()
	count := sig.GetEventCount()
	total := uint64(len(allRawEvents))

	if start > total {
		return fmt.Errorf("start event index %d exceeds events length %d", start, len(allRawEvents))
	}
	if count > total-start {
		return fmt.Errorf("signature event range [%d, %d) exceeds events length %d",
			start, start+count, len(allRawEvents))
	}

	end := start + count

	rawSlice := allRawEvents[start:end]

	// Verify the certificate was valid at the time of the last event in the
	// signed range. We unmarshal only the last event to extract its timestamp.
	lastRaw := rawSlice[len(rawSlice)-1]
	var lastEvent protos.HistoryEvent
	if err := proto.Unmarshal(lastRaw, &lastEvent); err != nil {
		return fmt.Errorf("failed to unmarshal last event in range: %w", err)
	}

	if lastEvent.GetTimestamp() == nil {
		return fmt.Errorf("last event in range [%d, %d) has no timestamp", start, end)
	}

	eventTime := lastEvent.GetTimestamp().AsTime()
	if eventTime.Before(leaf.NotBefore) || eventTime.After(leaf.NotAfter) {
		return fmt.Errorf("certificate not valid at event time %v (valid %v to %v)",
			eventTime, leaf.NotBefore, leaf.NotAfter)
	}

	// Recompute events digest
	eventsDigest := EventsDigest(rawSlice)

	if !bytes.Equal(eventsDigest, sig.GetEventsDigest()) {
		return fmt.Errorf("events digest mismatch for range [%d, %d)",
			sig.GetStartEventIndex(), end)
	}

	// Verify the cryptographic signature
	sigInput := SignatureInput(sig.GetPreviousSignatureDigest(), sig.GetEventsDigest())

	if err := s.Verify(sigInput, sig.GetSignature(), certDER); err != nil {
		return fmt.Errorf("signature verification failed: %w", err)
	}

	return nil
}

// VerifyChainOptions are the parameters for chain verification.
type VerifyChainOptions struct {
	// Signatures is the ordered list of HistorySignature entries.
	Signatures []*protos.HistorySignature
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
	if len(opts.Signatures) == 0 {
		if len(opts.AllRawEvents) == 0 {
			return nil
		}
		return fmt.Errorf("no signatures but %d events exist", len(opts.AllRawEvents))
	}

	if opts.Signer == nil {
		return errors.New("signer is required")
	}

	// Cache certificate chain-of-trust verification results per certificate
	// index to avoid redundant X.509 parsing and verification on long histories.
	verifiedCertIndices := make(map[uint64]bool)

	var expectedStart uint64
	for i, sig := range opts.Signatures {
		// Verify chain linkage
		if i == 0 {
			if sig.GetPreviousSignatureDigest() != nil {
				return fmt.Errorf("root signature (index 0) must have nil previousSignatureDigest")
			}
		} else {
			expectedPrevDigest, err := SignatureDigest(opts.Signatures[i-1])
			if err != nil {
				return err
			}
			if !bytes.Equal(sig.GetPreviousSignatureDigest(), expectedPrevDigest) {
				return fmt.Errorf("signature %d: previousSignatureDigest does not match digest of signature %d", i, i-1)
			}
		}

		// Verify contiguity
		if sig.GetStartEventIndex() != expectedStart {
			return fmt.Errorf("signature %d: expected start event index %d, got %d", i, expectedStart, sig.GetStartEventIndex())
		}
		expectedStart = sig.GetStartEventIndex() + sig.GetEventCount()

		if err := VerifySignature(opts.Signer, sig, opts.Certs, opts.AllRawEvents); err != nil {
			return fmt.Errorf("signature %d: %w", i, err)
		}

		// Verify certificate chain-of-trust against trust bundle (cached per cert index).
		certIdx := sig.GetCertificateIndex()
		if !verifiedCertIndices[certIdx] {
			if err := opts.Signer.VerifyCertChainOfTrust(opts.Certs[certIdx].GetCertificate()); err != nil {
				return fmt.Errorf("signature %d: %w", i, err)
			}
			verifiedCertIndices[certIdx] = true
		}
	}

	// Verify full coverage
	if expectedStart != uint64(len(opts.AllRawEvents)) {
		return fmt.Errorf("signatures cover events [0, %d) but %d events exist", expectedStart, len(opts.AllRawEvents))
	}

	return nil
}

// parseCertificateChainDER parses a DER-encoded X.509 certificate chain and
// returns the leaf certificate (the first in the chain).
func parseCertificateChainDER(chainDER []byte) (*x509.Certificate, error) {
	certs, err := x509.ParseCertificates(chainDER)
	if err != nil {
		return nil, err
	}
	if len(certs) == 0 {
		return nil, errors.New("certificate chain is empty")
	}
	return certs[0], nil
}
