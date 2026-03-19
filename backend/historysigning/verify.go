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
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rsa"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/spiffe/go-spiffe/v2/bundle/x509bundle"
	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"google.golang.org/protobuf/proto"

	"github.com/dapr/durabletask-go/api/protos"
)

// VerifySignature verifies a single HistorySignature against the raw event
// bytes and certificate table. The allRawEvents slice must contain the
// deterministically marshaled bytes of every history event, in order.
func VerifySignature(sig *protos.HistorySignature, certs []*protos.SigningCertificate, allRawEvents [][]byte) error {
	if sig.GetEventCount() == 0 {
		return errors.New("signature has zero event count")
	}

	if sig.GetCertificateIndex() >= uint64(len(certs)) {
		return fmt.Errorf("certificate index %d out of range [0, %d)", sig.GetCertificateIndex(), len(certs))
	}

	certEntry := certs[sig.GetCertificateIndex()]
	cert, err := parseCertificateChainDER(certEntry.GetCertificate())
	if err != nil {
		return fmt.Errorf("failed to parse certificate: %w", err)
	}

	start := sig.GetStartEventIndex()
	end := start + sig.GetEventCount()
	if end > uint64(len(allRawEvents)) {
		return fmt.Errorf("signature event range [%d, %d) exceeds events length %d",
			start, end, len(allRawEvents))
	}

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
	if eventTime.Before(cert.NotBefore) || eventTime.After(cert.NotAfter) {
		return fmt.Errorf("certificate not valid at event time %v (valid %v to %v)",
			eventTime, cert.NotBefore, cert.NotAfter)
	}

	// Recompute events digest
	eventsDigest := EventsDigest(rawSlice)

	if !bytes.Equal(eventsDigest, sig.GetEventsDigest()) {
		return fmt.Errorf("events digest mismatch for range [%d, %d)",
			sig.GetStartEventIndex(), end)
	}

	// Verify the cryptographic signature
	sigInput := SignatureInput(sig.GetPreviousSignatureDigest(), sig.GetEventsDigest())

	if err := verifyBytes(cert.PublicKey, sigInput, sig.GetSignature()); err != nil {
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
	// TrustBundleSource provides the X.509 trust bundle for verifying signing
	// certificates. The trust domain is extracted from each signing
	// certificate's SPIFFE ID (URI SAN).
	TrustBundleSource x509bundle.Source
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

	if opts.TrustBundleSource == nil {
		return errors.New("trust bundle source is required")
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

		if err := VerifySignature(sig, opts.Certs, opts.AllRawEvents); err != nil {
			return fmt.Errorf("signature %d: %w", i, err)
		}

		// Verify certificate chain-of-trust against trust bundle (cached per cert index).
		certIdx := sig.GetCertificateIndex()
		if !verifiedCertIndices[certIdx] {
			if err := verifyCertChainOfTrust(opts.Certs[certIdx].GetCertificate(), opts.TrustBundleSource); err != nil {
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

// verifyCertChainOfTrust verifies that the leaf certificate in chainDER
// chains to a trust anchor from the given bundle source. The trust domain
// is extracted from the leaf certificate's SPIFFE ID (URI SAN).
func verifyCertChainOfTrust(chainDER []byte, bundleSource x509bundle.Source) error {
	certs, err := x509.ParseCertificates(chainDER)
	if err != nil {
		return fmt.Errorf("failed to parse certificate chain: %w", err)
	}
	if len(certs) == 0 {
		return errors.New("certificate chain is empty")
	}

	leaf := certs[0]

	// Extract the SPIFFE ID from the leaf certificate to determine the trust domain.
	spiffeID, err := x509svid.IDFromCert(leaf)
	if err != nil {
		return fmt.Errorf("failed to extract SPIFFE ID from certificate: %w", err)
	}

	bundle, err := bundleSource.GetX509BundleForTrustDomain(spiffeID.TrustDomain())
	if err != nil {
		return fmt.Errorf("failed to get trust bundle for trust domain %q: %w", spiffeID.TrustDomain(), err)
	}

	trustAnchors := bundle.X509Authorities()
	if len(trustAnchors) == 0 {
		return fmt.Errorf("trust bundle for trust domain %q has no X.509 authorities", spiffeID.TrustDomain())
	}

	roots := x509.NewCertPool()
	for _, anchor := range trustAnchors {
		roots.AddCert(anchor)
	}

	intermediates := x509.NewCertPool()
	for _, c := range certs[1:] {
		intermediates.AddCert(c)
	}

	_, err = leaf.Verify(x509.VerifyOptions{
		Roots:         roots,
		Intermediates: intermediates,
		KeyUsages:     []x509.ExtKeyUsage{x509.ExtKeyUsageAny},
		// Use the leaf's NotAfter (minus one minute) as the verification time.
		// This avoids failures both from expired short-lived SVIDs and from
		// backdated NotBefore (sentry backdates SVIDs for clock-skew tolerance,
		// which can place NotBefore before the CA cert's NotBefore). The temporal
		// validity against event timestamps is already checked in VerifySignature.
		CurrentTime: leaf.NotAfter.Add(-time.Minute),
	})
	if err != nil {
		return fmt.Errorf("certificate chain-of-trust verification failed: %w", err)
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

// verifyBytes verifies a signature against the given digest and public key.
// The digest must already be a SHA-256 hash (as returned by SignatureInput).
func verifyBytes(pubKey crypto.PublicKey, digest, sig []byte) error {
	switch k := pubKey.(type) {
	case ed25519.PublicKey:
		if !ed25519.Verify(k, digest, sig) {
			return errors.New("ed25519 signature verification failed")
		}
		return nil
	case *ecdsa.PublicKey:
		byteLen := (k.Curve.Params().BitSize + 7) / 8
		if len(sig) != 2*byteLen {
			return fmt.Errorf("invalid ECDSA signature length: got %d, want %d", len(sig), 2*byteLen)
		}
		r := new(big.Int).SetBytes(sig[:byteLen])
		s := new(big.Int).SetBytes(sig[byteLen:])
		if !ecdsa.Verify(k, digest, r, s) {
			return errors.New("ecdsa signature verification failed")
		}
		return nil
	case *rsa.PublicKey:
		return rsa.VerifyPKCS1v15(k, crypto.SHA256, digest, sig)
	default:
		return fmt.Errorf("unsupported public key type: %T", pubKey)
	}
}
