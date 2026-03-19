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
	"crypto/rand"
	"crypto/rsa"
	"errors"
	"fmt"

	"github.com/spiffe/go-spiffe/v2/svid/x509svid"

	"github.com/dapr/durabletask-go/api/protos"
)

// Signer produces HistorySignature entries for a contiguous range of history
// events, chaining to the previous signature in the chain. It fetches the
// current X.509 SVID at sign time, so a single Signer instance can be
// long-lived and will transparently pick up certificate rotations.
type Signer struct {
	// source provides the current X.509 SVID (certificate chain + private key).
	source x509svid.Source
}

// SignResult is the output of a signing operation.
type SignResult struct {
	// Signature is the new HistorySignature entry.
	Signature *protos.HistorySignature

	// NewCert is non-nil only when the certificate rotated and a new
	// SigningCertificate entry needs to be appended to the certificate table.
	NewCert *protos.SigningCertificate

	// CertificateIndex is the index used in the signature's certificate_index field.
	CertificateIndex uint64
}

// SignOptions are the parameters for a signing operation.
type SignOptions struct {
	// RawEvents is the deterministically marshaled bytes of each event to sign.
	// These must come from MarshalEvent.
	RawEvents [][]byte
	// StartEventIndex is the index of the first event in the overall history.
	StartEventIndex uint64
	// PreviousSignature is the previous signature in the chain (nil for root).
	PreviousSignature *protos.HistorySignature
	// ExistingCerts is the current certificate table.
	ExistingCerts []*protos.SigningCertificate
}

// NewSigner creates a Signer that fetches its signing identity from the given
// source at sign time. The source is typically backed by a SPIFFE workload
// API and will automatically reflect certificate rotations.
func NewSigner(source x509svid.Source) *Signer {
	return &Signer{source: source}
}

// Sign creates a HistorySignature covering a range of events. The RawEvents
// field must contain the deterministically marshaled bytes of each event in
// the range (from MarshalEvent). It chains to the previous signature (if any)
// and resolves the certificate index against the existing certificate table.
// The current X.509 SVID is fetched from the source at call time.
func (s *Signer) Sign(opts SignOptions) (*SignResult, error) {
	if len(opts.RawEvents) == 0 {
		return nil, errors.New("raw events must not be empty")
	}

	svid, err := s.source.GetX509SVID()
	if err != nil {
		return nil, fmt.Errorf("failed to get X.509 SVID: %w", err)
	}

	if len(svid.Certificates) == 0 {
		return nil, errors.New("SVID has no certificates")
	}

	// Concatenate the full certificate chain (leaf + intermediates) as DER.
	var certChainDER []byte
	for _, cert := range svid.Certificates {
		certChainDER = append(certChainDER, cert.Raw...)
	}

	eventCount := uint64(len(opts.RawEvents))
	eventsDigest := EventsDigest(opts.RawEvents)

	// Determine previous signature digest
	var prevSigDigest []byte
	if opts.PreviousSignature != nil {
		prevSigDigest, err = SignatureDigest(opts.PreviousSignature)
		if err != nil {
			return nil, err
		}
	}

	// Compute the signature input
	sigInput := SignatureInput(prevSigDigest, eventsDigest)

	// Sign
	sigBytes, err := signWithKey(svid.PrivateKey, sigInput)
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}

	// Resolve certificate index
	certIdx, newCert := resolveCertificateIndex(certChainDER, opts.ExistingCerts)

	return &SignResult{
		Signature: &protos.HistorySignature{
			StartEventIndex:         opts.StartEventIndex,
			EventCount:              eventCount,
			PreviousSignatureDigest: prevSigDigest,
			EventsDigest:            eventsDigest,
			CertificateIndex:        certIdx,
			Signature:               sigBytes,
		},
		NewCert:          newCert,
		CertificateIndex: certIdx,
	}, nil
}

// resolveCertificateIndex checks if the current certificate matches the last
// entry in the certificate table. If so, returns that index. Otherwise,
// returns a new index and the certificate to append.
func resolveCertificateIndex(certChainDER []byte, existingCerts []*protos.SigningCertificate) (uint64, *protos.SigningCertificate) {
	if len(existingCerts) > 0 {
		last := existingCerts[len(existingCerts)-1]
		if bytes.Equal(last.GetCertificate(), certChainDER) {
			return uint64(len(existingCerts) - 1), nil
		}
	}
	newCert := &protos.SigningCertificate{Certificate: certChainDER}
	return uint64(len(existingCerts)), newCert
}

// signWithKey signs the given digest with the private key. The digest must
// already be a SHA-256 hash (as returned by SignatureInput).
func signWithKey(privateKey crypto.Signer, digest []byte) ([]byte, error) {
	switch k := privateKey.(type) {
	case ed25519.PrivateKey:
		return ed25519.Sign(k, digest), nil
	case *ecdsa.PrivateKey:
		// digest is already SHA-256; use it directly as the hash input.
		r, ss, err := ecdsa.Sign(rand.Reader, k, digest)
		if err != nil {
			return nil, err
		}
		// Encode as fixed-size r||s
		byteLen := (k.Curve.Params().BitSize + 7) / 8
		sig := make([]byte, 2*byteLen)
		rBytes := r.Bytes()
		sBytes := ss.Bytes()
		copy(sig[byteLen-len(rBytes):byteLen], rBytes)
		copy(sig[2*byteLen-len(sBytes):], sBytes)
		return sig, nil
	case *rsa.PrivateKey:
		// digest is already SHA-256; pass it directly.
		return rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, digest)
	default:
		return nil, fmt.Errorf("unsupported key type: %T", privateKey)
	}
}
