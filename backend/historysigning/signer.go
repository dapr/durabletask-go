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
	"errors"
	"fmt"

	"google.golang.org/protobuf/proto"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/crypto/spiffe/signer"
)

// SignResult is the output of a signing operation.
type SignResult struct {
	// Signature is the new HistorySignature entry.
	Signature *protos.HistorySignature

	// RawSignature is the deterministically marshaled bytes of the Signature.
	// These are the exact bytes that should be persisted to the state store
	// and used for digest computation in chain linking.
	RawSignature []byte

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
	// PreviousSignatureRaw is the raw serialized bytes of the previous
	// HistorySignature in the chain (nil for root). These must be the exact
	// bytes from the state store or from SignResult.RawSignature.
	PreviousSignatureRaw []byte
	// ExistingCerts is the current certificate table.
	ExistingCerts []*protos.SigningCertificate
}

// Sign creates a HistorySignature covering a range of events. The RawEvents
// field must contain the deterministically marshaled bytes of each event in
// the range (from MarshalEvent). It chains to the previous signature (if any)
// and resolves the certificate index against the existing certificate table.
func Sign(s *signer.Signer, opts SignOptions) (*SignResult, error) {
	if s == nil {
		return nil, errors.New("signer must not be nil")
	}
	if len(opts.RawEvents) == 0 {
		return nil, errors.New("raw events must not be empty")
	}

	eventCount := uint64(len(opts.RawEvents))
	eventsDigest := EventsDigest(opts.RawEvents)

	// Determine previous signature digest from raw bytes.
	var prevSigDigest []byte
	if opts.PreviousSignatureRaw != nil {
		prevSigDigest = SignatureDigest(opts.PreviousSignatureRaw)
	}

	// Compute the signature input
	sigInput := SignatureInput(prevSigDigest, eventsDigest)

	// Sign
	sigBytes, certChainDER, err := s.Sign(sigInput)
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}

	// Resolve certificate index
	certIdx, newCert := resolveCertificateIndex(certChainDER, opts.ExistingCerts)

	sig := &protos.HistorySignature{
		StartEventIndex:         opts.StartEventIndex,
		EventCount:              eventCount,
		PreviousSignatureDigest: prevSigDigest,
		EventsDigest:            eventsDigest,
		CertificateIndex:        certIdx,
		Signature:               sigBytes,
	}

	rawSig, err := proto.MarshalOptions{Deterministic: true}.Marshal(sig)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal HistorySignature: %w", err)
	}

	return &SignResult{
		Signature:        sig,
		RawSignature:     rawSig,
		NewCert:          newCert,
		CertificateIndex: certIdx,
	}, nil
}

// resolveCertificateIndex checks if the current certificate already exists in
// the certificate table. If so, returns its existing index. Otherwise,
// returns a new index and the certificate to append.
func resolveCertificateIndex(certChainDER []byte, existingCerts []*protos.SigningCertificate) (uint64, *protos.SigningCertificate) {
	for i, cert := range existingCerts {
		if bytes.Equal(cert.GetCertificate(), certChainDER) {
			return uint64(i), nil
		}
	}
	newCert := &protos.SigningCertificate{Certificate: certChainDER}
	return uint64(len(existingCerts)), newCert
}
