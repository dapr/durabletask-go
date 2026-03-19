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
	"crypto/sha256"
	"crypto/x509"
	"errors"
	"fmt"
	"math/big"

	"google.golang.org/protobuf/proto"

	"github.com/dapr/durabletask-go/api/protos"
)

// Signer produces HistorySignature entries for a contiguous range of history
// events, chaining to the previous signature in the chain.
type Signer struct {
	// certChainDER is the DER-encoded X.509 certificate chain (leaf first,
	// followed by intermediates). Multiple DER certificates are concatenated.
	certChainDER []byte

	// privateKey is the signing key corresponding to the leaf certificate.
	privateKey crypto.Signer
}

// NewSigner creates a Signer from a DER-encoded certificate chain and private
// key. The certChainDER should contain the leaf certificate first, optionally
// followed by intermediate certificates, all concatenated as raw DER.
func NewSigner(certChainDER []byte, privateKey crypto.Signer) (*Signer, error) {
	if len(certChainDER) == 0 {
		return nil, errors.New("certificate DER is empty")
	}
	if privateKey == nil {
		return nil, errors.New("private key is nil")
	}
	return &Signer{
		certChainDER: certChainDER,
		privateKey:   privateKey,
	}, nil
}

// SignResult is the output of a signing operation.
type SignResult struct {
	// Signature is the new HistorySignature entry.
	Signature *protos.HistorySignature

	// NewCert is non-nil only when the certificate rotated and a new
	// SigningCertificate entry needs to be appended to the certificate table.
	NewCert *protos.SigningCertificate

	// CertificateIndex is the index used in the signature's certificate_index field.
	CertificateIndex uint32
}

// Sign creates a HistorySignature covering a range of events. The RawEvents
// field must contain the deterministically marshaled bytes of each event in
// the range (from MarshalEvent). It chains to the previous signature (if any)
// and resolves the certificate index against the existing certificate table.
func (s *Signer) Sign(opts SignOptions) (*SignResult, error) {
	if len(opts.RawEvents) == 0 {
		return nil, errors.New("raw events must not be empty")
	}

	eventCount := uint32(len(opts.RawEvents))
	eventsDigest := EventsDigest(opts.RawEvents)

	// Determine previous signature digest
	var prevSigDigest []byte
	if opts.PreviousSignature != nil {
		var err error
		prevSigDigest, err = SignatureDigest(opts.PreviousSignature)
		if err != nil {
			return nil, err
		}
	}

	// Compute the signature input
	sigInput := SignatureInput(prevSigDigest, eventsDigest)

	// Sign
	sigBytes, err := s.signBytes(sigInput)
	if err != nil {
		return nil, fmt.Errorf("failed to sign: %w", err)
	}

	// Resolve certificate index
	certIdx, newCert := s.resolveCertificateIndex(opts.ExistingCerts)

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

// SignOptions are the parameters for a signing operation.
type SignOptions struct {
	// RawEvents is the deterministically marshaled bytes of each event to sign.
	// These must come from MarshalEvent.
	RawEvents [][]byte
	// StartEventIndex is the index of the first event in the overall history.
	StartEventIndex uint32
	// PreviousSignature is the previous signature in the chain (nil for root).
	PreviousSignature *protos.HistorySignature
	// ExistingCerts is the current certificate table.
	ExistingCerts []*protos.SigningCertificate
}

// resolveCertificateIndex checks if the current certificate matches the last
// entry in the certificate table. If so, returns that index. Otherwise,
// returns a new index and the certificate to append.
func (s *Signer) resolveCertificateIndex(existingCerts []*protos.SigningCertificate) (uint32, *protos.SigningCertificate) {
	if len(existingCerts) > 0 {
		last := existingCerts[len(existingCerts)-1]
		if bytes.Equal(last.GetCertificate(), s.certChainDER) {
			return uint32(len(existingCerts) - 1), nil
		}
	}
	newCert := &protos.SigningCertificate{Certificate: s.certChainDER}
	return uint32(len(existingCerts)), newCert
}

// signBytes signs the given digest bytes with the private key.
func (s *Signer) signBytes(digest []byte) ([]byte, error) {
	switch k := s.privateKey.(type) {
	case ed25519.PrivateKey:
		return ed25519.Sign(k, digest), nil
	case *ecdsa.PrivateKey:
		// For ECDSA, hash the input first
		hash := sha256.Sum256(digest)
		r, ss, err := ecdsa.Sign(rand.Reader, k, hash[:])
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
		hash := sha256.Sum256(digest)
		return rsa.SignPKCS1v15(rand.Reader, k, crypto.SHA256, hash[:])
	default:
		return nil, fmt.Errorf("unsupported key type: %T", s.privateKey)
	}
}

// VerifySignature verifies a single HistorySignature against the raw event
// bytes and certificate table. The allRawEvents slice must contain the
// deterministically marshaled bytes of every history event, in order.
func VerifySignature(sig *protos.HistorySignature, certs []*protos.SigningCertificate, allRawEvents [][]byte) error {
	if int(sig.GetCertificateIndex()) >= len(certs) {
		return fmt.Errorf("certificate index %d out of range [0, %d)", sig.GetCertificateIndex(), len(certs))
	}

	certEntry := certs[sig.GetCertificateIndex()]
	cert, err := parseCertificateChainDER(certEntry.GetCertificate())
	if err != nil {
		return fmt.Errorf("failed to parse certificate: %w", err)
	}

	end := sig.GetStartEventIndex() + sig.GetEventCount()
	if end > uint32(len(allRawEvents)) {
		return fmt.Errorf("signature event range [%d, %d) exceeds events length %d",
			sig.GetStartEventIndex(), end, len(allRawEvents))
	}

	rawSlice := allRawEvents[sig.GetStartEventIndex():end]

	// Verify the certificate was valid at the time of the last event in the
	// signed range. We unmarshal only the last event to extract its timestamp.
	lastRaw := rawSlice[len(rawSlice)-1]
	var lastEvent protos.HistoryEvent
	if err := proto.Unmarshal(lastRaw, &lastEvent); err != nil {
		return fmt.Errorf("failed to unmarshal last event in range: %w", err)
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

// VerifyChain walks the full signature chain and verifies each signature,
// including chain linkage via previousSignatureDigest. The allRawEvents slice
// must contain the raw marshaled bytes as stored in the state store.
func VerifyChain(signatures []*protos.HistorySignature, certs []*protos.SigningCertificate, allRawEvents [][]byte) error {
	for i, sig := range signatures {
		// Verify chain linkage
		if i == 0 {
			if sig.GetPreviousSignatureDigest() != nil {
				return fmt.Errorf("root signature (index 0) must have nil previousSignatureDigest")
			}
		} else {
			expectedPrevDigest, err := SignatureDigest(signatures[i-1])
			if err != nil {
				return err
			}
			if !bytes.Equal(sig.GetPreviousSignatureDigest(), expectedPrevDigest) {
				return fmt.Errorf("signature %d: previousSignatureDigest does not match digest of signature %d", i, i-1)
			}
		}

		if err := VerifySignature(sig, certs, allRawEvents); err != nil {
			return fmt.Errorf("signature %d: %w", i, err)
		}
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
func verifyBytes(pubKey crypto.PublicKey, digest, sig []byte) error {
	switch k := pubKey.(type) {
	case ed25519.PublicKey:
		if !ed25519.Verify(k, digest, sig) {
			return errors.New("ed25519 signature verification failed")
		}
		return nil
	case *ecdsa.PublicKey:
		hash := sha256.Sum256(digest)
		byteLen := (k.Curve.Params().BitSize + 7) / 8
		if len(sig) != 2*byteLen {
			return fmt.Errorf("invalid ECDSA signature length: got %d, want %d", len(sig), 2*byteLen)
		}
		r := new(big.Int).SetBytes(sig[:byteLen])
		s := new(big.Int).SetBytes(sig[byteLen:])
		if !ecdsa.Verify(k, hash[:], r, s) {
			return errors.New("ecdsa signature verification failed")
		}
		return nil
	case *rsa.PublicKey:
		hash := sha256.Sum256(digest)
		return rsa.VerifyPKCS1v15(k, crypto.SHA256, hash[:], sig)
	default:
		return fmt.Errorf("unsupported public key type: %T", pubKey)
	}
}
