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
	"errors"
	"fmt"
	"strings"

	"github.com/spiffe/go-spiffe/v2/svid/x509svid"
	"google.golang.org/protobuf/proto"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/crypto/spiffe/signer"
)

// BuildSignedChunk attaches a producer app's signing artifacts to a single
// PropagatedHistoryChunk that already has RawEvents populated by the
// assembler. rawSigs is the deterministic raw bytes of every HistorySignature
// covering chunk.RawEvents in order. certChainsDER is the DER-concatenated
// cert chain for each signing identity referenced by the signatures'
// certificateIndex. Receivers digest chunk.RawEvents directly and never
// re-marshal, so chunk verification does not depend on protobuf marshaler-
// version stability across producer and receiver - the signed bytes travel
// verbatim end-to-end.
func BuildSignedChunk(chunk *protos.PropagatedHistoryChunk, rawSigs, certChainsDER [][]byte) error {
	if chunk == nil {
		return errors.New("chunk must not be nil")
	}
	chunk.RawSignatures = rawSigs
	chunk.SigningCertChains = certChainsDER
	return nil
}

// VerifyPropagationOptions configures VerifyPropagatedHistory.
type VerifyPropagationOptions struct {
	// History is the propagated payload to verify. Must not be nil.
	History *protos.PropagatedHistory

	// Signer provides cryptographic verification and certificate
	// chain-of-trust checking against Sentry trust anchors.
	Signer *signer.Signer

	// ExpectedNamespace is the namespace each chunk's signing cert must be
	// scoped to. Without this, a holder of a Sentry-issued cert for the
	// same app-id in a different namespace could forge propagation chunks.
	// Must be non-empty.
	ExpectedNamespace string
}

// PropagationVerifyResult is the outcome of a successful
// VerifyPropagatedHistory call. It surfaces the certs that were referenced by
// verified signatures so the caller can absorb them into a content-addressed
// foreign-cert table (ext-sigcert).
type PropagationVerifyResult struct {
	// VerifiedCerts maps sha256(certChainDER) -> certChainDER for every
	// signing cert that (a) was referenced by at least one verified
	// HistorySignature.certificateIndex and (b) passed chain-of-trust during
	// this call. Unreferenced cert chains attached to a chunk are not
	// included. The digest is the raw 32-byte sha256 as a string key.
	VerifiedCerts map[string][]byte
}

// VerifyPropagatedHistory verifies every chunk in a PropagatedHistory.
// Each chunk is self-contained - it carries its own rawEvents along with
// rawSignatures and signingCertChains - so chunks are verified
// independently of one another. For each chunk the function checks:
//
//   - chunk is non-nil and chunk.appId is non-empty
//   - if chunk.rawEvents is empty, no signatures or cert chains are
//     attached (otherwise the producer is lying about what they signed)
//   - chunk.rawSignatures and chunk.signingCertChains are present
//   - each cert chain's leaf SPIFFE ID app and namespace components match
//     chunk.appId and opts.ExpectedNamespace, so a signer cannot
//     impersonate another app or claim cross-namespace identity
//   - the signatures form a chain-linked cover of chunk.rawEvents (via
//     VerifyChain over the signed bytes; rawEvents are digested directly,
//     never re-marshaled, so verification is independent of protobuf
//     marshaler-version stability across producer and receiver)
//   - chain-of-trust to a Sentry trust anchor
//
// On success, VerifiedCerts contains every cert that was referenced by a
// verified signature (by certificateIndex), so callers can absorb them
// into a content-addressed foreign-cert table. Failure on any chunk
// returns an error and no partial result; receivers must reject the
// whole payload.
func VerifyPropagatedHistory(opts VerifyPropagationOptions) (*PropagationVerifyResult, error) {
	if opts.History == nil {
		return nil, errors.New("history must not be nil")
	}
	if opts.Signer == nil {
		return nil, errors.New("signer must not be nil")
	}
	if opts.ExpectedNamespace == "" {
		return nil, errors.New("expectedNamespace must not be empty")
	}

	ph := opts.History

	res := &PropagationVerifyResult{
		VerifiedCerts: make(map[string][]byte),
	}

	for i, chunk := range ph.GetChunks() {
		if chunk == nil {
			return nil, fmt.Errorf("chunk %d is nil", i)
		}
		if chunk.GetAppId() == "" {
			return nil, fmt.Errorf("chunk %d: appId is empty", i)
		}

		// An empty chunk (no rawEvents) must also have no signatures or
		// certs. The producer signed nothing, so receivers should reject
		// any attached signing material as a mismatch.
		if len(chunk.GetRawEvents()) == 0 {
			if len(chunk.GetRawSignatures()) > 0 || len(chunk.GetSigningCertChains()) > 0 {
				return nil, fmt.Errorf("chunk %d (app %q): empty rawEvents but %d signatures, %d cert chains attached", i, chunk.GetAppId(), len(chunk.GetRawSignatures()), len(chunk.GetSigningCertChains()))
			}
			continue
		}

		if len(chunk.GetRawSignatures()) == 0 {
			return nil, fmt.Errorf("chunk %d (app %q): missing rawSignatures for %d events", i, chunk.GetAppId(), len(chunk.GetRawEvents()))
		}
		if len(chunk.GetSigningCertChains()) == 0 {
			return nil, fmt.Errorf("chunk %d (app %q): missing signingCertChains", i, chunk.GetAppId())
		}

		// Wrap raw cert chains in SigningCertificate so VerifyChain can
		// consume them. The chunk carries raw bytes to avoid a circular
		// proto import.
		certs := make([]*protos.SigningCertificate, len(chunk.GetSigningCertChains()))
		for j, der := range chunk.GetSigningCertChains() {
			if len(der) == 0 {
				return nil, fmt.Errorf("chunk %d (app %q): cert chain %d is empty", i, chunk.GetAppId(), j)
			}
			certs[j] = &protos.SigningCertificate{Certificate: der}
		}

		// Identity check: each cert's leaf SPIFFE ID app and namespace
		// components must match chunk.appId and opts.ExpectedNamespace.
		// Without the namespace check, a holder of a Sentry-issued cert
		// for the same app-id in another namespace could claim to be the
		// expected producer.
		for j, der := range chunk.GetSigningCertChains() {
			if err := VerifyCertAppIdentity(der, chunk.GetAppId(), opts.ExpectedNamespace); err != nil {
				return nil, fmt.Errorf("chunk %d (app %q): cert chain %d identity mismatch: %w", i, chunk.GetAppId(), j, err)
			}
		}

		// Digest the producer's signed bytes directly. Never re-marshal
		// from ph.events: the receiver's protobuf library version may
		// not produce the same deterministic output as the producer's,
		// and the whole point of carrying rawEvents per chunk is to
		// make verification independent of marshaler stability.
		if err := VerifyChain(VerifyChainOptions{
			RawSignatures: chunk.GetRawSignatures(),
			Certs:         certs,
			AllRawEvents:  chunk.GetRawEvents(),
			Signer:        opts.Signer,
		}); err != nil {
			return nil, fmt.Errorf("chunk %d (app %q): %w", i, chunk.GetAppId(), err)
		}

		// Collect cert indices actually referenced by verified
		// signatures - VerifyChain only chain-of-trusts the certs that
		// signatures pointed at; an unreferenced cert chain in
		// chunk.signingCertChains never had its chain-of-trust checked
		// and must not be reported as verified.
		referenced := make(map[uint64]struct{}, len(chunk.GetRawSignatures()))
		for _, raw := range chunk.GetRawSignatures() {
			var sig protos.HistorySignature
			if err := proto.Unmarshal(raw, &sig); err != nil {
				// VerifyChain already parsed and validated these; a
				// failure here is a logic error, not user input.
				return nil, fmt.Errorf("chunk %d (app %q): failed to re-parse signature for cert tracking: %w", i, chunk.GetAppId(), err)
			}
			referenced[sig.GetCertificateIndex()] = struct{}{}
		}
		for idx := range referenced {
			if idx >= uint64(len(chunk.GetSigningCertChains())) {
				// Should be unreachable: VerifyChain rejects
				// out-of-range certificateIndex before returning. If
				// we hit this, an invariant has been broken and we
				// should not silently misreport verified certs.
				return nil, fmt.Errorf("chunk %d (app %q): cert index %d out of range [0, %d) after VerifyChain succeeded", i, chunk.GetAppId(), idx, len(chunk.GetSigningCertChains()))
			}
			der := chunk.GetSigningCertChains()[idx]
			res.VerifiedCerts[string(CertDigest(der))] = der
		}
	}

	return res, nil
}

// VerifyCertAppIdentity checks that a DER-encoded signing certificate chain
// has a leaf SPIFFE ID whose namespace and app components match
// expectedNamespace and expectedAppID. SPIFFE IDs follow
// spiffe://<trust-domain>/ns/<namespace>/<app-id>; this validates the full
// path structure rather than just the trailing segment. Both expected
// values must be non-empty - the namespace check stops a holder of a
// Sentry-issued cert for the same app-id in a different namespace from
// claiming to be the expected producer. Lifted into historysigning so chunk
// identity checks (in VerifyPropagatedHistory) and own-history identity
// checks (in dapr's state.go verifySignatureChain) share one implementation.
func VerifyCertAppIdentity(certChainDER []byte, expectedAppID, expectedNamespace string) error {
	if expectedAppID == "" {
		return errors.New("expectedAppID must not be empty")
	}
	if expectedNamespace == "" {
		return errors.New("expectedNamespace must not be empty")
	}

	leaf, err := parseCertificateChainDER(certChainDER)
	if err != nil {
		return err
	}

	spiffeID, err := x509svid.IDFromCert(leaf)
	if err != nil {
		return fmt.Errorf("failed to extract SPIFFE ID: %w", err)
	}

	segments := strings.Split(spiffeID.Path(), "/")
	if len(segments) != 4 || segments[0] != "" || segments[1] != "ns" || segments[2] == "" || segments[3] == "" {
		return fmt.Errorf("SPIFFE ID %q does not match expected path format /ns/<namespace>/<app-id>", spiffeID)
	}
	certNamespace := segments[2]
	certAppID := segments[3]

	if certNamespace != expectedNamespace {
		return fmt.Errorf("certificate SPIFFE ID namespace %q does not match expected namespace %q", certNamespace, expectedNamespace)
	}
	if certAppID != expectedAppID {
		return fmt.Errorf("certificate SPIFFE ID app %q does not match expected app %q", certAppID, expectedAppID)
	}

	return nil
}
