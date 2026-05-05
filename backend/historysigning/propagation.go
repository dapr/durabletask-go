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
// PropagatedHistoryChunk. rawSigs is the deterministic raw bytes of every
// HistorySignature covering exactly the chunk's event range, in order.
// certChainsDER is the DER-concatenated cert chain for each signing identity
// referenced by the signatures' certificateIndex. The chunk's signature chain
// is verified independently by receivers via VerifyPropagatedHistory.
func BuildSignedChunk(chunk *protos.PropagatedHistoryChunk, rawSigs [][]byte, certChainsDER [][]byte) error {
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

// VerifyPropagatedHistory verifies every chunk in a PropagatedHistory:
//   - chunks form a contiguous, non-overlapping cover of [0, len(events))
//   - each chunk's signatures form a contiguous, chain-linked cover of the
//     chunk's event range, signed by the chunk's certs (via VerifyChain)
//   - the leaf cert SPIFFE ID app component matches chunk.appId, so a
//     signer cannot impersonate another app
//   - chain-of-trust to a Sentry trust anchor
//
// Verification is independent per chunk: corruption in chunk N does not affect
// verification of chunk M. Failure on any chunk returns an error and no
// partial result; receivers must reject the whole payload.
func VerifyPropagatedHistory(opts VerifyPropagationOptions) (*PropagationVerifyResult, error) {
	if opts.History == nil {
		return nil, errors.New("history must not be nil")
	}
	if opts.Signer == nil {
		return nil, errors.New("signer must not be nil")
	}

	ph := opts.History
	if err := validateChunkCoverage(ph); err != nil {
		return nil, err
	}

	res := &PropagationVerifyResult{
		VerifiedCerts: make(map[string][]byte),
	}

	for i, chunk := range ph.GetChunks() {
		if chunk.GetAppId() == "" {
			return nil, fmt.Errorf("chunk %d: appId is empty", i)
		}

		start := uint64(chunk.GetStartEventIndex())
		count := uint64(chunk.GetEventCount())
		end := start + count

		// Empty chunks have nothing to verify but must also have no
		// signatures or certs (otherwise the producer is lying about
		// what they signed).
		if count == 0 {
			if len(chunk.GetRawSignatures()) > 0 || len(chunk.GetSigningCertChains()) > 0 {
				return nil, fmt.Errorf("chunk %d (app %q): empty event range but %d signatures and %d cert chains attached", i, chunk.GetAppId(), len(chunk.GetRawSignatures()), len(chunk.GetSigningCertChains()))
			}
			continue
		}

		if len(chunk.GetRawSignatures()) == 0 {
			return nil, fmt.Errorf("chunk %d (app %q): missing rawSignatures for event range [%d, %d)", i, chunk.GetAppId(), start, end)
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

		// Identity check: each cert's leaf SPIFFE ID app component must
		// match chunk.appId. Without this, a holder of any Sentry-issued
		// SPIFFE cert could claim to be any app.
		for j, der := range chunk.GetSigningCertChains() {
			if err := VerifyCertAppIdentity(der, chunk.GetAppId()); err != nil {
				return nil, fmt.Errorf("chunk %d (app %q): cert chain %d identity mismatch: %w", i, chunk.GetAppId(), j, err)
			}
		}

		// Re-marshal the chunk's events deterministically. Both signer
		// and verifier use the same MarshalEvent on the same proto
		// schema, so bytes round-trip identically.
		chunkEvents := ph.GetEvents()[start:end]
		rawEvents := make([][]byte, len(chunkEvents))
		for j, e := range chunkEvents {
			b, err := MarshalEvent(e)
			if err != nil {
				return nil, fmt.Errorf("chunk %d (app %q): failed to marshal event %d: %w", i, chunk.GetAppId(), int(start)+j, err)
			}
			rawEvents[j] = b
		}

		if err := VerifyChain(VerifyChainOptions{
			RawSignatures: chunk.GetRawSignatures(),
			Certs:         certs,
			AllRawEvents:  rawEvents,
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
				continue
			}
			der := chunk.GetSigningCertChains()[idx]
			res.VerifiedCerts[string(CertDigest(der))] = der
		}
	}

	return res, nil
}

// validateChunkCoverage enforces the structural contract on
// PropagatedHistory.chunks: ordered, non-overlapping, and together covering
// every event in PropagatedHistory.events. Without this, a payload with gaps
// (or no chunks at all) would let unsigned events sneak through.
func validateChunkCoverage(ph *protos.PropagatedHistory) error {
	events := ph.GetEvents()
	//nolint:gosec // event count is bounded by proto field size
	n := int32(len(events))

	var nextExpected int32
	for i, c := range ph.GetChunks() {
		if c == nil {
			return fmt.Errorf("propagated history: chunk %d is nil", i)
		}
		start, count := c.GetStartEventIndex(), c.GetEventCount()
		if start < 0 || count < 0 {
			return fmt.Errorf("propagated history: chunk %d has negative index/count (start=%d count=%d)", i, start, count)
		}
		// int64 to dodge int32 overflow on the addition.
		if int64(start)+int64(count) > int64(n) {
			return fmt.Errorf("propagated history: chunk %d range [%d,%d) exceeds events length %d", i, start, int64(start)+int64(count), n)
		}
		if start != nextExpected {
			return fmt.Errorf("propagated history: chunk %d starts at %d, expected %d (chunks must be contiguous and non-overlapping)", i, start, nextExpected)
		}
		nextExpected = start + count
	}

	if nextExpected != n {
		return fmt.Errorf("propagated history: chunks cover %d events but events length is %d", nextExpected, n)
	}
	return nil
}

// VerifyCertAppIdentity checks that a DER-encoded signing certificate chain
// has a leaf SPIFFE ID whose app component matches expectedAppID. SPIFFE IDs
// follow spiffe://<trust-domain>/ns/<namespace>/<app-id>; this validates the
// full path structure rather than just the trailing segment. Lifted into
// historysigning so chunk identity checks (in VerifyPropagatedHistory) and
// own-history identity checks (in dapr's state.go verifySignatureChain) share
// one implementation.
func VerifyCertAppIdentity(certChainDER []byte, expectedAppID string) error {
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
	certAppID := segments[3]

	if certAppID != expectedAppID {
		return fmt.Errorf("certificate SPIFFE ID app %q does not match expected app %q", certAppID, expectedAppID)
	}

	return nil
}
