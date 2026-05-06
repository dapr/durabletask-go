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
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/crypto/spiffe/signer"
)

// signChunk marshals events, signs them, and returns the (rawEvents, rawSigs,
// certChains) triple a producer would attach to a PropagatedHistoryChunk.
func signChunk(t *testing.T, s *signer.Signer, events []*protos.HistoryEvent) (rawEvents, rawSigs, certs [][]byte) {
	t.Helper()
	rawEvents = marshalEvents(t, events)
	res, err := Sign(s, SignOptions{
		RawEvents:       rawEvents,
		StartEventIndex: 0,
		ExistingCerts:   nil,
	})
	require.NoError(t, err)
	require.NotNil(t, res.NewCert)
	return rawEvents, [][]byte{res.RawSignature}, [][]byte{res.NewCert.GetCertificate()}
}

// generateEd25519CertWithSpiffePath issues a fresh self-signed Ed25519 leaf
// for the given SPIFFE path, e.g. /ns/default/app-x.
func generateEd25519CertWithSpiffePath(t *testing.T, spiffePath string) ([]byte, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	nb, na := testCertValidity()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    nb,
		NotAfter:     na,
		URIs:         []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: spiffePath}},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	require.NoError(t, err)
	return der, priv
}

// makePropagatedHistory builds a single-chunk PropagatedHistory for tests.
// The chunk owns its rawEvents/rawSignatures/signingCertChains directly.
func makePropagatedHistory(appID, instanceID string, rawEvents, rawSigs, certs [][]byte) *protos.PropagatedHistory {
	return &protos.PropagatedHistory{
		Scope: protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY,
		Chunks: []*protos.PropagatedHistoryChunk{{
			AppId:             appID,
			InstanceId:        instanceID,
			WorkflowName:      "TestWorkflow",
			RawEvents:         rawEvents,
			RawSignatures:     rawSigs,
			SigningCertChains: certs,
		}},
	}
}

func TestVerifyPropagatedHistory_HappyPath(t *testing.T) {
	certDER, priv := generateEd25519Cert(t) // /ns/default/app-a
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	rawEvents, rawSigs, certs := signChunk(t, s, events)
	ph := makePropagatedHistory("app-a", "wf-1", rawEvents, rawSigs, certs)

	res, err := VerifyPropagatedHistory(VerifyPropagationOptions{
		History:           ph,
		Signer:            s,
		ExpectedNamespace: "default",
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Len(t, res.VerifiedCerts, 1, "the chunk's cert should be reported as freshly verified")
}

func TestVerifyPropagatedHistory_WrongAppID(t *testing.T) {
	certDER, priv := generateEd25519Cert(t) // /ns/default/app-a
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	rawEvents, rawSigs, certs := signChunk(t, s, events)

	// Producer's cert SPIFFE ID says app-a, but the chunk claims app-b.
	ph := makePropagatedHistory("app-b", "wf-1", rawEvents, rawSigs, certs)

	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s, ExpectedNamespace: "default"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "identity mismatch")
}

func TestVerifyPropagatedHistory_TamperedRawEvents(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	rawEvents, rawSigs, certs := signChunk(t, s, events)

	// Flip a byte in the signed bytes. VerifyChain's events digest check
	// must reject the chunk.
	require.NotEmpty(t, rawEvents[0])
	rawEvents[0][0] ^= 0xFF
	ph := makePropagatedHistory("app-a", "wf-1", rawEvents, rawSigs, certs)

	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s, ExpectedNamespace: "default"})
	require.Error(t, err)
}

func TestVerifyPropagatedHistory_MissingSignatures(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	rawEvents, _, certs := signChunk(t, s, events)

	ph := makePropagatedHistory("app-a", "wf-1", rawEvents, nil /* missing */, certs)

	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s, ExpectedNamespace: "default"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing rawSignatures")
}

func TestVerifyPropagatedHistory_MissingCerts(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	rawEvents, rawSigs, _ := signChunk(t, s, events)

	ph := makePropagatedHistory("app-a", "wf-1", rawEvents, rawSigs, nil)

	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s, ExpectedNamespace: "default"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing signingCertChains")
}

func TestVerifyPropagatedHistory_TwoChunksLineage(t *testing.T) {
	certA, privA := generateEd25519CertWithSpiffePath(t, "/ns/default/app-a")
	certB, privB := generateEd25519CertWithSpiffePath(t, "/ns/default/app-b")

	signerA := newTestSigner(t, certA, privA, parseCert(t, certA), parseCert(t, certB))
	signerB := newTestSigner(t, certB, privB, parseCert(t, certA), parseCert(t, certB))

	events := testEvents()
	require.GreaterOrEqual(t, len(events), 3)

	rawA, rawSigsA, certsA := signChunk(t, signerA, events[:2])
	rawB, rawSigsB, certsB := signChunk(t, signerB, events[2:])

	ph := &protos.PropagatedHistory{
		Scope: protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		Chunks: []*protos.PropagatedHistoryChunk{
			{
				AppId:             "app-a",
				InstanceId:        "wf-a",
				WorkflowName:      "A",
				RawEvents:         rawA,
				RawSignatures:     rawSigsA,
				SigningCertChains: certsA,
			},
			{
				AppId:             "app-b",
				InstanceId:        "wf-b",
				WorkflowName:      "B",
				RawEvents:         rawB,
				RawSignatures:     rawSigsB,
				SigningCertChains: certsB,
			},
		},
	}

	res, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: signerA, ExpectedNamespace: "default"})
	require.NoError(t, err)
	assert.Len(t, res.VerifiedCerts, 2)
}

func TestVerifyPropagatedHistory_EmptyChunkWithSigs(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	events := testEvents()
	_, rawSigs, certs := signChunk(t, s, events)

	// Zero rawEvents but signatures attached - the verifier must reject.
	ph := makePropagatedHistory("app-a", "wf-1", nil, rawSigs, certs)

	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s, ExpectedNamespace: "default"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty rawEvents")
}

// TestVerifyPropagatedHistory_EmptyChunkRejected verifies that any chunk
// with empty rawEvents is rejected, even when no signing material is
// attached. Producers never emit empty chunks, so an empty chunk on the
// wire is necessarily injected; accepting it would let an unauthenticated
// caller list arbitrary AppIds in lineage with nothing actually signed.
func TestVerifyPropagatedHistory_EmptyChunkRejected(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	ph := makePropagatedHistory("app-a", "wf-1", nil, nil, nil)
	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s, ExpectedNamespace: "default"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "empty rawEvents")
}

func TestVerifyPropagatedHistory_UnreferencedCertOmittedFromResult(t *testing.T) {
	certA, privA := generateEd25519CertWithSpiffePath(t, "/ns/default/app-a")
	certB, _ := generateEd25519CertWithSpiffePath(t, "/ns/default/app-a") // same SPIFFE so identity check passes
	s := newTestSigner(t, certA, privA, parseCert(t, certA), parseCert(t, certB))

	events := testEvents()
	rawEvents, rawSigs, certs := signChunk(t, s, events)
	require.Len(t, certs, 1)

	// Append an extra cert chain at index 1; no signature references it.
	certs = append(certs, certB)

	ph := makePropagatedHistory("app-a", "wf-1", rawEvents, rawSigs, certs)
	res, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s, ExpectedNamespace: "default"})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Len(t, res.VerifiedCerts, 1,
		"only certs referenced by verified signatures should appear in VerifiedCerts")
	_, hasReferenced := res.VerifiedCerts[string(CertDigest(certs[0]))]
	assert.True(t, hasReferenced, "the cert pointed at by certificateIndex=0 must be in VerifiedCerts")
	_, hasUnreferenced := res.VerifiedCerts[string(CertDigest(certB))]
	assert.False(t, hasUnreferenced, "the unreferenced cert at index 1 must not appear")
}

func TestVerifyCertAppIdentity(t *testing.T) {
	tests := []struct {
		name      string
		path      string
		appID     string
		namespace string
		wantErr   string // substring; empty means expect no error
	}{
		{"matches", "/ns/default/my-app", "my-app", "default", ""},
		{"mismatched-app", "/ns/default/other", "my-app", "default", "does not match expected app"},
		{"mismatched-namespace", "/ns/staging/my-app", "my-app", "prod", "does not match expected namespace"},
		{"missing-ns-prefix", "/default/my-app", "my-app", "default", "does not match expected path format"},
		{"empty-namespace", "/ns//my-app", "my-app", "default", "failed to extract SPIFFE ID"},
		{"empty-app", "/ns/default/", "my-app", "default", "failed to extract SPIFFE ID"},
		{"empty-expected-app", "/ns/default/my-app", "", "default", "expectedAppID must not be empty"},
		{"empty-expected-namespace", "/ns/default/my-app", "my-app", "", "expectedNamespace must not be empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			der, _ := generateEd25519CertWithSpiffePath(t, tc.path)
			err := VerifyCertAppIdentity(der, tc.appID, tc.namespace)
			if tc.wantErr == "" {
				require.NoError(t, err)
			} else {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tc.wantErr)
			}
		})
	}
}

// TestVerifyPropagatedHistory_WrongNamespace verifies that a chunk whose
// signing cert lives in a different namespace than the receiver expects is
// rejected, even when the cert's app component matches. This stops a holder
// of a Sentry-issued cert for the same app-id in another namespace from
// claiming to be the expected producer.
func TestVerifyPropagatedHistory_WrongNamespace(t *testing.T) {
	certDER, priv := generateEd25519CertWithSpiffePath(t, "/ns/staging/app-a")
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	rawEvents, rawSigs, certs := signChunk(t, s, events)

	ph := makePropagatedHistory("app-a", "wf-1", rawEvents, rawSigs, certs)
	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{
		History:           ph,
		Signer:            s,
		ExpectedNamespace: "prod",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "does not match expected namespace")
}
