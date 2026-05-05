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
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/crypto/spiffe/signer"
)

// signChunk produces signed chunk material for `events` using `s` and
// returns rawSignatures + cert chains, exactly as a producer would attach
// to a PropagatedHistoryChunk.
func signChunk(t *testing.T, s *signer.Signer, events []*protos.HistoryEvent) ([][]byte, [][]byte) {
	t.Helper()
	raw := marshalEvents(t, events)
	res, err := Sign(s, SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
		ExistingCerts:   nil,
	})
	require.NoError(t, err)
	require.NotNil(t, res.NewCert)
	return [][]byte{res.RawSignature}, [][]byte{res.NewCert.GetCertificate()}
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
func makePropagatedHistory(appID, instanceID string, events []*protos.HistoryEvent, rawSigs [][]byte, certs [][]byte) *protos.PropagatedHistory {
	return &protos.PropagatedHistory{
		Events: events,
		Scope:  protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_OWN_HISTORY,
		Chunks: []*protos.PropagatedHistoryChunk{{
			AppId:             appID,
			StartEventIndex:   0,
			EventCount:        int32(len(events)),
			InstanceId:        instanceID,
			WorkflowName:      "TestWorkflow",
			RawSignatures:     rawSigs,
			SigningCertChains: certs,
		}},
	}
}

func TestVerifyPropagatedHistory_HappyPath(t *testing.T) {
	certDER, priv := generateEd25519Cert(t) // /ns/default/app-a
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	rawSigs, certs := signChunk(t, s, events)
	ph := makePropagatedHistory("app-a", "wf-1", events, rawSigs, certs)

	res, err := VerifyPropagatedHistory(VerifyPropagationOptions{
		History: ph,
		Signer:  s,
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Len(t, res.VerifiedCerts, 1, "the chunk's cert should be reported as freshly verified")
}

func TestVerifyPropagatedHistory_WrongAppID(t *testing.T) {
	certDER, priv := generateEd25519Cert(t) // /ns/default/app-a
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	rawSigs, certs := signChunk(t, s, events)

	// Producer's cert SPIFFE ID says app-a, but the chunk claims app-b.
	ph := makePropagatedHistory("app-b", "wf-1", events, rawSigs, certs)

	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "identity mismatch")
}

func TestVerifyPropagatedHistory_TamperedEvent(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	rawSigs, certs := signChunk(t, s, events)

	// Flip a byte in an event AFTER signing.
	events[1].GetExecutionStarted().Name = "Tampered"
	ph := makePropagatedHistory("app-a", "wf-1", events, rawSigs, certs)

	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s})
	require.Error(t, err)
}

func TestVerifyPropagatedHistory_MissingSignatures(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	_, certs := signChunk(t, s, events)

	ph := makePropagatedHistory("app-a", "wf-1", events, nil /* missing */, certs)

	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing rawSignatures")
}

func TestVerifyPropagatedHistory_MissingCerts(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	rawSigs, _ := signChunk(t, s, events)

	ph := makePropagatedHistory("app-a", "wf-1", events, rawSigs, nil)

	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing signingCertChains")
}

func TestVerifyPropagatedHistory_OutOfBoundsRange(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	rawSigs, certs := signChunk(t, s, events)

	ph := makePropagatedHistory("app-a", "wf-1", events, rawSigs, certs)
	ph.Chunks[0].EventCount = int32(len(events)) + 5 // overshoot

	_, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: s})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds events length")
}

func TestVerifyPropagatedHistory_TwoChunksLineage(t *testing.T) {
	// app-a signs the first 2 events; app-b signs the remaining event.
	// Each chunk verifies independently against its own producer cert.
	certA, privA := generateEd25519CertWithSpiffePath(t, "/ns/default/app-a")
	certB, privB := generateEd25519CertWithSpiffePath(t, "/ns/default/app-b")

	signerA := newTestSigner(t, certA, privA, parseCert(t, certA), parseCert(t, certB))
	signerB := newTestSigner(t, certB, privB, parseCert(t, certA), parseCert(t, certB))

	events := testEvents()
	require.GreaterOrEqual(t, len(events), 3)

	// Sign each subset independently.
	rawA, certsA := signChunkRange(t, signerA, events[:2])
	rawB, certsB := signChunkRange(t, signerB, events[2:])

	ph := &protos.PropagatedHistory{
		Events: events,
		Scope:  protos.HistoryPropagationScope_HISTORY_PROPAGATION_SCOPE_LINEAGE,
		Chunks: []*protos.PropagatedHistoryChunk{
			{
				AppId:             "app-a",
				StartEventIndex:   0,
				EventCount:        2,
				InstanceId:        "wf-a",
				WorkflowName:      "A",
				RawSignatures:     rawA,
				SigningCertChains: certsA,
			},
			{
				AppId:             "app-b",
				StartEventIndex:   2,
				EventCount:        int32(len(events) - 2),
				InstanceId:        "wf-b",
				WorkflowName:      "B",
				RawSignatures:     rawB,
				SigningCertChains: certsB,
			},
		},
	}

	res, err := VerifyPropagatedHistory(VerifyPropagationOptions{History: ph, Signer: signerA})
	require.NoError(t, err)
	assert.Len(t, res.VerifiedCerts, 2)
}

// signChunkRange signs an arbitrary slice of events as a fresh chunk.
// The signer's existing cert is used; chunk-local rawSignatures and
// chunk-local signing cert chain are returned.
func signChunkRange(t *testing.T, s *signer.Signer, events []*protos.HistoryEvent) ([][]byte, [][]byte) {
	t.Helper()
	raw := marshalEvents(t, events)
	res, err := Sign(s, SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
		ExistingCerts:   nil,
	})
	require.NoError(t, err)
	require.NotNil(t, res.NewCert)
	return [][]byte{res.RawSignature}, [][]byte{res.NewCert.GetCertificate()}
}

func TestVerifyPropagatedHistory_SkipChainOfTrustOmitsCertFromResult(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))
	rawSigs, certs := signChunk(t, s, events)
	ph := makePropagatedHistory("app-a", "wf-1", events, rawSigs, certs)

	skip := map[string]struct{}{
		string(CertDigest(certs[0])): {},
	}
	res, err := VerifyPropagatedHistory(VerifyPropagationOptions{
		History:                     ph,
		Signer:                      s,
		SkipChainOfTrustCertDigests: skip,
	})
	require.NoError(t, err)
	require.NotNil(t, res)
	assert.Empty(t, res.VerifiedCerts, "cert in skip set should not appear in VerifiedCerts")
}

func TestVerifyCertAppIdentity(t *testing.T) {
	tests := []struct {
		name    string
		path    string
		appID   string
		wantErr bool
	}{
		{"matches", "/ns/default/my-app", "my-app", false},
		{"mismatched-app", "/ns/default/other", "my-app", true},
		{"missing-ns-prefix", "/default/my-app", "my-app", true},
		{"empty-namespace", "/ns//my-app", "my-app", true},
		{"empty-app", "/ns/default/", "my-app", true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			der, _ := generateEd25519CertWithSpiffePath(t, tc.path)
			err := VerifyCertAppIdentity(der, tc.appID)
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// silence unused-import warnings if/when test gets pruned
var _ = timestamppb.Now
var _ = wrapperspb.String
var _ time.Time
