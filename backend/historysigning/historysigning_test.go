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
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
)

func testEvents() []*protos.HistoryEvent {
	return []*protos.HistoryEvent{
		{
			EventId:   0,
			Timestamp: timestamppb.New(time.Date(2026, 3, 18, 12, 0, 0, 0, time.UTC)),
			EventType: &protos.HistoryEvent_OrchestratorStarted{
				OrchestratorStarted: &protos.OrchestratorStartedEvent{},
			},
		},
		{
			EventId:   1,
			Timestamp: timestamppb.New(time.Date(2026, 3, 18, 12, 0, 1, 0, time.UTC)),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "TestWorkflow",
					Input: wrapperspb.String(`{"key":"value"}`),
					OrchestrationInstance: &protos.OrchestrationInstance{
						InstanceId:  "test-instance-1",
						ExecutionId: wrapperspb.String("exec-1"),
					},
				},
			},
		},
		{
			EventId:   2,
			Timestamp: timestamppb.New(time.Date(2026, 3, 18, 12, 0, 2, 0, time.UTC)),
			EventType: &protos.HistoryEvent_TaskScheduled{
				TaskScheduled: &protos.TaskScheduledEvent{
					Name:  "MyActivity",
					Input: wrapperspb.String(`"hello"`),
				},
			},
		},
	}
}

func marshalEvents(t *testing.T, events []*protos.HistoryEvent) [][]byte {
	t.Helper()
	raw := make([][]byte, len(events))
	for i, e := range events {
		b, err := MarshalEvent(e)
		require.NoError(t, err)
		raw[i] = b
	}
	return raw
}

// testCertValidity returns a validity window that covers the timestamps
// used by testEvents (2026-03-18).
func testCertValidity() (notBefore, notAfter time.Time) {
	return time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)
}

func generateEd25519Cert(t *testing.T) ([]byte, ed25519.PrivateKey) {
	t.Helper()
	nb, na := testCertValidity()
	return generateEd25519CertWithValidity(t, nb, na)
}

func generateECDSACert(t *testing.T) ([]byte, *ecdsa.PrivateKey) {
	t.Helper()
	priv, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)

	nb, na := testCertValidity()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    nb,
		NotAfter:     na,
		URIs:         []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/ns/default/app-b"}},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)

	return certDER, priv
}

func generateRSACert(t *testing.T) ([]byte, *rsa.PrivateKey) {
	t.Helper()
	priv, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	nb, na := testCertValidity()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    nb,
		NotAfter:     na,
		URIs:         []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/ns/default/app-c"}},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, &priv.PublicKey, priv)
	require.NoError(t, err)

	return certDER, priv
}

func TestEventsDigestDeterminism(t *testing.T) {
	events := testEvents()
	raw := marshalEvents(t, events)

	d1 := EventsDigest(raw)
	d2 := EventsDigest(raw)

	assert.Equal(t, d1, d2, "digest must be deterministic")
}

func TestEventsDigestDiffersOnChange(t *testing.T) {
	events := testEvents()
	d1 := EventsDigest(marshalEvents(t, events))

	// Mutate an event
	events[1].GetExecutionStarted().Name = "DifferentWorkflow"
	d2 := EventsDigest(marshalEvents(t, events))

	assert.NotEqual(t, d1, d2, "digest must change when events change")
}

func TestSignAndVerifyEd25519(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	raw := marshalEvents(t, events)

	signer, err := NewSigner(certDER, priv)
	require.NoError(t, err)

	result, err := signer.Sign(SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
		ExistingCerts:   nil,
	})
	require.NoError(t, err)
	require.NotNil(t, result.NewCert)
	assert.Equal(t, uint64(0), result.CertificateIndex)

	// Verify
	certs := []*protos.SigningCertificate{result.NewCert}
	err = VerifySignature(result.Signature, certs, raw)
	require.NoError(t, err)
}

func TestSignAndVerifyECDSA(t *testing.T) {
	certDER, priv := generateECDSACert(t)
	events := testEvents()
	raw := marshalEvents(t, events)

	signer, err := NewSigner(certDER, priv)
	require.NoError(t, err)

	result, err := signer.Sign(SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
		ExistingCerts:   nil,
	})
	require.NoError(t, err)

	certs := []*protos.SigningCertificate{result.NewCert}
	err = VerifySignature(result.Signature, certs, raw)
	require.NoError(t, err)
}

func TestSignAndVerifyRSA(t *testing.T) {
	certDER, priv := generateRSACert(t)
	events := testEvents()
	raw := marshalEvents(t, events)

	signer, err := NewSigner(certDER, priv)
	require.NoError(t, err)

	result, err := signer.Sign(SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
		ExistingCerts:   nil,
	})
	require.NoError(t, err)

	certs := []*protos.SigningCertificate{result.NewCert}
	err = VerifySignature(result.Signature, certs, raw)
	require.NoError(t, err)
}

func TestSignChainAndVerify(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	raw := marshalEvents(t, events)

	signer, err := NewSigner(certDER, priv)
	require.NoError(t, err)

	// Sign first 2 events
	result1, err := signer.Sign(SignOptions{
		RawEvents:       raw[:2],
		StartEventIndex: 0,
		ExistingCerts:   nil,
	})
	require.NoError(t, err)

	certs := []*protos.SigningCertificate{result1.NewCert}

	// Sign remaining event, chained to first
	result2, err := signer.Sign(SignOptions{
		RawEvents:         raw[2:],
		StartEventIndex:   2,
		PreviousSignature: result1.Signature,
		ExistingCerts:     certs,
	})
	require.NoError(t, err)
	assert.Nil(t, result2.NewCert, "cert should be reused")
	assert.Equal(t, uint64(0), result2.CertificateIndex)

	// Verify chain
	sigs := []*protos.HistorySignature{result1.Signature, result2.Signature}
	err = VerifyChain(sigs, certs, raw)
	require.NoError(t, err)
}

func TestCertificateRotation(t *testing.T) {
	certDER1, priv1 := generateEd25519Cert(t)
	certDER2, priv2 := generateEd25519Cert(t)
	events := testEvents()
	raw := marshalEvents(t, events)

	signer1, err := NewSigner(certDER1, priv1)
	require.NoError(t, err)

	// Sign with first cert
	result1, err := signer1.Sign(SignOptions{
		RawEvents:       raw[:2],
		StartEventIndex: 0,
		ExistingCerts:   nil,
	})
	require.NoError(t, err)
	require.NotNil(t, result1.NewCert)

	certs := []*protos.SigningCertificate{result1.NewCert}

	// Sign with second cert (rotation)
	signer2, err := NewSigner(certDER2, priv2)
	require.NoError(t, err)

	result2, err := signer2.Sign(SignOptions{
		RawEvents:         raw[2:],
		StartEventIndex:   2,
		PreviousSignature: result1.Signature,
		ExistingCerts:     certs,
	})
	require.NoError(t, err)
	require.NotNil(t, result2.NewCert, "new cert should be added on rotation")
	assert.Equal(t, uint64(1), result2.CertificateIndex)

	certs = append(certs, result2.NewCert)

	// Verify chain
	sigs := []*protos.HistorySignature{result1.Signature, result2.Signature}
	err = VerifyChain(sigs, certs, raw)
	require.NoError(t, err)
}

func TestTamperedHistoryDetection(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	raw := marshalEvents(t, events)

	signer, err := NewSigner(certDER, priv)
	require.NoError(t, err)

	result, err := signer.Sign(SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
		ExistingCerts:   nil,
	})
	require.NoError(t, err)

	certs := []*protos.SigningCertificate{result.NewCert}

	// Tamper with an event's raw bytes
	events[1].GetExecutionStarted().Name = "TamperedWorkflow"
	raw[1], err = MarshalEvent(events[1])
	require.NoError(t, err)

	err = VerifySignature(result.Signature, certs, raw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "events digest mismatch")
}

func TestTruncatedChainDetection(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	raw := marshalEvents(t, events)

	signer, err := NewSigner(certDER, priv)
	require.NoError(t, err)

	result1, err := signer.Sign(SignOptions{
		RawEvents:       raw[:2],
		StartEventIndex: 0,
		ExistingCerts:   nil,
	})
	require.NoError(t, err)

	certs := []*protos.SigningCertificate{result1.NewCert}

	result2, err := signer.Sign(SignOptions{
		RawEvents:         raw[2:],
		StartEventIndex:   2,
		PreviousSignature: result1.Signature,
		ExistingCerts:     certs,
	})
	require.NoError(t, err)

	// Try to verify chain with first signature removed
	sigs := []*protos.HistorySignature{result2.Signature}
	err = VerifyChain(sigs, certs, raw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "root signature")
}

func generateEd25519CertWithValidity(t *testing.T, notBefore, notAfter time.Time) ([]byte, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "test"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		URIs:         []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/ns/default/app-a"}},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	require.NoError(t, err)

	return certDER, priv
}

func TestCertificateExpiredAtEventTime(t *testing.T) {
	// Certificate expired before events were created.
	certDER, priv := generateEd25519CertWithValidity(t,
		time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2021, 1, 1, 0, 0, 0, 0, time.UTC),
	)

	events := testEvents() // events have timestamps in 2026
	raw := marshalEvents(t, events)

	signer, err := NewSigner(certDER, priv)
	require.NoError(t, err)

	result, err := signer.Sign(SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
	})
	require.NoError(t, err)

	certs := []*protos.SigningCertificate{result.NewCert}
	err = VerifySignature(result.Signature, certs, raw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "certificate not valid at event time")
}

func TestCertificateNotYetValidAtEventTime(t *testing.T) {
	// Certificate validity starts after the events were created.
	certDER, priv := generateEd25519CertWithValidity(t,
		time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2031, 1, 1, 0, 0, 0, 0, time.UTC),
	)

	events := testEvents() // events have timestamps in 2026
	raw := marshalEvents(t, events)

	signer, err := NewSigner(certDER, priv)
	require.NoError(t, err)

	result, err := signer.Sign(SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
	})
	require.NoError(t, err)

	certs := []*protos.SigningCertificate{result.NewCert}
	err = VerifySignature(result.Signature, certs, raw)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "certificate not valid at event time")
}

func TestCertificateValidAtEventTime(t *testing.T) {
	// Certificate validity window covers the event timestamps.
	certDER, priv := generateEd25519CertWithValidity(t,
		time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC),
	)

	events := testEvents() // events have timestamps in 2026
	raw := marshalEvents(t, events)

	signer, err := NewSigner(certDER, priv)
	require.NoError(t, err)

	result, err := signer.Sign(SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
	})
	require.NoError(t, err)

	certs := []*protos.SigningCertificate{result.NewCert}
	err = VerifySignature(result.Signature, certs, raw)
	require.NoError(t, err)
}

func TestSignatureDigestDeterminism(t *testing.T) {
	sig := &protos.HistorySignature{
		StartEventIndex:        0,
		EventCount:             3,
		PreviousSignatureDigest: []byte{1, 2, 3},
		EventsDigest:           []byte{4, 5, 6},
		CertificateIndex:       0,
		Signature:              []byte{7, 8, 9},
	}

	d1, err := SignatureDigest(sig)
	require.NoError(t, err)
	d2, err := SignatureDigest(sig)
	require.NoError(t, err)
	assert.Equal(t, d1, d2)
}

func TestEventsDigestWithMapFields(t *testing.T) {
	events := []*protos.HistoryEvent{
		{
			EventId:   0,
			Timestamp: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
			EventType: &protos.HistoryEvent_ExecutionStarted{
				ExecutionStarted: &protos.ExecutionStartedEvent{
					Name: "test",
					Tags: map[string]string{
						"zebra":  "z",
						"alpha":  "a",
						"middle": "m",
					},
				},
			},
		},
	}

	raw := marshalEvents(t, events)
	d1 := EventsDigest(raw)

	// Re-marshal and check determinism
	raw2 := marshalEvents(t, events)
	d2 := EventsDigest(raw2)
	assert.Equal(t, d1, d2)
}

func TestEventsDigestIncludesUnknownFields(t *testing.T) {
	// Simulate forward compatibility: an event with an unknown field
	// (from a newer proto version) must be included in the digest.
	event := &protos.HistoryEvent{
		EventId:   5,
		Timestamp: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		EventType: &protos.HistoryEvent_OrchestratorStarted{
			OrchestratorStarted: &protos.OrchestratorStartedEvent{},
		},
	}

	raw, err := MarshalEvent(event)
	require.NoError(t, err)

	d1 := EventsDigest([][]byte{raw})

	// Append an unknown field (field 999, varint 42) to the raw bytes.
	// This simulates what the state store would contain if a newer binary
	// wrote an event with a field this binary doesn't know about.
	tampered := make([]byte, len(raw))
	copy(tampered, raw)
	tampered = protowire.AppendTag(tampered, 999, protowire.VarintType)
	tampered = protowire.AppendVarint(tampered, 42)

	d2 := EventsDigest([][]byte{tampered})
	assert.NotEqual(t, d1, d2, "unknown fields must affect the digest")
}

func TestRawBytesRoundTrip(t *testing.T) {
	// Verify that signing raw bytes and then verifying the same raw bytes
	// works even after the events are deserialized and re-serialized
	// (as long as deterministic marshaling is used).
	certDER, priv := generateEd25519Cert(t)
	events := testEvents()
	raw := marshalEvents(t, events)

	signer, err := NewSigner(certDER, priv)
	require.NoError(t, err)

	result, err := signer.Sign(SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
	})
	require.NoError(t, err)

	// Simulate read from store: unmarshal then re-marshal deterministically
	roundTripped := make([][]byte, len(raw))
	for i, b := range raw {
		var e protos.HistoryEvent
		require.NoError(t, proto.Unmarshal(b, &e))
		roundTripped[i], err = MarshalEvent(&e)
		require.NoError(t, err)
	}

	certs := []*protos.SigningCertificate{result.NewCert}
	err = VerifySignature(result.Signature, certs, roundTripped)
	require.NoError(t, err)
}

// generateCACert creates a self-signed CA certificate and returns its DER
// bytes, parsed certificate, and private key.
func generateCACert(t *testing.T) ([]byte, *x509.Certificate, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	nb, na := testCertValidity()
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Test CA"},
		NotBefore:             nb,
		NotAfter:              na,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}

	caDER, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	require.NoError(t, err)

	ca, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	return caDER, ca, priv
}

// generateLeafCertSignedByCA creates a leaf certificate signed by the given CA.
func generateLeafCertSignedByCA(t *testing.T, ca *x509.Certificate, caKey ed25519.PrivateKey) ([]byte, ed25519.PrivateKey) {
	t.Helper()
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	nb, na := testCertValidity()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "test leaf"},
		NotBefore:    nb,
		NotAfter:     na,
		URIs:         []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/ns/default/app-a"}},
	}

	leafDER, err := x509.CreateCertificate(rand.Reader, template, ca, pub, caKey)
	require.NoError(t, err)

	return leafDER, priv
}

func TestSignAndVerifyWithCertChain(t *testing.T) {
	// Create CA and leaf signed by CA.
	caDER, ca, caKey := generateCACert(t)
	leafDER, leafPriv := generateLeafCertSignedByCA(t, ca, caKey)

	// Build chain: leaf + CA concatenated DER.
	chainDER := append(leafDER, caDER...)

	events := testEvents()
	raw := marshalEvents(t, events)

	signer, err := NewSigner(chainDER, leafPriv)
	require.NoError(t, err)

	result, err := signer.Sign(SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
	})
	require.NoError(t, err)
	require.NotNil(t, result.NewCert)

	// The stored certificate should be the full chain.
	assert.Equal(t, chainDER, result.NewCert.GetCertificate())

	// Verification should succeed — parseCertificateChainDER extracts the leaf.
	certs := []*protos.SigningCertificate{result.NewCert}
	err = VerifySignature(result.Signature, certs, raw)
	require.NoError(t, err)
}

func TestSignChainVerifyWithCertChain(t *testing.T) {
	// Full signing chain with certificate chains (leaf+CA).
	caDER, ca, caKey := generateCACert(t)
	leafDER, leafPriv := generateLeafCertSignedByCA(t, ca, caKey)
	chainDER := append(leafDER, caDER...)

	events := testEvents()
	raw := marshalEvents(t, events)

	signer, err := NewSigner(chainDER, leafPriv)
	require.NoError(t, err)

	// Sign first batch.
	result1, err := signer.Sign(SignOptions{
		RawEvents:       raw[:2],
		StartEventIndex: 0,
	})
	require.NoError(t, err)

	certs := []*protos.SigningCertificate{result1.NewCert}

	// Sign second batch chained to first — cert should be reused.
	result2, err := signer.Sign(SignOptions{
		RawEvents:         raw[2:],
		StartEventIndex:   2,
		PreviousSignature: result1.Signature,
		ExistingCerts:     certs,
	})
	require.NoError(t, err)
	assert.Nil(t, result2.NewCert, "cert chain should be reused")
	assert.Equal(t, uint64(0), result2.CertificateIndex)

	sigs := []*protos.HistorySignature{result1.Signature, result2.Signature}
	err = VerifyChain(sigs, certs, raw)
	require.NoError(t, err)
}

func TestCertificateRotationWithChains(t *testing.T) {
	// Two different CAs, each signing a leaf. Simulate rotation between them.
	caDER1, ca1, caKey1 := generateCACert(t)
	leafDER1, leafPriv1 := generateLeafCertSignedByCA(t, ca1, caKey1)
	chainDER1 := append(leafDER1, caDER1...)

	caDER2, ca2, caKey2 := generateCACert(t)
	leafDER2, leafPriv2 := generateLeafCertSignedByCA(t, ca2, caKey2)
	chainDER2 := append(leafDER2, caDER2...)

	events := testEvents()
	raw := marshalEvents(t, events)

	signer1, err := NewSigner(chainDER1, leafPriv1)
	require.NoError(t, err)

	result1, err := signer1.Sign(SignOptions{
		RawEvents:       raw[:2],
		StartEventIndex: 0,
	})
	require.NoError(t, err)
	require.NotNil(t, result1.NewCert)

	certs := []*protos.SigningCertificate{result1.NewCert}

	// Rotate to second identity.
	signer2, err := NewSigner(chainDER2, leafPriv2)
	require.NoError(t, err)

	result2, err := signer2.Sign(SignOptions{
		RawEvents:         raw[2:],
		StartEventIndex:   2,
		PreviousSignature: result1.Signature,
		ExistingCerts:     certs,
	})
	require.NoError(t, err)
	require.NotNil(t, result2.NewCert, "rotation should produce a new cert entry")
	assert.Equal(t, uint64(1), result2.CertificateIndex)

	certs = append(certs, result2.NewCert)

	sigs := []*protos.HistorySignature{result1.Signature, result2.Signature}
	err = VerifyChain(sigs, certs, raw)
	require.NoError(t, err)
}

func TestSignWithIntermediateCertChain(t *testing.T) {
	// Root CA -> Intermediate CA -> Leaf, stored as leaf+intermediate+root.
	rootPub, rootPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	nb, na := testCertValidity()
	rootTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "Root CA"},
		NotBefore:             nb,
		NotAfter:              na,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	rootDER, err := x509.CreateCertificate(rand.Reader, rootTemplate, rootTemplate, rootPub, rootPriv)
	require.NoError(t, err)
	rootCert, err := x509.ParseCertificate(rootDER)
	require.NoError(t, err)

	intermPub, intermPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	intermTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(2),
		Subject:               pkix.Name{CommonName: "Intermediate CA"},
		NotBefore:             nb,
		NotAfter:              na,
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	intermDER, err := x509.CreateCertificate(rand.Reader, intermTemplate, rootCert, intermPub, rootPriv)
	require.NoError(t, err)
	intermCert, err := x509.ParseCertificate(intermDER)
	require.NoError(t, err)

	leafPub, leafPriv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	leafTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: "leaf"},
		NotBefore:    nb,
		NotAfter:     na,
		URIs:         []*url.URL{{Scheme: "spiffe", Host: "example.org", Path: "/ns/default/app-a"}},
	}
	leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, intermCert, leafPub, intermPriv)
	require.NoError(t, err)

	// Chain: leaf + intermediate + root
	var chainDER []byte
	chainDER = append(chainDER, leafDER...)
	chainDER = append(chainDER, intermDER...)
	chainDER = append(chainDER, rootDER...)

	events := testEvents()
	raw := marshalEvents(t, events)

	signer, err := NewSigner(chainDER, leafPriv)
	require.NoError(t, err)

	result, err := signer.Sign(SignOptions{
		RawEvents:       raw,
		StartEventIndex: 0,
	})
	require.NoError(t, err)
	require.NotNil(t, result.NewCert)

	// Verify — the leaf's public key should be used for verification.
	certs := []*protos.SigningCertificate{result.NewCert}
	err = VerifySignature(result.Signature, certs, raw)
	require.NoError(t, err)
}

func TestNewSignerErrors(t *testing.T) {
	t.Run("empty cert", func(t *testing.T) {
		_, err := NewSigner(nil, ed25519.NewKeyFromSeed(make([]byte, 32)))
		require.Error(t, err)
	})

	t.Run("nil key", func(t *testing.T) {
		_, err := NewSigner([]byte("cert"), nil)
		require.Error(t, err)
	})
}
