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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/crypto/spiffe/signer"
)

const (
	testParentInstanceID = "parent-instance-1"
	testParentTaskID     = int32(7)
	testActivityName     = "DoThing"
)

func testEventTime() time.Time {
	return time.Date(2026, 3, 18, 12, 0, 0, 0, time.UTC)
}

// --- Canonical byte helpers ---

func TestCanonicalInputUnsetReturnsEmpty(t *testing.T) {
	assert.Equal(t, []byte(nil), CanonicalInput(nil))
}

func TestCanonicalInputUTF8ValueOnly(t *testing.T) {
	// Canonical bytes are exactly the UTF-8 of the value, with no envelope.
	assert.Equal(t, []byte("hello"), CanonicalInput(wrapperspb.String("hello")))
	assert.Equal(t, []byte("π≈3.14"), CanonicalInput(wrapperspb.String("π≈3.14")))
	assert.Equal(t, []byte(""), CanonicalInput(wrapperspb.String("")))
}

func TestCanonicalFailureOutputEmptyFailure(t *testing.T) {
	// All zero-length fields; no inner.
	//   u32be(0) || u32be(0) || u32be(0) || u8(0)
	want := bytes.Repeat([]byte{0}, 4+4+4+1)
	got, err := CanonicalFailureOutput(&protos.TaskFailureDetails{})
	require.NoError(t, err)
	assert.Equal(t, want, got)
}

func TestCanonicalFailureOutputNilTreatedAsEmpty(t *testing.T) {
	a, err := CanonicalFailureOutput(&protos.TaskFailureDetails{})
	require.NoError(t, err)
	b, err := CanonicalFailureOutput(nil)
	require.NoError(t, err)
	assert.Equal(t, a, b)
}

func TestCanonicalFailureOutputAllFieldsSet(t *testing.T) {
	fd := &protos.TaskFailureDetails{
		ErrorType:    "ValueError",
		ErrorMessage: "boom",
		StackTrace:   wrapperspb.String("frame1\nframe2"),
	}

	got, err := CanonicalFailureOutput(fd)
	require.NoError(t, err)

	// Manually construct expected:
	expected := []byte{}
	expected = appendU32Len(expected, "ValueError")
	expected = appendU32Len(expected, "boom")
	expected = appendU32Len(expected, "frame1\nframe2")
	expected = append(expected, 0) // no inner

	assert.Equal(t, expected, got)
}

func TestCanonicalFailureOutputIsNonRetriableExcluded(t *testing.T) {
	// isNonRetriable is a framework retry-policy hint, not semantic content
	// of the failure — changing it must not change the canonical digest.
	a, err := CanonicalFailureOutput(&protos.TaskFailureDetails{
		ErrorType:      "X",
		IsNonRetriable: true,
	})
	require.NoError(t, err)
	b, err := CanonicalFailureOutput(&protos.TaskFailureDetails{
		ErrorType:      "X",
		IsNonRetriable: false,
	})
	require.NoError(t, err)
	assert.Equal(t, a, b)
}

func TestCanonicalFailureOutputWithInner(t *testing.T) {
	fd := &protos.TaskFailureDetails{
		ErrorType:    "Outer",
		ErrorMessage: "wraps",
		InnerFailure: &protos.TaskFailureDetails{
			ErrorType:    "Inner",
			ErrorMessage: "root cause",
		},
	}

	got, err := CanonicalFailureOutput(fd)
	require.NoError(t, err)

	expected := []byte{}
	expected = appendU32Len(expected, "Outer")
	expected = appendU32Len(expected, "wraps")
	expected = appendU32Len(expected, "") // stackTrace unset
	expected = append(expected, 1)        // inner present
	expected = appendU32Len(expected, "Inner")
	expected = appendU32Len(expected, "root cause")
	expected = appendU32Len(expected, "") // inner stackTrace unset
	expected = append(expected, 0)        // no inner-inner

	assert.Equal(t, expected, got)
}

func TestCanonicalFailureOutputRejectsOverlyDeepChain(t *testing.T) {
	// Build a chain just beyond the recursion limit. The signer must
	// refuse to produce canonical bytes for it rather than stack-overflow.
	root := &protos.TaskFailureDetails{ErrorType: "root"}
	current := root
	for i := 0; i < maxFailureRecursionDepth+5; i++ {
		current.InnerFailure = &protos.TaskFailureDetails{ErrorType: "nested"}
		current = current.InnerFailure
	}

	_, err := CanonicalFailureOutput(root)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "maximum depth")
}

func TestCanonicalFailureOutputAcceptsChainAtLimit(t *testing.T) {
	// A chain of exactly maxFailureRecursionDepth levels must be
	// accepted — the limit is an upper bound, not an off-by-one trip.
	root := &protos.TaskFailureDetails{ErrorType: "root"}
	current := root
	for i := 1; i < maxFailureRecursionDepth; i++ {
		current.InnerFailure = &protos.TaskFailureDetails{ErrorType: "nested"}
		current = current.InnerFailure
	}

	_, err := CanonicalFailureOutput(root)
	require.NoError(t, err)
}

func TestCanonicalInputNFCNormalization(t *testing.T) {
	// é encoded as NFD (U+0065 U+0301) vs NFC (U+00E9) must produce
	// identical canonical bytes so cross-language SDKs interoperate.
	nfd := wrapperspb.String("e\u0301")
	nfc := wrapperspb.String("\u00e9")
	assert.Equal(t, CanonicalInput(nfc), CanonicalInput(nfd))
	assert.Equal(t, CanonicalSuccessOutput(nfc), CanonicalSuccessOutput(nfd))
}

func TestCanonicalFailureOutputNFCNormalization(t *testing.T) {
	nfdFailure := &protos.TaskFailureDetails{
		ErrorType:    "e\u0301rror",
		ErrorMessage: "bo\u00f6m",
	}
	nfcFailure := &protos.TaskFailureDetails{
		ErrorType:    "\u00e9rror",
		ErrorMessage: "bo\u00f6m",
	}

	a, err := CanonicalFailureOutput(nfdFailure)
	require.NoError(t, err)
	b, err := CanonicalFailureOutput(nfcFailure)
	require.NoError(t, err)
	assert.Equal(t, a, b)
}

func TestIODigestDeterministic(t *testing.T) {
	input := []byte("in")
	output := []byte("out")
	assert.Equal(t, IODigest(input, output), IODigest(input, output))
}

func TestIODigestLengthPrefixingPreventsConcatAmbiguity(t *testing.T) {
	// "ab" + "c" vs "a" + "bc" must produce different digests — would be equal
	// without length prefixing.
	d1 := IODigest([]byte("ab"), []byte("c"))
	d2 := IODigest([]byte("a"), []byte("bc"))
	assert.NotEqual(t, d1, d2)
}

func TestIODigestChangesOnAnyChange(t *testing.T) {
	base := IODigest([]byte("x"), []byte("y"))
	assert.NotEqual(t, base, IODigest([]byte("x"), []byte("y!")))
	assert.NotEqual(t, base, IODigest([]byte("x!"), []byte("y")))
}

func TestCertDigestDistinctForDistinctCerts(t *testing.T) {
	certA, _ := generateEd25519Cert(t)
	certB, _ := generateEd25519Cert(t)
	assert.NotEqual(t, CertDigest(certA), CertDigest(certB))
}

// --- Child attestation build & verify ---

// signingAlgorithm wraps a cert-and-key generator so round-trip tests can
// run under each supported signing algorithm (Ed25519, ECDSA P-256, RSA
// 2048). Signatures produced by different algorithms have different
// formats and verify through different code paths in the kit signer —
// exercising all three catches algorithm-specific regressions.
type signingAlgorithm struct {
	name string
	gen  func(t *testing.T) ([]byte, crypto.Signer)
}

func signingAlgorithms(t *testing.T) []signingAlgorithm {
	t.Helper()
	return []signingAlgorithm{
		{
			name: "Ed25519",
			gen: func(t *testing.T) ([]byte, crypto.Signer) {
				cert, priv := generateEd25519Cert(t)
				return cert, priv
			},
		},
		{
			name: "ECDSA",
			gen: func(t *testing.T) ([]byte, crypto.Signer) {
				cert, priv := generateECDSACert(t)
				return cert, priv
			},
		},
		{
			name: "RSA",
			gen: func(t *testing.T) ([]byte, crypto.Signer) {
				cert, priv := generateRSACert(t)
				return cert, priv
			},
		},
	}
}

func TestBuildVerifyChildAttestationAllAlgorithms(t *testing.T) {
	for _, alg := range signingAlgorithms(t) {
		t.Run(alg.name, func(t *testing.T) {
			certDER, priv := alg.gen(t)
			s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

			input := wrapperspb.String("in-" + alg.name)
			output := wrapperspb.String("out-" + alg.name)

			att, certChain, err := BuildChildAttestation(s, ChildAttestationInput{
				ParentInstanceId:      testParentInstanceID,
				ParentTaskScheduledId: testParentTaskID,
				Input:                 input,
				Output:                output,
				TerminalStatus:        protos.TerminalStatus_TERMINAL_STATUS_COMPLETED,
			})
			require.NoError(t, err)

			_, err = VerifyChildAttestation(VerifyChildOptions{
				Attestation:                   att,
				SignerCertDER:                 certChain,
				EventTimestamp:                testEventTime(),
				ExpectedParentInstanceId:      testParentInstanceID,
				ExpectedParentTaskScheduledId: testParentTaskID,
				ClaimedInput:                  input,
				ClaimedOutput:                 output,
				Signer:                        s,
			})
			require.NoError(t, err)
		})
	}
}

func TestBuildVerifyActivityAttestationAllAlgorithms(t *testing.T) {
	for _, alg := range signingAlgorithms(t) {
		t.Run(alg.name, func(t *testing.T) {
			certDER, priv := alg.gen(t)
			s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

			input := wrapperspb.String("in-" + alg.name)
			output := wrapperspb.String("out-" + alg.name)

			att, certChain, err := BuildActivityAttestation(s, ActivityAttestationInput{
				ParentInstanceId:      testParentInstanceID,
				ParentTaskScheduledId: testParentTaskID,
				ActivityName:          testActivityName,
				Input:                 input,
				Output:                output,
				TerminalStatus:        protos.ActivityTerminalStatus_ACTIVITY_TERMINAL_STATUS_COMPLETED,
			})
			require.NoError(t, err)

			_, err = VerifyActivityAttestation(VerifyActivityOptions{
				Attestation:                   att,
				SignerCertDER:                 certChain,
				EventTimestamp:                testEventTime(),
				ExpectedParentInstanceId:      testParentInstanceID,
				ExpectedParentTaskScheduledId: testParentTaskID,
				ExpectedActivityName:          testActivityName,
				ClaimedInput:                  input,
				ClaimedOutput:                 output,
				Signer:                        s,
			})
			require.NoError(t, err)
		})
	}
}

func TestBuildVerifyChildAttestationRoundTripCompleted(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	input := wrapperspb.String(`{"k":"v"}`)
	output := wrapperspb.String(`{"ok":true}`)

	att, certChain, err := BuildChildAttestation(s, ChildAttestationInput{
		ParentInstanceId:      testParentInstanceID,
		ParentTaskScheduledId: testParentTaskID,
		Input:                 input,
		Output:                output,
		TerminalStatus:        protos.TerminalStatus_TERMINAL_STATUS_COMPLETED,
	})
	require.NoError(t, err)
	require.NotNil(t, att)
	require.NotEmpty(t, att.GetPayload())
	require.NotEmpty(t, att.GetSignature())
	require.NotEmpty(t, certChain)

	payload, err := VerifyChildAttestation(VerifyChildOptions{
		Attestation:                   att,
		SignerCertDER:                 certChain,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      testParentInstanceID,
		ExpectedParentTaskScheduledId: testParentTaskID,
		ClaimedInput:                  input,
		ClaimedOutput:                 output,
		Signer:                        s,
	})
	require.NoError(t, err)
	assert.Equal(t, testParentInstanceID, payload.GetParentInstanceId())
	assert.Equal(t, testParentTaskID, payload.GetParentTaskScheduledId())
	assert.Equal(t, protos.TerminalStatus_TERMINAL_STATUS_COMPLETED, payload.GetTerminalStatus())
	assert.Equal(t, CanonicalSpecVersion, payload.GetCanonicalSpecVersion())
	assert.Equal(t, CertDigest(certChain), payload.GetSignerCertDigest())
}

func TestBuildVerifyChildAttestationRoundTripFailed(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	input := wrapperspb.String(`{"k":"v"}`)
	failure := &protos.TaskFailureDetails{
		ErrorType:    "SomeError",
		ErrorMessage: "bad",
	}

	att, certChain, err := BuildChildAttestation(s, ChildAttestationInput{
		ParentInstanceId:      testParentInstanceID,
		ParentTaskScheduledId: testParentTaskID,
		Input:                 input,
		FailureDetails:        failure,
		TerminalStatus:        protos.TerminalStatus_TERMINAL_STATUS_FAILED,
	})
	require.NoError(t, err)

	_, err = VerifyChildAttestation(VerifyChildOptions{
		Attestation:                   att,
		SignerCertDER:                 certChain,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      testParentInstanceID,
		ExpectedParentTaskScheduledId: testParentTaskID,
		ClaimedInput:                  input,
		ClaimedFailure:                failure,
		Signer:                        s,
	})
	require.NoError(t, err)
}

func TestBuildChildAttestationRejectsUnspecifiedStatus(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	_, _, err := BuildChildAttestation(s, ChildAttestationInput{
		ParentInstanceId:      testParentInstanceID,
		ParentTaskScheduledId: testParentTaskID,
		TerminalStatus:        protos.TerminalStatus_TERMINAL_STATUS_UNSPECIFIED,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "terminal status")
}

func TestBuildChildAttestationRejectsEmptyParent(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	_, _, err := BuildChildAttestation(s, ChildAttestationInput{
		TerminalStatus: protos.TerminalStatus_TERMINAL_STATUS_COMPLETED,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parent instance ID")
}

// --- Child verification rejection cases ---

func TestVerifyChildRejectsWrongCompanionCert(t *testing.T) {
	s, att, _ := newChildAttestationForTest(t)
	otherCertDER, _ := generateEd25519Cert(t)

	_, err := VerifyChildAttestation(VerifyChildOptions{
		Attestation:                   att,
		SignerCertDER:                 otherCertDER,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      testParentInstanceID,
		ExpectedParentTaskScheduledId: testParentTaskID,
		ClaimedInput:                  wrapperspb.String("in"),
		ClaimedOutput:                 wrapperspb.String("out"),
		Signer:                        s,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "signer cert digest mismatch")
}

func TestVerifyChildRejectsTamperedSignature(t *testing.T) {
	s, att, certChain := newChildAttestationForTest(t)

	tampered := proto.Clone(att).(*protos.ChildCompletionAttestation)
	tampered.Signature = bytes.Repeat([]byte{0xFF}, len(att.GetSignature()))

	_, err := VerifyChildAttestation(VerifyChildOptions{
		Attestation:                   tampered,
		SignerCertDER:                 certChain,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      testParentInstanceID,
		ExpectedParentTaskScheduledId: testParentTaskID,
		ClaimedInput:                  wrapperspb.String("in"),
		ClaimedOutput:                 wrapperspb.String("out"),
		Signer:                        s,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "signature verification failed")
}

func TestVerifyChildRejectsTamperedPayload(t *testing.T) {
	s, att, certChain := newChildAttestationForTest(t)

	// Swap payload bytes for a different, freshly-marshaled payload. The
	// signature was computed over the original payload, so verification
	// must fail.
	other := &protos.ChildCompletionAttestationPayload{
		ParentInstanceId:      "other",
		ParentTaskScheduledId: 99,
		IoDigest:              make([]byte, 32),
		SignerCertDigest:      CertDigest(certChain),
		TerminalStatus:        protos.TerminalStatus_TERMINAL_STATUS_COMPLETED,
		CanonicalSpecVersion:  CanonicalSpecVersion,
	}
	otherBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(other)
	require.NoError(t, err)

	tampered := proto.Clone(att).(*protos.ChildCompletionAttestation)
	tampered.Payload = otherBytes

	_, err = VerifyChildAttestation(VerifyChildOptions{
		Attestation:                   tampered,
		SignerCertDER:                 certChain,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      "other",
		ExpectedParentTaskScheduledId: 99,
		ClaimedInput:                  wrapperspb.String(""),
		ClaimedOutput:                 wrapperspb.String(""),
		Signer:                        s,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "signature verification failed")
}

func TestVerifyChildRejectsWrongParent(t *testing.T) {
	s, att, certChain := newChildAttestationForTest(t)

	_, err := VerifyChildAttestation(VerifyChildOptions{
		Attestation:                   att,
		SignerCertDER:                 certChain,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      "some-other-parent",
		ExpectedParentTaskScheduledId: testParentTaskID,
		ClaimedInput:                  wrapperspb.String("in"),
		ClaimedOutput:                 wrapperspb.String("out"),
		Signer:                        s,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parent instance ID mismatch")
}

func TestVerifyChildRejectsWrongTaskID(t *testing.T) {
	s, att, certChain := newChildAttestationForTest(t)

	_, err := VerifyChildAttestation(VerifyChildOptions{
		Attestation:                   att,
		SignerCertDER:                 certChain,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      testParentInstanceID,
		ExpectedParentTaskScheduledId: testParentTaskID + 1,
		ClaimedInput:                  wrapperspb.String("in"),
		ClaimedOutput:                 wrapperspb.String("out"),
		Signer:                        s,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parent task scheduled ID mismatch")
}

func TestVerifyChildRejectsWrongClaimedInput(t *testing.T) {
	s, att, certChain := newChildAttestationForTest(t)

	_, err := VerifyChildAttestation(VerifyChildOptions{
		Attestation:                   att,
		SignerCertDER:                 certChain,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      testParentInstanceID,
		ExpectedParentTaskScheduledId: testParentTaskID,
		ClaimedInput:                  wrapperspb.String("tampered-input"),
		ClaimedOutput:                 wrapperspb.String("out"),
		Signer:                        s,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ioDigest mismatch")
}

func TestVerifyChildRejectsWrongClaimedOutput(t *testing.T) {
	s, att, certChain := newChildAttestationForTest(t)

	_, err := VerifyChildAttestation(VerifyChildOptions{
		Attestation:                   att,
		SignerCertDER:                 certChain,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      testParentInstanceID,
		ExpectedParentTaskScheduledId: testParentTaskID,
		ClaimedInput:                  wrapperspb.String("in"),
		ClaimedOutput:                 wrapperspb.String("tampered-output"),
		Signer:                        s,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ioDigest mismatch")
}

func TestVerifyChildRejectsEventTimestampAfterCertExpiry(t *testing.T) {
	notBefore := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	notAfter := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	certDER, priv := generateEd25519CertWithValidity(t, notBefore, notAfter)
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	att, certChain, err := BuildChildAttestation(s, ChildAttestationInput{
		ParentInstanceId:      testParentInstanceID,
		ParentTaskScheduledId: testParentTaskID,
		Input:                 wrapperspb.String("in"),
		Output:                wrapperspb.String("out"),
		TerminalStatus:        protos.TerminalStatus_TERMINAL_STATUS_COMPLETED,
	})
	require.NoError(t, err)

	_, err = VerifyChildAttestation(VerifyChildOptions{
		Attestation:                   att,
		SignerCertDER:                 certChain,
		EventTimestamp:                notAfter.Add(24 * time.Hour),
		ExpectedParentInstanceId:      testParentInstanceID,
		ExpectedParentTaskScheduledId: testParentTaskID,
		ClaimedInput:                  wrapperspb.String("in"),
		ClaimedOutput:                 wrapperspb.String("out"),
		Signer:                        s,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "chain-of-trust")
}

func TestVerifyChildRejectsUnknownSpecVersion(t *testing.T) {
	s, att, certChain := newChildAttestationForTest(t)

	// Construct a payload with an unknown spec version, re-sign with the
	// same key so the signature is valid — verifier must still reject.
	var p protos.ChildCompletionAttestationPayload
	require.NoError(t, proto.Unmarshal(att.GetPayload(), &p))
	p.CanonicalSpecVersion = 999

	bad, err := proto.MarshalOptions{Deterministic: true}.Marshal(&p)
	require.NoError(t, err)

	sig, _, err := s.Sign(PayloadSignatureInput(bad))
	require.NoError(t, err)

	tampered := &protos.ChildCompletionAttestation{Payload: bad, Signature: sig}

	_, err = VerifyChildAttestation(VerifyChildOptions{
		Attestation:                   tampered,
		SignerCertDER:                 certChain,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      testParentInstanceID,
		ExpectedParentTaskScheduledId: testParentTaskID,
		ClaimedInput:                  wrapperspb.String("in"),
		ClaimedOutput:                 wrapperspb.String("out"),
		Signer:                        s,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "canonical spec version")
}

func TestVerifyChildRejectsNilAttestation(t *testing.T) {
	_, err := VerifyChildAttestation(VerifyChildOptions{})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "attestation must not be nil")
}

// --- Activity attestation build & verify ---

func TestBuildVerifyActivityAttestationRoundTripCompleted(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	input := wrapperspb.String(`"hello"`)
	output := wrapperspb.String(`"world"`)

	att, certChain, err := BuildActivityAttestation(s, ActivityAttestationInput{
		ParentInstanceId:      testParentInstanceID,
		ParentTaskScheduledId: testParentTaskID,
		ActivityName:          testActivityName,
		Input:                 input,
		Output:                output,
		TerminalStatus:        protos.ActivityTerminalStatus_ACTIVITY_TERMINAL_STATUS_COMPLETED,
	})
	require.NoError(t, err)

	payload, err := VerifyActivityAttestation(VerifyActivityOptions{
		Attestation:                   att,
		SignerCertDER:                 certChain,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      testParentInstanceID,
		ExpectedParentTaskScheduledId: testParentTaskID,
		ExpectedActivityName:          testActivityName,
		ClaimedInput:                  input,
		ClaimedOutput:                 output,
		Signer:                        s,
	})
	require.NoError(t, err)
	assert.Equal(t, testActivityName, payload.GetActivityName())
}

func TestBuildActivityRejectsEmptyActivityName(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	_, _, err := BuildActivityAttestation(s, ActivityAttestationInput{
		ParentInstanceId:      testParentInstanceID,
		ParentTaskScheduledId: testParentTaskID,
		TerminalStatus:        protos.ActivityTerminalStatus_ACTIVITY_TERMINAL_STATUS_COMPLETED,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "activity name")
}

func TestVerifyActivityRejectsWrongActivityName(t *testing.T) {
	certDER, priv := generateEd25519Cert(t)
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	att, certChain, err := BuildActivityAttestation(s, ActivityAttestationInput{
		ParentInstanceId:      testParentInstanceID,
		ParentTaskScheduledId: testParentTaskID,
		ActivityName:          testActivityName,
		Input:                 wrapperspb.String("in"),
		Output:                wrapperspb.String("out"),
		TerminalStatus:        protos.ActivityTerminalStatus_ACTIVITY_TERMINAL_STATUS_COMPLETED,
	})
	require.NoError(t, err)

	_, err = VerifyActivityAttestation(VerifyActivityOptions{
		Attestation:                   att,
		SignerCertDER:                 certChain,
		EventTimestamp:                testEventTime(),
		ExpectedParentInstanceId:      testParentInstanceID,
		ExpectedParentTaskScheduledId: testParentTaskID,
		ExpectedActivityName:          "SomeOtherActivity",
		ClaimedInput:                  wrapperspb.String("in"),
		ClaimedOutput:                 wrapperspb.String("out"),
		Signer:                        s,
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "activity name mismatch")
}

// --- helpers ---

func newChildAttestationForTest(t *testing.T) (*signer.Signer, *protos.ChildCompletionAttestation, []byte) {
	t.Helper()
	certDER, priv := generateEd25519Cert(t)
	s := newTestSigner(t, certDER, priv, parseCert(t, certDER))

	a, c, err := BuildChildAttestation(s, ChildAttestationInput{
		ParentInstanceId:      testParentInstanceID,
		ParentTaskScheduledId: testParentTaskID,
		Input:                 wrapperspb.String("in"),
		Output:                wrapperspb.String("out"),
		TerminalStatus:        protos.TerminalStatus_TERMINAL_STATUS_COMPLETED,
	})
	require.NoError(t, err)
	return s, a, c
}

func appendU32Len(buf []byte, s string) []byte {
	out := append(buf, byte(len(s)>>24), byte(len(s)>>16), byte(len(s)>>8), byte(len(s)))
	return append(out, s...)
}
