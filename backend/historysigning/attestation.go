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
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"golang.org/x/text/unicode/norm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/crypto/spiffe/signer"
)

// CanonicalSpecVersion is the current version of the canonical byte
// serialization spec documented in attestation.proto. Build side writes this
// into the payload; verify side rejects values it doesn't recognize.
const CanonicalSpecVersion uint32 = 1

// maxFailureRecursionDepth caps TaskFailureDetails.innerFailure recursion
// during canonical serialization. Deeply nested or self-referential chains
// must not be allowed to stack-overflow either the signer (trusted) or the
// verifier (attacker-controlled input). A limit of 32 is well above any
// plausible real-world error chain (typical depths are 1-3) while still
// giving generous headroom.
const maxFailureRecursionDepth = 32

// canonicalString returns the NFC-normalized UTF-8 bytes of s. NFC is the
// required canonical form for all user-visible string content that feeds
// into ioDigest computation, so SDKs in different languages (which may
// default to different Unicode normalization forms) produce identical
// bytes for semantically equal strings.
func canonicalString(s string) []byte {
	return norm.NFC.Bytes([]byte(s))
}

// CanonicalInput returns the canonical bytes for an invocation's input as
// defined in attestation.proto: NFC-normalized UTF-8 bytes of the
// StringValue's value field, or zero-length when the wrapper is unset.
// NFC normalization ensures semantically equal strings produced by
// different-language SDKs hash identically.
func CanonicalInput(sv *wrapperspb.StringValue) []byte {
	if sv == nil {
		return nil
	}
	return canonicalString(sv.GetValue())
}

// CanonicalSuccessOutput returns the canonical bytes for a COMPLETED
// invocation's output. Same rule as CanonicalInput.
func CanonicalSuccessOutput(sv *wrapperspb.StringValue) []byte {
	if sv == nil {
		return nil
	}
	return canonicalString(sv.GetValue())
}

// CanonicalFailureOutput returns the canonical bytes for a FAILED
// invocation's output: a spec-defined serialization of TaskFailureDetails
// independent of protobuf wire format. See the "Output bytes, FAILED"
// section of the spec block in attestation.proto for the exact rules.
//
// Returns an error when the innerFailure chain is deeper than
// maxFailureRecursionDepth (DoS guard against attacker-supplied malformed
// chains). A nil input is serialized as a TaskFailureDetails with all fields
// at their zero values. This keeps the output deterministic rather than
// short-circuiting.
func CanonicalFailureOutput(fd *protos.TaskFailureDetails) ([]byte, error) {
	var buf bytes.Buffer
	if err := appendCanonicalFailure(&buf, fd, 0); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func appendCanonicalFailure(buf *bytes.Buffer, fd *protos.TaskFailureDetails, depth int) error {
	if depth >= maxFailureRecursionDepth {
		return fmt.Errorf("TaskFailureDetails.innerFailure chain exceeds maximum depth %d", maxFailureRecursionDepth)
	}

	var (
		errorType    string
		errorMessage string
		stackTrace   string
		inner        *protos.TaskFailureDetails
	)
	if fd != nil {
		errorType = fd.GetErrorType()
		errorMessage = fd.GetErrorMessage()
		if sv := fd.GetStackTrace(); sv != nil {
			stackTrace = sv.GetValue()
		}
		inner = fd.GetInnerFailure()
	}

	writeLenPrefixedCanonicalString(buf, errorType)
	writeLenPrefixedCanonicalString(buf, errorMessage)
	writeLenPrefixedCanonicalString(buf, stackTrace)
	if inner != nil {
		buf.WriteByte(1)
		if err := appendCanonicalFailure(buf, inner, depth+1); err != nil {
			return err
		}
	} else {
		buf.WriteByte(0)
	}
	return nil
}

func writeLenPrefixedCanonicalString(buf *bytes.Buffer, s string) {
	nfc := canonicalString(s)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(nfc)))
	buf.Write(lenBuf[:])
	buf.Write(nfc)
}

// IODigest computes the ioDigest as defined in attestation.proto:
// sha256( u64be(len(input)) || input || u64be(len(output)) || output ).
// Input and output bytes must already be in canonical form from
// CanonicalInput / CanonicalSuccessOutput / CanonicalFailureOutput as
// appropriate for the invocation's terminal status.
func IODigest(inputBytes, outputBytes []byte) []byte {
	h := sha256.New()
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(inputBytes)))
	h.Write(lenBuf[:])
	h.Write(inputBytes)
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(outputBytes)))
	h.Write(lenBuf[:])
	h.Write(outputBytes)
	return h.Sum(nil)
}

// CertDigest computes the signerCertDigest as defined in attestation.proto:
// sha256 of the DER-encoded X.509 certificate chain bytes (leaf first,
// intermediates concatenated). Computed directly over DER bytes, never over a
// protobuf envelope, so the digest is stable across protobuf version changes.
func CertDigest(certChainDER []byte) []byte {
	d := sha256.Sum256(certChainDER)
	return d[:]
}

// PayloadSignatureInput is the exact byte sequence the signer signs over
// (and the verifier re-hashes) for both child and activity attestations:
// sha256(payloadBytes). The payload bytes are the deterministic marshal of
// the inner ...Payload message, produced once on the build side and
// treated as opaque bytes thereafter.
func PayloadSignatureInput(payloadBytes []byte) []byte {
	d := sha256.Sum256(payloadBytes)
	return d[:]
}

// --- Child workflow attestations ---

// ChildAttestationInput carries everything BuildChildAttestation needs.
// Exactly one of Output / FailureDetails should be set, depending on
// TerminalStatus: COMPLETED uses Output; FAILED uses FailureDetails.
type ChildAttestationInput struct {
	// ParentInstanceId is the instance ID of the parent workflow that
	// scheduled this child.
	ParentInstanceId string
	// ParentTaskScheduledId is the taskScheduledId from the parent's
	// ChildWorkflowInstanceCreatedEvent.
	ParentTaskScheduledId int32
	// Input is the StringValue that was delivered to the child in its
	// ExecutionStartedEvent.input.
	Input *wrapperspb.StringValue
	// Output is the child's result on COMPLETED; nil otherwise.
	Output *wrapperspb.StringValue
	// FailureDetails is the child's failure on FAILED; nil otherwise.
	FailureDetails *protos.TaskFailureDetails
	// TerminalStatus records how the child ended.
	TerminalStatus protos.TerminalStatus
}

// BuildChildAttestation constructs, signs, and returns a child completion
// attestation ready to attach to an outbound ChildWorkflowInstanceCompleted
// or ChildWorkflowInstanceFailed event. The returned certChainDER is the
// companion certificate to include alongside the attestation on the wire
// (receivers strip it after absorbing into their external cert table).
func BuildChildAttestation(s *signer.Signer, in ChildAttestationInput) (*protos.ChildCompletionAttestation, []byte, error) {
	if s == nil {
		return nil, nil, errors.New("signer must not be nil")
	}
	if in.ParentInstanceId == "" {
		return nil, nil, errors.New("parent instance ID must not be empty")
	}
	if in.TerminalStatus == protos.TerminalStatus_TERMINAL_STATUS_UNSPECIFIED {
		return nil, nil, errors.New("terminal status must not be unspecified")
	}

	outputBytes, err := canonicalChildOutput(in.TerminalStatus, in.Output, in.FailureDetails)
	if err != nil {
		return nil, nil, err
	}
	ioDigest := IODigest(CanonicalInput(in.Input), outputBytes)

	// Retrieve the cert chain up front so the signerCertDigest field can
	// be committed into the signed payload before signing.
	certChainDER, err := s.CertChainDER()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve signer cert chain: %w", err)
	}

	payload := &protos.ChildCompletionAttestationPayload{
		ParentInstanceId:      in.ParentInstanceId,
		ParentTaskScheduledId: in.ParentTaskScheduledId,
		IoDigest:              ioDigest,
		SignerCertDigest:      CertDigest(certChainDER),
		TerminalStatus:        in.TerminalStatus,
		CanonicalSpecVersion:  CanonicalSpecVersion,
	}

	payloadBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal child attestation payload: %w", err)
	}

	sig, sigCertChainDER, err := s.Sign(PayloadSignatureInput(payloadBytes))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to sign child attestation payload: %w", err)
	}
	if !bytes.Equal(sigCertChainDER, certChainDER) {
		// SVID rotated between CertChainDER and Sign. The payload's
		// signerCertDigest no longer matches the signing cert. Abort
		// rather than emit a broken attestation.
		return nil, nil, errors.New("signer cert rotated mid-build; retry")
	}

	return &protos.ChildCompletionAttestation{
		Payload:   payloadBytes,
		Signature: sig,
	}, certChainDER, nil
}

func canonicalChildOutput(status protos.TerminalStatus, output *wrapperspb.StringValue, fd *protos.TaskFailureDetails) ([]byte, error) {
	switch status {
	case protos.TerminalStatus_TERMINAL_STATUS_COMPLETED:
		return CanonicalSuccessOutput(output), nil
	case protos.TerminalStatus_TERMINAL_STATUS_FAILED:
		return CanonicalFailureOutput(fd)
	default:
		return nil, fmt.Errorf("unsupported child terminal status %v", status)
	}
}

// VerifyChildOptions are the parameters for VerifyChildAttestation.
type VerifyChildOptions struct {
	// Attestation is the wrapper to verify.
	Attestation *protos.ChildCompletionAttestation
	// SignerCertDER is the companion signing certificate (DER chain) that
	// travelled alongside the attestation. Must hash to the payload's
	// signerCertDigest field.
	SignerCertDER []byte
	// EventTimestamp is the timestamp used for the certificate-validity
	// check. See the "Certificate validity" section of the spec block in
	// attestation.proto for the correct source at ingestion vs. stored
	// history.
	EventTimestamp time.Time
	// ExpectedParentInstanceId is the instance ID of the workflow
	// performing verification. The attestation's parentInstanceId must
	// match this value exactly.
	ExpectedParentInstanceId string
	// ExpectedParentTaskScheduledId is the taskScheduledId this attestation
	// is claimed to correspond to (from a signed
	// ChildWorkflowInstanceCreatedEvent in the verifier's history).
	ExpectedParentTaskScheduledId int32
	// ClaimedInput is the input bytes the verifier believes B was invoked
	// with (taken from the ChildWorkflowInstanceCreatedEvent in the
	// verifier's signed history). Compared against the payload's ioDigest
	// after canonicalization.
	ClaimedInput *wrapperspb.StringValue
	// ClaimedOutput is the result bytes the verifier has (from the
	// ChildWorkflowInstanceCompletedEvent) when TerminalStatus is
	// COMPLETED. Nil otherwise.
	ClaimedOutput *wrapperspb.StringValue
	// ClaimedFailure is the failure details the verifier has (from the
	// ChildWorkflowInstanceFailedEvent) when TerminalStatus is FAILED.
	// Nil otherwise.
	ClaimedFailure *protos.TaskFailureDetails
	// Signer provides cryptographic verification and certificate chain-of-
	// trust checking against Sentry trust anchors.
	Signer *signer.Signer
	// SkipChainOfTrust skips the VerifyCertChainOfTrust call and trusts
	// the caller to have verified the cert chain independently (e.g. via
	// a per-orchestrator cache keyed by signerCertDigest). Signature,
	// digest, and parent binding checks are still performed. Use only
	// when the caller can guarantee the cert chain was previously
	// verified at a time covered by EventTimestamp.
	SkipChainOfTrust bool
}

// VerifyChildAttestation verifies a child completion attestation end to
// end: payload parse, required-field presence, spec version, cert/digest
// binding, chain-of-trust at EventTimestamp, signature, parent binding,
// and ioDigest against the claimed input/output. Returns the parsed
// payload on success for callers that want to read its fields (e.g.,
// terminal status, final signature digest).
func VerifyChildAttestation(opts VerifyChildOptions) (*protos.ChildCompletionAttestationPayload, error) {
	if opts.Attestation == nil {
		return nil, errors.New("attestation must not be nil")
	}
	if opts.Signer == nil {
		return nil, errors.New("signer must not be nil")
	}
	if len(opts.Attestation.GetPayload()) == 0 {
		return nil, errors.New("attestation payload must not be empty")
	}
	if len(opts.Attestation.GetSignature()) == 0 {
		return nil, errors.New("attestation signature must not be empty")
	}
	if len(opts.SignerCertDER) == 0 {
		return nil, errors.New("signer cert must not be empty")
	}

	var payload protos.ChildCompletionAttestationPayload
	if err := proto.Unmarshal(opts.Attestation.GetPayload(), &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal attestation payload: %w", err)
	}

	if err := validateChildPayloadFields(&payload); err != nil {
		return nil, err
	}

	// Cert binding: companion must hash to committed digest.
	if gotDigest := CertDigest(opts.SignerCertDER); !bytes.Equal(gotDigest, payload.GetSignerCertDigest()) {
		return nil, errors.New("signer cert digest mismatch: companion cert does not match committed digest")
	}

	// Cryptographic signature over sha256(payload). Verified before the
	// (potentially expensive) chain-of-trust check so that an invalid
	// signature short-circuits without paying trust validation cost. This
	// matches the order used by VerifyChain in verify.go.
	if err := opts.Signer.VerifySignature(PayloadSignatureInput(opts.Attestation.GetPayload()), opts.Attestation.GetSignature(), opts.SignerCertDER); err != nil {
		return nil, fmt.Errorf("attestation signature verification failed: %w", err)
	}

	// Chain-of-trust at event timestamp. Skipped when the caller has
	// verified this cert chain at a time covering EventTimestamp (e.g.
	// via a per-orchestrator cert cache).
	if !opts.SkipChainOfTrust {
		if err := opts.Signer.VerifyCertChainOfTrust(opts.SignerCertDER, opts.EventTimestamp); err != nil {
			return nil, fmt.Errorf("signer cert chain-of-trust verification failed at %v: %w", opts.EventTimestamp, err)
		}
	}

	// Parent and task binding.
	if payload.GetParentInstanceId() != opts.ExpectedParentInstanceId {
		return nil, fmt.Errorf("parent instance ID mismatch: attestation claims %q, verifier expected %q",
			payload.GetParentInstanceId(), opts.ExpectedParentInstanceId)
	}
	if payload.GetParentTaskScheduledId() != opts.ExpectedParentTaskScheduledId {
		return nil, fmt.Errorf("parent task scheduled ID mismatch: attestation claims %d, verifier expected %d",
			payload.GetParentTaskScheduledId(), opts.ExpectedParentTaskScheduledId)
	}

	// IO digest cross-check against the verifier's claimed values.
	claimedOutputBytes, err := canonicalChildOutput(payload.GetTerminalStatus(), opts.ClaimedOutput, opts.ClaimedFailure)
	if err != nil {
		return nil, err
	}
	expectedIODigest := IODigest(CanonicalInput(opts.ClaimedInput), claimedOutputBytes)
	if !bytes.Equal(expectedIODigest, payload.GetIoDigest()) {
		return nil, errors.New("ioDigest mismatch: claimed input/output does not match committed digest")
	}

	return &payload, nil
}

func validateChildPayloadFields(p *protos.ChildCompletionAttestationPayload) error {
	if p.GetCanonicalSpecVersion() != CanonicalSpecVersion {
		return fmt.Errorf("unsupported canonical spec version %d (this build supports %d)",
			p.GetCanonicalSpecVersion(), CanonicalSpecVersion)
	}
	if p.GetParentInstanceId() == "" {
		return errors.New("payload parent instance ID must not be empty")
	}
	if len(p.GetIoDigest()) != sha256.Size {
		return fmt.Errorf("payload ioDigest must be %d bytes, got %d", sha256.Size, len(p.GetIoDigest()))
	}
	if len(p.GetSignerCertDigest()) != sha256.Size {
		return fmt.Errorf("payload signerCertDigest must be %d bytes, got %d", sha256.Size, len(p.GetSignerCertDigest()))
	}
	switch p.GetTerminalStatus() {
	case protos.TerminalStatus_TERMINAL_STATUS_COMPLETED,
		protos.TerminalStatus_TERMINAL_STATUS_FAILED:
		// ok
	default:
		return fmt.Errorf("payload terminalStatus must be set to a known value, got %v", p.GetTerminalStatus())
	}
	return nil
}

// --- Activity attestations ---

// ActivityAttestationInput carries everything BuildActivityAttestation
// needs. Exactly one of Output / FailureDetails should be set, matching
// TerminalStatus.
type ActivityAttestationInput struct {
	ParentInstanceId      string
	ParentTaskScheduledId int32
	ActivityName          string
	Input                 *wrapperspb.StringValue
	Output                *wrapperspb.StringValue
	FailureDetails        *protos.TaskFailureDetails
	TerminalStatus        protos.ActivityTerminalStatus
}

// BuildActivityAttestation is the activity-side analogue of
// BuildChildAttestation. Activities have no signed history, so there is no
// final-signature anchor.
func BuildActivityAttestation(s *signer.Signer, in ActivityAttestationInput) (*protos.ActivityCompletionAttestation, []byte, error) {
	if s == nil {
		return nil, nil, errors.New("signer must not be nil")
	}
	if in.ParentInstanceId == "" {
		return nil, nil, errors.New("parent instance ID must not be empty")
	}
	if in.ActivityName == "" {
		return nil, nil, errors.New("activity name must not be empty")
	}
	if in.TerminalStatus == protos.ActivityTerminalStatus_ACTIVITY_TERMINAL_STATUS_UNSPECIFIED {
		return nil, nil, errors.New("terminal status must not be unspecified")
	}

	outputBytes, err := canonicalActivityOutput(in.TerminalStatus, in.Output, in.FailureDetails)
	if err != nil {
		return nil, nil, err
	}
	ioDigest := IODigest(CanonicalInput(in.Input), outputBytes)

	certChainDER, err := s.CertChainDER()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to retrieve signer cert chain: %w", err)
	}

	payload := &protos.ActivityCompletionAttestationPayload{
		ParentInstanceId:      in.ParentInstanceId,
		ParentTaskScheduledId: in.ParentTaskScheduledId,
		ActivityName:          in.ActivityName,
		IoDigest:              ioDigest,
		SignerCertDigest:      CertDigest(certChainDER),
		TerminalStatus:        in.TerminalStatus,
		CanonicalSpecVersion:  CanonicalSpecVersion,
	}

	payloadBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(payload)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal activity attestation payload: %w", err)
	}

	sig, sigCertChainDER, err := s.Sign(PayloadSignatureInput(payloadBytes))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to sign activity attestation payload: %w", err)
	}
	if !bytes.Equal(sigCertChainDER, certChainDER) {
		return nil, nil, errors.New("signer cert rotated mid-build; retry")
	}

	return &protos.ActivityCompletionAttestation{
		Payload:   payloadBytes,
		Signature: sig,
	}, certChainDER, nil
}

func canonicalActivityOutput(status protos.ActivityTerminalStatus, output *wrapperspb.StringValue, fd *protos.TaskFailureDetails) ([]byte, error) {
	switch status {
	case protos.ActivityTerminalStatus_ACTIVITY_TERMINAL_STATUS_COMPLETED:
		return CanonicalSuccessOutput(output), nil
	case protos.ActivityTerminalStatus_ACTIVITY_TERMINAL_STATUS_FAILED:
		return CanonicalFailureOutput(fd)
	default:
		return nil, fmt.Errorf("unsupported activity terminal status %v", status)
	}
}

// VerifyActivityOptions are the parameters for VerifyActivityAttestation.
type VerifyActivityOptions struct {
	Attestation                   *protos.ActivityCompletionAttestation
	SignerCertDER                 []byte
	EventTimestamp                time.Time
	ExpectedParentInstanceId      string
	ExpectedParentTaskScheduledId int32
	ExpectedActivityName          string
	ClaimedInput                  *wrapperspb.StringValue
	ClaimedOutput                 *wrapperspb.StringValue
	ClaimedFailure                *protos.TaskFailureDetails
	Signer                        *signer.Signer
	// SkipChainOfTrust skips the VerifyCertChainOfTrust call and trusts
	// the caller to have verified the cert chain independently. See
	// VerifyChildOptions.SkipChainOfTrust.
	SkipChainOfTrust bool
}

// VerifyActivityAttestation is the activity-side analogue of
// VerifyChildAttestation. Additionally cross-checks the activity name.
func VerifyActivityAttestation(opts VerifyActivityOptions) (*protos.ActivityCompletionAttestationPayload, error) {
	if opts.Attestation == nil {
		return nil, errors.New("attestation must not be nil")
	}
	if opts.Signer == nil {
		return nil, errors.New("signer must not be nil")
	}
	if len(opts.Attestation.GetPayload()) == 0 {
		return nil, errors.New("attestation payload must not be empty")
	}
	if len(opts.Attestation.GetSignature()) == 0 {
		return nil, errors.New("attestation signature must not be empty")
	}
	if len(opts.SignerCertDER) == 0 {
		return nil, errors.New("signer cert must not be empty")
	}

	var payload protos.ActivityCompletionAttestationPayload
	if err := proto.Unmarshal(opts.Attestation.GetPayload(), &payload); err != nil {
		return nil, fmt.Errorf("failed to unmarshal attestation payload: %w", err)
	}

	if err := validateActivityPayloadFields(&payload); err != nil {
		return nil, err
	}

	if gotDigest := CertDigest(opts.SignerCertDER); !bytes.Equal(gotDigest, payload.GetSignerCertDigest()) {
		return nil, errors.New("signer cert digest mismatch: companion cert does not match committed digest")
	}

	// Cryptographic signature over sha256(payload). Verified before the
	// (potentially expensive) chain-of-trust check so that an invalid
	// signature short-circuits without paying trust validation cost. This
	// matches the order used by VerifyChain in verify.go.
	if err := opts.Signer.VerifySignature(PayloadSignatureInput(opts.Attestation.GetPayload()), opts.Attestation.GetSignature(), opts.SignerCertDER); err != nil {
		return nil, fmt.Errorf("attestation signature verification failed: %w", err)
	}

	if !opts.SkipChainOfTrust {
		if err := opts.Signer.VerifyCertChainOfTrust(opts.SignerCertDER, opts.EventTimestamp); err != nil {
			return nil, fmt.Errorf("signer cert chain-of-trust verification failed at %v: %w", opts.EventTimestamp, err)
		}
	}

	if payload.GetParentInstanceId() != opts.ExpectedParentInstanceId {
		return nil, fmt.Errorf("parent instance ID mismatch: attestation claims %q, verifier expected %q",
			payload.GetParentInstanceId(), opts.ExpectedParentInstanceId)
	}
	if payload.GetParentTaskScheduledId() != opts.ExpectedParentTaskScheduledId {
		return nil, fmt.Errorf("parent task scheduled ID mismatch: attestation claims %d, verifier expected %d",
			payload.GetParentTaskScheduledId(), opts.ExpectedParentTaskScheduledId)
	}
	if payload.GetActivityName() != opts.ExpectedActivityName {
		return nil, fmt.Errorf("activity name mismatch: attestation claims %q, verifier expected %q",
			payload.GetActivityName(), opts.ExpectedActivityName)
	}

	claimedOutputBytes, err := canonicalActivityOutput(payload.GetTerminalStatus(), opts.ClaimedOutput, opts.ClaimedFailure)
	if err != nil {
		return nil, err
	}
	expectedIODigest := IODigest(CanonicalInput(opts.ClaimedInput), claimedOutputBytes)
	if !bytes.Equal(expectedIODigest, payload.GetIoDigest()) {
		return nil, errors.New("ioDigest mismatch: claimed input/output does not match committed digest")
	}

	return &payload, nil
}

func validateActivityPayloadFields(p *protos.ActivityCompletionAttestationPayload) error {
	if p.GetCanonicalSpecVersion() != CanonicalSpecVersion {
		return fmt.Errorf("unsupported canonical spec version %d (this build supports %d)",
			p.GetCanonicalSpecVersion(), CanonicalSpecVersion)
	}
	if p.GetParentInstanceId() == "" {
		return errors.New("payload parent instance ID must not be empty")
	}
	if p.GetActivityName() == "" {
		return errors.New("payload activity name must not be empty")
	}
	if len(p.GetIoDigest()) != sha256.Size {
		return fmt.Errorf("payload ioDigest must be %d bytes, got %d", sha256.Size, len(p.GetIoDigest()))
	}
	if len(p.GetSignerCertDigest()) != sha256.Size {
		return fmt.Errorf("payload signerCertDigest must be %d bytes, got %d", sha256.Size, len(p.GetSignerCertDigest()))
	}
	switch p.GetTerminalStatus() {
	case protos.ActivityTerminalStatus_ACTIVITY_TERMINAL_STATUS_COMPLETED,
		protos.ActivityTerminalStatus_ACTIVITY_TERMINAL_STATUS_FAILED:
	default:
		return fmt.Errorf("payload terminalStatus must be set to a known value, got %v", p.GetTerminalStatus())
	}
	return nil
}
