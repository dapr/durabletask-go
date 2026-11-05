package api

import (
	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/durabletask-go/internal/staged"
)

// A sidecar with no workflow worker connected can accept an operation, save
// it, and run it once a worker connects. Such an operation is "staged". The
// results and options in this file report whether an operation was staged,
// so a caller can tell it apart from one that ran at once. Sidecars that never
// stage operations always report false.

// ScheduleResult is the result of scheduling a new workflow.
type ScheduleResult struct {
	// InstanceID is the ID of the new workflow instance.
	InstanceID InstanceID
	// Staged is true when the sidecar staged the start.
	Staged bool
}

// RaiseEventResult is the result of raising an event.
type RaiseEventResult struct {
	// Staged is true when the sidecar staged the event.
	Staged bool
}

// TerminateResult is the result of terminating a workflow.
type TerminateResult struct {
	// Staged is true when the sidecar staged the termination.
	Staged bool
}

// SuspendResult is the result of suspending a workflow.
type SuspendResult struct {
	// Staged is true when the sidecar staged the suspension.
	Staged bool
}

// ResumeResult is the result of resuming a workflow.
type ResumeResult struct {
	// Staged is true when the sidecar staged the resumption.
	Staged bool
}

// RerunResult is the result of rerunning a workflow from an event.
type RerunResult struct {
	// NewInstanceID is the ID of the new workflow instance.
	NewInstanceID InstanceID
	// Staged is true when the sidecar staged the rerun.
	Staged bool
}

// WithStartStaged sets *dst to whether the sidecar staged the start. It is
// set once the call returns without an error.
func WithStartStaged(dst *bool) NewWorkflowOptions {
	return func(req *protos.CreateInstanceRequest) error {
		staged.Register(req, dst)
		return nil
	}
}

// WithRaiseEventStaged sets *dst to whether the sidecar staged the event.
// It is set once the call returns without an error.
func WithRaiseEventStaged(dst *bool) RaiseEventOptions {
	return func(req *protos.RaiseEventRequest) error {
		staged.Register(req, dst)
		return nil
	}
}

// WithTerminateStaged sets *dst to whether the sidecar staged the
// termination. It is set once the call returns without an error.
func WithTerminateStaged(dst *bool) TerminateOptions {
	return func(req *protos.TerminateRequest) error {
		staged.Register(req, dst)
		return nil
	}
}

// WithSuspendStaged sets *dst to whether the sidecar staged the
// suspension. It is set once the call returns without an error.
func WithSuspendStaged(dst *bool) SuspendOptions {
	return func(req *protos.SuspendRequest) error {
		staged.Register(req, dst)
		return nil
	}
}

// WithResumeStaged sets *dst to whether the sidecar staged the
// resumption. It is set once the call returns without an error.
func WithResumeStaged(dst *bool) ResumeOptions {
	return func(req *protos.ResumeRequest) error {
		staged.Register(req, dst)
		return nil
	}
}

// WithRerunStaged sets *dst to whether the sidecar staged the rerun. It is
// set once the call returns without an error.
func WithRerunStaged(dst *bool) RerunOptions {
	return func(req *protos.RerunWorkflowFromEventRequest) error {
		staged.Register(req, dst)
		return nil
	}
}
