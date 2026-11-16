package api

// A sidecar with no workflow worker connected can accept an operation, save
// it, and run it once a worker connects. Such an operation is "staged". The
// results in this file report whether an operation was staged, so a
// caller can tell it apart from one that ran at once. Sidecars that never
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
