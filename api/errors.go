package api

import (
	"errors"
	"fmt"
)

var (
	ErrInstanceNotFound  = errors.New("no such instance exists")
	ErrNotStarted        = errors.New("orchestration has not started")
	ErrNotCompleted      = errors.New("orchestration has not yet completed")
	ErrNoFailures        = errors.New("orchestration did not report failure details")
	ErrDuplicateInstance = errors.New("orchestration instance already exists")
	ErrIgnoreInstance    = errors.New("ignore creating orchestration instance")
	ErrTaskCancelled     = errors.New("task was cancelled")

	EmptyInstanceID = InstanceID("")
)

type UnknownTaskIDError struct {
	TaskID     int32
	InstanceID string
}

func NewUnknownTaskIDError(instanceID string, taskID int32) error {
	return &UnknownTaskIDError{
		TaskID:     taskID,
		InstanceID: instanceID,
	}
}

func (e *UnknownTaskIDError) Error() string {
	return fmt.Sprintf("unknown instance ID/task ID combo: %s/%d", e.InstanceID, e.TaskID)
}

func IsUnknownTaskIDError(err error) bool {
	_, ok := err.(*UnknownTaskIDError)
	return ok
}

type UnknownInstanceIDError struct {
	InstanceID string
}

func NewUnknownInstanceIDError(instanceID string) error {
	return &UnknownInstanceIDError{
		InstanceID: instanceID,
	}
}

func (e *UnknownInstanceIDError) Error() string {
	return fmt.Sprintf("unknown instance ID: %s", e.InstanceID)
}

func IsUnknownInstanceIDError(err error) bool {
	_, ok := err.(*UnknownInstanceIDError)
	return ok
}
