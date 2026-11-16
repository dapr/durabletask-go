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

package backend

import "errors"

var (
	ErrTaskHubExists         = errors.New("task hub already exists")
	ErrTaskHubNotFound       = errors.New("task hub not found")
	ErrNotInitialized        = errors.New("backend not initialized")
	ErrWorkItemLockLost      = errors.New("lock on work-item was lost")
	ErrBackendAlreadyStarted = errors.New("backend is already started")

	// ErrOperationStaged is returned by CreateWorkflowInstance,
	// AddNewWorkflowEvent and RerunWorkflowFromEvent when the backend accepted
	// and stored the operation but cannot apply it yet, for example because no
	// worker can run the instance until it moves elsewhere. Callers treat it
	// as success. The executor does not wait for the instance to start or to
	// change state after a staged operation, as it would not until the
	// operation is applied. RerunWorkflowFromEvent returns the new instance ID
	// along with it.
	ErrOperationStaged = errors.New("operation staged")
)

// isStaged reports whether err says the backend staged the operation, and
// returns err unless it did.
func isStaged(err error) (bool, error) {
	if errors.Is(err, ErrOperationStaged) {
		return true, nil
	}
	return false, err
}
