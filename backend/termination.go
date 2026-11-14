// Copyright 2026 The Dapr Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package backend

import (
	"fmt"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TerminationReasonFromHistoryRows returns the reason from the most recent
// ExecutionTerminated event in rows ordered by descending history sequence.
// The caller owns the rows and must close them.
func TerminationReasonFromHistoryRows(rows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}) (*wrapperspb.StringValue, error) {
	for rows.Next() {
		var payload []byte
		if err := rows.Scan(&payload); err != nil {
			return nil, fmt.Errorf("failed to scan workflow history event: %w", err)
		}

		event, err := UnmarshalHistoryEvent(payload)
		if err != nil {
			return nil, fmt.Errorf("failed to unmarshal workflow history event: %w", err)
		}

		if terminated := event.GetExecutionTerminated(); terminated != nil {
			return terminated.GetReason(), nil
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate workflow history: %w", err)
	}

	return nil, nil
}
