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

package api

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/dapr/durabletask-go/api/protos"
	"github.com/dapr/kit/ptr"
)

func applyListInstancesOptions(t *testing.T, opts ...ListInstancesOptions) *protos.ListInstancesRequest {
	t.Helper()
	req := new(protos.ListInstancesRequest)
	for _, o := range opts {
		require.NoError(t, o(req))
	}
	return req
}

func TestListInstancesOptions(t *testing.T) {
	createdFrom := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	createdTo := createdFrom.Add(24 * time.Hour)
	completedFrom := createdFrom.Add(time.Hour)
	completedTo := createdFrom.Add(2 * time.Hour)

	t.Run("no options leave the filter unset", func(t *testing.T) {
		req := applyListInstancesOptions(t)
		assert.Nil(t, req.GetFilter())
	})

	t.Run("every option sets its field", func(t *testing.T) {
		req := applyListInstancesOptions(t,
			WithListInstancesPageSize(10),
			WithListInstancesContinuationToken("tok"),
			WithListInstancesInstanceIDPrefix("order-"),
			WithListInstancesRuntimeStatus(RUNTIME_STATUS_RUNNING, RUNTIME_STATUS_SUSPENDED),
			WithListInstancesCreatedTimeFrom(createdFrom),
			WithListInstancesCreatedTimeTo(createdTo),
			WithListInstancesCompletedTimeFrom(completedFrom),
			WithListInstancesCompletedTimeTo(completedTo),
			WithListInstancesMinDuration(time.Minute),
			WithListInstancesMaxDuration(time.Hour),
		)

		want := &protos.ListInstancesRequest{
			PageSize:          ptr.Of(uint32(10)),
			ContinuationToken: ptr.Of("tok"),
			InstanceIdPrefix:  ptr.Of("order-"),
			Filter: &protos.ListInstancesFilter{
				CreatedTimeFrom:   timestamppb.New(createdFrom),
				CreatedTimeTo:     timestamppb.New(createdTo),
				RuntimeStatus:     []protos.OrchestrationStatus{RUNTIME_STATUS_RUNNING, RUNTIME_STATUS_SUSPENDED},
				CompletedTimeFrom: timestamppb.New(completedFrom),
				CompletedTimeTo:   timestamppb.New(completedTo),
				MinDuration:       durationpb.New(time.Minute),
				MaxDuration:       durationpb.New(time.Hour),
			},
		}
		assert.True(t, proto.Equal(want, req), "want %v, got %v", want, req)
	})

	t.Run("statuses add up over uses", func(t *testing.T) {
		req := applyListInstancesOptions(t,
			WithListInstancesRuntimeStatus(RUNTIME_STATUS_COMPLETED),
			WithListInstancesRuntimeStatus(RUNTIME_STATUS_FAILED, RUNTIME_STATUS_TERMINATED),
		)
		assert.Equal(t, []protos.OrchestrationStatus{
			RUNTIME_STATUS_COMPLETED, RUNTIME_STATUS_FAILED, RUNTIME_STATUS_TERMINATED,
		}, req.GetFilter().GetRuntimeStatus())
	})

	t.Run("a zero time removes the bound", func(t *testing.T) {
		req := applyListInstancesOptions(t,
			WithListInstancesCreatedTimeFrom(createdFrom),
			WithListInstancesCreatedTimeTo(createdTo),
			WithListInstancesCompletedTimeFrom(completedFrom),
			WithListInstancesCompletedTimeTo(completedTo),
			WithListInstancesCreatedTimeFrom(time.Time{}),
			WithListInstancesCreatedTimeTo(time.Time{}),
			WithListInstancesCompletedTimeFrom(time.Time{}),
			WithListInstancesCompletedTimeTo(time.Time{}),
		)
		assert.True(t, proto.Equal(new(protos.ListInstancesFilter), req.GetFilter()), "got %v", req.GetFilter())
	})

	t.Run("a zero or negative duration removes the bound", func(t *testing.T) {
		req := applyListInstancesOptions(t,
			WithListInstancesMinDuration(time.Minute),
			WithListInstancesMaxDuration(time.Hour),
			WithListInstancesMinDuration(0),
			WithListInstancesMaxDuration(-time.Second),
		)
		assert.True(t, proto.Equal(new(protos.ListInstancesFilter), req.GetFilter()), "got %v", req.GetFilter())
	})

	t.Run("the last duration option wins", func(t *testing.T) {
		req := applyListInstancesOptions(t,
			WithListInstancesMinDuration(time.Minute),
			WithListInstancesMinDuration(2*time.Minute),
		)
		assert.Equal(t, 2*time.Minute, req.GetFilter().GetMinDuration().AsDuration())
		assert.Nil(t, req.GetFilter().GetMaxDuration())
	})

	t.Run("an unknown runtime status is an error", func(t *testing.T) {
		req := new(protos.ListInstancesRequest)
		err := WithListInstancesRuntimeStatus(RUNTIME_STATUS_RUNNING, OrchestrationStatus(99))(req)
		require.ErrorContains(t, err, "unknown runtime status 99")
		assert.Empty(t, req.GetFilter().GetRuntimeStatus())
	})
}
