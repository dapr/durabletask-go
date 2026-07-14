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

import (
	"context"

	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/dapr/durabletask-go/backend/payloadstore"
)

// resolveMetadataPayloads best-effort resolves offloaded payload
// references in the metadata's input and output so metadata consumers
// (queries, waiters) see full payloads. Resolution failures leave the
// reference value in place instead of failing the read: metadata must
// stay available even when the payload store cannot serve a reference,
// e.g. a workflow terminally failed for carrying a forged reference must
// still be observable as FAILED. This is deliberately laxer than the
// execution path, where an unresolvable reference fails the work item.
// The Input/Output pointers are replaced, never mutated, as they may
// alias history events in backend-cached workflow state.
func resolveMetadataPayloads(ctx context.Context, store payloadstore.Store, logger Logger, meta *WorkflowMetadata) {
	if store == nil || meta == nil {
		return
	}

	for _, field := range []**wrapperspb.StringValue{&meta.Input, &meta.Output} {
		if *field == nil {
			continue
		}
		resolved, ok, err := payloadstore.Resolve(ctx, store, meta.GetInstanceId(), (*field).GetValue())
		if err != nil {
			if logger != nil {
				logger.Warnf("Failed to resolve offloaded payload in metadata of workflow '%s'; returning the reference: %v",
					meta.GetInstanceId(), err)
			}
			continue
		}
		if ok {
			*field = wrapperspb.String(resolved)
		}
	}
}
