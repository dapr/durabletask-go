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

package dedup

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPackKey_DistinctKindsDoNotCollide(t *testing.T) {
	t.Parallel()

	kinds := []Kind{KindNone, KindTask, KindTimer, KindChild}
	ids := []int32{0, 1, -1, 7, 1<<31 - 1, -1 << 31}

	seen := make(map[setKey]struct{}, len(kinds)*len(ids))
	for _, k := range kinds {
		for _, id := range ids {
			pk := packKey(k, id)
			if _, dup := seen[pk]; dup {
				t.Fatalf("packKey collision for (%v, %d) -> %d", k, id, pk)
			}
			seen[pk] = struct{}{}
		}
	}
	assert.Len(t, seen, len(kinds)*len(ids))
}
