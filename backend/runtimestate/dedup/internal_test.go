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
