package routing

import (
	"testing"

	"codefloe.com/pat-s/zendrite/internal/reconciliation"
)

func TestSplitBuckets(t *testing.T) {
	got := splitBuckets([]reconciliation.BucketRequest{{Depth: 0, Prefix: 0, Capacity: 4}})
	if len(got) != 2 {
		t.Fatalf("got %d buckets, want 2", len(got))
	}
	if got[0].Depth != 1 || got[0].Prefix != 0 || got[1].Depth != 1 || got[1].Prefix != 1 {
		t.Fatalf("unexpected children: %+v", got)
	}
	if got[0].Capacity != 4 || got[1].Capacity != 4 {
		t.Fatalf("capacity was not preserved: %+v", got)
	}
}

func TestSplitBucketsHonoursLimits(t *testing.T) {
	requests := make([]reconciliation.BucketRequest, 0, 128)
	for i := 0; i < 128; i++ {
		requests = append(requests, reconciliation.BucketRequest{Depth: 0, Prefix: 0, Capacity: 1})
	}
	if got := splitBuckets(requests); len(got) > 128 {
		t.Fatalf("split produced %d buckets, want at most 128", len(got))
	}
	if got := splitBuckets([]reconciliation.BucketRequest{{Depth: 32, Prefix: 0, Capacity: 1}}); len(got) != 0 {
		t.Fatalf("split at max depth produced %d buckets", len(got))
	}
}
