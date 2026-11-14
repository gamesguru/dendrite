//go:build rezzy && linux && cgo

package reconciliation

import (
	"crypto/sha256"
	"encoding/base64"
	"testing"
)

func testEventID(seed string) string {
	sum := sha256.Sum256([]byte(seed))
	return "$" + base64.RawURLEncoding.EncodeToString(sum[:])
}

func TestSnapshotAndBucketDiff(t *testing.T) {
	base := []string{
		testEventID("create"), testEventID("join"), testEventID("message-1"),
		testEventID("message-2"), testEventID("message-3"),
	}
	extraLocal := testEventID("local-only")
	extraRemote := testEventID("remote-only")
	local := append(append([]string{}, base...), extraLocal)
	remote := append(append([]string{}, base...), extraRemote)

	localSnapshot, err := Snapshot(local, "v4plus")
	if err != nil {
		t.Fatalf("local snapshot: %v", err)
	}
	remoteSnapshot, err := Snapshot(remote, "v4plus")
	if err != nil {
		t.Fatalf("remote snapshot: %v", err)
	}
	if localSnapshot.Count != uint64(len(local)) {
		t.Fatalf("count = %d, want %d", localSnapshot.Count, len(local))
	}
	if localSnapshot.Digest == remoteSnapshot.Digest {
		t.Fatal("different sets produced the same digest")
	}
	if len(localSnapshot.Strata) != 32 {
		t.Fatalf("strata count = %d, want 32", len(localSnapshot.Strata))
	}

	requests := []BucketRequest{{Depth: 0, Prefix: 0, Capacity: 4}}
	sketches, err := BucketSketches(remote, "v4plus", requests)
	if err != nil {
		t.Fatalf("bucket sketches: %v", err)
	}
	result, err := BucketDiff(local, "v4plus", requests, sketches, remoteSnapshot.Digest, remoteSnapshot.Count)
	if err != nil {
		t.Fatalf("bucket diff: %v", err)
	}
	if result.Status != "decoded" {
		t.Fatalf("status = %q, want decoded", result.Status)
	}
	if len(result.MissingEventIDs) != 1 || result.MissingEventIDs[0] != extraLocal {
		t.Fatalf("missing = %v, want [%s]", result.MissingEventIDs, extraLocal)
	}
	wantRemoteShort := shortID(t, extraRemote)
	if len(result.RequesterOnlyShortIDs) != 1 || result.RequesterOnlyShortIDs[0] != wantRemoteShort {
		t.Fatalf("requester-only = %v, want [%s]", result.RequesterOnlyShortIDs, wantRemoteShort)
	}
}

func TestIdenticalSetsDecodeEmpty(t *testing.T) {
	ids := []string{testEventID("a"), testEventID("b")}
	snapshot, err := Snapshot(ids, "v4plus")
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	requests := []BucketRequest{{Depth: 0, Prefix: 0, Capacity: 2}}
	sketches, err := BucketSketches(ids, "v4plus", requests)
	if err != nil {
		t.Fatalf("bucket sketches: %v", err)
	}
	result, err := BucketDiff(ids, "v4plus", requests, sketches, snapshot.Digest, snapshot.Count)
	if err != nil {
		t.Fatalf("bucket diff: %v", err)
	}
	if result.Status != "decoded" || len(result.MissingEventIDs) != 0 || len(result.RequesterOnlyShortIDs) != 0 {
		t.Fatalf("unexpected result: %+v", result)
	}
	if result.ExpectedRequesterSideAccumulator != "AAAAAAAAAAAAAAAAAAAAAA" {
		t.Fatalf("expected accumulator = %q", result.ExpectedRequesterSideAccumulator)
	}
}

func shortID(t *testing.T, eventID string) string {
	t.Helper()
	raw, err := base64.RawURLEncoding.DecodeString(eventID[1:])
	if err != nil {
		t.Fatalf("decode event ID: %v", err)
	}
	if len(raw) != sha256.Size {
		t.Fatalf("event ID is %d bytes, want %d", len(raw), sha256.Size)
	}
	var h64 uint64
	for i := 0; i < 4; i++ {
		var chunk uint64
		for _, b := range raw[i*8 : i*8+8] {
			chunk = chunk<<8 | uint64(b)
		}
		if chunk != 0 {
			h64 = chunk
			break
		}
	}
	if h64 == 0 {
		h64 = 1
	}
	var bytes [8]byte
	for i := 7; i >= 0; i-- {
		bytes[i] = byte(h64)
		h64 >>= 8
	}
	return base64.RawURLEncoding.EncodeToString(bytes[:])
}
