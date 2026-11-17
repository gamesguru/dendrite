package streams

import (
	"context"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

func TestTypingStreamProviderCompleteSyncReturnsCurrentTypingOnly(t *testing.T) {
	const roomID = "!room:test"
	const userID = "@alice:test"

	cache := caching.NewTypingCache()
	provider := &TypingStreamProvider{EDUCache: cache}
	req := &types.SyncRequest{
		Context:  context.Background(),
		Response: types.NewResponse(),
		Rooms:    map[string]string{roomID: spec.Join},
		Timeout:  time.Second,
	}

	cache.AddTypingUser(userID, roomID, nil)

	pos := provider.CompleteSync(context.Background(), nil, req)
	if pos == 0 {
		t.Fatal("expected typing stream position to advance")
	}
	jr := req.Response.Rooms.Join[roomID]
	if jr == nil || len(jr.Ephemeral.Events) != 1 {
		t.Fatalf("expected one typing event, got %+v", jr)
	}
}

func TestTypingStreamProviderCompleteSyncDoesNotWaitForFutureTyping(t *testing.T) {
	const roomID = "!room:test"

	cache := caching.NewTypingCache()
	provider := &TypingStreamProvider{EDUCache: cache}
	req := &types.SyncRequest{
		Context:  context.Background(),
		Response: types.NewResponse(),
		Rooms:    map[string]string{roomID: spec.Join},
		Timeout:  0,
	}

	start := time.Now()
	pos := provider.CompleteSync(context.Background(), nil, req)
	if elapsed := time.Since(start); elapsed > 40*time.Millisecond {
		t.Fatalf("expected complete sync to return quickly, took %s", elapsed)
	}
	if pos != 0 {
		t.Fatalf("expected no typing stream advance, got %d", pos)
	}

	jr := req.Response.Rooms.Join[roomID]
	if jr != nil && len(jr.Ephemeral.Events) != 0 {
		t.Fatalf("expected no typing events, got %+v", jr)
	}
}

func TestTypingStreamProviderIncrementalSyncWaitsForPendingTyping(t *testing.T) {
	const roomID = "!room:test"
	const userID = "@alice:test"

	cache := caching.NewTypingCache()
	provider := &TypingStreamProvider{EDUCache: cache}
	req := &types.SyncRequest{
		Context:  context.Background(),
		Response: types.NewResponse(),
		Rooms:    map[string]string{roomID: spec.Join},
		Timeout:  time.Second,
	}

	// The interval is normally 50ms. A loaded CI machine can delay the
	// goroutine below past that, returning before the typing update arrives.
	// req.Timeout (1s) still bounds the wait, so match it.
	previousInterval := typingWaitInterval
	typingWaitInterval = time.Second
	t.Cleanup(func() { typingWaitInterval = previousInterval })

	go func() {
		time.Sleep(5 * time.Millisecond)
		cache.AddTypingUser(userID, roomID, nil)
	}()

	pos := provider.IncrementalSync(context.Background(), nil, req, 0, 0)
	if pos == 0 {
		t.Fatal("expected typing stream position to advance")
	}
	jr := req.Response.Rooms.Join[roomID]
	if jr == nil || len(jr.Ephemeral.Events) != 1 {
		t.Fatalf("expected one typing event, got %+v", jr)
	}
}

func TestTypingStreamProviderImmediateSyncDoesNotWait(t *testing.T) {
	const roomID = "!room:test"

	cache := caching.NewTypingCache()
	provider := &TypingStreamProvider{EDUCache: cache}
	req := &types.SyncRequest{
		Context:  context.Background(),
		Response: types.NewResponse(),
		Rooms:    map[string]string{roomID: spec.Join},
		Timeout:  0,
	}

	start := time.Now()
	pos := provider.IncrementalSync(context.Background(), nil, req, 0, 0)
	if elapsed := time.Since(start); elapsed > 40*time.Millisecond {
		t.Fatalf("expected immediate sync to return quickly, took %s", elapsed)
	}
	if pos != 0 {
		t.Fatalf("expected no typing stream advance, got %d", pos)
	}
}

// A typing update that lands after the cursor was read is still included in
// the snapshot, and the returned cursor covers it.
func TestTypingStreamProviderPositionIncludesInterleavedTypingUpdate(t *testing.T) {
	const roomID = "!room:test"
	const userID = "@alice:test"

	cache := caching.NewTypingCache()
	provider := &TypingStreamProvider{EDUCache: cache}
	from := types.StreamPosition(0)
	req := &types.SyncRequest{
		Context:  context.Background(),
		Response: types.NewResponse(),
		Rooms:    map[string]string{roomID: spec.Join},
	}

	// Reproduce waitForTypingEvents' read order: the global position is read,
	// then a typing update lands before the snapshot is taken.
	_ = cache.GetLatestSyncPosition()
	updatePosition := cache.AddTypingUser(userID, roomID, nil)
	added, latest := provider.addTypingEvents(req, from)
	if !added {
		t.Fatal("expected the interleaved typing update in the response")
	}
	if latest != types.StreamPosition(updatePosition) {
		t.Fatalf("expected returned position %d to cover the included update, got %d", updatePosition, latest)
	}
	if got := req.Response.Rooms.Join[roomID].Ephemeral.Events; len(got) != 1 {
		t.Fatalf("expected one typing event, got %d", len(got))
	}
}

// Updates to two joined rooms after one sync's snapshot must both be delivered
// on the next sync. The earlier room's update must not be skipped when the
// later room's position advances the shared cursor.
func TestTypingStreamProviderDoesNotSkipEarlierRoomUpdate(t *testing.T) {
	const roomA, roomB = "!a:test", "!b:test"

	cache := caching.NewTypingCache()
	provider := &TypingStreamProvider{EDUCache: cache}
	rooms := map[string]string{roomA: spec.Join, roomB: spec.Join}
	firstReq := &types.SyncRequest{
		Context:  context.Background(),
		Response: types.NewResponse(),
		Rooms:    rooms,
	}
	added, cursor := provider.addTypingEvents(firstReq, 0)
	if added || cursor != 0 {
		t.Fatalf("expected an empty initial snapshot at 0, got added=%v cursor=%d", added, cursor)
	}

	posA := cache.AddTypingUser("@alice:test", roomA, nil)
	posB := cache.AddTypingUser("@bob:test", roomB, nil)
	if posA >= posB {
		t.Fatalf("expected increasing positions, got %d then %d", posA, posB)
	}
	if int64(cursor) >= posA {
		t.Fatalf("pre-update cursor %d must not cover room A's update at %d", cursor, posA)
	}

	secondReq := &types.SyncRequest{
		Context:  context.Background(),
		Response: types.NewResponse(),
		Rooms:    rooms,
	}
	added, next := provider.addTypingEvents(secondReq, cursor)
	if !added {
		t.Fatal("expected the next sync to include typing updates")
	}
	if next != types.StreamPosition(posB) {
		t.Fatalf("expected next cursor %d, got %d", posB, next)
	}
	for _, roomID := range []string{roomA, roomB} {
		if got := secondReq.Response.Rooms.Join[roomID].Ephemeral.Events; len(got) != 1 {
			t.Fatalf("%s: expected its typing update in the next sync, got %d events", roomID, len(got))
		}
	}
}

// With typing updates in several rooms, the cursor is the snapshot's latest
// position and both rooms are delivered.
func TestTypingStreamProviderAddTypingEventsCoversAllRooms(t *testing.T) {
	const roomA, roomB = "!a:test", "!b:test"

	cache := caching.NewTypingCache()
	provider := &TypingStreamProvider{EDUCache: cache}
	cache.AddTypingUser("@alice:test", roomA, nil)
	posB := cache.AddTypingUser("@bob:test", roomB, nil)

	req := &types.SyncRequest{
		Context:  context.Background(),
		Response: types.NewResponse(),
		Rooms:    map[string]string{roomA: spec.Join, roomB: spec.Join},
	}
	added, latest := provider.addTypingEvents(req, 0)
	if !added {
		t.Fatal("expected typing events to be added")
	}
	if int64(latest) != posB {
		t.Fatalf("expected the snapshot position %d, got %d", posB, latest)
	}
	for _, roomID := range []string{roomA, roomB} {
		if got := req.Response.Rooms.Join[roomID].Ephemeral.Events; len(got) != 1 {
			t.Fatalf("%s: expected one typing event, got %d", roomID, len(got))
		}
	}
}
