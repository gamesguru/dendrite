package streams

import (
	"context"
	"testing"
	"time"

	"github.com/element-hq/dendrite/internal/caching"
	"github.com/element-hq/dendrite/syncapi/types"
	"github.com/matrix-org/gomatrixserverlib/spec"
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
