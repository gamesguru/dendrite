package caching_test

import (
	"testing"
	"time"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
)

// Ristretto applies writes asynchronously, so a fill made before an
// invalidation can still be buffered when it returns. The event must not be
// readable once invalidation has returned.
func TestRoomServerEventCacheInvalidationDrainsBufferedFills(t *testing.T) {
	cache := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
	event := test.NewRoom(t, test.NewUser(t)).Events()[0]
	for i := 0; i < 2000; i++ {
		eventNID := types.EventNID(i + 1)
		unlock := cache.LockRoomServerEventCacheRead()
		cache.StoreRoomServerEvent(eventNID, event)
		unlock()
		cache.InvalidateRoomServerEvent(eventNID)
		if _, ok := cache.GetRoomServerEvent(eventNID); ok {
			t.Fatalf("event %d was readable after invalidation returned", eventNID)
		}
	}
}
