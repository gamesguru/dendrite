package caching

import (
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// RoomServerEventsCache contains the subset of functions needed for
// a roomserver event cache.
type RoomServerEventsCache interface {
	GetRoomServerEvent(eventNID types.EventNID) (*types.HeaderedEvent, bool)
	LockRoomServerEventCacheRead() func()
	StoreRoomServerEvent(eventNID types.EventNID, event *types.HeaderedEvent)
	InvalidateRoomServerEvent(eventNID types.EventNID)
}

func (c Caches) GetRoomServerEvent(eventNID types.EventNID) (*types.HeaderedEvent, bool) {
	return c.RoomServerEvents.Get(int64(eventNID))
}

func (c Caches) StoreRoomServerEvent(eventNID types.EventNID, event *types.HeaderedEvent) {
	c.RoomServerEvents.Set(int64(eventNID), event)
}

// LockRoomServerEventCacheRead holds a read lock while events are loaded from
// the database and added to the cache. Invalidation waits for such loads.
func (c Caches) LockRoomServerEventCacheRead() func() {
	if c.roomServerEventCacheMu == nil {
		return func() {}
	}
	c.roomServerEventCacheMu.RLock()
	return c.roomServerEventCacheMu.RUnlock
}

func (c Caches) InvalidateRoomServerEvent(eventNID types.EventNID) {
	if c.roomServerEventCacheMu != nil {
		c.roomServerEventCacheMu.Lock()
		defer c.roomServerEventCacheMu.Unlock()
	}
	// Ristretto applies fills asynchronously. A fill that finished before the
	// write lock was taken can still be buffered and would become visible after
	// the delete if it were applied later. Drain prior writes before deleting.
	// Unset itself removes the entry synchronously, so no drain is needed after
	// it; the queued delete marker cannot re-add the entry.
	if w, ok := c.RoomServerEvents.(interface{ Wait() }); ok {
		w.Wait()
	}
	c.RoomServerEvents.Unset(int64(eventNID))
}
