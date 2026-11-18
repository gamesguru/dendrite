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
	c.RoomServerEvents.Unset(int64(eventNID))
}
