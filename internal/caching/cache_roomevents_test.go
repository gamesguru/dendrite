package caching

import (
	"sync"
	"testing"
	"time"

	"codefloe.com/pat-s/zendrite/roomserver/types"
)

type roomEventTestCache struct {
	entries map[int64]*types.HeaderedEvent
}

func (c *roomEventTestCache) Get(key int64) (*types.HeaderedEvent, bool) {
	event, ok := c.entries[key]
	return event, ok
}

func (c *roomEventTestCache) Set(key int64, event *types.HeaderedEvent) {
	c.entries[key] = event
}

func (c *roomEventTestCache) Unset(key int64) {
	delete(c.entries, key)
}

func TestRoomServerEventCacheRejectsStaleFill(t *testing.T) {
	eventNID := types.EventNID(7)
	cache := Caches{
		RoomServerEvents:       &roomEventTestCache{entries: make(map[int64]*types.HeaderedEvent)},
		roomServerEventCacheMu: &sync.RWMutex{},
	}
	unlock := cache.LockRoomServerEventCacheRead()
	cache.StoreRoomServerEvent(eventNID, &types.HeaderedEvent{})
	invalidated := make(chan struct{})
	started := make(chan struct{})
	go func() {
		close(started)
		cache.InvalidateRoomServerEvent(eventNID)
		close(invalidated)
	}()
	<-started
	time.Sleep(50 * time.Millisecond)
	select {
	case <-invalidated:
		t.Fatal("invalidation did not wait for the in-flight cache fill")
	default:
	}
	unlock()
	<-invalidated
	if _, ok := cache.GetRoomServerEvent(eventNID); ok {
		t.Fatal("invalidation left the stale event cached")
	}

	unlock = cache.LockRoomServerEventCacheRead()
	fullEvent := &types.HeaderedEvent{}
	cache.StoreRoomServerEvent(eventNID, fullEvent)
	unlock()
	if got, ok := cache.GetRoomServerEvent(eventNID); !ok || got != fullEvent {
		t.Fatal("event loaded after invalidation was not cached")
	}
}
