package caching

import (
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

type RoomServerCaches interface {
	RoomServerNIDsCache
	RoomVersionCache
	RoomServerEventsCache
	RoomHierarchyCache
	EventStateKeyCache
	EventTypeCache
}

// RoomServerNIDsCache contains the subset of functions needed for
// a roomserver NID cache.
type RoomServerNIDsCache interface {
	GetRoomServerRoomID(roomNID types.RoomNID) (string, bool)
	// StoreRoomServerRoomID stores roomNID -> roomID and roomID -> roomNID
	StoreRoomServerRoomID(roomNID types.RoomNID, roomID string)
	GetRoomServerRoomNID(roomID string) (types.RoomNID, bool)
	// InvalidateRoom removes all room-keyed cache entries for the given room.
	// Call this after the room has been purged from the database, so that
	// subsequent lookups by roomID (e.g. after a rejoin) don't hit stale data.
	InvalidateRoom(roomID string)
}

func (c Caches) GetRoomServerRoomID(roomNID types.RoomNID) (string, bool) {
	return c.RoomServerRoomIDs.Get(roomNID)
}

// StoreRoomServerRoomID stores roomNID -> roomID and roomID -> roomNID.
func (c Caches) StoreRoomServerRoomID(roomNID types.RoomNID, roomID string) {
	c.RoomServerRoomNIDs.Set(roomID, roomNID)
	c.RoomServerRoomIDs.Set(roomNID, roomID)
}

func (c Caches) GetRoomServerRoomNID(roomID string) (types.RoomNID, bool) {
	return c.RoomServerRoomNIDs.Get(roomID)
}

// InvalidateRoom drops the cache entries that would otherwise serve stale
// values after a purge. RoomServerRoomNIDs (room ID -> room NID) is the only
// room-ID-keyed mapping that genuinely re-binds across a purge, since a rejoin
// allocates a fresh NID. Room version and roomNID -> room ID are immutable per
// the Matrix spec (a room ID's version cannot change and NIDs are never reused),
// so their entries remain correct and are left to expire via TTL alongside the
// other NID-keyed caches.
func (c Caches) InvalidateRoom(roomID string) {
	c.RoomServerRoomNIDs.Unset(roomID)
	c.RoomHierarchies.Unset(roomID)
	c.RoomHierarchyFailures.Unset(roomID)
	c.InvalidateRoomSummary(roomID)
}
