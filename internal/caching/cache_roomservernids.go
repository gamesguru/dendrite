package caching

import (
	"github.com/element-hq/dendrite/roomserver/types"
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
	// InvalidateRoomServerRoomID removes both directions of the roomNID <-> roomID
	// mapping. This must be called whenever a room's row is removed from the
	// database (e.g. PurgeRoom) so that a subsequent GetOrCreateRoomInfo can't be
	// satisfied by a stale cache hit that never touches the database.
	InvalidateRoomServerRoomID(roomNID types.RoomNID, roomID string)
}

func (c Caches) GetRoomServerRoomID(roomNID types.RoomNID) (string, bool) {
	return c.RoomServerRoomIDs.Get(roomNID)
}

// StoreRoomServerRoomID stores roomNID -> roomID and roomID -> roomNID
func (c Caches) StoreRoomServerRoomID(roomNID types.RoomNID, roomID string) {
	c.RoomServerRoomNIDs.Set(roomID, roomNID)
	c.RoomServerRoomIDs.Set(roomNID, roomID)
}

func (c Caches) GetRoomServerRoomNID(roomID string) (types.RoomNID, bool) {
	return c.RoomServerRoomNIDs.Get(roomID)
}

// InvalidateRoomServerRoomID removes both directions of the roomNID <-> roomID
// mapping from the cache.
func (c Caches) InvalidateRoomServerRoomID(roomNID types.RoomNID, roomID string) {
	c.RoomServerRoomNIDs.Unset(roomID)
	c.RoomServerRoomIDs.Unset(roomNID)
}
