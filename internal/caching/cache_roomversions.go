package caching

import "github.com/matrix-org/gomatrixserverlib"

// RoomVersionsCache contains the subset of functions needed for
// a room version cache.
type RoomVersionCache interface {
	GetRoomVersion(roomID string) (roomVersion gomatrixserverlib.RoomVersion, ok bool)
	StoreRoomVersion(roomID string, roomVersion gomatrixserverlib.RoomVersion)
	InvalidateRoomVersion(roomID string)
}

func (c Caches) GetRoomVersion(roomID string) (gomatrixserverlib.RoomVersion, bool) {
	return c.RoomVersions.Get(roomID)
}

func (c Caches) StoreRoomVersion(roomID string, roomVersion gomatrixserverlib.RoomVersion) {
	c.RoomVersions.Set(roomID, roomVersion)
}

// InvalidateRoomVersion removes any cached room version for roomID. This
// must be called whenever a room's row is removed from the database (e.g.
// PurgeRoom) so that a subsequent GetOrCreateRoomInfo can't be satisfied by
// a stale cache hit that never touches the database.
func (c Caches) InvalidateRoomVersion(roomID string) {
	c.RoomVersions.Unset(roomID)
}
