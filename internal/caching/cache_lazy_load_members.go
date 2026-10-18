package caching

import (
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type lazyLoadingCacheKey struct {
	UserID       string // the user we're querying on behalf of
	DeviceID     string // the user we're querying on behalf of
	RoomID       string // the room in question
	TargetUserID string // the user whose membership we're asking about
}

// LazyLoadedMember records the membership event a device was sent for a user.
type LazyLoadedMember struct {
	EventID string
	// SentAt is the PDU stream position of the /sync response that carried
	// the membership, or 0 if another endpoint sent it. A client has only
	// seen a membership sent by /sync once it syncs from SentAt or later: a
	// response the client never received is retried from an earlier token.
	SentAt int64
}

type LazyLoadCache interface {
	StoreLazyLoadedUser(device *userapi.Device, roomID, userID string, member LazyLoadedMember)
	IsLazyLoadedUserCached(device *userapi.Device, roomID, userID string) (LazyLoadedMember, bool)
	InvalidateLazyLoadedUser(device *userapi.Device, roomID, userID string)
}

func (c Caches) StoreLazyLoadedUser(device *userapi.Device, roomID, userID string, member LazyLoadedMember) {
	c.LazyLoading.Set(lazyLoadingCacheKey{
		UserID:       device.UserID,
		DeviceID:     device.ID,
		RoomID:       roomID,
		TargetUserID: userID,
	}, member)
}

func (c Caches) IsLazyLoadedUserCached(device *userapi.Device, roomID, userID string) (LazyLoadedMember, bool) {
	return c.LazyLoading.Get(lazyLoadingCacheKey{
		UserID:       device.UserID,
		DeviceID:     device.ID,
		RoomID:       roomID,
		TargetUserID: userID,
	})
}

func (c Caches) InvalidateLazyLoadedUser(device *userapi.Device, roomID, userID string) {
	c.LazyLoading.Unset(lazyLoadingCacheKey{
		UserID:       device.UserID,
		DeviceID:     device.ID,
		RoomID:       roomID,
		TargetUserID: userID,
	})
}
