// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.
package perform

import (
	"context"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/state"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
)

// fakePurgedRoomDB simulates a room that has been purged while a federation
// backfill was in flight: RoomInfo reports the room no longer exists, but the
// legacy GetOrCreateRoomInfo path would happily re-create it.
type fakePurgedRoomDB struct {
	storage.Database
	getOrCreateCalled bool
	storeEventCalled  bool
}

func (f *fakePurgedRoomDB) EventNIDs(ctx context.Context, eventIDs []string) (map[string]types.EventMetadata, error) {
	return map[string]types.EventMetadata{}, nil
}

func (f *fakePurgedRoomDB) RoomInfo(ctx context.Context, roomID string) (*types.RoomInfo, error) {
	// The room was purged, so it no longer exists.
	return nil, nil
}

func (f *fakePurgedRoomDB) GetOrCreateRoomInfo(ctx context.Context, event gomatrixserverlib.PDU) (*types.RoomInfo, error) {
	f.getOrCreateCalled = true
	return &types.RoomInfo{RoomNID: 1}, nil
}

func (f *fakePurgedRoomDB) GetOrCreateEventTypeNID(ctx context.Context, eventType string) (types.EventTypeNID, error) {
	return 1, nil
}

func (f *fakePurgedRoomDB) GetOrCreateEventStateKeyNID(ctx context.Context, eventStateKey *string) (types.EventStateKeyNID, error) {
	return 1, nil
}

func (f *fakePurgedRoomDB) StoreEvent(ctx context.Context, event gomatrixserverlib.PDU, roomInfo *types.RoomInfo, eventTypeNID types.EventTypeNID, eventStateKeyNID types.EventStateKeyNID, authEventNIDs []types.EventNID, isRejected bool) (types.EventNID, types.StateAtEvent, error) {
	f.storeEventCalled = true
	return 1, types.StateAtEvent{}, nil
}

func (f *fakePurgedRoomDB) MaybeRedactEvent(ctx context.Context, roomInfo *types.RoomInfo, eventNID types.EventNID, event gomatrixserverlib.PDU, plResolver state.PowerLevelResolver, querier api.QuerySenderIDAPI) (gomatrixserverlib.PDU, gomatrixserverlib.PDU, error) {
	return nil, nil, nil
}

// TestPersistEventsDoesNotRecreatePurgedRoom guards against issue #224: an
// in-flight backfill must not re-add a room that has been purged.
func TestPersistEventsDoesNotRecreatePurgedRoom(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	events := room.Events()

	db := &fakePurgedRoomDB{}
	pdus := []gomatrixserverlib.PDU{events[len(events)-1].PDU}

	_, stored := persistEvents(context.Background(), db, nil, pdus)

	if db.getOrCreateCalled {
		t.Error("persistEvents called GetOrCreateRoomInfo, which re-creates purged rooms; it must look the room up with RoomInfo instead")
	}
	if db.storeEventCalled {
		t.Error("persistEvents stored a backfilled event into a purged (non-existent) room, re-creating it")
	}
	if len(stored) != 0 {
		t.Errorf("expected no stored events for a purged room, got %d", len(stored))
	}
}
