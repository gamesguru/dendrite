// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// TestProcessAccountDataExtension_RoomReenteringAfterGap_StillDeliversAccountData
// guards against account_data queried from the connection-wide fromPos
// instead of a room's own last-sent token (see accountDataFloorForRoom):
// a room excluded from prior responses would have changes skipped once it
// re-enters view.
func TestProcessAccountDataExtension_RoomReenteringAfterGap_StillDeliversAccountData(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	userID := "@alice:localhost"

	const (
		roomSteady = "!roomA:localhost" // continuously in view, no gap
		roomGappy  = "!roomB:localhost" // was excluded for a while, now back in view
	)

	fullyReadSteady, err := json.Marshal(map[string]string{"event_id": "$readA:localhost"})
	require.NoError(t, err)
	fullyReadGap, err := json.Marshal(map[string]string{"event_id": "$readB-missed-while-excluded:localhost"})
	require.NoError(t, err)

	snapshot := newMockSnapshot()
	// roomGappy's change at 60 is inside the gap between its own last-sent
	// position (50) and the connection's since token (90) - see below.
	snapshot.SetAccountDataChange(roomSteady, "m.fully_read", 95)
	snapshot.SetAccountDataChange(roomGappy, "m.fully_read", 60)

	db := newMockStreamStateDB()
	rp := &RequestPool{
		db: db,
		userAPI: &mockUserAPI{
			roomAccountData: map[string]map[string]json.RawMessage{
				roomSteady: {"m.fully_read": fullyReadSteady},
				roomGappy:  {"m.fully_read": fullyReadGap},
			},
		},
	}

	fromPos := &types.StreamingToken{AccountDataPosition: 90}
	toPos := types.StreamingToken{AccountDataPosition: 200}

	connState := &V4ConnectionState{
		ConnectionKey: 1,
		// A real response position: gap recovery is only attempted when the
		// result can be recorded, so pinning this to 0 would silently disable
		// the behavior under test.
		ConnectionPosition: 42,
		PreviousStreamStates: map[string]map[string]*types.SlidingSyncStreamState{
			roomSteady: {
				accountDataStream: {LastToken: (&types.StreamingToken{AccountDataPosition: 90}).String()},
			},
			roomGappy: {
				accountDataStream: {LastToken: (&types.StreamingToken{AccountDataPosition: 50}).String()},
			},
		},
	}

	// Both rooms are relevant to THIS response - roomGappy has just re-entered
	// view (e.g. the client re-subscribed or it re-entered a list range).
	actualLists := map[string]types.SlidingList{}
	actualRoomSubscriptions := map[string]bool{roomSteady: true, roomGappy: true}

	resp, _, err := rp.processAccountDataExtension(
		ctx, snapshot, connState, userID,
		nil, nil, // requestedLists, requestedRooms: default wildcard
		fromPos, toPos,
		actualLists, actualRoomSubscriptions,
	)
	require.NoError(t, err)

	assert.Contains(t, resp.Rooms, roomSteady,
		"a continuously-live room's account data must still be delivered")
	assert.Contains(t, resp.Rooms, roomGappy,
		"account data missed while a room was excluded from responses must be delivered once it's relevant again, not dropped forever")

	// Both rooms must record how far they were actually served, or the gap
	// reopens on the next response.
	for _, roomID := range []string{roomSteady, roomGappy} {
		key := streamKey(roomID, accountDataStream)
		require.Contains(t, db.writes, key, "every relevant room records its delivered position")
		assert.Equal(t, int64(42), db.writes[key].position)
	}
	assert.True(t, connState.AccountDataWritten[roomGappy],
		"a freshly written room must not then be copied forward at its stale position")
}
