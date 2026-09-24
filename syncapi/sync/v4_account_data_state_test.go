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

const accountDataTestPosition = int64(42)

// TestCopyForwardAccountDataState covers the half of per-room account_data
// tracking that runs outside the extension. Old connection positions are
// cascade-deleted on the next request, so any room whose row is not carried
// onto the new position loses its floor and gets rescanned from 0.
func TestCopyForwardAccountDataState(t *testing.T) {
	t.Parallel()

	const (
		carried = "!carried:localhost"
		fresh   = "!fresh:localhost"
		token   = "s100_50_25_10_5_3_1_0_8"
	)

	connState := func() *V4ConnectionState {
		return &V4ConnectionState{
			ConnectionPosition: accountDataTestPosition,
			PreviousStreamStates: map[string]map[string]*types.SlidingSyncStreamState{
				carried: {accountDataStream: {RoomStatus: "live", LastToken: token}},
				fresh:   {accountDataStream: {RoomStatus: "live", LastToken: token}},
			},
		}
	}

	t.Run("rooms the extension did not write are carried over", func(t *testing.T) {
		t.Parallel()

		db := newMockStreamStateDB()
		rp := &RequestPool{db: db}
		state := connState()
		state.AccountDataWritten = map[string]bool{fresh: true}

		rp.copyForwardAccountDataState(context.Background(), state)

		require.Contains(t, db.writes, streamKey(carried, accountDataStream))
		assert.Equal(t, token, db.writes[streamKey(carried, accountDataStream)].lastToken,
			"the floor must keep pointing at where the room was last actually served")
		assert.NotContains(t, db.writes, streamKey(fresh, accountDataStream),
			"a room already written at the fresh position must not be overwritten with its stale token")
	})

	t.Run("runs when the extension was never processed", func(t *testing.T) {
		t.Parallel()

		db := newMockStreamStateDB()
		rp := &RequestPool{db: db}

		// AccountDataWritten is nil: the account_data extension was disabled or
		// errored on this request, so nothing was written for any room.
		rp.copyForwardAccountDataState(context.Background(), connState())

		assert.Contains(t, db.writes, streamKey(carried, accountDataStream))
		assert.Contains(t, db.writes, streamKey(fresh, accountDataStream))
	})

	t.Run("interrupt responses write nothing", func(t *testing.T) {
		t.Parallel()

		db := newMockStreamStateDB()
		rp := &RequestPool{db: db}
		state := connState()
		state.ConnectionPosition = 0

		rp.copyForwardAccountDataState(context.Background(), state)

		assert.Empty(t, db.writes, "there is no new position to write rows to")
	})

	t.Run("rooms with no account_data row are left alone", func(t *testing.T) {
		t.Parallel()

		db := newMockStreamStateDB()
		rp := &RequestPool{db: db}

		rp.copyForwardAccountDataState(context.Background(), &V4ConnectionState{
			ConnectionPosition: accountDataTestPosition,
			PreviousStreamStates: map[string]map[string]*types.SlidingSyncStreamState{
				carried: {"events": {RoomStatus: "live", LastToken: token}},
			},
		})

		assert.Empty(t, db.writes)
	})
}

// TestProcessAccountDataExtension_RecordsDeliveredPositionNotRequestedTop is the
// guard against the limited query silently losing data: the range query is
// capped and ordered by id ASC, so a room must never be recorded as served up
// to toPos when the query only returned rows up to an earlier position.
func TestProcessAccountDataExtension_RecordsDeliveredPositionNotRequestedTop(t *testing.T) {
	t.Parallel()

	const roomID = "!room:localhost"

	fullyRead, err := json.Marshal(map[string]string{"event_id": "$read:localhost"})
	require.NoError(t, err)

	snapshot := newMockSnapshot()
	snapshot.SetAccountDataChange(roomID, "m.fully_read", 95)

	db := newMockStreamStateDB()
	rp := &RequestPool{
		db: db,
		userAPI: &mockUserAPI{
			roomAccountData: map[string]map[string]json.RawMessage{
				roomID: {"m.fully_read": fullyRead},
			},
		},
	}

	fromPos := &types.StreamingToken{AccountDataPosition: 90}
	toPos := types.StreamingToken{AccountDataPosition: 200}

	_, _, err = rp.processAccountDataExtension(
		context.Background(), snapshot,
		&V4ConnectionState{ConnectionKey: 1, ConnectionPosition: accountDataTestPosition},
		"@alice:localhost",
		nil, nil,
		fromPos, toPos,
		map[string]types.SlidingList{}, map[string]bool{roomID: true},
	)
	require.NoError(t, err)

	key := streamKey(roomID, accountDataStream)
	require.Contains(t, db.writes, key)
	stored, err := types.NewStreamTokenFromString(db.writes[key].lastToken)
	require.NoError(t, err)
	assert.Equal(t, types.StreamPosition(95), stored.AccountDataPosition,
		"the room is recorded at the position the query actually returned, not the top of the requested range")
}

// TestProcessAccountDataExtension_InterruptResponseSkipsGapRecovery pins the
// pairing between widening and recording: an interrupt response returns the
// unchanged pos and writes no state, so if it consumed a room's gap it would
// redo the same scan and resend the same payload on every long-poll timeout.
func TestProcessAccountDataExtension_InterruptResponseSkipsGapRecovery(t *testing.T) {
	t.Parallel()

	const roomGappy = "!gappy:localhost"

	fullyRead, err := json.Marshal(map[string]string{"event_id": "$missed:localhost"})
	require.NoError(t, err)

	snapshot := newMockSnapshot()
	snapshot.SetAccountDataChange(roomGappy, "m.fully_read", 60)

	db := newMockStreamStateDB()
	rp := &RequestPool{
		db: db,
		userAPI: &mockUserAPI{
			roomAccountData: map[string]map[string]json.RawMessage{
				roomGappy: {"m.fully_read": fullyRead},
			},
		},
	}

	connState := &V4ConnectionState{
		ConnectionKey:      1,
		ConnectionPosition: 0, // interrupt/timeout response
		PreviousStreamStates: map[string]map[string]*types.SlidingSyncStreamState{
			roomGappy: {
				accountDataStream: {LastToken: (&types.StreamingToken{AccountDataPosition: 50}).String()},
			},
		},
	}

	resp, _, err := rp.processAccountDataExtension(
		context.Background(), snapshot, connState, "@alice:localhost",
		nil, nil,
		&types.StreamingToken{AccountDataPosition: 90}, types.StreamingToken{AccountDataPosition: 200},
		map[string]types.SlidingList{}, map[string]bool{roomGappy: true},
	)
	require.NoError(t, err)

	assert.NotContains(t, resp.Rooms, roomGappy,
		"the gap is left for the next real response, which can record that it was delivered")
	assert.Empty(t, db.writes)
}
