// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// streamStateWrite records one UpdateConnectionStream call.
type streamStateWrite struct {
	position  int64
	stream    string
	status    string
	lastToken string
}

// mockStreamStateDB implements storage.Database for testing, recording the
// stream state writes instead of persisting them. Only the method under test
// is overridden; anything else panics on a nil interface, which keeps the
// test honest about what it exercises.
type mockStreamStateDB struct {
	storage.Database

	// Keyed by streamKey: one room has a row per stream, so keying by room
	// alone would let an account_data write mask an events write.
	writes  map[string]streamStateWrite
	failFor map[string]bool
}

// streamKey identifies one (room, stream) row, matching the connection stream
// table's primary key.
func streamKey(roomID, stream string) string {
	return roomID + "|" + stream
}

func newMockStreamStateDB() *mockStreamStateDB {
	return &mockStreamStateDB{
		writes:  make(map[string]streamStateWrite),
		failFor: make(map[string]bool),
	}
}

func (m *mockStreamStateDB) UpdateConnectionStream(_ context.Context, connectionPosition int64, roomID, stream, roomStatus, lastToken string) error {
	if m.failFor[roomID] {
		return errors.New("write failed")
	}
	m.writes[streamKey(roomID, stream)] = streamStateWrite{
		position:  connectionPosition,
		stream:    stream,
		status:    roomStatus,
		lastToken: lastToken,
	}
	return nil
}

// TestCopyForwardStreamStates covers the state a room is left in when it was
// sent on an earlier response but is absent from this one. It must be recorded
// as PREVIOUSLY (Synapse's record_unsent_rooms semantic) and keep the token of
// the position it was last actually sent at.
func TestCopyForwardStreamStates(t *testing.T) {
	t.Parallel()

	const (
		newPosition = int64(42)
		droppedRoom = "!dropped:localhost"
		sentRoom    = "!sent:localhost"
		lastToken   = "s100_50_25_10_5_3_1_0_8"
	)

	connState := func() *V4ConnectionState {
		return &V4ConnectionState{
			ConnectionKey:      1,
			ConnectionPosition: newPosition,
			PreviousStreamStates: map[string]map[string]*types.SlidingSyncStreamState{
				droppedRoom: {"events": {RoomStatus: "live", LastToken: lastToken}},
				sentRoom:    {"events": {RoomStatus: "live", LastToken: lastToken}},
			},
		}
	}

	t.Run("room absent from the response is downgraded to previously", func(t *testing.T) {
		t.Parallel()

		db := newMockStreamStateDB()
		rp := &RequestPool{db: db}

		copied, err := rp.copyForwardStreamStates(context.Background(), connState(), map[string]types.RoomSubscriptionConfig{
			sentRoom: {TimelineLimit: 10},
		})

		require.NoError(t, err)
		assert.Equal(t, 1, copied)
		require.Contains(t, db.writes, streamKey(droppedRoom, "events"))
		assert.Equal(t, streamStateWrite{
			position:  newPosition,
			stream:    "events",
			status:    types.HaveSentRoomPreviously.String(),
			lastToken: lastToken,
		}, db.writes[streamKey(droppedRoom, "events")])
	})

	t.Run("room sent in the response is left alone", func(t *testing.T) {
		t.Parallel()

		db := newMockStreamStateDB()
		rp := &RequestPool{db: db}

		_, err := rp.copyForwardStreamStates(context.Background(), connState(), map[string]types.RoomSubscriptionConfig{
			sentRoom: {TimelineLimit: 10},
		})

		require.NoError(t, err)
		// The room is written by the main response loop instead, which marks it live.
		assert.NotContains(t, db.writes, streamKey(sentRoom, "events"))
	})

	t.Run("a room already previously keeps its original token", func(t *testing.T) {
		t.Parallel()

		db := newMockStreamStateDB()
		rp := &RequestPool{db: db}
		state := connState()
		state.PreviousStreamStates[droppedRoom]["events"].RoomStatus = types.HaveSentRoomPreviously.String()

		copied, err := rp.copyForwardStreamStates(context.Background(), state, nil)

		require.NoError(t, err)
		assert.Equal(t, 2, copied)
		assert.Equal(t, lastToken, db.writes[streamKey(droppedRoom, "events")].lastToken,
			"the gap start must not move forward while a room stays unsent")
	})

	t.Run("room with no events stream is skipped", func(t *testing.T) {
		t.Parallel()

		db := newMockStreamStateDB()
		rp := &RequestPool{db: db}
		state := &V4ConnectionState{
			ConnectionPosition: newPosition,
			PreviousStreamStates: map[string]map[string]*types.SlidingSyncStreamState{
				droppedRoom: {"receipts": {RoomStatus: "live", LastToken: lastToken}},
			},
		}

		copied, err := rp.copyForwardStreamStates(context.Background(), state, nil)

		require.NoError(t, err)
		assert.Equal(t, 0, copied)
		assert.Empty(t, db.writes)
	})

	t.Run("a failed write does not stop the remaining rooms", func(t *testing.T) {
		t.Parallel()

		db := newMockStreamStateDB()
		db.failFor[droppedRoom] = true
		rp := &RequestPool{db: db}

		copied, err := rp.copyForwardStreamStates(context.Background(), connState(), nil)

		require.NoError(t, err, "a genuine write failure is logged, not treated as cancellation")
		assert.Equal(t, 1, copied)
		assert.NotContains(t, db.writes, streamKey(droppedRoom, "events"))
		assert.Contains(t, db.writes, streamKey(sentRoom, "events"))
	})
}
