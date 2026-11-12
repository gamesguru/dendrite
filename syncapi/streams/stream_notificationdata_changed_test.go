// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"encoding/json"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// A notification count can change with no event in the sync window, because
// its row lands after the event it belongs to was already synced. The
// notification cursor must not move past such a change without sending it.
func TestNotificationCountChangeWithoutNewEvent(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)
		ctx := t.Context()

		alice := test.NewUser(t)
		joined := test.NewRoom(t, alice)
		left := test.NewRoom(t, alice) // alice never joins: no membership row is written
		for _, ev := range joined.Events() {
			var addState []*rstypes.HeaderedEvent
			var addStateIDs []string
			if ev.StateKey() != nil {
				addState = []*rstypes.HeaderedEvent{ev}
				addStateIDs = []string{ev.EventID()}
			}
			_, err = db.WriteEvent(ctx, ev, addState, addStateIDs, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
		}

		upsert := func(roomID string, notifications, highlights int) types.StreamPosition {
			pos, err := db.UpsertRoomUnreadNotificationCounts(ctx, alice.ID, &eventutil.NotificationData{
				RoomID: roomID, UnreadNotificationCount: notifications, UnreadHighlightCount: highlights,
			})
			require.NoError(t, err)
			return pos
		}
		// run does one incremental notification pass for a client whose cursor is from,
		// where no PDU stream added any room: req.Rooms and the join response are empty.
		run := func(from types.StreamPosition, complete bool) (types.StreamPosition, map[string]json.RawMessage) {
			snapshot, err := db.NewDatabaseSnapshot(ctx)
			require.NoError(t, err)
			defer snapshot.Rollback() //nolint:errcheck
			req := &types.SyncRequest{
				Device:   &userapi.Device{UserID: alice.ID},
				Rooms:    map[string]string{},
				Response: types.NewResponse(),
				Filter:   synctypes.DefaultFilter(),
			}
			provider := &NotificationDataStreamProvider{DefaultStreamProvider: DefaultStreamProvider{DB: db}}
			var next types.StreamPosition
			if complete {
				next = provider.CompleteSync(ctx, snapshot, req)
			} else {
				next = provider.IncrementalSync(ctx, snapshot, req, from, 0)
			}
			rooms := map[string]json.RawMessage{}
			for roomID, jr := range req.Response.Rooms.Join {
				raw, err := json.Marshal(jr)
				require.NoError(t, err)
				rooms[roomID] = raw
			}
			return next, rooms
		}

		t.Run("a changed count adds its joined room with the new count", func(t *testing.T) {
			from := upsert(joined.ID, 5, 2)
			latest := upsert(joined.ID, 6, 2)
			next, rooms := run(from, false)
			require.Equal(t, latest, next)
			require.Contains(t, rooms, joined.ID)
			require.JSONEq(t, `{"highlight_count":2,"notification_count":6}`, field(t, rooms[joined.ID], "unread_notifications"))
		})

		t.Run("a cleared count is sent explicitly", func(t *testing.T) {
			from := upsert(joined.ID, 6, 2)
			latest := upsert(joined.ID, 0, 0)
			next, rooms := run(from, false)
			require.Equal(t, latest, next)
			require.Contains(t, rooms, joined.ID)
			require.JSONEq(t, `{"highlight_count":0,"notification_count":0}`, field(t, rooms[joined.ID], "unread_notifications"))
		})

		t.Run("a room the user is not joined to is not added", func(t *testing.T) {
			from := upsert(joined.ID, 6, 2)
			latest := upsert(left.ID, 9, 0)
			next, rooms := run(from, false)
			require.Equal(t, latest, next)
			require.NotContains(t, rooms, left.ID)
		})

		t.Run("nothing changed since the cursor adds nothing", func(t *testing.T) {
			latest := upsert(joined.ID, 6, 2)
			next, rooms := run(latest, false)
			require.Equal(t, latest, next)
			require.Empty(t, rooms)
		})

		t.Run("a complete sync does not add rooms the PDU stream did not", func(t *testing.T) {
			upsert(joined.ID, 7, 2)
			_, rooms := run(0, true)
			require.Empty(t, rooms)
		})

		t.Run("incremental sync from zero includes a changed count", func(t *testing.T) {
			latest := upsert(joined.ID, 8, 3)
			next, rooms := run(0, false)
			require.Equal(t, latest, next)
			require.Contains(t, rooms, joined.ID)
			require.JSONEq(t, `{"highlight_count":3,"notification_count":8}`, field(t, rooms[joined.ID], "unread_notifications"))
		})
	})
}

func TestRoomSelected(t *testing.T) {
	const roomID = "!room:test"
	none := []string{}
	some := []string{"!other:test", roomID}
	excluded := []string{roomID}

	require.True(t, roomSelected(nil, nil, roomID), "no filter selects every room")
	require.False(t, roomSelected(&none, nil, roomID), "an explicit empty allow-list selects no room")
	require.True(t, roomSelected(&some, nil, roomID))
	require.False(t, roomSelected(&some, nil, "!missing:test"))
	require.False(t, roomSelected(nil, &excluded, roomID))
	require.False(t, roomSelected(&some, &excluded, roomID), "exclusions take precedence")
}
