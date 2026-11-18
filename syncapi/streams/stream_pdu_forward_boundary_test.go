// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// A backfilled event stored after the room's normal activity is the last row
// before the token, but it is not the furthest the client has been sent. The
// boundary is the highest delivered topological position, so a later backfill
// that sits below that frontier is still history.
func TestIncrementalSyncBoundaryIgnoresLateStoredHistory(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)
		ctx := t.Context()

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "a"})
		backfilled := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "backfilled"})
		mid := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "mid"})
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "c"})
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "d"})

		write := func(ev *rstypes.HeaderedEvent) {
			var state []*rstypes.HeaderedEvent
			var addState []string
			if ev.StateKey() != nil {
				state, addState = []*rstypes.HeaderedEvent{ev}, []string{ev.EventID()}
			}
			_, werr := db.WriteEvent(ctx, ev, state, addState, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, werr)
		}
		position := func() types.StreamPosition {
			snapshot, serr := db.NewDatabaseSnapshot(ctx)
			require.NoError(t, serr)
			defer snapshot.Rollback() //nolint:errcheck
			pos, serr := snapshot.MaxStreamPositionForPDUs(ctx)
			require.NoError(t, serr)
			return pos
		}

		// The two backfills are held back while the rest of the room arrives.
		for _, ev := range room.Events() {
			if ev == backfilled || ev == mid {
				continue
			}
			write(ev)
		}
		// The first is stored last before the token, at a low topological
		// position. It must not lower the boundary for the events after it.
		write(backfilled)
		since := position()

		// This backfill arrives after the token but still below the room's
		// frontier, so it is history.
		write(mid)
		live := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "live"})
		write(live)
		to := position()
		require.Greater(t, to, since)

		snapshot, err := db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		defer snapshot.Rollback() //nolint:errcheck

		req := &types.SyncRequest{
			Context:           ctx,
			Device:            &userapi.Device{UserID: alice.ID},
			Rooms:             map[string]string{},
			Response:          types.NewResponse(),
			Filter:            synctypes.DefaultFilter(),
			Log:               logrus.NewEntry(logrus.StandardLogger()),
			MembershipChanges: map[string]struct{}{},
			NewlyJoinedRooms:  map[string]struct{}{},
		}
		provider := &PDUStreamProvider{rsAPI: leaveTestRoomserver{}}
		require.Equal(t, to, provider.IncrementalSync(ctx, snapshot, req, since, to))

		jr, ok := req.Response.Rooms.Join[room.ID]
		require.True(t, ok)
		timeline := make([]string, 0, len(jr.Timeline.Events))
		for _, ev := range jr.Timeline.Events {
			timeline = append(timeline, ev.EventID)
		}
		require.NotContains(t, timeline, mid.EventID(), "a late store below the frontier is history")
		require.Contains(t, timeline, live.EventID(), "new activity is still delivered")
	})
}

// first topological position. A forward incremental sync must not deliver it as
// new activity, while a genuinely later event still arrives.
// A create stored after the rest of its room has a new stream position but the
// first topological position. A forward incremental sync must not deliver it as
// new activity, while a genuinely later event still arrives.
func TestIncrementalSyncDropsLateEventBeforeDeliveryBoundary(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)
		ctx := t.Context()

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		events := room.Events()
		require.Equal(t, spec.MRoomCreate, events[0].Type())

		write := func(ev *rstypes.HeaderedEvent) {
			var state []*rstypes.HeaderedEvent
			var addState []string
			if ev.StateKey() != nil {
				state, addState = []*rstypes.HeaderedEvent{ev}, []string{ev.EventID()}
			}
			_, werr := db.WriteEvent(ctx, ev, state, addState, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, werr)
		}
		position := func() types.StreamPosition {
			snapshot, serr := db.NewDatabaseSnapshot(ctx)
			require.NoError(t, serr)
			defer snapshot.Rollback() //nolint:errcheck
			pos, serr := snapshot.MaxStreamPositionForPDUs(ctx)
			require.NoError(t, serr)
			return pos
		}

		// Everything but the create arrives normally, so the client's token is
		// already past the room's live state.
		for _, ev := range events[1:] {
			write(ev)
		}
		since := position()

		// The create is stored late, as a backfilled event, and gets a stream
		// position newer than anything the client has seen while its place in
		// the room stays at the start.
		write(events[0])

		// Activity that really is new must still be delivered.
		live := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "after"})
		write(live)
		to := position()
		require.Greater(t, to, since)

		snapshot, err := db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		defer snapshot.Rollback() //nolint:errcheck

		req := &types.SyncRequest{
			Context:           ctx,
			Device:            &userapi.Device{UserID: alice.ID},
			Rooms:             map[string]string{},
			Response:          types.NewResponse(),
			Filter:            synctypes.DefaultFilter(),
			Log:               logrus.NewEntry(logrus.StandardLogger()),
			MembershipChanges: map[string]struct{}{},
			NewlyJoinedRooms:  map[string]struct{}{},
		}
		provider := &PDUStreamProvider{rsAPI: leaveTestRoomserver{}}
		require.Equal(t, to, provider.IncrementalSync(ctx, snapshot, req, since, to))

		jr, ok := req.Response.Rooms.Join[room.ID]
		require.True(t, ok, "the room still carries the new activity")
		timeline := make([]string, 0, len(jr.Timeline.Events))
		for _, ev := range jr.Timeline.Events {
			timeline = append(timeline, ev.EventID)
		}
		require.NotContains(t, timeline, events[0].EventID(), "a late event behind the frontier is history")
		require.Contains(t, timeline, live.EventID(), "new activity is still delivered")
	})
}
