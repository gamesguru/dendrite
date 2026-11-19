// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"fmt"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// A state event that forks off early in the DAG but arrives late must not end
// the timeline of a complete sync; it belongs in the state
// (TestSyncOmitsStateChangeOnFilteredEvents).
func TestTopologicalRecentEventsSkipsLateForkedEvent(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)
		ctx := t.Context()

		alice := test.NewUser(t)
		r := test.NewRoom(t, alice)
		e1 := r.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "E1"})
		s2 := r.CreateEvent(t, alice, "m.room.name", map[string]any{"name": "S2"}, test.WithStateKey(""), test.WithPrevEvents(e1.EventID()))
		e3 := r.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "E3"})
		e4 := r.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "E4"})
		e5 := r.CreateAndInsert(t, alice, "please_filter_me", map[string]any{"body": "E5"}, test.WithPrevEvents(e4.EventID(), s2.EventID()))

		write := func(evs ...*rstypes.HeaderedEvent) {
			for _, ev := range evs {
				var addState []string
				if ev.StateKey() != nil {
					addState = []string{ev.EventID()}
				}
				_, err := db.WriteEvent(ctx, ev, nil, addState, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
				require.NoError(t, err)
			}
		}
		write(r.Events()[:len(r.Events())-4]...)
		write(e3, e4, s2, e5)

		snapshot, err := db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		defer snapshot.Rollback() //nolint:errcheck
		to, err := snapshot.MaxStreamPositionForPDUs(ctx)
		require.NoError(t, err)

		notTypes := []string{"please_filter_me"}
		filter := &synctypes.RoomEventFilter{Limit: 1, NotTypes: &notTypes}
		recent, err := topologicalRecentEvents(ctx, snapshot, []string{r.ID}, to, filter)
		require.NoError(t, err)
		events := recent[r.ID].Events
		require.Len(t, events, 1)
		require.Equal(t, e4.EventID(), events[0].EventID(), "the deepest matching event ends the timeline, not the late fork")
		require.True(t, recent[r.ID].Limited)
	})
}

// Backfilled events are excluded from sync. When they are the deepest events
// in the room they must not fill the batch limit, which would leave a complete
// sync with a short or empty timeline.
func TestTopologicalRecentEventsSkipsBackfilledBatches(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)
		ctx := t.Context()

		alice := test.NewUser(t)
		r := test.NewRoom(t, alice)
		setupEvents := len(r.Events())
		// More excluded events than the three batches a complete sync walks
		// through, all deeper than the sync-visible ones.
		for i := range 40 {
			r.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": fmt.Sprintf("backfilled %d", i)})
		}

		for i, ev := range r.Events() {
			exclude := i >= setupEvents
			_, err := db.WriteEvent(ctx, ev, nil, nil, nil, nil, exclude, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
		}

		snapshot, err := db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		defer snapshot.Rollback() //nolint:errcheck
		to, err := snapshot.MaxStreamPositionForPDUs(ctx)
		require.NoError(t, err)

		filter := &synctypes.RoomEventFilter{Limit: 1}
		recent, err := topologicalRecentEvents(ctx, snapshot, []string{r.ID}, to, filter)
		require.NoError(t, err)
		events := recent[r.ID].Events
		require.Len(t, events, 1, "excluded events must not consume the batch limit")
		require.Equal(t, r.Events()[setupEvents-1].EventID(), events[0].EventID(),
			"the deepest sync-visible event is returned")
	})
}

// A visible create followed by hidden events must be left out of the complete
// sync timeline, which would otherwise anchor prev_batch at the newest event and
// make the hidden events paginate in above the create. Without a hidden gap the
// create stays in the timeline.
func TestTopologicalRecentEventsLeavesCreateForStateOnHiddenGap(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		t.Run("hidden history after create", func(t *testing.T) {
			cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
			defer closeDB()
			cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
			db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
			require.NoError(t, err)
			ctx := t.Context()

			alice := test.NewUser(t)
			r := test.NewRoom(t, alice)
			setupEvents := append([]*rstypes.HeaderedEvent(nil), r.Events()...)
			create := setupEvents[0]
			m1 := r.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "one"})
			m2 := r.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "two"})

			write := func(ev *rstypes.HeaderedEvent, exclude bool) types.StreamPosition {
				var addState []string
				var addStateEvents []*rstypes.HeaderedEvent
				if ev.StateKey() != nil {
					addState = []string{ev.EventID()}
					addStateEvents = []*rstypes.HeaderedEvent{ev}
				}
				position, writeErr := db.WriteEvent(ctx, ev, addStateEvents, addState, nil, nil, exclude, gomatrixserverlib.HistoryVisibilityShared)
				require.NoError(t, writeErr)
				return position
			}

			// Persist the visible timeline before its hidden parents/configuration,
			// matching a room that was joined before its history was backfilled.
			write(create, false)
			m1Pos := write(m1, false)
			write(m2, false)
			var hiddenConfig []*rstypes.HeaderedEvent
			for _, ev := range setupEvents[1:] {
				write(ev, true)
				hiddenConfig = append(hiddenConfig, ev)
			}

			snapshot, err := db.NewDatabaseSnapshot(ctx)
			require.NoError(t, err)
			defer snapshot.Rollback() //nolint:errcheck
			to, err := snapshot.MaxStreamPositionForPDUs(ctx)
			require.NoError(t, err)

			recent, err := topologicalRecentEvents(ctx, snapshot, []string{r.ID}, to, &synctypes.RoomEventFilter{Limit: 20})
			require.NoError(t, err)
			events := recent[r.ID].Events
			require.Len(t, events, 2, "the create is left for backward pagination across the hidden gap")
			require.Equal(t, m1.EventID(), events[0].EventID())
			require.Equal(t, m2.EventID(), events[1].EventID())
			require.True(t, recent[r.ID].Limited)

			// The create remains in the state snapshot; removing it from the
			// timeline must not lose it from a complete sync response.
			stateFilter := synctypes.DefaultStateFilter()
			state, err := snapshot.CurrentState(ctx, r.ID, &stateFilter, nil)
			require.NoError(t, err)
			stateIDs := make(map[string]struct{}, len(state))
			for _, ev := range state {
				stateIDs[ev.EventID()] = struct{}{}
			}
			require.Contains(t, stateIDs, create.EventID())

			// The prev_batch is immediately before the first retained event.
			depth, streamPos, err := snapshot.PositionInTopology(ctx, events[0].EventID())
			require.NoError(t, err)
			prevBatch := types.TopologyToken{Depth: depth, PDUPosition: streamPos}
			prevBatch.Decrement()
			require.Equal(t, m1Pos-1, prevBatch.PDUPosition)

			// Exercise the stream-token backfill path: hidden config rows were
			// persisted after m1, but are topologically between m1 and the create.
			page, _, _, err := snapshot.GetEventsBeforeStreamPosition(
				ctx, prevBatch.PDUPosition, &types.TopologyToken{}, r.ID,
				&synctypes.RoomEventFilter{Limit: 50}, true,
			)
			require.NoError(t, err)
			pageIDs := make([]string, 0, len(page))
			for _, ev := range page {
				pageIDs = append(pageIDs, ev.EventID())
			}
			require.Len(t, pageIDs, len(hiddenConfig)+1)
			require.Equal(t, create.EventID(), pageIDs[len(pageIDs)-1], "the create is last in the backward page")
			for _, ev := range hiddenConfig {
				require.Contains(t, pageIDs, ev.EventID(), "hidden config stored after the cursor remains pageable")
			}
		})

		t.Run("visible history is unchanged", func(t *testing.T) {
			cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
			defer closeDB()
			cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
			db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
			require.NoError(t, err)
			ctx := t.Context()

			alice := test.NewUser(t)
			r := test.NewRoom(t, alice)
			for _, ev := range r.Events() {
				var addState []string
				if ev.StateKey() != nil {
					addState = []string{ev.EventID()}
				}
				_, err := db.WriteEvent(ctx, ev, nil, addState, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
				require.NoError(t, err)
			}
			var messages []*rstypes.HeaderedEvent
			for i := range 25 {
				messages = append(messages, r.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": fmt.Sprintf("%d", i)}))
			}
			for _, ev := range messages {
				_, err := db.WriteEvent(ctx, ev, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
				require.NoError(t, err)
			}

			snapshot, err := db.NewDatabaseSnapshot(ctx)
			require.NoError(t, err)
			defer snapshot.Rollback() //nolint:errcheck
			to, err := snapshot.MaxStreamPositionForPDUs(ctx)
			require.NoError(t, err)
			recent, err := topologicalRecentEvents(ctx, snapshot, []string{r.ID}, to, &synctypes.RoomEventFilter{Limit: 20})
			require.NoError(t, err)
			events := recent[r.ID].Events
			all := r.Events()
			var want []string
			for _, ev := range all[len(all)-20:] {
				want = append(want, ev.EventID())
			}
			var got []string
			for _, ev := range events {
				got = append(got, ev.EventID())
			}
			require.Equal(t, want, got, "exactly the newest 20 events, in order")
			require.True(t, recent[r.ID].Limited)

			// A client limit large enough to reach the create keeps it first when
			// no excluded events follow it.
			recent, err = topologicalRecentEvents(ctx, snapshot, []string{r.ID}, to, &synctypes.RoomEventFilter{Limit: 100})
			require.NoError(t, err)
			got = nil
			for _, ev := range recent[r.ID].Events {
				got = append(got, ev.EventID())
			}
			want = nil
			for _, ev := range all {
				want = append(want, ev.EventID())
			}
			require.Equal(t, want, got, "the whole room, create first")
			require.False(t, recent[r.ID].Limited)
		})

		t.Run("create-only room remains unchanged", func(t *testing.T) {
			cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
			defer closeDB()
			cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
			db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
			require.NoError(t, err)
			ctx := t.Context()

			alice := test.NewUser(t)
			r := test.NewRoom(t, alice)
			create := r.Events()[0]
			_, err = db.WriteEvent(ctx, create, []*rstypes.HeaderedEvent{create}, []string{create.EventID()}, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)

			snapshot, err := db.NewDatabaseSnapshot(ctx)
			require.NoError(t, err)
			defer snapshot.Rollback() //nolint:errcheck
			to, err := snapshot.MaxStreamPositionForPDUs(ctx)
			require.NoError(t, err)
			recent, err := topologicalRecentEvents(ctx, snapshot, []string{r.ID}, to, &synctypes.RoomEventFilter{Limit: 20})
			require.NoError(t, err)
			require.Len(t, recent[r.ID].Events, 1)
			require.Equal(t, create.EventID(), recent[r.ID].Events[0].EventID())
			require.False(t, recent[r.ID].Limited)
		})
	})
}
