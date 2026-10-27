// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
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
