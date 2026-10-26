// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/test"
)

type backfillOnlyAPI struct {
	api.SyncRoomserverAPI
	events []*rstypes.HeaderedEvent
}

func (f *backfillOnlyAPI) PerformBackfill(_ context.Context, _ *api.PerformBackfillRequest, res *api.PerformBackfillResponse) error {
	res.Events = f.events
	res.HistoryVisibility = gomatrixserverlib.HistoryVisibilityShared
	return nil
}

func TestBackfillEventsPurgesFilledBackwardExtremities(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		older := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "older"})
		newer := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "newer"})

		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewSyncServerDatasource(context.Background(), cm, &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		})
		require.NoError(t, err)

		ctx := context.Background()
		// Store the newer event without its prev event, as happens when a
		// re-join arrives before the history behind it.
		_, err = db.WriteEvent(ctx, newer, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)

		snapshot, err := db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		extremities, err := snapshot.BackwardExtremitiesForRoom(ctx, room.ID)
		require.NoError(t, err)
		_ = snapshot.Rollback()
		require.Contains(t, extremities, newer.EventID())

		// Backfilling the prev event makes the newer event's marker stale.
		_, err = backfillEvents(ctx, &backfillOnlyAPI{events: []*rstypes.HeaderedEvent{older}},
			db, "hs2", "hs2", room.ID,
			map[string][]string{newer.EventID(): {older.EventID()}}, 10)
		require.NoError(t, err)

		snapshot, err = db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		defer func() { _ = snapshot.Rollback() }()
		extremities, err = snapshot.BackwardExtremitiesForRoom(ctx, room.ID)
		require.NoError(t, err)
		assert.NotContains(t, extremities, newer.EventID(), "filled extremity must be cleared")
		assert.Contains(t, extremities, older.EventID(), "the new boundary extremity must remain")
	})
}
