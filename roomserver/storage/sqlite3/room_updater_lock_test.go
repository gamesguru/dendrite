// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3_test

import (
	"context"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

func mustOpen(t *testing.T) *sqlite3.Database {
	t.Helper()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypeSQLite)
	t.Cleanup(closeDB)
	cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	cache := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
	db, err := sqlite3.Open(context.Background(), cm, &config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}, cache)
	require.NoError(t, err)
	return db
}

func mustRoom(t *testing.T, db *sqlite3.Database, roomID string) *types.RoomInfo {
	t.Helper()
	id, err := spec.NewRoomID(roomID)
	require.NoError(t, err)
	_, err = db.AssignRoomNID(context.Background(), *id, gomatrixserverlib.RoomVersionV10)
	require.NoError(t, err)
	info, err := db.RoomInfo(context.Background(), roomID)
	require.NoError(t, err)
	require.NotNil(t, info)
	return info
}

// SQLite room updaters have no transaction, so the partial state resync could
// read a room's latest events while the input worker was updating the room,
// then overwrite what it wrote. An updater must hold its room until it ends.
func TestRoomUpdaterHoldsTheRoomUntilItEnds(t *testing.T) {
	db := mustOpen(t)
	ctx := context.Background()
	room := mustRoom(t, db, "!one:test")
	other := mustRoom(t, db, "!two:test")

	first, err := db.GetRoomUpdater(ctx, room)
	require.NoError(t, err)

	// Another room is not held up.
	otherUpdater, err := db.GetRoomUpdater(ctx, other)
	require.NoError(t, err)
	require.NoError(t, otherUpdater.Commit())

	got := make(chan error, 1)
	go func() {
		second, err := db.GetRoomUpdater(ctx, room)
		if err == nil {
			err = second.Commit()
		}
		got <- err
	}()

	select {
	case <-got:
		t.Fatal("a second updater for the same room started while the first was open")
	case <-time.After(200 * time.Millisecond):
	}

	require.NoError(t, first.Commit())
	select {
	case err := <-got:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("the second updater did not start after the first ended")
	}
}

func TestRoomUpdaterIsReleasedByRollbackAndHonoursContext(t *testing.T) {
	db := mustOpen(t)
	room := mustRoom(t, db, "!one:test")

	first, err := db.GetRoomUpdater(context.Background(), room)
	require.NoError(t, err)

	// Waiting for a held room gives up when the context ends.
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = db.GetRoomUpdater(ctx, room)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	require.NoError(t, first.Rollback())
	again, err := db.GetRoomUpdater(context.Background(), room)
	require.NoError(t, err)
	require.NoError(t, again.Commit())
	// Finishing twice must not release a lock it no longer holds.
	require.NoError(t, again.Commit())
	third, err := db.GetRoomUpdater(context.Background(), room)
	require.NoError(t, err)
	require.NoError(t, third.Commit())
}
