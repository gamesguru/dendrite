// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package tables_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage/postgres"
	"codefloe.com/pat-s/zendrite/syncapi/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

// The live start of a room is the lowest topological position among the events
// sync delivers. Events sync keeps out do not count, and a room with none has
// no live start.
func TestTopologyLiveStartPosition(t *testing.T) {
	ctx := context.Background()
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		db, err := sqlutil.Open(&config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}, sqlutil.NewExclusiveWriter())
		require.NoError(t, err)

		var events tables.Events
		var topology tables.Topology
		switch dbType {
		case test.DBTypePostgres:
			events, err = postgres.NewPostgresEventsTable(ctx, db)
			require.NoError(t, err)
			topology, err = postgres.NewPostgresTopologyTable(db)
		case test.DBTypeSQLite:
			var stream sqlite3.StreamIDStatements
			require.NoError(t, stream.Prepare(db))
			events, err = sqlite3.NewSqliteEventsTable(ctx, db, &stream)
			require.NoError(t, err)
			topology, err = sqlite3.NewSqliteTopologyTable(db)
		}
		require.NoError(t, err)

		evs := room.Events()
		require.GreaterOrEqual(t, len(evs), 4)
		insert := func(txn *sql.Tx, i int, exclude bool) {
			pos, err := events.InsertEvent(ctx, txn, evs[i], nil, nil, nil, exclude, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
			_, err = topology.InsertEventInTopology(ctx, txn, evs[i], pos)
			require.NoError(t, err)
		}

		require.NoError(t, sqlutil.WithTransaction(db, func(txn *sql.Tx) error {
			// Nothing stored: no live start.
			_, ok, err := topology.SelectLiveStartPosition(ctx, txn, room.ID)
			require.NoError(t, err)
			require.False(t, ok)

			// Only excluded events: still no live start.
			insert(txn, 0, true)
			insert(txn, 1, true)
			_, ok, err = topology.SelectLiveStartPosition(ctx, txn, room.ID)
			require.NoError(t, err)
			require.False(t, ok, "excluded events do not start the live part of a room")

			// The lowest delivered event is the live start, whatever order they
			// were stored in.
			insert(txn, 3, false)
			insert(txn, 2, false)
			pos, ok, err := topology.SelectLiveStartPosition(ctx, txn, room.ID)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, types.StreamPosition(evs[2].Depth()), pos)

			// Another room is independent.
			_, ok, err = topology.SelectLiveStartPosition(ctx, txn, "!other:localhost")
			require.NoError(t, err)
			require.False(t, ok)
			return nil
		}))
	})
}
