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

// The live start of a room is the lowest topological position among delivered
// non-create state events. Backfilled messages and room creation must not drag
// the boundary into history. A room with no such event has no live start.
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

			// A delivered create and excluded state events still do not establish
			// the live state boundary.
			insert(txn, 0, false)
			insert(txn, 1, true)
			_, ok, err = topology.SelectLiveStartPosition(ctx, txn, room.ID)
			require.NoError(t, err)
			require.False(t, ok, "create and excluded events do not start the live state timeline")

			// A backfilled message can have a very low topological position but
			// must not drag the state boundary below a later live state event.
			message := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "old"}, test.WithDepth(1), test.WithPrevEvents(evs[0].EventID()))
			pos, err := events.InsertEvent(ctx, txn, message, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
			_, err = topology.InsertEventInTopology(ctx, txn, message, pos)
			require.NoError(t, err)

			// The lowest delivered state event is the live start, regardless of
			// the older backfilled message's topological position.
			insert(txn, 3, false)
			insert(txn, 2, false)
			pos, ok, err = topology.SelectLiveStartPosition(ctx, txn, room.ID)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, types.StreamPosition(evs[2].Depth()), pos)

			// Another room is independent.
			_, ok, err = topology.SelectLiveStartPosition(ctx, txn, "!other:localhost")
			require.NoError(t, err)
			require.False(t, ok)

			// PostgreSQL rejects the \u0000 and lone-surrogate escapes that Go
			// accepts. A delivered state event carrying one must still establish
			// the live start instead of failing the whole query.
			escaped := room.CreateEvent(t, alice, "m.test.state", map[string]any{"body": "before\x00after"}, test.WithStateKey(""), test.WithDepth(1))
			pos, err = events.InsertEvent(ctx, txn, escaped, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
			_, err = topology.InsertEventInTopology(ctx, txn, escaped, pos)
			require.NoError(t, err)
			pos, ok, err = topology.SelectLiveStartPosition(ctx, txn, room.ID)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, types.StreamPosition(escaped.Depth()), pos)

			surrogateJSON := `{"content":{"body":"\ud800"},"state_key":"","type":"m.test.state"}`
			var insertSurrogateSQL string
			if dbType == test.DBTypePostgres {
				insertSurrogateSQL = `INSERT INTO syncapi_output_room_events
					(event_id, room_id, headered_event_json, type, sender, contains_url, exclude_from_sync, origin_server_ts)
					VALUES ('$surrogate', $1, $2, 'm.test.state', $3, false, false, 1)`
			} else {
				insertSurrogateSQL = `INSERT INTO syncapi_output_room_events
					(event_id, room_id, headered_event_json, type, sender, contains_url, exclude_from_sync)
					VALUES ('$surrogate', $1, $2, 'm.test.state', $3, false, false)`
			}
			_, err = txn.ExecContext(ctx, insertSurrogateSQL, room.ID, surrogateJSON, alice.ID)
			require.NoError(t, err)
			_, err = txn.ExecContext(ctx, `INSERT INTO syncapi_output_room_events_topology
				(event_id, topological_position, room_id, stream_position) VALUES ('$surrogate', -1, $1, 0)`, room.ID)
			require.NoError(t, err)
			pos, ok, err = topology.SelectLiveStartPosition(ctx, txn, room.ID)
			require.NoError(t, err)
			require.True(t, ok)
			require.Equal(t, types.StreamPosition(-1), pos)
			return nil
		}))
	})
}
