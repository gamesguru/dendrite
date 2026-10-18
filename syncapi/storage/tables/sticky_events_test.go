package tables_test

import (
	"context"
	"database/sql"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage/postgres"
	"codefloe.com/pat-s/zendrite/syncapi/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

func newStickyEventsTable(t *testing.T, dbType test.DBType) (tables.StickyEvents, *sql.DB, func()) {
	t.Helper()
	connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, sqlutil.NewExclusiveWriter())
	require.NoError(t, err)

	var tab tables.StickyEvents
	switch dbType {
	case test.DBTypePostgres:
		tab, err = postgres.NewPostgresStickyEventsTable(db)
	case test.DBTypeSQLite:
		var stream sqlite3.StreamIDStatements
		require.NoError(t, stream.Prepare(db))
		tab, err = sqlite3.NewSqliteStickyEventsTable(db, &stream)
	}
	require.NoError(t, err)
	return tab, db, closeDB
}

func stickyEventIDs(events []types.StickyEvent) []string {
	ids := make([]string, 0, len(events))
	for _, ev := range events {
		ids = append(ids, ev.EventID)
	}
	return ids
}

func TestStickyEventsTable(t *testing.T) {
	ctx := context.Background()
	const (
		roomA = "!a:server"
		roomB = "!b:server"
		alice = "@alice:server"
		now   = spec.Timestamp(1_000_000)
	)
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		tab, db, closeDB := newStickyEventsTable(t, dbType)
		defer closeDB()

		maxID, err := tab.SelectMaxStickyEventID(ctx, nil)
		require.NoError(t, err)
		require.Zero(t, maxID, "an empty stream starts at zero")

		// Positions are handed out in insertion order, across rooms.
		insert := func(roomID, eventID string, expiresAt spec.Timestamp) types.StreamPosition {
			t.Helper()
			var pos types.StreamPosition
			require.NoError(t, sqlutil.WithTransaction(db, func(txn *sql.Tx) error {
				pos, err = tab.InsertStickyEvent(ctx, txn, roomID, eventID, alice, expiresAt)
				return err
			}))
			return pos
		}
		p1 := insert(roomA, "$a1", now+60_000)
		p2 := insert(roomB, "$b1", now+60_000)
		p3 := insert(roomA, "$a2", now+10)
		p4 := insert(roomA, "$a3", now+60_000)
		require.Less(t, p1, p2)
		require.Less(t, p2, p3)
		require.Less(t, p3, p4)

		// Inserting an event again keeps its position.
		require.Equal(t, p1, insert(roomA, "$a1", now+60_000))

		maxID, err = tab.SelectMaxStickyEventID(ctx, nil)
		require.NoError(t, err)
		require.GreaterOrEqual(t, types.StreamPosition(maxID), p4)

		// Only the requested rooms, in stream order.
		events, err := tab.SelectStickyEventsInRooms(ctx, nil, []string{roomA}, 0, p4, now, 0)
		require.NoError(t, err)
		require.Equal(t, []string{"$a1", "$a2", "$a3"}, stickyEventIDs(events))
		require.Equal(t, types.StickyEvent{Position: p1, RoomID: roomA, EventID: "$a1", Sender: alice, ExpiresAt: now + 60_000}, events[0])

		// The range excludes from and includes to.
		events, err = tab.SelectStickyEventsInRooms(ctx, nil, []string{roomA, roomB}, p1, p3, now, 0)
		require.NoError(t, err)
		require.Equal(t, []string{"$b1", "$a2"}, stickyEventIDs(events))

		// The limit keeps the oldest positions.
		events, err = tab.SelectStickyEventsInRooms(ctx, nil, []string{roomA, roomB}, 0, p4, now, 2)
		require.NoError(t, err)
		require.Equal(t, []string{"$a1", "$b1"}, stickyEventIDs(events))

		// Expired events are left out even before they are deleted.
		events, err = tab.SelectStickyEventsInRooms(ctx, nil, []string{roomA}, 0, p4, now+10, 0)
		require.NoError(t, err)
		require.Equal(t, []string{"$a1", "$a3"}, stickyEventIDs(events))

		events, err = tab.SelectStickyEventsByID(ctx, nil, []string{"$a1", "$a2", "$unknown"}, now+10)
		require.NoError(t, err)
		require.Equal(t, []string{"$a1"}, stickyEventIDs(events))

		// Nothing to look up.
		events, err = tab.SelectStickyEventsInRooms(ctx, nil, nil, 0, p4, now, 0)
		require.NoError(t, err)
		require.Empty(t, events)
		events, err = tab.SelectStickyEventsInRooms(ctx, nil, []string{roomA}, p4, p4, now, 0)
		require.NoError(t, err)
		require.Empty(t, events)

		// Deleting rows, by expiry, redaction or purge, does not move the
		// stream back.
		require.NoError(t, sqlutil.WithTransaction(db, func(txn *sql.Tx) error {
			if err = tab.DeleteExpiredStickyEvents(ctx, txn, now+10); err != nil {
				return err
			}
			if err = tab.DeleteStickyEvent(ctx, txn, "$a1"); err != nil {
				return err
			}
			return tab.PurgeStickyEvents(ctx, txn, roomB)
		}))
		events, err = tab.SelectStickyEventsInRooms(ctx, nil, []string{roomA, roomB}, 0, p4, now, 0)
		require.NoError(t, err)
		require.Equal(t, []string{"$a3"}, stickyEventIDs(events))
		maxAfterDelete, err := tab.SelectMaxStickyEventID(ctx, nil)
		require.NoError(t, err)
		require.Equal(t, maxID, maxAfterDelete)

		// A deleted event can be inserted again at a new position.
		require.Greater(t, insert(roomA, "$a1", now+60_000), p4)
	})
}
