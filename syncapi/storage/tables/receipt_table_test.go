package tables_test

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage/postgres"
	"codefloe.com/pat-s/zendrite/syncapi/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/test"
)

func TestThreadedReceiptPersistence(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, backend test.DBType) {
		ctx := context.Background()
		conn, cleanup := test.PrepareDBConnectionString(t, backend)
		defer cleanup()
		open := func() (*sql.DB, tables.Receipts) {
			db, err := sqlutil.Open(&config.DatabaseOptions{ConnectionString: config.DataSource(conn)}, sqlutil.NewExclusiveWriter())
			require.NoError(t, err)
			var tab tables.Receipts
			if backend == test.DBTypePostgres {
				tab, err = postgres.NewPostgresReceiptsTable(db)
			} else {
				stream := &sqlite3.StreamIDStatements{}
				require.NoError(t, stream.Prepare(db))
				tab, err = sqlite3.NewSqliteReceiptsTable(db, stream)
			}
			require.NoError(t, err)
			return db, tab
		}
		// Seed the pre-threading schema to exercise a real upgrade.
		legacy, err := sqlutil.Open(&config.DatabaseOptions{ConnectionString: config.DataSource(conn)}, sqlutil.NewExclusiveWriter())
		require.NoError(t, err)
		_, err = legacy.ExecContext(ctx, `CREATE TABLE syncapi_receipts (
   id BIGINT PRIMARY KEY, room_id TEXT NOT NULL, receipt_type TEXT NOT NULL, user_id TEXT NOT NULL,
   event_id TEXT NOT NULL, receipt_ts BIGINT NOT NULL,
   CONSTRAINT syncapi_receipts_unique UNIQUE(room_id, receipt_type, user_id));
   INSERT INTO syncapi_receipts VALUES (1, '!room:test', 'm.read', '@alice:test', '$legacy', 1);`)
		require.NoError(t, err)
		if backend == test.DBTypePostgres {
			_, err = legacy.ExecContext(ctx, `CREATE SEQUENCE syncapi_receipt_id START 2; ALTER TABLE syncapi_receipts ALTER COLUMN id SET DEFAULT nextval('syncapi_receipt_id')`)
			require.NoError(t, err)
		}
		m := sqlutil.NewMigrator(legacy)
		m.AddMigrations(sqlutil.Migration{Version: "syncapi: fix sequences", Up: func(context.Context, *sql.Tx) error { return nil }})
		require.NoError(t, m.Up(ctx))
		require.NoError(t, legacy.Close())
		db, tab := open()
		_, got, err := tab.SelectRoomReceiptsAfter(ctx, nil, []string{"!room:test"}, 0)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Empty(t, got[0].ThreadID)
		for _, thread := range []string{"", "main", "$a", "$b"} {
			_, err = tab.UpsertReceipt(ctx, nil, "!room:test", "m.read", "@alice:test", "$read"+thread, thread, 2)
			require.NoError(t, err)
		}
		_, err = tab.UpsertReceipt(ctx, nil, "!room:test", "m.read.private", "@alice:test", "$private", "$a", 3)
		require.NoError(t, err)
		_, err = db.ExecContext(ctx, `INSERT INTO syncapi_sliding_sync_connections (connection_key,user_id,device_id,conn_id,created_ts) VALUES (1,'@alice:test','device','conn',1)`)
		require.NoError(t, err)
		pending, err := tab.SelectLatestUserReceiptsForConnection(ctx, nil, 1, []string{"!room:test"}, "@alice:test")
		require.NoError(t, err)
		require.Len(t, pending, 5)
		for _, receipt := range pending {
			require.NoError(t, tab.UpsertConnectionReceipt(ctx, nil, 1, receipt.RoomID, receipt.Type, receipt.UserID, receipt.EventID, receipt.ThreadID, receipt.Timestamp))
		}
		require.NoError(t, db.Close())
		db, tab = open()
		defer db.Close()
		db.SetMaxOpenConns(1)
		txn, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)
		pending, err = tab.SelectLatestUserReceiptsForConnection(ctx, txn, 1, []string{"!room:test"}, "@alice:test")
		require.NoError(t, err)
		require.Empty(t, pending)
		require.NoError(t, txn.Rollback())
		pending, err = tab.SelectLatestUserReceiptsForConnection(ctx, nil, 1, []string{"!room:test"}, "@alice:test")
		require.NoError(t, err)
		require.Empty(t, pending)
		before, _, err := tab.SelectRoomReceiptsAfter(ctx, nil, []string{"!room:test"}, 0)
		require.NoError(t, err)
		_, err = tab.UpsertReceipt(ctx, nil, "!room:test", "m.read", "@alice:test", "$a-next", "$a", 4)
		require.NoError(t, err)
		_, got, err = tab.SelectRoomReceiptsAfter(ctx, nil, []string{"!room:test"}, before)
		require.NoError(t, err)
		require.Len(t, got, 1)
		require.Equal(t, "$a", got[0].ThreadID)
		pending, err = tab.SelectLatestUserReceiptsForConnection(ctx, nil, 1, []string{"!room:test"}, "@alice:test")
		require.NoError(t, err)
		require.Equal(t, got, pending)
		_, all, err := tab.SelectRoomReceiptsAfter(ctx, nil, []string{"!room:test"}, 0)
		require.NoError(t, err)
		require.Len(t, all, 5)
	})
}
