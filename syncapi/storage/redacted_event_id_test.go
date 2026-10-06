package storage_test

import (
	"context"
	"database/sql"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	postgresdeltas "codefloe.com/pat-s/zendrite/syncapi/storage/postgres/deltas"
	sqlitedeltas "codefloe.com/pat-s/zendrite/syncapi/storage/sqlite3/deltas"
	"codefloe.com/pat-s/zendrite/test"
)

// Redacting a room v3+ event used to store its headered JSON with an empty
// _event_id. Check that a redaction keeps it, and that the migration repairs
// rows written before the fix.
func TestRedactionKeepsStoredEventID(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice, test.RoomVersion("10"))
		redacted := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hi"})
		redaction := room.CreateEvent(t, alice, spec.MRoomRedaction, map[string]string{"redacts": redacted.EventID()})

		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		t.Cleanup(closeDB)
		db, err := storage.NewSyncServerDatasource(context.Background(), sqlutil.NewConnectionManager(nil, config.DatabaseOptions{}), &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		})
		require.NoError(t, err)
		MustWriteEvents(t, db, room.Events())
		require.NoError(t, db.RedactEvent(context.Background(), redacted.EventID(), redaction, &FakeQuerier{}))

		driver, up := "pgx", postgresdeltas.UpRestoreOutputRoomEventIDs
		if dbType == test.DBTypeSQLite {
			driver, up = sqlutil.SQLITE_DRIVER_NAME, sqlitedeltas.UpRestoreOutputRoomEventIDs
		}
		raw, err := sql.Open(driver, connStr)
		require.NoError(t, err)
		t.Cleanup(func() { _ = raw.Close() })
		storedEventID := func() string {
			var eventJSON string
			require.NoError(t, raw.QueryRow(`SELECT headered_event_json FROM syncapi_output_room_events WHERE event_id = $1`, redacted.EventID()).Scan(&eventJSON))
			require.True(t, gjson.Get(eventJSON, "content").IsObject())
			return gjson.Get(eventJSON, "_event_id").String()
		}
		require.Equal(t, redacted.EventID(), storedEventID())

		var eventJSON string
		require.NoError(t, raw.QueryRow(`SELECT headered_event_json FROM syncapi_output_room_events WHERE event_id = $1`, redacted.EventID()).Scan(&eventJSON))
		broken, err := sjson.Set(eventJSON, "_event_id", "")
		require.NoError(t, err)
		_, err = raw.Exec(`UPDATE syncapi_output_room_events SET headered_event_json = $1 WHERE event_id = $2`, broken, redacted.EventID())
		require.NoError(t, err)
		require.Empty(t, storedEventID())

		tx, err := raw.Begin()
		require.NoError(t, err)
		require.NoError(t, up(context.Background(), tx))
		require.NoError(t, tx.Commit())
		require.Equal(t, redacted.EventID(), storedEventID())
	})
}
