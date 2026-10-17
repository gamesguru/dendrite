package postgres

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

func TestOutputEventTimestampUpgrade(t *testing.T) {
	for _, oldIndex := range []bool{false, true} {
		t.Run(fmt.Sprintf("existing_index=%t", oldIndex), func(t *testing.T) {
			ctx := t.Context()
			connStr, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
			defer cleanup()
			db, err := sqlutil.Open(&config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}, sqlutil.NewExclusiveWriter())
			require.NoError(t, err)
			defer db.Close()
			_, err = db.ExecContext(ctx, outputRoomEventsSchema)
			require.NoError(t, err)
			alice := test.NewUser(t)
			room := test.NewRoom(t, alice)
			body := "escaped null: \x00, literal escape: \\u0000, emoji: 😀"
			event := room.CreateEvent(t, alice, "m.room.message", map[string]string{"body": body}, test.WithTimestamp(time.UnixMilli(1234)))
			eventJSON, err := json.Marshal(event)
			require.NoError(t, err)
			legacyJSON := eventJSON
			if oldIndex {
				// A server that successfully installed the old index also needs upgrading.
				legacyJSON = []byte(`{"origin_server_ts":1234}`)
			}
			// Cross the migration batch boundary using the schema from before #322.
			_, err = db.ExecContext(ctx, `INSERT INTO syncapi_output_room_events
				(event_id, room_id, headered_event_json, type, sender, contains_url)
				SELECT '$legacy' || n, $1, $2, 'm.room.message', $3, false FROM generate_series(1, 1001) AS n`, room.ID, string(legacyJSON), alice.ID)
			require.NoError(t, err)
			const oldIndexSQL = `CREATE INDEX syncapi_output_room_events_timestamp_idx ON syncapi_output_room_events
				(room_id, (((headered_event_json::json ->> 'origin_server_ts')::BIGINT)), id)`
			_, err = db.ExecContext(ctx, oldIndexSQL)
			if oldIndex {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "unsupported Unicode escape sequence")
			}
			// PostgreSQL reads the plain row, Go decodes the ones its JSON parser rejects.
			mixed := map[string]string{
				"$plain":   `{"content":{"body":"plain"},"origin_server_ts":5678}`,
				"$missing": `{"content":{"body":"no timestamp"}}`,
			}
			want := map[string]int64{"$plain": 5678, "$missing": 0}
			if !oldIndex {
				// The old index rejects lone surrogates, so only servers without it have them.
				mixed["$surrogate"] = `{"content":{"body":"\ud800"},"origin_server_ts":91011}`
				want["$surrogate"] = 91011
			}
			for eventID, mixedJSON := range mixed {
				_, err = db.ExecContext(ctx, `INSERT INTO syncapi_output_room_events
					(event_id, room_id, headered_event_json, type, sender, contains_url)
					VALUES ($1, $2, $3, 'm.room.message', $4, false)`, eventID, room.ID, mixedJSON, alice.ID)
				require.NoError(t, err)
			}

			tab, err := NewPostgresEventsTable(ctx, db)
			require.NoError(t, err)
			var count int
			require.NoError(t, db.QueryRowContext(ctx, `SELECT count(*) FROM syncapi_output_room_events WHERE origin_server_ts = 1234 AND headered_event_json = $1`, string(legacyJSON)).Scan(&count))
			require.Equal(t, 1001, count)
			for eventID, ts := range want {
				var got int64
				require.NoError(t, db.QueryRowContext(ctx, `SELECT origin_server_ts FROM syncapi_output_room_events WHERE event_id = $1`, eventID).Scan(&got))
				require.Equal(t, ts, got, eventID)
			}
			// Startup is repeatable, without rebuilding or repeating the backfill.
			_, err = NewPostgresEventsTable(ctx, db)
			require.NoError(t, err)
			_, err = db.ExecContext(ctx, `DELETE FROM syncapi_output_room_events`)
			require.NoError(t, err)

			_, err = tab.InsertEvent(ctx, nil, event, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
			require.NoError(t, tab.UpdateEventJSON(ctx, nil, event))
			for _, backwards := range []bool{false, true} {
				events, err := tab.SelectEventsByTimestamp(ctx, nil, room.ID, 1234, 0, backwards, 10)
				require.NoError(t, err)
				require.Len(t, events, 1)
				require.Equal(t, event.EventID(), events[0].EventID())
				require.JSONEq(t, string(event.Content()), string(events[0].Content()))
			}
		})
	}
}
