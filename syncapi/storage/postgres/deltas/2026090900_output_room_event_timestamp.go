package deltas

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
)

// UpAddOutputRoomEventTimestamp replaces the JSON expression index, which cannot
// process otherwise valid events containing escaped nulls, with a numeric column.
func UpAddOutputRoomEventTimestamp(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `
		DROP INDEX IF EXISTS syncapi_output_room_events_timestamp_idx;
		ALTER TABLE syncapi_output_room_events ADD COLUMN origin_server_ts BIGINT;
	`); err != nil {
		return err
	}

	// Bound memory and close each result set before writing on the transaction.
	const batchSize = 1000
	var afterID int64
	for {
		rows, err := tx.QueryContext(ctx, `SELECT id, headered_event_json
			FROM syncapi_output_room_events WHERE id > $1 ORDER BY id LIMIT $2`, afterID, batchSize)
		if err != nil {
			return err
		}
		ids, timestamps, err := outputEventTimestamps(rows)
		if err != nil {
			return err
		}
		if len(ids) == 0 {
			break
		}
		if _, err = tx.ExecContext(ctx, `UPDATE syncapi_output_room_events AS events
			SET origin_server_ts = batch.ts
			FROM unnest($1::bigint[], $2::bigint[]) AS batch(id, ts)
			WHERE events.id = batch.id`, sqlutil.Int64Array(ids), sqlutil.Int64Array(timestamps)); err != nil {
			return err
		}
		afterID = ids[len(ids)-1]
	}
	_, err := tx.ExecContext(ctx, `
		ALTER TABLE syncapi_output_room_events ALTER COLUMN origin_server_ts SET NOT NULL;
		CREATE INDEX syncapi_output_room_events_timestamp_idx
		ON syncapi_output_room_events (room_id, origin_server_ts, id);
	`)
	return err
}

func outputEventTimestamps(rows *sql.Rows) (ids, timestamps []int64, err error) {
	defer rows.Close()
	for rows.Next() {
		var id int64
		var eventJSON []byte
		if err = rows.Scan(&id, &eventJSON); err != nil {
			return nil, nil, err
		}
		var event struct {
			Timestamp int64 `json:"origin_server_ts"`
		}
		if err = json.Unmarshal(eventJSON, &event); err != nil {
			return nil, nil, fmt.Errorf("decode timestamp for output event %d: %w", id, err)
		}
		ids = append(ids, id)
		timestamps = append(timestamps, event.Timestamp)
	}
	return ids, timestamps, rows.Err()
}
