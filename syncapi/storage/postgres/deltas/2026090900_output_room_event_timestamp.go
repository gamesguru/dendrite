package deltas

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
)

// outputEventTimestampSQL reads the timestamp in PostgreSQL for rows without
// \u escapes. PostgreSQL rejects escaped nulls and lone surrogates that Go
// accepts, so those rows stay NULL and are decoded in Go afterwards. The nested
// CASE guarantees the JSON is never parsed for them.
const outputEventTimestampSQL = `CASE
	WHEN headered_event_json LIKE '%\\u%' THEN NULL
	ELSE (headered_event_json::json ->> 'origin_server_ts')::BIGINT
END`

const (
	outputEventTimestampBatchSize = 1000
	outputEventTimestampLogEvery  = 100000
	outputEventCancelTimeout      = 5 * time.Second
)

// UpAddOutputRoomEventTimestamp replaces the JSON expression index, which cannot
// process otherwise valid events containing escaped nulls, with a numeric column.
// The pool is only used to cancel the running statement when ctx ends.
func UpAddOutputRoomEventTimestamp(ctx context.Context, db *sql.DB, tx *sql.Tx) error {
	logrus.Info("Storing event timestamps for the sync API, this rewrites syncapi_output_room_events once and can take a while on large databases")
	start := time.Now()
	stop, err := cancelBackendOnDone(ctx, db, tx)
	if err != nil {
		return err
	}
	defer stop()
	if _, err = tx.ExecContext(ctx, `DROP INDEX IF EXISTS syncapi_output_room_events_timestamp_idx`); err != nil {
		return err
	}
	if err = fillOutputEventTimestampsInSQL(ctx, tx); err != nil {
		return err
	}
	decoded, err := fillOutputEventTimestampsInGo(ctx, tx)
	if err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `
		ALTER TABLE syncapi_output_room_events ALTER COLUMN origin_server_ts SET NOT NULL;
		CREATE INDEX syncapi_output_room_events_timestamp_idx
		ON syncapi_output_room_events (room_id, origin_server_ts, id);
	`); err != nil {
		return err
	}
	logrus.WithFields(logrus.Fields{
		"duration":      time.Since(start).Round(time.Millisecond),
		"decoded_in_go": decoded,
	}).Info("Stored event timestamps for the sync API")
	return nil
}

// cancelBackendOnDone cancels the transaction's statement on the server when
// ctx ends. Otherwise pgx only closes the connection, and PostgreSQL finishes
// the table rewrite while holding the table lock before rolling it back.
// The returned function waits for a started cancellation, so it is sent before
// the process exits.
func cancelBackendOnDone(ctx context.Context, db *sql.DB, tx *sql.Tx) (func(), error) {
	var pid int
	if err := tx.QueryRowContext(ctx, `SELECT pg_backend_pid()`).Scan(&pid); err != nil {
		return nil, err
	}
	canceled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		defer close(canceled)
		cancelCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), outputEventCancelTimeout)
		defer cancel()
		if _, err := db.ExecContext(cancelCtx, `SELECT pg_cancel_backend($1)`, pid); err != nil {
			logrus.WithError(err).Warn("Could not cancel the event timestamp migration in PostgreSQL")
		}
	})
	return func() {
		if !stop() {
			<-canceled
		}
	}, nil
}

// fillOutputEventTimestampsInSQL adds the column and fills it with a single
// table rewrite. Retyping the column with a USING expression makes PostgreSQL
// rewrite the table and rebuild its indexes in bulk, which writes about half
// as much as updating every row through all of its indexes.
func fillOutputEventTimestampsInSQL(ctx context.Context, tx *sql.Tx) error {
	if _, err := tx.ExecContext(ctx, `SAVEPOINT output_event_timestamps`); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `
		ALTER TABLE syncapi_output_room_events ADD COLUMN origin_server_ts BIGINT;
		ALTER TABLE syncapi_output_room_events ALTER COLUMN origin_server_ts TYPE BIGINT USING `+outputEventTimestampSQL)
	if err == nil {
		_, err = tx.ExecContext(ctx, `RELEASE SAVEPOINT output_event_timestamps`)
		return err
	}
	if ctx.Err() != nil {
		return err
	}
	// Leave every row to the Go decoder, which matches the previous behavior.
	logrus.WithError(err).Warn("Could not read event timestamps in PostgreSQL, decoding every event instead, which is slower")
	if _, err = tx.ExecContext(ctx, `ROLLBACK TO SAVEPOINT output_event_timestamps`); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `ALTER TABLE syncapi_output_room_events ADD COLUMN origin_server_ts BIGINT`)
	return err
}

// fillOutputEventTimestampsInGo decodes the rows PostgreSQL left NULL, and
// returns how many it updated.
func fillOutputEventTimestampsInGo(ctx context.Context, tx *sql.Tx) (int, error) {
	var afterID int64
	total, nextLog := 0, outputEventTimestampLogEvery
	for {
		// Bound memory and close each result set before writing on the transaction.
		rows, err := tx.QueryContext(ctx, `SELECT id, headered_event_json
			FROM syncapi_output_room_events WHERE origin_server_ts IS NULL AND id > $1
			ORDER BY id LIMIT $2`, afterID, outputEventTimestampBatchSize)
		if err != nil {
			return total, err
		}
		ids, timestamps, err := outputEventTimestamps(rows)
		if err != nil {
			return total, err
		}
		if len(ids) == 0 {
			return total, nil
		}
		if _, err = tx.ExecContext(ctx, `UPDATE syncapi_output_room_events AS events
			SET origin_server_ts = batch.ts
			FROM unnest($1::bigint[], $2::bigint[]) AS batch(id, ts)
			WHERE events.id = batch.id`, sqlutil.Int64Array(ids), sqlutil.Int64Array(timestamps)); err != nil {
			return total, err
		}
		afterID = ids[len(ids)-1]
		total += len(ids)
		if total >= nextLog {
			logrus.WithField("events", total).Info("Still storing event timestamps for the sync API")
			nextLog += outputEventTimestampLogEvery
		}
	}
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
