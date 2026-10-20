package deltas

import (
	"context"
	"database/sql"
)

// UpThreadedReceipts preserves independent read and delivery positions per thread.
func UpThreadedReceipts(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE syncapi_receipts_threaded (
 id BIGINT, room_id TEXT NOT NULL, receipt_type TEXT NOT NULL, user_id TEXT NOT NULL,
 event_id TEXT NOT NULL, receipt_ts BIGINT NOT NULL, thread_id TEXT NOT NULL DEFAULT '',
 UNIQUE(room_id, receipt_type, user_id, thread_id));
INSERT INTO syncapi_receipts_threaded SELECT *, '' FROM syncapi_receipts;
DROP TABLE syncapi_receipts;
ALTER TABLE syncapi_receipts_threaded RENAME TO syncapi_receipts;
CREATE INDEX syncapi_receipts_room_id_idx ON syncapi_receipts(room_id);
CREATE TABLE syncapi_connection_receipts_threaded (
 connection_key INTEGER NOT NULL REFERENCES syncapi_sliding_sync_connections(connection_key) ON DELETE CASCADE,
 room_id TEXT NOT NULL, receipt_type TEXT NOT NULL, user_id TEXT NOT NULL,
 last_delivered_event_id TEXT NOT NULL, last_delivered_ts INTEGER NOT NULL, thread_id TEXT NOT NULL DEFAULT '',
 PRIMARY KEY(connection_key, room_id, receipt_type, user_id, thread_id));
INSERT INTO syncapi_connection_receipts_threaded SELECT *, '' FROM syncapi_sliding_sync_connection_receipts;
DROP TABLE syncapi_sliding_sync_connection_receipts;
ALTER TABLE syncapi_connection_receipts_threaded RENAME TO syncapi_sliding_sync_connection_receipts;
CREATE INDEX syncapi_sliding_sync_connection_receipts_conn_idx ON syncapi_sliding_sync_connection_receipts(connection_key);
CREATE INDEX syncapi_sliding_sync_connection_receipts_room_idx ON syncapi_sliding_sync_connection_receipts(room_id);
`)
	return err
}
