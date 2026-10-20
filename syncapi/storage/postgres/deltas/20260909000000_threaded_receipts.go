package deltas

import (
	"context"
	"database/sql"
)

// UpThreadedReceipts preserves independent read and delivery positions per thread.
func UpThreadedReceipts(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
ALTER TABLE syncapi_receipts ADD COLUMN thread_id TEXT NOT NULL DEFAULT '';
ALTER TABLE syncapi_receipts DROP CONSTRAINT syncapi_receipts_unique;
ALTER TABLE syncapi_receipts ADD CONSTRAINT syncapi_receipts_unique UNIQUE (room_id, receipt_type, user_id, thread_id);
ALTER TABLE syncapi_sliding_sync_connection_receipts ADD COLUMN thread_id TEXT NOT NULL DEFAULT '';
ALTER TABLE syncapi_sliding_sync_connection_receipts DROP CONSTRAINT syncapi_sliding_sync_connection_receipts_pkey;
ALTER TABLE syncapi_sliding_sync_connection_receipts ADD PRIMARY KEY (connection_key, room_id, receipt_type, user_id, thread_id);
`)
	return err
}
