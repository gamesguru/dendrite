// Copyright 2026 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package deltas

import (
	"context"
	"database/sql"
)

// UpEnsureConnectionReceiptCleanup emulates the foreign-key cascades for
// connection receipts and list windows because SQLite foreign-key enforcement
// is not enabled for these connections. The triggers also cover rows and
// tables created before this migration, which the earlier migrations cannot
// change.
func UpEnsureConnectionReceiptCleanup(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
CREATE TRIGGER IF NOT EXISTS syncapi_sliding_sync_connection_receipts_delete
AFTER DELETE ON syncapi_sliding_sync_connections
BEGIN
    DELETE FROM syncapi_sliding_sync_connection_receipts
    WHERE connection_key = OLD.connection_key;
END;
CREATE TRIGGER IF NOT EXISTS syncapi_sliding_sync_list_windows_conn_delete
AFTER DELETE ON syncapi_sliding_sync_connections
BEGIN
    DELETE FROM syncapi_sliding_sync_connection_list_windows
    WHERE connection_key = OLD.connection_key;
END;
CREATE TRIGGER IF NOT EXISTS syncapi_sliding_sync_list_windows_pos_delete
AFTER DELETE ON syncapi_sliding_sync_connection_positions
BEGIN
    DELETE FROM syncapi_sliding_sync_connection_list_windows
    WHERE connection_position = OLD.connection_position;
END;
`)
	return err
}
