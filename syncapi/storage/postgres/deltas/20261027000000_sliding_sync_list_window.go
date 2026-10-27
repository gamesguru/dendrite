// Copyright 2026 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package deltas

import (
	"context"
	"database/sql"
)

// UpCreateSlidingSyncListWindows creates storage for list windows at connection
// positions if it does not exist, returning any SQL execution error. Deleting a
// referenced connection or position also deletes its windows.
func UpCreateSlidingSyncListWindows(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS syncapi_sliding_sync_connection_list_windows (
    connection_key BIGINT NOT NULL REFERENCES syncapi_sliding_sync_connections(connection_key) ON DELETE CASCADE,
    list_name TEXT NOT NULL,
    connection_position BIGINT NOT NULL REFERENCES syncapi_sliding_sync_connection_positions(connection_position) ON DELETE CASCADE,
    room_ids TEXT NOT NULL,
    PRIMARY KEY (connection_key, list_name, connection_position)
);
`)
	return err
}
