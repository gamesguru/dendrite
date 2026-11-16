// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package deltas

import (
	"context"
	"database/sql"
	"fmt"
)

// UpStateSnapshotsIncomplete records rooms whose stored event snapshots may
// have been created while the room had partial state.
func UpStateSnapshotsIncomplete(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE roomserver_rooms ADD COLUMN IF NOT EXISTS state_snapshots_incomplete BOOLEAN NOT NULL DEFAULT FALSE;`)
	if err != nil {
		return fmt.Errorf("failed to execute upgrade: %w", err)
	}
	_, err = tx.ExecContext(ctx, `UPDATE roomserver_rooms SET state_snapshots_incomplete = TRUE WHERE resync_state_nid <> 0;`)
	if err != nil {
		return fmt.Errorf("failed to mark previously resynced rooms: %w", err)
	}
	return nil
}

func DownStateSnapshotsIncomplete(ctx context.Context, tx *sql.Tx) error {
	_, err := tx.ExecContext(ctx, `ALTER TABLE roomserver_rooms DROP COLUMN IF EXISTS state_snapshots_incomplete;`)
	if err != nil {
		return fmt.Errorf("failed to execute downgrade: %w", err)
	}
	return nil
}
