// Copyright 2026 The Zendrite Authors
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
	var columnExists bool
	err := tx.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.columns
			WHERE table_schema = current_schema()
			  AND table_name = 'roomserver_rooms'
			  AND column_name = 'state_snapshots_incomplete'
	)`).Scan(&columnExists)
	if err != nil {
		return fmt.Errorf("failed to check column existence: %w", err)
	}
	_, err = tx.ExecContext(ctx, `ALTER TABLE roomserver_rooms ADD COLUMN IF NOT EXISTS state_snapshots_incomplete BOOLEAN NOT NULL DEFAULT FALSE;`)
	if err != nil {
		return fmt.Errorf("failed to execute upgrade: %w", err)
	}
	if !columnExists {
		// The pre-marker data cannot distinguish rooms whose partial-state join
		// was abandoned (which clears the partial flag without a resync marker)
		// from rooms that were always complete. Conservatively distrust every
		// existing room snapshot; rooms created after this migration keep FALSE.
		_, err = tx.ExecContext(ctx, `UPDATE roomserver_rooms SET state_snapshots_incomplete = TRUE;`)
		if err != nil {
			return fmt.Errorf("failed to mark existing room snapshots incomplete: %w", err)
		}
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
