// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package deltas

import (
	"context"
	"database/sql"
	"fmt"
)

// UpAddLocalDepth adds the locally computed topological depth. -1 marks events
// whose depth has not been computed yet.
func UpAddLocalDepth(ctx context.Context, tx *sql.Tx) error {
	// SQLite doesn't support IF NOT EXISTS for ADD COLUMN, so check first.
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('roomserver_events') WHERE name = 'local_depth'`).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check column existence: %w", err)
	}
	if count == 0 {
		if _, err = tx.ExecContext(ctx, `ALTER TABLE roomserver_events ADD COLUMN local_depth INTEGER NOT NULL DEFAULT -1;`); err != nil {
			return fmt.Errorf("failed to execute upgrade: %w", err)
		}
	}
	return nil
}

func DownAddLocalDepth(ctx context.Context, tx *sql.Tx) error {
	// SQLite can't drop columns on older versions; leave it.
	return nil
}
