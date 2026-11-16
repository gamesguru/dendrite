package deltas

import (
	"context"
	"database/sql"
	"fmt"
)

// UpStateSnapshotsIncomplete records rooms whose stored event snapshots may
// have been created while the room had partial state.
func UpStateSnapshotsIncomplete(ctx context.Context, tx *sql.Tx) error {
	var count int
	err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM pragma_table_info('roomserver_rooms') WHERE name = 'state_snapshots_incomplete'`).Scan(&count)
	if err != nil {
		return fmt.Errorf("failed to check column existence: %w", err)
	}
	if count == 0 {
		_, err = tx.ExecContext(ctx, `ALTER TABLE roomserver_rooms ADD COLUMN state_snapshots_incomplete BOOLEAN NOT NULL DEFAULT FALSE;`)
		if err != nil {
			return fmt.Errorf("failed to execute upgrade: %w", err)
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE roomserver_rooms SET state_snapshots_incomplete = TRUE WHERE resync_state_nid <> 0;`)
	if err != nil {
		return fmt.Errorf("failed to mark previously resynced rooms: %w", err)
	}
	return nil
}

func DownStateSnapshotsIncomplete(context.Context, *sql.Tx) error {
	// SQLite's table rebuild is intentionally avoided for downgrade paths.
	return nil
}
