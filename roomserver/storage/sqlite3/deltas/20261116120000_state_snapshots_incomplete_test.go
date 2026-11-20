package deltas

import (
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

func TestUpStateSnapshotsIncompleteMarksLegacyRoomsOnce(t *testing.T) {
	cfg, ctx, close := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer close()
	db, err := sqlutil.Open(&cfg.RoomServer.Database, sqlutil.NewExclusiveWriter())
	require.NoError(t, err)
	defer func() {
		if err := db.Close(); err != nil {
			t.Errorf("failed to close test database: %v", err)
		}
	}()

	_, err = db.ExecContext(ctx.Context(), `
		CREATE TABLE roomserver_rooms (
			room_nid INTEGER PRIMARY KEY,
			resync_state_nid INTEGER NOT NULL DEFAULT 0
		);
		INSERT INTO roomserver_rooms (room_nid, resync_state_nid) VALUES (1, 0), (2, 7);
	`)
	require.NoError(t, err)

	txn, err := db.BeginTx(ctx.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, UpStateSnapshotsIncomplete(ctx.Context(), txn))
	require.NoError(t, txn.Commit())

	var marked int
	require.NoError(t, db.QueryRowContext(ctx.Context(), `SELECT count(*) FROM roomserver_rooms WHERE state_snapshots_incomplete`).Scan(&marked))
	require.Equal(t, 2, marked, "legacy rows with no resync marker must also be treated as potentially partial")

	// Re-running an idempotent delta must not mark rooms created after migration.
	_, err = db.ExecContext(ctx.Context(), `INSERT INTO roomserver_rooms (room_nid, resync_state_nid) VALUES (3, 0)`)
	require.NoError(t, err)
	txn, err = db.BeginTx(ctx.Context(), nil)
	require.NoError(t, err)
	require.NoError(t, UpStateSnapshotsIncomplete(ctx.Context(), txn))
	require.NoError(t, txn.Commit())

	var incomplete bool
	require.NoError(t, db.QueryRowContext(ctx.Context(), `SELECT state_snapshots_incomplete FROM roomserver_rooms WHERE room_nid = 3`).Scan(&incomplete))
	require.False(t, incomplete)
}
