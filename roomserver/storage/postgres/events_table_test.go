// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

func openEventsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	connStr, closeDB := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(closeDB)
	db, err := sqlutil.Open(&config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}, sqlutil.NewExclusiveWriter())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func TestCreateRoomEventNIDIndexRecoversInvalidIndex(t *testing.T) {
	db := openEventsTestDB(t)
	ctx := context.Background()
	_, err := db.ExecContext(ctx, eventsSchema)
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, `INSERT INTO roomserver_events (room_nid, event_type_nid, event_state_key_nid, depth, event_id, auth_event_nids)
VALUES (0, 1, 0, 1, '$invalid-index-test', '{}')`)
	require.NoError(t, err)

	// Division by zero aborts a concurrent index build but leaves its index
	// catalog entry behind with indisvalid=false.
	_, err = db.ExecContext(ctx, `CREATE INDEX CONCURRENTLY roomserver_events_room_event_nid_idx
ON roomserver_events ((1 / room_nid))`)
	require.Error(t, err)
	var valid bool
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('roomserver_events_room_event_nid_idx')`,
	).Scan(&valid))
	require.False(t, valid)

	require.NoError(t, createRoomEventNIDIndex(ctx, db))
	require.NoError(t, db.QueryRowContext(ctx,
		`SELECT indisvalid FROM pg_index WHERE indexrelid = to_regclass('roomserver_events_room_event_nid_idx')`,
	).Scan(&valid))
	require.True(t, valid)
}

func TestCreateRoomEventNIDIndexLockCanBeCanceled(t *testing.T) {
	db := openEventsTestDB(t)
	lockConn, err := db.Conn(context.Background())
	require.NoError(t, err)
	defer func() { _ = lockConn.Close() }()
	_, err = lockConn.ExecContext(context.Background(), "SELECT pg_advisory_lock(351, 1)")
	require.NoError(t, err)
	defer func() {
		_, _ = lockConn.ExecContext(context.Background(), "SELECT pg_advisory_unlock(351, 1)")
	}()

	ctx, cancel := context.WithCancelCause(context.Background())
	started := make(chan error, 1)
	go func() { started <- createRoomEventNIDIndex(ctx, db) }()
	// Wait until the initializer is actually polling for the lock: a session
	// other than ours whose last statement is its pg_try_advisory_lock call.
	require.Eventually(t, func() bool {
		var n int
		err := db.QueryRowContext(context.Background(),
			`SELECT count(*) FROM pg_stat_activity
WHERE datname = current_database() AND pid <> pg_backend_pid()
AND query LIKE '%pg_try_advisory_lock(351, 1)%'`).Scan(&n)
		return err == nil && n > 0
	}, 5*time.Second, 10*time.Millisecond, "initializer never started waiting for the lock")
	select {
	case err = <-started:
		t.Fatalf("index initialization returned while the lock was held: %v", err)
	default:
	}
	cancel(nil)
	select {
	case err = <-started:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(3 * time.Second):
		t.Fatal("index initialization did not stop after its context was canceled")
	}
}
