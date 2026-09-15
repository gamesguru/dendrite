package sqlutil

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

func TestGuardedConnectionsCarryTheActivationMarker(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	props := &config.DatabaseOptions{ConnectionString: config.DataSource(dsn)}

	db, err := Open(props, NewDummyWriter(), WithConnectionGuard("zendrite:abcd1234:7:ef567890", nil))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var stored string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()").Scan(&stored))
	require.Equal(t, "zendrite:abcd1234:7:ef567890", stored)
}

func TestGuardedOpenRefusesRetiredOwnership(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	props := &config.DatabaseOptions{ConnectionString: config.DataSource(dsn)}
	retired := errors.New("ownership generation is retired")

	_, err := Open(props, NewDummyWriter(), WithConnectionGuard("zendrite:abcd1234:7:ef567890",
		func(context.Context, *pgx.Conn) error { return retired }))
	require.ErrorIs(t, err, retired)
}

// TestGuardedReconnectRefusesRetiredOwnership covers the case that matters for
// a resumed owner: the pool was opened while it still held ownership, and the
// generation was retired afterwards.
func TestGuardedReconnectRefusesRetiredOwnership(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	props := &config.DatabaseOptions{ConnectionString: config.DataSource(dsn)}
	retired := errors.New("ownership generation is retired")
	var lost atomic.Bool

	db, err := Open(props, NewDummyWriter(), WithConnectionGuard("zendrite:abcd1234:7:ef567890",
		func(context.Context, *pgx.Conn) error {
			if lost.Load() {
				return retired
			}
			return nil
		}))
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(t.Context()))

	lost.Store(true)
	// Discard the pooled connections the way a terminated backend would be.
	db.SetMaxIdleConns(0)
	require.Eventually(t, func() bool {
		return errors.Is(db.PingContext(t.Context()), retired)
	}, time.Second, 10*time.Millisecond, "a reconnection must be refused once ownership is retired")
}

func TestUnguardedConnectionsKeepStandaloneBehaviour(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	props := &config.DatabaseOptions{ConnectionString: config.DataSource(dsn)}

	db, err := Open(props, NewDummyWriter())
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })
	require.NoError(t, db.PingContext(t.Context()))
}

func TestConnectionManagerAppliesTheGuard(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	cm := NewConnectionManager(nil, config.DatabaseOptions{ConnectionString: config.DataSource(dsn)},
		WithConnectionGuard("zendrite:abcd1234:7:ef567890", nil))

	db, _, err := cm.Connection(&config.DatabaseOptions{})
	require.NoError(t, err)
	t.Cleanup(func() { _ = db.Close() })

	var stored string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()").Scan(&stored))
	require.Equal(t, "zendrite:abcd1234:7:ef567890", stored)
}
