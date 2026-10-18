//go:build !wasm

package sqlutil

import (
	"context"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"net"
	"net/url"
	"syscall"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
)

func TestIsTransient(t *testing.T) {
	// Nothing listens on port 1, which is what a promoted primary's address
	// looks like before the service moves to it.
	_, connectErr := pgx.Connect(t.Context(), "postgres://zendrite@127.0.0.1:1/zendrite?sslmode=disable&connect_timeout=1")
	require.Error(t, connectErr)

	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"connection refused":     {fmt.Errorf("sqlutil.WithTransaction.Begin: %w", connectErr), true},
		"bad connection":         {driver.ErrBadConn, true},
		"connection exception":   {&pgconn.PgError{Code: "08006"}, true},
		"administrator shutdown": {&pgconn.PgError{Code: "57P01"}, true},
		"unique violation":       {&pgconn.PgError{Code: "23505"}, false},
		"cancellation":           {context.Canceled, false},
		"application error":      {errors.New("cannot update the room state using the deltas"), false},
		"no error":               {nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, IsTransient(tc.err))
		})
	}
}

func TestIsDatabaseUnavailable(t *testing.T) {
	_, connectErr := pgx.Connect(t.Context(), "postgres://zendrite@127.0.0.1:1/zendrite?sslmode=disable&connect_timeout=1")
	require.Error(t, connectErr)
	// A remote server that is down fails an HTTP request with the same
	// network errors a database connection would.
	httpErr := &url.Error{Op: "Get", URL: "https://remote.test/_matrix/federation/v1/get_missing_events", Err: &net.OpError{
		Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED,
	}}

	for name, tc := range map[string]struct {
		err  error
		want bool
	}{
		"connection refused":     {fmt.Errorf("r.DB.RoomInfo: %w", connectErr), true},
		"bad connection":         {driver.ErrBadConn, true},
		"administrator shutdown": {fmt.Errorf("fast forward: %w", &pgconn.PgError{Code: "57P01"}), true},
		"unique violation":       {&pgconn.PgError{Code: "23505"}, false},
		"remote server down":     {fmt.Errorf("LookupMissingEvents: %w", httpErr), false},
		"remote response cut":    {fmt.Errorf("LookupMissingEvents: %w", io.ErrUnexpectedEOF), false},
		"cancellation":           {context.Canceled, false},
		"no error":               {nil, false},
	} {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, tc.want, IsDatabaseUnavailable(tc.err))
		})
	}
	require.True(t, IsTransient(httpErr), "IsTransient does not tell a remote server from the database")
}

func TestRetryTransient(t *testing.T) {
	calls := 0
	err := RetryTransient(t.Context(), "write", func() error {
		calls++
		if calls <= 2 {
			return driver.ErrBadConn
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, calls, "a transient failure must be retried")

	calls = 0
	rejected := errors.New("cannot update the room state using the deltas")
	err = RetryTransient(t.Context(), "write", func() error {
		calls++
		return rejected
	})
	require.ErrorIs(t, err, rejected)
	require.Equal(t, 1, calls, "a rejected write must not be retried")
}
