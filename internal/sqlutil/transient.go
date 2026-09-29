//go:build !wasm

package sqlutil

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"net"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"
)

// IsTransient reports whether err says only that the database could not be
// reached or dropped the connection, as it does during a PostgreSQL failover,
// rather than that it rejected the statement. Work that must be neither lost
// nor reordered can retry such an error in place instead of failing.
//
// Cancellation is not transient: it means the caller is giving up.
func IsTransient(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || errors.Is(err, net.ErrClosed) {
		return true
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) || pgconn.SafeToRetry(err) {
		return true
	}
	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		return isConnectionCode(pgErr.Code)
	}
	var netErr net.Error
	return errors.As(err, &netErr)
}

// IsDatabaseUnavailable is IsTransient restricted to errors the PostgreSQL
// driver reports itself. Code that also talks to other servers needs it: a
// plain network error or EOF there may come from a remote server's HTTP
// response as easily as from the database.
func IsDatabaseUnavailable(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	if errors.Is(err, driver.ErrBadConn) {
		return true
	}
	var connectErr *pgconn.ConnectError
	if errors.As(err, &connectErr) || pgconn.SafeToRetry(err) {
		return true
	}
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && isConnectionCode(pgErr.Code)
}

// isConnectionCode reports SQLSTATE codes that describe the connection rather
// than the statement: class 08 is a connection exception, and 57P01 to 57P03
// are the server shutting down, crashing, or not yet accepting connections.
func isConnectionCode(code string) bool {
	return strings.HasPrefix(code, "08") || code == "57P01" || code == "57P02" || code == "57P03"
}

const (
	retryTransientBaseDelay = 250 * time.Millisecond
	retryTransientMaxDelay  = 5 * time.Second
)

// RetryTransient runs fn until it succeeds, fails with an error that is not
// transient, or ctx ends, and returns its last error.
//
// It is for work that must be neither lost nor reordered, such as applying a
// room event whose successors depend on it: refusing the work would let later
// work overtake it, and crashing the process to force a retry costs an HA
// leadership change.
func RetryTransient(ctx context.Context, what string, fn func() error) error {
	delay := retryTransientBaseDelay
	for {
		err := fn()
		if !IsTransient(err) || ctx.Err() != nil {
			return err
		}
		logrus.WithError(err).Warnf("%s failed while the database was unavailable; retrying", what)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
		delay = min(delay+delay, retryTransientMaxDelay)
	}
}
