package sqlutil

import (
	"context"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

// SQLiteRetryPolicy bounds a lock-contention retry loop.
type SQLiteRetryPolicy struct {
	MaxAttempts int
	DelayStep   time.Duration
	MaxDelay    time.Duration
}

const (
	sqliteMaxRetryAttempts = 10
	sqliteRetryDelayStep   = 25 * time.Millisecond
	sqliteMaxRetryDelay    = 250 * time.Millisecond
)

// SQLiteLockRetryPolicy waits out a normal room-creation or backfill write
// burst while staying bounded for callers that cannot be blocked indefinitely.
var SQLiteLockRetryPolicy = SQLiteRetryPolicy{
	MaxAttempts: sqliteMaxRetryAttempts,
	DelayStep:   sqliteRetryDelayStep,
	MaxDelay:    sqliteMaxRetryDelay,
}

// RetrySQLiteBusy runs fn, retrying with a bounded backoff while it fails with
// SQLITE_BUSY. Read-then-write transactions can fail to upgrade their lock
// immediately even though the SQLite busy timeout is set, so the whole
// transaction and its lookup must be retried.
func RetrySQLiteBusy(ctx context.Context, policy SQLiteRetryPolicy, fn func() error) error {
	var err error
	for attempt := 0; attempt < policy.MaxAttempts; attempt++ {
		err = fn()
		if err == nil || !IsSQLiteBusy(err) || attempt == policy.MaxAttempts-1 {
			return err
		}
		delay := time.Duration(attempt+1) * policy.DelayStep
		if policy.MaxDelay > 0 && delay > policy.MaxDelay {
			delay = policy.MaxDelay
		}
		logrus.WithError(err).WithField("attempt", attempt+1).Debug("SQLite database busy, retrying")
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	return err
}

// IsSQLiteBusy reports whether err is SQLite's lock-contention error. It is
// matched by text because the pure-Go and cgo drivers expose different error
// types and the build only links one of them.
func IsSQLiteBusy(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "SQLITE_BUSY") || strings.Contains(msg, "database is locked")
}
