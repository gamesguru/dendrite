package sqlutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errDatabaseLocked = errors.New("database is locked")

func TestRetrySQLiteBusyRetriesLockContention(t *testing.T) {
	policy := SQLiteRetryPolicy{MaxAttempts: 5, DelayStep: time.Millisecond, MaxDelay: 2 * time.Millisecond}
	calls := 0
	err := RetrySQLiteBusy(context.Background(), policy, func() error {
		calls++
		if calls < 3 {
			return errDatabaseLocked
		}
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 3, calls, "the call is retried until it succeeds")
}

func TestRetrySQLiteBusyStopsOnOtherErrors(t *testing.T) {
	policy := SQLiteRetryPolicy{MaxAttempts: 5, DelayStep: time.Millisecond}
	other := errors.New("constraint failed")
	calls := 0
	err := RetrySQLiteBusy(context.Background(), policy, func() error {
		calls++
		return other
	})
	require.ErrorIs(t, err, other)
	assert.Equal(t, 1, calls, "a non-busy error is not retried")
}

func TestRetrySQLiteBusyReturnsLastBusyError(t *testing.T) {
	policy := SQLiteRetryPolicy{MaxAttempts: 3, DelayStep: time.Millisecond}
	calls := 0
	err := RetrySQLiteBusy(context.Background(), policy, func() error {
		calls++
		return errors.New("SQLITE_BUSY: database is locked")
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "SQLITE_BUSY")
	assert.Equal(t, policy.MaxAttempts, calls, "the attempt bound is respected")
}

func TestRetrySQLiteBusyStopsOnContextCancellation(t *testing.T) {
	policy := SQLiteRetryPolicy{MaxAttempts: 10, DelayStep: time.Hour, MaxDelay: time.Hour}
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(context.Canceled)
	calls := 0
	err := RetrySQLiteBusy(ctx, policy, func() error {
		calls++
		return errDatabaseLocked
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls, "cancellation during the backoff stops retrying")
}

func TestIsSQLiteBusy(t *testing.T) {
	assert.False(t, IsSQLiteBusy(nil))
	assert.False(t, IsSQLiteBusy(errors.New("constraint failed")))
	assert.True(t, IsSQLiteBusy(errDatabaseLocked))
	assert.True(t, IsSQLiteBusy(errors.New("SQLITE_BUSY (5)")))
}

func TestRetrySQLiteBusyRunsOnceWithZeroPolicy(t *testing.T) {
	calls := 0
	err := RetrySQLiteBusy(context.Background(), SQLiteRetryPolicy{}, func() error {
		calls++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 1, calls, "a zero-value policy must not skip the call")
}
