// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
)

// shortInputRetries makes the retry budget fit a test.
func shortInputRetries(t *testing.T, budget time.Duration) {
	t.Helper()
	base, maxDelay, oldBudget := inputRetryBaseDelay, inputRetryMaxDelay, inputRetryBudget
	inputRetryBaseDelay, inputRetryMaxDelay, inputRetryBudget = time.Millisecond, 20*time.Millisecond, budget
	t.Cleanup(func() { inputRetryBaseDelay, inputRetryMaxDelay, inputRetryBudget = base, maxDelay, oldBudget })
}

type fakeOwnership struct {
	valid   atomic.Bool
	retired atomic.Bool
}

func (*fakeOwnership) Generation() int64 { return 1 }
func (*fakeOwnership) Identity() string  { return "test" }
func (o *fakeOwnership) Retired() bool   { return o.retired.Load() }
func (o *fakeOwnership) Valid() error {
	if o.valid.Load() {
		return nil
	}
	return errors.New("ownership not confirmed")
}

var (
	errRefused = fmt.Errorf("u.api.WriteOutputEvents: %w: ownership has not been established", jetstream.ErrPublicationRefused)
	// A query cut off by a PostgreSQL failover fails with the plain network
	// error, as in #426.
	errConnectionReset = fmt.Errorf("r.DB.RoomInfo: %w", &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET})
)

func TestRetryWhileUnavailable(t *testing.T) {
	shortInputRetries(t, time.Minute)
	logger := logrus.NewEntry(logrus.StandardLogger())

	calls := 0
	err := retryWhileUnavailable(t.Context(), nil, logger, func(int) error {
		calls++
		if calls <= 2 {
			return errConnectionReset
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, calls, "a dropped database connection is retried")

	for name, failure := range map[string]error{
		"rejection":         types.RejectedError("state resolution cooldown active"),
		"application error": errors.New("room !a:test does not exist for event $b"),
	} {
		calls = 0
		err = retryWhileUnavailable(t.Context(), nil, logger, func(int) error { calls++; return failure })
		require.ErrorIs(t, err, failure)
		require.Equal(t, 1, calls, "%s is retried", name)
	}
}

func TestRetryWhileUnavailableIsBounded(t *testing.T) {
	shortInputRetries(t, 200*time.Millisecond)
	started := time.Now()
	calls := 0
	err := retryWhileUnavailable(t.Context(), nil, logrus.NewEntry(logrus.StandardLogger()), func(int) error {
		calls++
		return errRefused
	})
	require.ErrorIs(t, err, jetstream.ErrPublicationRefused)
	require.Greater(t, calls, 1)
	require.Less(t, time.Since(started), 5*time.Second, "a permanently failing input is given up")
}

func TestRetryWhileUnavailableStopsWithoutOwnershipOrServer(t *testing.T) {
	shortInputRetries(t, time.Minute)
	logger := logrus.NewEntry(logrus.StandardLogger())

	// A retired activation leaves the input to its successor.
	owner := &fakeOwnership{}
	owner.retired.Store(true)
	calls := 0
	err := retryWhileUnavailable(t.Context(), owner, logger, func(int) error { calls++; return errRefused })
	require.ErrorIs(t, err, jetstream.ErrPublicationRefused)
	require.Equal(t, 1, calls, "a retired activation retried")

	// A stopping server leaves the input for after the restart.
	ctx, cancel := context.WithCancelCause(t.Context())
	calls = 0
	err = retryWhileUnavailable(ctx, nil, logger, func(int) error { calls++; cancel(errors.New("shutting down")); return errConnectionReset })
	require.Error(t, err)
	require.Equal(t, 1, calls, "a stopping server retried")

	// Ownership that is only unconfirmed, as while the database fails over,
	// is waited for, and the input is processed once it is confirmed again.
	owner = &fakeOwnership{}
	calls = 0
	err = retryWhileUnavailable(t.Context(), owner, logger, func(int) error {
		calls++
		if owner.valid.Load() {
			return nil
		}
		time.AfterFunc(600*time.Millisecond, func() { owner.valid.Store(true) })
		return errRefused
	})
	require.NoError(t, err)
	require.Equal(t, 2, calls, "the input is retried once, after ownership is confirmed")
}
