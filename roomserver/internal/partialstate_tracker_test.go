// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAwaitUnPartialState(t *testing.T) {
	const roomID = "!room:test"
	partial := func(v bool) func(context.Context) (bool, error) {
		return func(context.Context) (bool, error) { return v, nil }
	}

	t.Run("returns at once when the room is not partial", func(t *testing.T) {
		tracker := NewPartialStateTracker()
		require.NoError(t, tracker.AwaitUnPartialState(context.Background(), roomID, partial(false)))
		assert.False(t, tracker.HasObservers(roomID), "the observer is removed")
	})

	t.Run("waits for the resync to finish", func(t *testing.T) {
		tracker := NewPartialStateTracker()
		done := make(chan error, 1)
		go func() { done <- tracker.AwaitUnPartialState(context.Background(), roomID, partial(true)) }()

		require.Eventually(t, func() bool { return tracker.HasObservers(roomID) }, time.Second, time.Millisecond)
		select {
		case err := <-done:
			t.Fatalf("returned while the room was still partial: %v", err)
		case <-time.After(20 * time.Millisecond):
		}
		tracker.NotifyUnPartialStated(roomID)
		require.NoError(t, <-done)
	})

	t.Run("a resync that finishes during the check is not missed", func(t *testing.T) {
		tracker := NewPartialStateTracker()
		// The room is reported partial, but it finishes before the wait begins.
		finishesDuringCheck := func(context.Context) (bool, error) {
			tracker.NotifyUnPartialStated(roomID)
			return true, nil
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		require.NoError(t, tracker.AwaitUnPartialState(ctx, roomID, finishesDuringCheck))
	})

	t.Run("gives up when the context ends", func(t *testing.T) {
		tracker := NewPartialStateTracker()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, tracker.AwaitUnPartialState(ctx, roomID, partial(true)), context.DeadlineExceeded)
		assert.False(t, tracker.HasObservers(roomID))
	})
}

// Clearing the partial flag is not the end of a resync for a request that reads the
// sync API's copy of the room: it must wait until that has caught up.
func TestAwaitUnPartialStateHoldsForTheSyncAPI(t *testing.T) {
	const roomID = "!room:test"
	notPartial := func(context.Context) (bool, error) { return false, nil }

	t.Run("waits after the flag is cleared until the sync API acknowledges", func(t *testing.T) {
		tracker := NewPartialStateTracker()
		tracker.MarkSyncPending(roomID, time.Minute)

		done := make(chan error, 1)
		go func() { done <- tracker.AwaitUnPartialState(context.Background(), roomID, notPartial) }()
		require.Eventually(t, func() bool { return tracker.HasObservers(roomID) }, time.Second, time.Millisecond)
		select {
		case err := <-done:
			t.Fatalf("returned before the sync API caught up: %v", err)
		case <-time.After(20 * time.Millisecond):
		}

		tracker.NotifyUnPartialStated(roomID)
		require.NoError(t, <-done)
		assert.False(t, tracker.isSyncPending(roomID), "the acknowledgement ends the hold")
	})

	t.Run("a sync API that never answers does not hold requests for good", func(t *testing.T) {
		now := time.Unix(1000, 0)
		tracker := NewPartialStateTracker()
		tracker.now = func() time.Time { return now }
		tracker.MarkSyncPending(roomID, 5*time.Second)
		assert.True(t, tracker.isSyncPending(roomID))

		now = now.Add(5 * time.Second)
		assert.False(t, tracker.isSyncPending(roomID), "the hold expires")
		require.NoError(t, tracker.AwaitUnPartialState(context.Background(), roomID, notPartial))
	})

	t.Run("a room that never resynced is not held", func(t *testing.T) {
		tracker := NewPartialStateTracker()
		require.NoError(t, tracker.AwaitUnPartialState(context.Background(), roomID, notPartial))
	})
}
