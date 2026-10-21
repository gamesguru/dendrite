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
