// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestPurgeTracker_BeginPurge_FirstCallerOwns(t *testing.T) {
	pt := NewPurgeTracker()
	done, owner := pt.BeginPurge("!room1")
	if !owner {
		t.Fatalf("first caller should be owner")
	}
	if done == nil {
		t.Fatalf("first caller should get a non-nil channel")
	}
}

func TestPurgeTracker_BeginPurge_SecondCallerCoalesces(t *testing.T) {
	pt := NewPurgeTracker()
	first, _ := pt.BeginPurge("!room1")
	second, owner := pt.BeginPurge("!room1")
	if owner {
		t.Fatalf("second caller should not be owner")
	}
	if first != second {
		t.Fatalf("second caller should share the first caller's channel")
	}
}

func TestPurgeTracker_FinishPurge_ClosesChannelAndAllowsRestart(t *testing.T) {
	pt := NewPurgeTracker()
	done, _ := pt.BeginPurge("!room1")
	pt.FinishPurge("!room1")
	select {
	case <-done:
		// good: channel was closed
	default:
		t.Fatalf("FinishPurge should close the channel")
	}
	_, owner := pt.BeginPurge("!room1")
	if !owner {
		t.Fatalf("after FinishPurge a new BeginPurge should own the slot")
	}
}

func TestPurgeTracker_WaitFor_NotInFlight_ReturnsImmediately(t *testing.T) {
	pt := NewPurgeTracker()
	if err := pt.WaitFor(context.Background(), "!room1", time.Second); err != nil {
		t.Fatalf("WaitFor on unknown room should return nil; got %v", err)
	}
}

func TestPurgeTracker_WaitFor_InFlight_UnblocksOnFinish(t *testing.T) {
	pt := NewPurgeTracker()
	pt.BeginPurge("!room1")
	go func() {
		time.Sleep(20 * time.Millisecond)
		pt.FinishPurge("!room1")
	}()
	if err := pt.WaitFor(context.Background(), "!room1", time.Second); err != nil {
		t.Fatalf("WaitFor should unblock on FinishPurge; got %v", err)
	}
}

func TestPurgeTracker_WaitFor_Timeout(t *testing.T) {
	pt := NewPurgeTracker()
	pt.BeginPurge("!room1")
	defer pt.FinishPurge("!room1")
	err := pt.WaitFor(context.Background(), "!room1", 20*time.Millisecond)
	if !errors.Is(err, ErrPurgeInProgress) {
		t.Fatalf("WaitFor should return ErrPurgeInProgress on timeout; got %v", err)
	}
}

func TestPurgeTracker_WaitFor_ContextCanceled(t *testing.T) {
	pt := NewPurgeTracker()
	pt.BeginPurge("!room1")
	defer pt.FinishPurge("!room1")
	ctx, cancel := context.WithCancelCause(context.Background())
	go func() {
		time.Sleep(20 * time.Millisecond)
		cancel(nil)
	}()
	err := pt.WaitFor(ctx, "!room1", time.Second)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitFor should return context.Canceled on context cancellation; got %v", err)
	}
}
