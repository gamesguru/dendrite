// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform

import (
	"context"
	"errors"
	"sync"
	"time"
)

// ErrPurgeInProgress is returned by PurgeTracker.WaitFor when the wait
// timed out before the purge completed.
var ErrPurgeInProgress = errors.New("room is being purged")

// PurgeTracker tracks rooms whose auto-purge is currently in flight, so that
// concurrent operations (such as a rejoin attempt) can wait for the purge to
// drain before proceeding.
//
// The tracker is in-memory only. If the process restarts mid-purge the
// startup empty-rooms sweep will pick up any room left in the empty state.
type PurgeTracker struct {
	mu       sync.Mutex
	inFlight map[string]chan struct{}
}

// NewPurgeTracker returns a ready-to-use PurgeTracker.
func NewPurgeTracker() *PurgeTracker {
	return &PurgeTracker{
		inFlight: make(map[string]chan struct{}),
	}
}

// BeginPurge reserves a purge slot for roomID. If no purge was previously
// in flight, owner is true and the caller must eventually call FinishPurge.
// If a purge was already in flight, owner is false and the caller should
// skip the actual purge work; the returned channel closes when the
// in-flight purge finishes.
func (p *PurgeTracker) BeginPurge(roomID string) (done chan struct{}, owner bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ch, ok := p.inFlight[roomID]; ok {
		return ch, false
	}
	ch := make(chan struct{})
	p.inFlight[roomID] = ch
	return ch, true
}

// FinishPurge closes the in-flight channel for roomID and removes the
// tracker entry. Safe to call when no entry exists (no-op).
func (p *PurgeTracker) FinishPurge(roomID string) {
	p.mu.Lock()
	ch, ok := p.inFlight[roomID]
	if ok {
		delete(p.inFlight, roomID)
	}
	p.mu.Unlock()
	if ok {
		close(ch)
	}
}

// WaitFor returns nil immediately if no purge is in flight for roomID.
// Otherwise it blocks until the purge finishes, ctx is canceled, or
// timeout elapses. Returns ctx.Err() on cancellation, ErrPurgeInProgress
// on timeout.
func (p *PurgeTracker) WaitFor(ctx context.Context, roomID string, timeout time.Duration) error {
	p.mu.Lock()
	ch, ok := p.inFlight[roomID]
	p.mu.Unlock()
	if !ok {
		return nil
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-ch:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return ErrPurgeInProgress
	}
}
