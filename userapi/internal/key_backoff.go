// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"sync"
	"time"
)

const (
	keyQueryBackoffBase = 30 * time.Second
	keyQueryBackoffMax  = 10 * time.Minute
)

// remoteKeyBackoff remembers servers whose /keys/query recently failed, so
// clients that ask for keys on every sync do not wait out the timeout against
// the same dead server again and again. The zero value is ready to use.
type remoteKeyBackoff struct {
	mu      sync.Mutex
	servers map[string]*keyBackoffEntry
	now     func() time.Time
}

type keyBackoffEntry struct {
	failures int
	until    time.Time
}

func (b *remoteKeyBackoff) time() time.Time {
	if b.now != nil {
		return b.now()
	}
	return time.Now()
}

// remaining returns how long queries to the server should still be skipped.
func (b *remoteKeyBackoff) remaining(server string) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.servers[server]
	if entry == nil {
		return 0
	}
	return max(0, entry.until.Sub(b.time()))
}

// failure records a failed query and doubles the wait, up to a limit.
func (b *remoteKeyBackoff) failure(server string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.servers == nil {
		b.servers = make(map[string]*keyBackoffEntry)
	}
	entry := b.servers[server]
	if entry == nil {
		entry = &keyBackoffEntry{}
		b.servers[server] = entry
	}
	entry.failures++
	wait := keyQueryBackoffBase << min(entry.failures-1, 16) //nolint:mnd // Bounds the shift; the wait is capped below.
	entry.until = b.time().Add(min(wait, keyQueryBackoffMax))
}

// success forgets any earlier failures.
func (b *remoteKeyBackoff) success(server string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.servers, server)
}
