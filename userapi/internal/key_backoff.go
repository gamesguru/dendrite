// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"

	fedsenderapi "codefloe.com/pat-s/zendrite/federationapi/api"
)

const (
	keyQueryBackoffBase = 30 * time.Second
	keyQueryBackoffMax  = 10 * time.Minute
)

// remoteKeyBackoff remembers servers whose /keys/query recently failed, so
// clients that ask for keys on every sync do not wait out the timeout against
// the same dead server again and again. After the wait, a single query probes
// the server while the rest stay away, so a crowd of syncs cannot all pile onto
// a server that is still down. The zero value is ready to use.
type remoteKeyBackoff struct {
	mu      sync.Mutex
	servers map[string]*keyBackoffEntry
	now     func() time.Time
}

type keyBackoffEntry struct {
	failures int
	until    time.Time
	probing  bool
}

func (b *remoteKeyBackoff) time() time.Time {
	if b.now != nil {
		return b.now()
	}
	return time.Now()
}

// remaining returns how long queries to the server should still be skipped. It
// is a read-only view: acquire is what admits a query.
func (b *remoteKeyBackoff) remaining(server string) time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.servers[server]
	if entry == nil {
		return 0
	}
	return max(0, entry.until.Sub(b.time()))
}

// acquire admits a query to the server, or reports how long to stay away. A
// server that has never failed admits every query. One that failed admits none
// until its wait is over, and then only one probe at a time. Each admitted query
// must end with success, failure or release.
func (b *remoteKeyBackoff) acquire(server string) (time.Duration, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	entry := b.servers[server]
	if entry == nil {
		return 0, true
	}
	if wait := entry.until.Sub(b.time()); wait > 0 {
		return wait, false
	}
	if entry.probing {
		return keyQueryBackoffBase, false
	}
	entry.probing = true
	return 0, true
}

// failure records a failed query and doubles the wait, up to a limit. Queries
// already in flight when the first one fails also fail, and count as that one
// failure rather than doubling the wait several times over.
func (b *remoteKeyBackoff) failure(server string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := b.time()
	if b.servers == nil {
		b.servers = make(map[string]*keyBackoffEntry)
	}
	// Forget servers that stayed quiet for longer than the longest wait, so
	// failures from many distinct servers do not grow the map without bound.
	for name, e := range b.servers {
		if !e.probing && now.After(e.until.Add(keyQueryBackoffMax)) {
			delete(b.servers, name)
		}
	}
	entry := b.servers[server]
	if entry == nil {
		entry = &keyBackoffEntry{}
		b.servers[server] = entry
	} else if now.Before(entry.until) {
		return
	}
	entry.probing = false
	entry.failures++
	wait := keyQueryBackoffBase << min(entry.failures-1, 16) //nolint:mnd // Bounds the shift; the wait is capped below.
	entry.until = now.Add(min(wait, keyQueryBackoffMax))
}

// success forgets any earlier failures.
func (b *remoteKeyBackoff) success(server string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	delete(b.servers, server)
}

// release ends a probe that proved nothing about the server, such as one the
// caller canceled, so that another query can probe it.
func (b *remoteKeyBackoff) release(server string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if entry := b.servers[server]; entry != nil {
		entry.probing = false
	}
}

// isTransientKeyQueryError reports whether a failed /keys/query says the server
// is unreachable or overloaded, as opposed to refusing this particular request.
// Only the former is worth backing off from: a 4xx for one user's keys must not
// stop queries for everyone else on that server.
func isTransientKeyQueryError(err error) bool {
	var fedErr *fedsenderapi.FederationClientError
	if errors.As(err, &fedErr) {
		return fedErr.Blacklisted || fedErr.Code == 0 || isTransientStatus(fedErr.Code)
	}
	var httpErr spec.HTTPError
	if errors.As(err, &httpErr) {
		return isTransientStatus(httpErr.Code)
	}
	// Timeouts and connection errors.
	return true
}

func isTransientStatus(code int) bool {
	return code >= http.StatusInternalServerError || code == http.StatusTooManyRequests
}
