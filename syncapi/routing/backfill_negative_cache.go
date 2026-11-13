// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"sort"
	"strings"
	"sync"
	"time"
)

// terminalBackfillTTL is how long a definitive "no server has these events"
// answer is remembered. Within it, /messages requests that reach the same gap
// are answered from memory instead of asking every remote server again.
const terminalBackfillTTL = 5 * time.Minute

// terminalBackfillMaxEntries bounds the cache so a busy server cannot grow it
// without limit; expired entries are dropped first.
const terminalBackfillMaxEntries = 1024

// terminalBackfillCache remembers backfill attempts that every server
// definitively failed to answer. Clients retry /messages constantly (a refresh
// per timeline gap), and each retry otherwise repeats the whole federation
// round, which for dead remote servers means waiting on timeouts every time.
type terminalBackfillCache struct {
	mu      sync.Mutex
	entries map[string]time.Time
	now     func() time.Time
}

var defaultTerminalBackfillCache = &terminalBackfillCache{now: time.Now}

// terminalBackfillKey identifies a backfill request by room and extremity set.
func terminalBackfillKey(roomID string, backwardsExtremities map[string][]string) string {
	ids := make([]string, 0, len(backwardsExtremities))
	for extremityID, prevs := range backwardsExtremities {
		ids = append(ids, extremityID+"<"+strings.Join(prevs, ","))
	}
	sort.Strings(ids)
	return roomID + "|" + strings.Join(ids, ";")
}

// recentlyFailed reports whether the same request definitively failed within
// the TTL.
func (c *terminalBackfillCache) recentlyFailed(key string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	at, ok := c.entries[key]
	if !ok {
		return false
	}
	if c.now().Sub(at) >= terminalBackfillTTL {
		delete(c.entries, key)
		return false
	}
	return true
}

// recordFailure remembers a definitive failure.
func (c *terminalBackfillCache) recordFailure(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.entries == nil {
		c.entries = make(map[string]time.Time)
	}
	now := c.now()
	if len(c.entries) >= terminalBackfillMaxEntries {
		for k, at := range c.entries {
			if now.Sub(at) >= terminalBackfillTTL {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= terminalBackfillMaxEntries {
			// Still full of live entries: skip caching this one.
			return
		}
	}
	c.entries[key] = now
}

// forget drops an entry, for example after the same request succeeded.
func (c *terminalBackfillCache) forget(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}
