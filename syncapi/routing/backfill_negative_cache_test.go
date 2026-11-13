// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
)

func TestTerminalBackfillCacheExpires(t *testing.T) {
	now := time.Unix(1000, 0)
	c := &terminalBackfillCache{now: func() time.Time { return now }}
	key := terminalBackfillKey("s", "v", "!r:test", map[string][]string{"$a": {"$b", "$c"}})

	assert.False(t, c.recentlyFailed(key))
	c.recordFailure(key)
	assert.True(t, c.recentlyFailed(key))

	now = now.Add(terminalBackfillTTL - time.Second)
	assert.True(t, c.recentlyFailed(key), "still inside the TTL")
	now = now.Add(2 * time.Second)
	assert.False(t, c.recentlyFailed(key), "expired after the TTL")
}

func TestTerminalBackfillKeyIgnoresMapOrderButNotContent(t *testing.T) {
	a := terminalBackfillKey("s", "v", "!r:test", map[string][]string{"$a": {"$p"}, "$b": {"$q"}})
	b := terminalBackfillKey("s", "v", "!r:test", map[string][]string{"$b": {"$q"}, "$a": {"$p"}})
	assert.Equal(t, a, b)
	assert.NotEqual(t, a, terminalBackfillKey("s", "v", "!r:test", map[string][]string{"$a": {"$other"}}))
	assert.NotEqual(t, a, terminalBackfillKey("s", "v", "!other:test", map[string][]string{"$a": {"$p"}, "$b": {"$q"}}))
}

type countingTerminalAPI struct {
	api.SyncRoomserverAPI
	calls int
}

func (c *countingTerminalAPI) PerformBackfill(context.Context, *api.PerformBackfillRequest, *api.PerformBackfillResponse) error {
	c.calls++
	return &api.BackfillUnavailableError{Retryable: false}
}

// Repeated requests for a gap that every server already failed to fill must
// not repeat the federation round.
func TestBackfillEventsSkipsRecentTerminalFailure(t *testing.T) {
	rs := &countingTerminalAPI{}
	extremities := map[string][]string{"$ext": {"$missing"}}
	roomID := "!terminal-cache:test"
	defaultTerminalBackfillCache.forget(terminalBackfillKey("s", "v", roomID, extremities))
	t.Cleanup(func() { defaultTerminalBackfillCache.forget(terminalBackfillKey("s", "v", roomID, extremities)) })

	for i := 0; i < 3; i++ {
		_, err := backfillEvents(context.Background(), rs, stubWriteDB{}, "hs", "hs", roomID, extremities, 10)
		require.Error(t, err)
		assert.True(t, isTerminalBackfillFailure(err), "the cached answer stays terminal so callers stop paginating")
	}
	assert.Equal(t, 1, rs.calls, "only the first request reaches federation")
}

func TestTerminalBackfillKeyIgnoresPrevOrderAndSeparatesHosts(t *testing.T) {
	a := terminalBackfillKey("s", "v", "!r:test", map[string][]string{"$a": {"$b", "$c"}})
	b := terminalBackfillKey("s", "v", "!r:test", map[string][]string{"$a": {"$c", "$b"}})
	assert.Equal(t, a, b)
	assert.NotEqual(t, a, terminalBackfillKey("s", "other", "!r:test", map[string][]string{"$a": {"$b", "$c"}}))
	assert.NotEqual(t, a, terminalBackfillKey("other", "v", "!r:test", map[string][]string{"$a": {"$b", "$c"}}))
}
