// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRemoteKeyBackoff(t *testing.T) {
	now := time.Unix(1000, 0)
	b := &remoteKeyBackoff{now: func() time.Time { return now }}

	assert.Zero(t, b.remaining("dead"), "an unknown server is not backing off")

	b.failure("dead")
	assert.Equal(t, keyQueryBackoffBase, b.remaining("dead"))
	assert.Zero(t, b.remaining("other"), "backoff is per server")

	now = now.Add(keyQueryBackoffBase)
	assert.Zero(t, b.remaining("dead"), "backoff expires")

	b.failure("dead")
	assert.Equal(t, 2*keyQueryBackoffBase, b.remaining("dead"), "each failure doubles the wait")

	for range 20 {
		b.failure("dead")
	}
	assert.Equal(t, keyQueryBackoffMax, b.remaining("dead"), "the wait is capped")

	b.success("dead")
	assert.Zero(t, b.remaining("dead"), "a success clears the backoff")
}
