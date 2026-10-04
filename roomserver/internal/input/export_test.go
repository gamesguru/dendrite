// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input

import "time"

// ExpireMissingStateCooldowns ends every room's missing-state cooldown, as if
// its duration had passed.
func (r *Inputer) ExpireMissingStateCooldowns() {
	r.missingStateCooldown.Clear()
}

// SetSyncInputReplyOverdue changes how long synchronous input waits before it
// reports unanswered inputs, returning a function that restores it.
func SetSyncInputReplyOverdue(d time.Duration) func() {
	old := syncInputReplyOverdue
	syncInputReplyOverdue = d
	return func() { syncInputReplyOverdue = old }
}
