// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input

import (
	"context"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/roomserver/types"
)

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

// LookupEventLocalFirstForTest invokes the missing-event lookup with its local-first path enabled.
func LookupEventLocalFirstForTest(
	ctx context.Context, inputer *Inputer, roomInfo *types.RoomInfo, eventID string, servers []spec.ServerName,
) (gomatrixserverlib.PDU, error) {
	lookup := missingStateReq{
		log:              logrus.NewEntry(logrus.New()),
		virtualHost:      inputer.ServerName,
		inputer:          inputer,
		db:               inputer.DB,
		roomInfo:         roomInfo,
		federation:       inputer.FSAPI,
		keys:             inputer.KeyRing,
		servers:          servers,
		hadEvents:        make(map[string]bool),
		haveEvents:       make(map[string]gomatrixserverlib.PDU),
		unfindableEvents: make(map[string]bool),
	}
	return lookup.lookupEvent(ctx, roomInfo.RoomVersion, "", eventID, true)
}
