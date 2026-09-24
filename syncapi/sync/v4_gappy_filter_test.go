// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"codefloe.com/pat-s/zendrite/syncapi/types"
)

func sentUpTo(status types.HaveSentRoomFlag, pos types.StreamPosition) types.RoomStreamState {
	token := types.StreamingToken{PDUPosition: pos}
	return types.RoomStreamState{Status: status, LastToken: &token}
}

// TestLaggingRooms_SelectsOnlyRoomsBehindSince covers the filtering gap that
// left the widened timeline lower bound unreachable: a room whose missed events
// all predate the connection's since token is invisible to the since-keyed
// RoomsWithEventsSince filter, so it has to be kept explicitly or it never
// reaches getTimelineEvents at all.
func TestLaggingRooms_SelectsOnlyRoomsBehindSince(t *testing.T) {
	t.Parallel()

	const (
		gappy    = "!gappy:localhost"
		older    = "!older:localhost"
		steady   = "!steady:localhost"
		brandNew = "!new:localhost"
	)

	roomStates := map[string]types.RoomStreamState{
		// Excluded from prior responses; delivered only up to 50 and 30.
		gappy: sentUpTo(types.HaveSentRoomPreviously, 50),
		older: sentUpTo(types.HaveSentRoomPreviously, 30),
		// Included in every response, so its token matches since.
		steady: sentUpTo(types.HaveSentRoomLive, 90),
		// Never sent: no token to lag behind, handled by the status_never path.
		brandNew: {Status: types.HaveSentRoomNever},
	}

	lagging := laggingRooms(roomStates, 90)

	assert.ElementsMatch(t, []string{gappy, older}, lagging.roomIDs,
		"only rooms whose last-sent token is behind since have a gap to recover")
	assert.Equal(t, types.StreamPosition(30), lagging.oldestToken,
		"the batched lookup must start from the oldest lagging token, not the newest")
}

// TestLaggingRooms_NoLaggingRooms is the steady state: every room was included
// in the previous response, so nothing needs rescuing from the filter.
func TestLaggingRooms_NoLaggingRooms(t *testing.T) {
	t.Parallel()

	lagging := laggingRooms(map[string]types.RoomStreamState{
		"!steady:localhost": sentUpTo(types.HaveSentRoomLive, 90),
	}, 90)

	assert.Empty(t, lagging.roomIDs)
}
