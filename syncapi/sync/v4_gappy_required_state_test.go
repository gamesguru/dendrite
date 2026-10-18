// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// TestGetRequiredState_NonLazyPattern_DeliversStateChangedDuringGap exercises
// getRequiredStateDelta directly: a power_levels change that happened in the
// gap between a room's own last-sent position and the connection's current
// position must still be delivered once the room is relevant again.
func TestGetRequiredState_NonLazyPattern_DeliversStateChangedDuringGap(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	roomID := "!gappy-state:localhost"
	userID := "@alice:localhost"
	rp := &RequestPool{rsAPI: &mockRoomserverAPI{}}

	snapshot := newMockSnapshot()
	// The power_levels change happened at position 60 - inside the gap between
	// this room's own last-sent position (50) and the connection's current
	// position (100).
	snapshot.SetRecentEvents(roomID, types.RecentEvents{
		Events: []types.StreamEvent{
			createMockStateStreamEvent("$plmissed:localhost", "m.room.power_levels", "", 60,
				`{"users":{"@alice:localhost":100},"events_default":0}`),
		},
	})

	config := &types.RequiredStateConfig{
		Include: [][]string{{"m.room.power_levels", ""}},
	}
	roomState := types.RoomStreamState{
		Status:    types.HaveSentRoomPreviously,
		LastToken: &types.StreamingToken{PDUPosition: 50},
	}
	currentPos := types.StreamingToken{PDUPosition: 100}

	requiredState, err := rp.getRequiredState(
		ctx, snapshot, roomID, userID, config,
		[]synctypes.ClientEvent{}, // no timeline events - not relevant to a non-$LAZY pattern
		false,                     // isInitialForRoom
		roomState, currentPos,
	)
	require.NoError(t, err)

	require.Len(t, requiredState, 1,
		"the power_levels change from inside the gap must be delivered now that the room is relevant again")
	assert.Equal(t, "m.room.power_levels", requiredState[0].Type)
}

// TestGetRequiredState_NonLazyPattern_NoChangeSinceLastSent_ReturnsEmpty ensures
// the delta computation doesn't regress the original Phase-12 fix: unchanged
// required_state must NOT be resent on every incremental sync (that's what
// confused matrix-rust-sdk's @ badge calculation in the first place).
func TestGetRequiredState_NonLazyPattern_NoChangeSinceLastSent_ReturnsEmpty(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	roomID := "!steady-state:localhost"
	userID := "@alice:localhost"
	rp := &RequestPool{rsAPI: &mockRoomserverAPI{}}

	// No recent events configured at all for this room - nothing changed.
	snapshot := newMockSnapshot()

	config := &types.RequiredStateConfig{
		Include: [][]string{{"m.room.power_levels", ""}},
	}
	roomState := types.RoomStreamState{
		Status:    types.HaveSentRoomLive,
		LastToken: &types.StreamingToken{PDUPosition: 90},
	}
	currentPos := types.StreamingToken{PDUPosition: 100}

	requiredState, err := rp.getRequiredState(
		ctx, snapshot, roomID, userID, config,
		[]synctypes.ClientEvent{},
		false, roomState, currentPos,
	)
	require.NoError(t, err)
	assert.Empty(t, requiredState, "unchanged required_state must not be resent on every incremental sync")
}
