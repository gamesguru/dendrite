// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
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
	powerLevels := createMockStateStreamEvent("$plmissed:localhost", "m.room.power_levels", "", 60,
		`{"users":{"@alice:localhost":100},"events_default":0}`)
	snapshot.SetRecentEvents(roomID, types.RecentEvents{
		Events: []types.StreamEvent{
			powerLevels,
		},
	})
	snapshot.SetStateEvent(roomID, "m.room.power_levels", "", powerLevels.HeaderedEvent)

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

type pseudoIDRequiredStateRoomserver struct {
	mockRoomserverAPI
	userID string
}

func (r pseudoIDRequiredStateRoomserver) QueryUserIDForSender(context.Context, spec.RoomID, spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(r.userID, true)
}

func TestGetRequiredStateDeltaMatchesResolvedPseudoID(t *testing.T) {
	t.Parallel()

	roomID := "!pseudo-required-state:localhost"
	userID := "@alice:localhost"
	memberID := "@bob:localhost"
	const pseudoID = "pseudo-bob"
	rp := &RequestPool{rsAPI: &pseudoIDRequiredStateRoomserver{userID: memberID}}
	snapshot := newMockSnapshot()
	member := createMockStateStreamEvent("$member:localhost", "m.room.member", pseudoID, 60, `{"membership":"join"}`)
	snapshot.SetRecentEvents(roomID, types.RecentEvents{Events: []types.StreamEvent{member}})
	snapshot.SetStateEvent(roomID, "m.room.member", memberID, member.HeaderedEvent)

	requiredState, err := rp.getRequiredState(
		context.Background(), snapshot, roomID, userID,
		&types.RequiredStateConfig{Include: [][]string{{"m.room.member", memberID}}},
		[]synctypes.ClientEvent{}, false,
		types.RoomStreamState{Status: types.HaveSentRoomPreviously, LastToken: &types.StreamingToken{PDUPosition: 50}},
		types.StreamingToken{PDUPosition: 100},
	)
	require.NoError(t, err)
	require.Len(t, requiredState, 1)
	assert.Equal(t, member.EventID(), requiredState[0].EventID)
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

// excludedStateEvent is a state PDU the sync API stored but hides from
// timelines, as it does for events fed in out of order or by a state resync.
func excludedStateEvent(eventID, eventType, stateKey string, pos types.StreamPosition, content string) types.StreamEvent {
	ev := createMockStateStreamEvent(eventID, eventType, stateKey, pos, content)
	ev.ExcludeFromSync = true
	return ev
}

// TestGetRequiredState_DeliversCurrentEventForExcludedStateChange covers a
// join that reached the sync API as an excluded event: the client still holds
// the invite it saw earlier, and the join never appeared in any delta. The
// delta must send the slot's current event, not the excluded PDU itself, which
// may be stale. The result must also stay in the order slots first changed.
func TestGetRequiredState_DeliversCurrentEventForExcludedStateChange(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	roomID := "!excluded-join:localhost"
	userID := "@alice:localhost"
	target := "@galaxyshock:matrix.org"
	rp := &RequestPool{rsAPI: &mockRoomserverAPI{}}

	snapshot := newMockSnapshot()
	invite := excludedStateEvent("$invite:localhost", "m.room.member", target, 60, `{"membership":"invite"}`)
	join := excludedStateEvent("$join:localhost", "m.room.member", target, 70, `{"membership":"join"}`)
	power := excludedStateEvent("$pl:localhost", "m.room.power_levels", "", 65, `{"events_default":0}`)
	// The invite is excluded too, so only the slot, not its payload, may be
	// trusted: current state holds the join.
	snapshot.SetRecentEvents(roomID, types.RecentEvents{Events: []types.StreamEvent{invite, power, join}})
	snapshot.SetStateEvent(roomID, "m.room.member", target, join.HeaderedEvent)
	snapshot.SetStateEvent(roomID, "m.room.power_levels", "", power.HeaderedEvent)

	config := &types.RequiredStateConfig{
		Include: [][]string{{"m.room.member", target}, {"m.room.power_levels", ""}},
	}
	roomState := types.RoomStreamState{
		Status:    types.HaveSentRoomLive,
		LastToken: &types.StreamingToken{PDUPosition: 50},
	}
	currentPos := types.StreamingToken{PDUPosition: 100}

	requiredState, err := rp.getRequiredState(
		ctx, snapshot, roomID, userID, config,
		[]synctypes.ClientEvent{}, false, roomState, currentPos,
	)
	require.NoError(t, err)

	require.Len(t, requiredState, 2)
	assert.Equal(t, "m.room.member", requiredState[0].Type, "slots keep the order they first changed in")
	assert.Equal(t, join.EventID(), requiredState[0].EventID, "the current join, not the stale invite")
	assert.Equal(t, "m.room.power_levels", requiredState[1].Type)
}

// TestGetRequiredState_ExcludedStateChangeWithoutCurrentState covers an
// excluded state event whose slot has no current-state row. Excluded events are
// mostly historical (backfill and gap-fill never write current state), so
// sending one as a fallback would put a stale event in the client's required
// state. The slot is dropped instead.
func TestGetRequiredState_ExcludedStateChangeWithoutCurrentState(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	roomID := "!excluded-join-no-state:localhost"
	userID := "@alice:localhost"
	target := "@galaxyshock:matrix.org"
	rp := &RequestPool{rsAPI: &mockRoomserverAPI{}}

	snapshot := newMockSnapshot()
	invite := excludedStateEvent("$invite:localhost", "m.room.member", target, 60, `{"membership":"invite"}`)
	join := excludedStateEvent("$join:localhost", "m.room.member", target, 70, `{"membership":"join"}`)
	snapshot.SetRecentEvents(roomID, types.RecentEvents{Events: []types.StreamEvent{invite, join}})

	config := &types.RequiredStateConfig{
		Include: [][]string{{"m.room.member", target}},
	}
	roomState := types.RoomStreamState{
		Status:    types.HaveSentRoomLive,
		LastToken: &types.StreamingToken{PDUPosition: 50},
	}

	requiredState, err := rp.getRequiredState(
		ctx, snapshot, roomID, userID, config,
		[]synctypes.ClientEvent{}, false, roomState,
		types.StreamingToken{PDUPosition: 100},
	)
	require.NoError(t, err)

	assert.Empty(t, requiredState, "a historical excluded event must not be sent as current state")
}
