// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"context"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// unPartialSnapshot serves a sync range that holds nothing but the marker of a room
// that finished its partial state resync: the join is before it, and the resync
// changes no state.
type unPartialSnapshot struct {
	failoverSnapshot
	state []*rstypes.HeaderedEvent
}

func (s unPartialSnapshot) GetStateDeltas(context.Context, *userapi.Device, types.Range, string, *synctypes.StateFilter, roomserverAPI.SyncRoomserverAPI) ([]types.StateDelta, []string, error) {
	return nil, nil, nil
}

func (s unPartialSnapshot) RecentEvents(context.Context, []string, types.Range, *synctypes.RoomEventFilter, bool, bool) (map[string]types.RecentEvents, error) {
	return map[string]types.RecentEvents{}, nil
}

func (s unPartialSnapshot) UnPartialStatedRoomsInRange(context.Context, string, types.Range) ([]string, error) {
	return []string{s.roomID}, nil
}

func (s unPartialSnapshot) FullStateDeltas(context.Context, *userapi.Device, []string, *synctypes.StateFilter, roomserverAPI.SyncRoomserverAPI) ([]types.StateDelta, error) {
	return []types.StateDelta{{RoomID: s.roomID, Membership: "join", NewlyJoined: true, StateEvents: s.state}}, nil
}

func (s unPartialSnapshot) GetBackwardTopologyPos(context.Context, []*rstypes.HeaderedEvent) (types.TopologyToken, error) {
	return types.TopologyToken{}, nil
}

func (s unPartialSnapshot) GetRoomSummary(context.Context, string, string) (*types.Summary, error) {
	return &types.Summary{}, nil
}

func (s unPartialSnapshot) PositionInTopology(context.Context, string) (types.StreamPosition, types.StreamPosition, error) {
	return 0, 0, nil
}

func (s unPartialSnapshot) MaxStreamPositionForRelations(context.Context) (types.StreamPosition, error) {
	return 0, nil
}

// unPartialMembershipSnapshot serves a fixed full delta for the room, to check
// which delta addUnPartialStatedRooms keeps.
type unPartialMembershipSnapshot struct {
	unPartialSnapshot
	full types.StateDelta
}

func (s unPartialMembershipSnapshot) FullStateDeltas(context.Context, *userapi.Device, []string, *synctypes.StateFilter, roomserverAPI.SyncRoomserverAPI) ([]types.StateDelta, error) {
	return []types.StateDelta{s.full}, nil
}

func newUnPartialRequest(t *testing.T, userID string, lazyLoad bool) *types.SyncRequest {
	filter := synctypes.DefaultFilter()
	filter.Room.State.LazyLoadMembers = lazyLoad
	return &types.SyncRequest{
		Context:           t.Context(),
		Device:            &userapi.Device{UserID: userID},
		Rooms:             map[string]string{},
		Response:          types.NewResponse(),
		Filter:            filter,
		Log:               logrus.NewEntry(logrus.StandardLogger()),
		MembershipChanges: map[string]struct{}{},
		NewlyJoinedRooms:  map[string]struct{}{},
	}
}

// A client that does not lazy load members was not sent the room while its state was
// partial, and its token is already past its join. When the resync finishes, the
// room must reach it, with its whole state, though no event or state change did.
func TestIncrementalSyncSendsARoomWhoseResyncFinished(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	snapshot := unPartialSnapshot{
		failoverSnapshot: failoverSnapshot{roomID: room.ID},
		state:            room.Events(),
	}
	provider := &PDUStreamProvider{rsAPI: noPartialStateRooms{}}

	t.Run("non-lazy client is sent the room as newly joined", func(t *testing.T) {
		req := newUnPartialRequest(t, alice.ID, false)
		pos := provider.IncrementalSync(t.Context(), snapshot, req, 10, 20)

		require.Contains(t, req.Response.Rooms.Join, room.ID)
		assert.NotEmpty(t, req.Response.Rooms.Join[room.ID].State.Events, "the room's state comes with it")
		assert.Contains(t, req.NewlyJoinedRooms, room.ID)
		assert.Equal(t, req.Rooms[room.ID], "join")
		assert.GreaterOrEqual(t, pos, types.StreamPosition(10))
	})

	t.Run("lazy loading client already has the room", func(t *testing.T) {
		req := newUnPartialRequest(t, alice.ID, true)
		provider.IncrementalSync(t.Context(), snapshot, req, 10, 20)
		assert.NotContains(t, req.Response.Rooms.Join, room.ID, "it saw the room during the resync")
	})
}

// TestAddUnPartialStatedRoomsKeepsMembershipChanges checks that the whole-state
// delta added when a resync finishes does not replace a leave or ban the range
// already carries, nor mark that room as joined.
func TestAddUnPartialStatedRoomsKeepsMembershipChanges(t *testing.T) {
	const roomID = "!room:example.com"
	full := types.StateDelta{RoomID: roomID, Membership: spec.Join, NewlyJoined: true}

	tests := []struct {
		name        string
		existing    []types.StateDelta
		wantMembers string
		wantFull    bool
		wantJoined  bool
	}{
		{
			name:        "no existing delta is replaced by the full state",
			wantMembers: spec.Join,
			wantFull:    true,
			wantJoined:  true,
		},
		{
			name:        "an existing join delta is replaced by the full state",
			existing:    []types.StateDelta{{RoomID: roomID, Membership: spec.Join}},
			wantMembers: spec.Join,
			wantFull:    true,
			wantJoined:  true,
		},
		{
			name:        "a leave delta is kept",
			existing:    []types.StateDelta{{RoomID: roomID, Membership: spec.Leave}},
			wantMembers: spec.Leave,
		},
		{
			name:        "a ban delta is kept",
			existing:    []types.StateDelta{{RoomID: roomID, Membership: spec.Ban}},
			wantMembers: spec.Ban,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := unPartialMembershipSnapshot{
				unPartialSnapshot: unPartialSnapshot{
					failoverSnapshot: failoverSnapshot{roomID: roomID},
				},
				full: full,
			}
			req := newUnPartialRequest(t, "@alice:example.com", false)
			provider := &PDUStreamProvider{}

			merged, joined, err := provider.addUnPartialStatedRooms(
				t.Context(), snapshot, req, types.Range{From: 10, To: 20},
				&synctypes.StateFilter{}, tc.existing, nil,
			)
			require.NoError(t, err)
			require.Len(t, merged, 1)
			assert.Equal(t, tc.wantMembers, merged[0].Membership)
			assert.Equal(t, tc.wantFull, merged[0].NewlyJoined, "whether the full-state delta was used")
			assert.Equal(t, tc.wantJoined, len(joined) == 1, "whether the room is marked joined")
		})
	}
}
