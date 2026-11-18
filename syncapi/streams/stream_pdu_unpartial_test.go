// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"context"
	"math"
	"testing"

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
	// membership is the user's membership when the range ends; empty means join.
	membership string
	// membershipAtRangeEnd is the membership at the range end when it differs
	// from the current membership, as when a marker precedes the join.
	membershipAtRangeEnd string
	// marker is the marker's position. Zero means it is in every range.
	marker types.StreamPosition
	// deltas are the state deltas the range already holds.
	deltas []types.StateDelta
}

func (s unPartialSnapshot) SelectMembershipForUser(_ context.Context, _, _ string, pos int64) (string, int64, error) {
	if s.membershipAtRangeEnd != "" && pos != math.MaxInt64 {
		return s.membershipAtRangeEnd, 0, nil
	}
	if s.membership == "" {
		return "join", 0, nil
	}
	return s.membership, 0, nil
}

func (s unPartialSnapshot) GetStateDeltas(context.Context, *userapi.Device, types.Range, string, *synctypes.StateFilter, roomserverAPI.SyncRoomserverAPI) ([]types.StateDelta, []string, error) {
	return s.deltas, nil, nil
}

func (s unPartialSnapshot) RecentEvents(context.Context, []string, types.Range, *synctypes.RoomEventFilter, bool, bool) (map[string]types.RecentEvents, error) {
	return map[string]types.RecentEvents{}, nil
}

func (s unPartialSnapshot) UnPartialStatedRoomsInRange(_ context.Context, _ string, r types.Range) ([]string, error) {
	if s.marker > 0 && (s.marker <= r.Low() || s.marker > r.High()) {
		return nil, nil
	}
	return []string{s.roomID}, nil
}

func (s unPartialSnapshot) FullStateDeltas(context.Context, *userapi.Device, []string, *synctypes.StateFilter, roomserverAPI.SyncRoomserverAPI) ([]types.StateDelta, error) {
	return []types.StateDelta{{RoomID: s.roomID, Membership: "join", NewlyJoined: true, StateEvents: s.state}}, nil
}

func (s unPartialSnapshot) RoomStateDelta(context.Context, *userapi.Device, string, types.Range, *synctypes.StateFilter, roomserverAPI.SyncRoomserverAPI) ([]*rstypes.HeaderedEvent, error) {
	return s.state, nil
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

func (s unPartialSnapshot) BackwardExtremitiesForRoom(context.Context, string) (map[string][]string, error) {
	return map[string][]string{}, nil
}

func (s unPartialSnapshot) MaxStreamPositionForRelations(context.Context) (types.StreamPosition, error) {
	return 0, nil
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

	t.Run("a room the user has left is not sent as joined", func(t *testing.T) {
		left := snapshot
		left.membership = "leave"
		req := newUnPartialRequest(t, alice.ID, false)
		provider.IncrementalSync(t.Context(), left, req, 10, 20)
		assert.NotContains(t, req.Response.Rooms.Join, room.ID)
		assert.NotContains(t, req.NewlyJoinedRooms, room.ID)
	})

	t.Run("a leave delta is kept", func(t *testing.T) {
		left := snapshot
		left.membership = "leave"
		left.deltas = []types.StateDelta{{RoomID: room.ID, Membership: "leave"}}
		req := newUnPartialRequest(t, alice.ID, false)
		provider.IncrementalSync(t.Context(), left, req, 10, 20)
		assert.NotContains(t, req.Response.Rooms.Join, room.ID)
		assert.Contains(t, req.Response.Rooms.Leave, room.ID)
	})

	t.Run("a marker before the join uses the current membership", func(t *testing.T) {
		// The range still ends in leave because the join event comes after it,
		// but the user is currently joined, so the room must be sent.
		rejoined := snapshot
		rejoined.marker = 15
		rejoined.membershipAtRangeEnd = "leave"
		req := newUnPartialRequest(t, alice.ID, false)
		provider.IncrementalSync(t.Context(), rejoined, req, 10, 20)

		require.Contains(t, req.Response.Rooms.Join, room.ID)
		assert.NotEmpty(t, req.Response.Rooms.Join[room.ID].State.Events, "the room's state comes with it")
	})

	t.Run("a marker before the token is sent with the join", func(t *testing.T) {
		// Resync finished before the join event reached sync: the token advanced
		// past the marker, and the join arrives in this range instead.
		rejoined := snapshot
		rejoined.marker = 15
		rejoined.deltas = []types.StateDelta{{RoomID: room.ID, Membership: "join", NewlyJoined: true}}
		req := newUnPartialRequest(t, alice.ID, false)
		pos := provider.IncrementalSync(t.Context(), rejoined, req, 20, 30)

		require.Contains(t, req.Response.Rooms.Join, room.ID)
		assert.NotEmpty(t, req.Response.Rooms.Join[room.ID].State.Events, "the room's state comes with it")
		assert.Contains(t, req.NewlyJoinedRooms, room.ID)
		assert.GreaterOrEqual(t, pos, types.StreamPosition(20))
	})

	t.Run("a marker before the token without a join is not resent", func(t *testing.T) {
		rejoined := snapshot
		rejoined.marker = 15
		req := newUnPartialRequest(t, alice.ID, false)
		provider.IncrementalSync(t.Context(), rejoined, req, 20, 30)
		assert.NotContains(t, req.Response.Rooms.Join, room.ID)
	})

	t.Run("an ordinary joined room does not recover an old marker", func(t *testing.T) {
		rejoined := snapshot
		rejoined.marker = 15
		rejoined.deltas = []types.StateDelta{{RoomID: room.ID, Membership: "join"}}
		req := newUnPartialRequest(t, alice.ID, false)
		provider.IncrementalSync(t.Context(), rejoined, req, 20, 30)
		assert.NotContains(t, req.Response.Rooms.Join, room.ID)
	})
}

// partialStateRooms reports its rooms as still resyncing their state.
type partialStateRooms struct {
	roomserverAPI.SyncRoomserverAPI
	rooms []string
}

func (p partialStateRooms) GetPartialStateRoomIDs(context.Context) ([]string, error) {
	return p.rooms, nil
}

func (partialStateRooms) QueryMembershipForUser(context.Context, *roomserverAPI.QueryMembershipForUserRequest, *roomserverAPI.QueryMembershipForUserResponse) error {
	return nil
}

// A user who is removed from a room whose partial state resync has not finished,
// and may never, as the server that removed them can refuse to serve it, must still
// be told: leaving needs no room state. A join is held back until the resync ends.
func TestIncrementalSyncDeliversRemovalFromAPartialStateRoom(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	provider := &PDUStreamProvider{rsAPI: partialStateRooms{rooms: []string{room.ID}}}

	for _, membership := range []string{"leave", "ban"} {
		t.Run(membership+" is delivered", func(t *testing.T) {
			snapshot := unPartialSnapshot{
				failoverSnapshot: failoverSnapshot{roomID: room.ID},
				state:            room.Events()[:1],
				membership:       membership,
				marker:           1000, // the resync has not finished: no marker in range
				deltas:           []types.StateDelta{{RoomID: room.ID, Membership: membership, MembershipPos: 15, StateEvents: room.Events()[:1]}},
			}
			req := newUnPartialRequest(t, alice.ID, false)
			provider.IncrementalSync(t.Context(), snapshot, req, 10, 20)
			assert.Contains(t, req.Response.Rooms.Leave, room.ID)
		})
	}

	t.Run("a join is still held back", func(t *testing.T) {
		snapshot := unPartialSnapshot{
			failoverSnapshot: failoverSnapshot{roomID: room.ID},
			marker:           1000,
			deltas:           []types.StateDelta{{RoomID: room.ID, Membership: "join", NewlyJoined: true}},
		}
		req := newUnPartialRequest(t, alice.ID, false)
		provider.IncrementalSync(t.Context(), snapshot, req, 10, 20)
		assert.NotContains(t, req.Response.Rooms.Join, room.ID)
	})
}
