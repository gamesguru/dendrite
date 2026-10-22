// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input_test

import (
	"context"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

func stateEventIDs(events []*types.HeaderedEvent) []string {
	ids := make([]string, 0, len(events))
	for _, ev := range events {
		ids = append(ids, ev.EventID())
	}
	return ids
}

// withResyncRoomserver runs fn against a roomserver backed by each test database.
func withResyncRoomserver(t *testing.T, fn func(context.Context, api.RoomserverInternalAPI)) {
	t.Helper()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		natsInstance := &jetstream.NATSInstance{}
		_, _ = natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		fn(processCtx.Context(), rsAPI)
	})
}

func mustCurrentMembership(
	t *testing.T, ctx context.Context, rsAPI api.RoomserverInternalAPI, roomID, userID string,
) string {
	t.Helper()
	tuple := gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: userID}
	res := &api.QueryCurrentStateResponse{}
	require.NoError(t, rsAPI.QueryCurrentState(ctx, &api.QueryCurrentStateRequest{
		RoomID:      roomID,
		StateTuples: []gomatrixserverlib.StateKeyTuple{tuple},
	}, res))
	ev, ok := res.StateEvents[tuple]
	require.True(t, ok, "expected a current membership event for %s", userID)
	membership, err := ev.Membership()
	require.NoError(t, err)
	return membership
}

func assertMembershipRow(
	t *testing.T, ctx context.Context, rsAPI api.RoomserverInternalAPI,
	roomID, userID string, wantJoined bool,
) {
	t.Helper()
	res := &api.QueryMembershipsForRoomResponse{}
	require.NoError(t, rsAPI.QueryMembershipsForRoom(ctx, &api.QueryMembershipsForRoomRequest{RoomID: roomID}, res))
	joined := false
	for _, ev := range res.JoinEvents {
		if ev.StateKey != nil && *ev.StateKey == userID {
			joined = true
			break
		}
	}
	assert.Equal(t, wantJoined, joined, "membership row for %s", userID)
}

// A partial-state resync used to build its final snapshot from the state fetched
// for one anchor branch. If that fetch predates an event received during the
// partial state (such as a kick that was only re-accepted after the state was
// complete), the event was left out of the current state. Once the state since
// the join has been recomputed, the final state must instead be resolved from
// all latest events.
func TestResyncAppliesStateAfterTheRecomputedLatestEvents(t *testing.T) {
	withResyncRoomserver(t, func(ctx context.Context, rsAPI api.RoomserverInternalAPI) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		room := test.NewRoom(t, alice)
		join := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{
			"membership": spec.Join,
		}, test.WithStateKey(bob.ID))
		leave := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{
			"membership": spec.Leave,
		}, test.WithStateKey(bob.ID))

		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))

		roomID, err := spec.NewRoomID(room.ID)
		require.NoError(t, err)
		roomInfo, err := rsAPI.QueryRoomInfo(ctx, *roomID)
		require.NoError(t, err)
		require.NotNil(t, roomInfo)

		// The partial state started with Bob's join.
		require.NoError(t, rsAPI.SetRoomPartialState(ctx, roomInfo.RoomNID, join.EventID(), "test", []string{"test"}, 0))

		// The state before the join, used as the recompute anchor.
		beforeJoin := &api.QueryStateAfterEventsResponse{}
		require.NoError(t, rsAPI.QueryStateAfterEvents(ctx, &api.QueryStateAfterEventsRequest{
			RoomID:       room.ID,
			PrevEventIDs: join.PrevEventIDs(),
		}, beforeJoin))
		anchorStateIDs := stateEventIDs(beforeJoin.StateEvents)

		// The stale state fetched for the join's branch: Bob is still joined.
		// Applying this directly, as the fallback does, would lose his leave.
		afterJoin := &api.QueryStateAfterEventsResponse{}
		require.NoError(t, rsAPI.QueryStateAfterEvents(ctx, &api.QueryStateAfterEventsRequest{
			RoomID:       room.ID,
			PrevEventIDs: []string{join.EventID()},
		}, afterJoin))
		staleStateIDs := stateEventIDs(afterJoin.StateEvents)

		latest, _, _, err := rsAPI.LatestEventIDs(ctx, roomInfo.RoomNID)
		require.NoError(t, err)
		require.Equal(t, []string{leave.EventID()}, latest)

		require.NoError(t, rsAPI.UpdateCurrentStateAfterResync(ctx, room.ID, staleStateIDs, anchorStateIDs, latest))

		assert.Equal(t, spec.Leave, mustCurrentMembership(t, ctx, rsAPI, room.ID, bob.ID),
			"the leave after the join must be in the final state")
		assertMembershipRow(t, ctx, rsAPI, room.ID, bob.ID, false)
	})
}

// A local user's join may only exist on the partial-state branch. The recomputed
// path bypasses reconcileResyncMembers, so verify the join still reaches the
// final state when the fetched state predates it.
func TestResyncKeepsLocalJoinFromRecomputedState(t *testing.T) {
	withResyncRoomserver(t, func(ctx context.Context, rsAPI api.RoomserverInternalAPI) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		room := test.NewRoom(t, alice)
		join := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{
			"membership": spec.Join,
		}, test.WithStateKey(bob.ID))

		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))

		roomID, err := spec.NewRoomID(room.ID)
		require.NoError(t, err)
		roomInfo, err := rsAPI.QueryRoomInfo(ctx, *roomID)
		require.NoError(t, err)
		require.NotNil(t, roomInfo)

		require.NoError(t, rsAPI.SetRoomPartialState(ctx, roomInfo.RoomNID, join.EventID(), "test", []string{"test"}, 0))

		beforeJoin := &api.QueryStateAfterEventsResponse{}
		require.NoError(t, rsAPI.QueryStateAfterEvents(ctx, &api.QueryStateAfterEventsRequest{
			RoomID:       room.ID,
			PrevEventIDs: join.PrevEventIDs(),
		}, beforeJoin))
		// The fetched state predates Bob's join, so it does not contain him.
		staleStateIDs := stateEventIDs(beforeJoin.StateEvents)

		latest, _, _, err := rsAPI.LatestEventIDs(ctx, roomInfo.RoomNID)
		require.NoError(t, err)
		require.Equal(t, []string{join.EventID()}, latest)

		require.NoError(t, rsAPI.UpdateCurrentStateAfterResync(ctx, room.ID, staleStateIDs, staleStateIDs, latest))

		assert.Equal(t, spec.Join, mustCurrentMembership(t, ctx, rsAPI, room.ID, bob.ID),
			"the local join must survive the recomputed resync")
		assertMembershipRow(t, ctx, rsAPI, room.ID, bob.ID, true)
	})
}
