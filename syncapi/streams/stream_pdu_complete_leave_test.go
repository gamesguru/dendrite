// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"context"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type leaveTestRoomserver struct {
	noPartialStateRooms
}

func (leaveTestRoomserver) QueryMembershipForUser(_ context.Context, _ *roomserverAPI.QueryMembershipForUserRequest, res *roomserverAPI.QueryMembershipForUserResponse) error {
	res.RoomExists = true
	return nil
}

func (leaveTestRoomserver) QueryMembershipAtEvent(context.Context, spec.RoomID, []string, spec.SenderID) (map[string]*rstypes.HeaderedEvent, error) {
	return map[string]*rstypes.HeaderedEvent{}, nil
}

func (leaveTestRoomserver) QueryUserIDForSender(_ context.Context, _ spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

func (leaveTestRoomserver) QuerySenderIDForUser(_ context.Context, _ spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	s := spec.SenderIDFromUserID(userID)
	return &s, nil
}

// A complete sync reads a snapshot taken after the request arrived, and its
// next_batch moves past everything in it. A ban that landed in between would
// then never reach the client: the room is not joined any more, so it is not
// in the response, and no later sync starts before the ban.
func TestCompleteSyncReportsLeaveThatRacedTheRequest(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)
		ctx := t.Context()

		alice, bob := test.NewUser(t), test.NewUser(t)
		room := test.NewRoom(t, alice)
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		for _, ev := range room.Events() {
			var state []*rstypes.HeaderedEvent
			var addState []string
			if ev.StateKey() != nil {
				state, addState = []*rstypes.HeaderedEvent{ev}, []string{ev.EventID()}
			}
			_, err = db.WriteEvent(ctx, ev, state, addState, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
		}
		arrived := func() types.StreamPosition {
			snapshot, serr := db.NewDatabaseSnapshot(ctx)
			require.NoError(t, serr)
			defer snapshot.Rollback() //nolint:errcheck
			pos, serr := snapshot.MaxStreamPositionForPDUs(ctx)
			require.NoError(t, serr)
			return pos
		}
		beforeBan := arrived()

		ban := room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Ban}, test.WithStateKey(bob.ID))
		_, err = db.WriteEvent(ctx, ban, []*rstypes.HeaderedEvent{ban}, []string{ban.EventID()}, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)
		afterBan := arrived()
		require.Greater(t, afterBan, beforeBan)

		sync := func(arrivedAt types.StreamPosition) *types.SyncRequest {
			snapshot, serr := db.NewDatabaseSnapshot(ctx)
			require.NoError(t, serr)
			defer snapshot.Rollback() //nolint:errcheck
			req := &types.SyncRequest{
				Context:              ctx,
				Device:               &userapi.Device{UserID: bob.ID},
				Rooms:                map[string]string{},
				Response:             types.NewResponse(),
				Filter:               synctypes.DefaultFilter(),
				Log:                  logrus.NewEntry(logrus.StandardLogger()),
				MembershipChanges:    map[string]struct{}{},
				NewlyJoinedRooms:     map[string]struct{}{},
				ArrivedAtPDUPosition: arrivedAt,
			}
			provider := &PDUStreamProvider{rsAPI: leaveTestRoomserver{}}
			require.Equal(t, afterBan, provider.CompleteSync(ctx, snapshot, req))
			return req
		}

		req := sync(beforeBan)
		require.NotContains(t, req.Response.Rooms.Join, room.ID)
		require.Contains(t, req.Response.Rooms.Leave, room.ID, "the ban landed after the request arrived")

		req = sync(afterBan)
		require.NotContains(t, req.Response.Rooms.Leave, room.ID, "a ban the client could have seen is not reported to a fresh sync")
	})
}
