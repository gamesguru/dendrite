// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type leaveBoundRoomserver struct {
	api.SyncRoomserverAPI
	leaveEventID string
	membership   *rstypes.HeaderedEvent
}

func (r leaveBoundRoomserver) QueryMembershipForUser(_ context.Context, _ *api.QueryMembershipForUserRequest, res *api.QueryMembershipForUserResponse) error {
	res.RoomExists = true
	res.Membership = spec.Leave
	res.EventID = r.leaveEventID
	return nil
}

func (leaveBoundRoomserver) QuerySenderIDForUser(_ context.Context, _ spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	senderID := spec.SenderID(userID.String())
	return &senderID, nil
}

func (leaveBoundRoomserver) QueryUserIDForSender(_ context.Context, _ spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	userID, err := spec.NewUserID(string(senderID), true)
	return userID, err
}

func (r leaveBoundRoomserver) QueryMembershipAtEvent(
	_ context.Context, _ spec.RoomID, eventIDs []string, _ spec.SenderID,
) (map[string]*rstypes.HeaderedEvent, error) {
	memberships := make(map[string]*rstypes.HeaderedEvent, len(eventIDs))
	for _, eventID := range eventIDs {
		memberships[eventID] = r.membership
	}
	return memberships, nil
}

// A /sync prev_batch issued before hs1 backfilled a federated room can be a
// stream token at position 2 (for example, when PostgreSQL assigned positions
// 1 and 2 to events in other rooms). It is anchored at the depth of hs1's join,
// which is also the join's own depth because the history sits below it. The
// join is therefore a backward extremity at the cursor's depth, not strictly
// below it, and a backward /messages from that token must still treat it as
// reachable and backfill the history below it. Otherwise clients see an empty
// page and never reach the history (Complement
// TestFederationBackfilledHistoryReachableFromPrevBatch).
func TestBackwardMessagesFromPrevBatchReachesFederatedJoinHistory(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewSyncServerDatasource(context.Background(), cm, &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		})
		require.NoError(t, err)

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		history := room.Events()
		// The join is deeper than the remote room's history: hs1's server had
		// not seen those events, so its depth counter is further along.
		bob := test.NewUser(t)
		join := room.CreateEvent(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join},
			test.WithStateKey(bob.ID), test.WithDepth(35), test.WithPrevEvents(history[len(history)-1].EventID()))

		ctx := context.Background()
		// PostgreSQL may assign early global PDU positions to events in other
		// rooms. Put two events there so the join is at position 3, matching the
		// s2 prev_batch emitted by the Complement failure.
		paddingUser := test.NewUser(t)
		paddingRoom := test.NewRoom(t, paddingUser)
		for i := 0; i < 2; i++ {
			padding := paddingRoom.CreateEvent(t, paddingUser, "m.room.message", map[string]any{"body": "padding"})
			_, err = db.WriteEvent(ctx, padding, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
		}
		_, err = db.WriteEvent(ctx, join, []*rstypes.HeaderedEvent{join}, []string{join.EventID()}, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)

		snapshot, err := db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		defer func() { _ = snapshot.Rollback() }()

		// The prev_batch token anchored by StreamToTopologicalPosition.
		from, err := snapshot.StreamToTopologicalPosition(ctx, room.ID, 2, true)
		require.NoError(t, err)
		assert.Equal(t, types.StreamPosition(35), from.Depth,
			"a stream token immediately before the join is anchored at the join's depth")
		assert.Equal(t, types.StreamPosition(2), from.PDUPosition)

		// The join is the backward extremity; its missing parent is the history.
		extremities, err := snapshot.BackwardExtremitiesForRoom(ctx, room.ID)
		require.NoError(t, err)
		require.Contains(t, extremities, join.EventID())

		federation := &sequencedBackfillAPI{batches: [][]*rstypes.HeaderedEvent{history}}
		req := &messagesReq{
			ctx:              ctx,
			db:               db,
			snapshot:         snapshot,
			rsAPI:            federation,
			cfg:              &config.SyncAPI{Matrix: &config.Global{ServerName: "test"}},
			roomID:           room.ID,
			from:             &from,
			fromStream:       &types.StreamingToken{PDUPosition: 2},
			to:               &types.TopologyToken{},
			device:           &userapi.Device{UserID: "@alice:test"},
			backwardOrdering: true,
			filter:           &synctypes.RoomEventFilter{Limit: 10},
		}

		events, err := req.handleEmptyEventsSlice()
		require.NoError(t, err, "the join's gap must be backfilled, not treated as the end of history")
		assert.Equal(t, 1, federation.calls, "one federation batch holds the whole history")

		got := make(map[string]struct{}, len(events))
		for _, ev := range events {
			got[ev.EventID()] = struct{}{}
		}
		require.NotEmpty(t, got)
		for _, ev := range history {
			assert.Contains(t, got, ev.EventID(), "history below the federated join must be reachable from the prev_batch")
		}
	})
}

// A backward /messages request for a user who has left is bounded at the leave
// event. A later event must not re-anchor that bound upward, or the history
// before the leave becomes unreachable.
func TestBackwardMessagesLeaveBoundIgnoresLatePostLeaveEvent(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewSyncServerDatasource(context.Background(), cm, &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		})
		require.NoError(t, err)

		alice := test.NewUser(t)
		bob := test.NewUser(t)
		room := test.NewRoom(t, alice)
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join},
			test.WithStateKey(bob.ID))
		beforeLeave := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "before leave"})
		leave := room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Leave},
			test.WithStateKey(alice.ID))
		afterLeave := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "after leave"})

		ctx := context.Background()
		for _, ev := range room.Events() {
			var state []*rstypes.HeaderedEvent
			var addState []string
			if ev.StateKey() != nil {
				state = []*rstypes.HeaderedEvent{ev}
				addState = []string{ev.EventID()}
			}
			_, err = db.WriteEvent(ctx, ev, state, addState, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
		}

		snapshot, err := db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		defer func() { _ = snapshot.Rollback() }()
		from, err := snapshot.EventPositionInTopology(ctx, leave.EventID())
		require.NoError(t, err)
		late, err := snapshot.EventPositionInTopology(ctx, afterLeave.EventID())
		require.NoError(t, err)
		require.Greater(t, late.Depth, from.Depth, "the late event would raise the leave bound")
		leaveDepth := from.Depth

		req := &messagesReq{
			ctx:              ctx,
			db:               db,
			snapshot:         snapshot,
			roomID:           room.ID,
			from:             &from,
			to:               &types.TopologyToken{},
			backwardOrdering: true,
			leaveBound:       true,
			filter:           &synctypes.RoomEventFilter{Limit: 10},
		}
		require.NoError(t, req.reanchorBackwardCursor(ctx))
		require.Equal(t, leaveDepth, req.from.Depth, "the leave bound was not re-anchored")

		events, _, _, err := snapshot.GetEventsInTopologicalRange(
			ctx, req.from, req.to, room.ID, req.filter, true,
		)
		require.NoError(t, err)
		got := make(map[string]struct{}, len(events))
		for _, ev := range events {
			got[ev.EventID()] = struct{}{}
		}
		require.Contains(t, got, beforeLeave.EventID(), "pre-leave history is still returned")
		require.NotContains(t, got, afterLeave.EventID(), "the post-leave event is not returned from the leave bound")
	})
}

func TestMessagesHandlerUsesLeaveBoundInsteadOfClientStreamCursor(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewSyncServerDatasource(context.Background(), cm, &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		})
		require.NoError(t, err)

		alice := test.NewUser(t)
		bob := test.NewUser(t)
		room := test.NewRoom(t, alice)
		bobJoin := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		beforeLeave := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "before leave"})
		leave := room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Leave}, test.WithStateKey(alice.ID))
		afterLeave := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "after leave"})

		var afterLeavePosition types.StreamPosition
		var beforeLeavePosition types.StreamPosition
		for _, ev := range room.Events() {
			var state []*rstypes.HeaderedEvent
			var addState []string
			if ev.StateKey() != nil {
				state = []*rstypes.HeaderedEvent{ev}
				addState = []string{ev.EventID()}
			}
			position, writeErr := db.WriteEvent(context.Background(), ev, state, addState, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, writeErr)
			if ev.EventID() == afterLeave.EventID() {
				afterLeavePosition = position
			}
			if ev.EventID() == beforeLeave.EventID() {
				beforeLeavePosition = position
			}
		}

		from := types.StreamingToken{PDUPosition: afterLeavePosition}.String()
		req := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/messages?dir=b&from=%s&limit=10", from), nil)
		device := &userapi.Device{UserID: alice.ID}
		response := OnIncomingMessagesRequest(
			req, db, room.ID, device,
			leaveBoundRoomserver{leaveEventID: leave.EventID(), membership: bobJoin},
			&config.SyncAPI{}, nil, nil,
		)
		require.Equal(t, http.StatusOK, response.Code)
		body, ok := response.JSON.(messagesResp)
		require.True(t, ok)
		got := make(map[string]struct{}, len(body.Chunk))
		for _, ev := range body.Chunk {
			got[ev.EventID] = struct{}{}
		}
		require.Contains(t, got, beforeLeave.EventID())
		require.NotContains(t, got, afterLeave.EventID())
		require.Empty(t, body.StartStream, "the overridden client stream cursor must not be returned")

		// An earlier client cursor is already the tighter bound. The leave
		// path must not widen it and replay the event at that cursor.
		earlierFrom := types.StreamingToken{PDUPosition: beforeLeavePosition}.String()
		earlierReq := httptest.NewRequest(http.MethodGet, fmt.Sprintf("/messages?dir=b&from=%s&limit=10", earlierFrom), nil)
		earlierResponse := OnIncomingMessagesRequest(
			earlierReq, db, room.ID, device,
			leaveBoundRoomserver{leaveEventID: leave.EventID(), membership: bobJoin},
			&config.SyncAPI{}, nil, nil,
		)
		require.Equal(t, http.StatusOK, earlierResponse.Code)
		earlierBody, ok := earlierResponse.JSON.(messagesResp)
		require.True(t, ok)
		for _, ev := range earlierBody.Chunk {
			require.NotEqual(t, beforeLeave.EventID(), ev.EventID,
				"the earlier client cursor must not be widened to the leave event")
			require.NotEqual(t, afterLeave.EventID(), ev.EventID)
		}
	})
}
