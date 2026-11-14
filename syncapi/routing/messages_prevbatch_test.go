// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

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
