// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"context"
	"slices"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/notifier"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// QueryErasedUsers reports no erased users for the history visibility filter.
func (leaveTestRoomserver) QueryErasedUsers(context.Context, []string) (map[string]struct{}, error) {
	return map[string]struct{}{}, nil
}

func storeRoomEvents(
	t *testing.T, db storage.Database, room *test.Room,
	hidden func(*rstypes.HeaderedEvent) bool,
) {
	t.Helper()
	ctx := t.Context()
	for _, ev := range room.Events() {
		var state []*rstypes.HeaderedEvent
		var addState []string
		if ev.StateKey() != nil {
			state = []*rstypes.HeaderedEvent{ev}
			addState = []string{ev.EventID()}
		}
		_, err := db.WriteEvent(
			ctx, ev, state, addState, nil, nil, hidden(ev),
			gomatrixserverlib.HistoryVisibilityShared,
		)
		require.NoError(t, err)
	}
}

func completeSyncRoom(t *testing.T, db storage.Database, userID, roomID string, timelineLimit ...int) *types.JoinResponse {
	t.Helper()
	ctx := t.Context()
	snapshot, err := db.NewDatabaseSnapshot(ctx)
	require.NoError(t, err)
	defer snapshot.Rollback() //nolint:errcheck

	pos, err := snapshot.MaxStreamPositionForPDUs(ctx)
	require.NoError(t, err)

	filter := synctypes.DefaultFilter()
	if len(timelineLimit) > 0 {
		filter.Room.Timeline.Limit = timelineLimit[0]
	}
	req := &types.SyncRequest{
		Context:              ctx,
		Device:               &userapi.Device{UserID: userID},
		Rooms:                map[string]string{},
		Response:             types.NewResponse(),
		Filter:               filter,
		Log:                  logrus.NewEntry(logrus.StandardLogger()),
		MembershipChanges:    map[string]struct{}{},
		NewlyJoinedRooms:     map[string]struct{}{},
		ArrivedAtPDUPosition: pos,
	}
	provider := &PDUStreamProvider{rsAPI: leaveTestRoomserver{}, notifier: notifier.NewNotifier(leaveTestRoomserver{})}
	provider.CompleteSync(ctx, snapshot, req)

	jr, ok := req.Response.Rooms.Join[roomID]
	require.True(t, ok, "room must be joined in the complete sync")
	return jr
}

func containsClientEventType(events []synctypes.ClientEvent, evType string) bool {
	return slices.ContainsFunc(events, func(ev synctypes.ClientEvent) bool {
		return ev.Type == evType
	})
}

// Keep the create in the timeline when hidden backfill follows it, but anchor
// prev_batch at the create so backward pagination cannot place the hidden rows
// above it in the client's timeline.
func TestCompleteSyncKeepsCreateAheadOfBackfilledHistory(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		create := room.Events()[0]
		m1 := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "one"})
		m2 := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "two"})
		storeRoomEvents(t, db, room, func(ev *rstypes.HeaderedEvent) bool {
			// The room's initial configuration arrived as backfill and is not
			// delivered by sync; only the create and the messages are visible.
			return ev.Type() != spec.MRoomCreate && ev.StateKey() != nil
		})

		jr := completeSyncRoom(t, db, alice.ID, room.ID)

		require.NotNil(t, jr.Timeline.PrevBatch)
		require.False(t, jr.Timeline.Limited, "all sync-visible events fit in the window")
		var got []string
		for _, ev := range jr.Timeline.Events {
			got = append(got, ev.EventID)
		}
		require.Equal(t, []string{create.EventID(), m1.EventID(), m2.EventID()}, got)
		require.NotNil(t, jr.State)
		require.NotContains(t, clientEventTypes(jr.State.Events), spec.MRoomCreate,
			"the create is already in the timeline")

		// Backward pagination from the create boundary must not return hidden
		// events whose topology is after it, or replay them above the create.
		snapshot, err := db.NewDatabaseSnapshot(t.Context())
		require.NoError(t, err)
		defer snapshot.Rollback() //nolint:errcheck

		tok, err := types.NewStreamTokenFromString(*jr.Timeline.PrevBatch)
		require.NoError(t, err)
		events, _, _, err := snapshot.GetEventsBeforeStreamPosition(
			t.Context(), tok.PDUPosition, &types.TopologyToken{}, room.ID,
			&synctypes.RoomEventFilter{Limit: 100}, true,
		)
		require.NoError(t, err)
		require.Empty(t, events, "the create is the room's history boundary")
	})
}

// The same room without a backfilled gap between the create and the next
// delivered event keeps its previous shape: the create stays in the window and
// the timeline is not marked limited.
func TestCompleteSyncKeepsCreateWhenNoBackfillGap(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "one"})
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "two"})
		storeRoomEvents(t, db, room, func(*rstypes.HeaderedEvent) bool { return false })

		jr := completeSyncRoom(t, db, alice.ID, room.ID)

		var want, got []string
		for _, ev := range room.Events() {
			want = append(want, ev.EventID())
		}
		for _, ev := range jr.Timeline.Events {
			got = append(got, ev.EventID)
		}
		require.Equal(t, want, got, "the whole room comes back in order")
		require.True(t, containsClientEventType(jr.Timeline.Events, spec.MRoomCreate),
			"with no hidden gap the create stays in the window")
		require.False(t, jr.Timeline.Limited)
	})
}

func TestCompleteSyncLargeLimitKeepsCreateWhenExcludedEventsFollow(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		for range 25 {
			room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "visible"})
		}
		hiddenTopic := room.CreateAndInsert(t, alice, "m.room.topic", map[string]any{"topic": "backfilled later"})
		storeRoomEvents(t, db, room, func(ev *rstypes.HeaderedEvent) bool {
			return ev.EventID() == hiddenTopic.EventID()
		})

		jr := completeSyncRoom(t, db, alice.ID, room.ID, 100)

		require.False(t, jr.Timeline.Limited)
		var want, got []string
		for _, ev := range room.Events() {
			if ev.EventID() != hiddenTopic.EventID() {
				want = append(want, ev.EventID())
			}
		}
		for _, ev := range jr.Timeline.Events {
			got = append(got, ev.EventID)
		}
		require.Equal(t, want, got, "large limit preserves all visible events in order")
		require.Contains(t, clientEventTypes(jr.Timeline.Events), spec.MRoomCreate)
		require.NotContains(t, clientEventTypes(jr.State.Events), spec.MRoomCreate)
	})
}

func clientEventTypes(events []synctypes.ClientEvent) []string {
	result := make([]string, 0, len(events))
	for _, ev := range events {
		result = append(result, ev.Type)
	}
	return result
}
