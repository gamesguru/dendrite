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

func containsCreateEvent(events []synctypes.ClientEvent) bool {
	return slices.ContainsFunc(events, func(ev synctypes.ClientEvent) bool {
		return ev.Type == spec.MRoomCreate
	})
}

// A complete sync whose visible window starts with the room create, while
// backfilled (excluded from sync) state sits between the create and the next
// delivered event, must not leave the create in the window. Otherwise the
// prev_batch shortcut anchors at the newest event, backwards pagination
// replays the hidden history above the create, and clients stop rendering the
// create as the room's first event. The create belongs in state, and the
// window must be limited so clients paginate past the hidden history to it.
func TestCompleteSyncSplitsCreateBehindBackfilledHistory(t *testing.T) {
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
		storeRoomEvents(t, db, room, func(ev *rstypes.HeaderedEvent) bool {
			// The room's initial configuration arrived as backfill and is not
			// delivered by sync; only the create and the messages are visible.
			return ev.Type() != spec.MRoomCreate && ev.StateKey() != nil
		})

		jr := completeSyncRoom(t, db, alice.ID, room.ID)

		require.NotNil(t, jr.Timeline.PrevBatch)
		require.True(t, jr.Timeline.Limited, "the window is not the start of the room")
		for _, ev := range jr.Timeline.Events {
			require.NotEqual(t, spec.MRoomCreate, ev.Type, "create must not be in the live window")
		}
		require.NotNil(t, jr.State)
		require.True(t, containsCreateEvent(jr.State.Events), "create must be in state")

		// Backwards pagination from the window's prev_batch must deliver the
		// hidden configuration events and then the create, which is where
		// clients render it as the start of the room.
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
		require.NotEmpty(t, events)
		require.Equal(t, spec.MRoomCreate, events[len(events)-1].Type(), "create is the oldest event")
		require.True(t, slices.ContainsFunc(events, func(ev types.StreamEvent) bool {
			return ev.Type() == spec.MRoomPowerLevels && ev.ExcludeFromSync
		}), "hidden configuration history is delivered before the create")
	})
}

// A hidden event between two delivered events starts the complete-sync
// prev_batch after that gap, so backward pagination can still return the
// hidden event. The create is left to state as usual.
func TestCompleteSyncPrevBatchReachesHiddenGapAfterVisibleJoin(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		create := room.Events()[0]
		join := room.Events()[1]
		hiddenMessage := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hidden"})
		visibleMessage := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "visible"})
		visible := map[string]struct{}{
			create.EventID():         {},
			join.EventID():           {},
			visibleMessage.EventID(): {},
		}
		storeRoomEvents(t, db, room, func(ev *rstypes.HeaderedEvent) bool {
			_, ok := visible[ev.EventID()]
			return !ok
		})

		jr := completeSyncRoom(t, db, alice.ID, room.ID)
		require.NotNil(t, jr.Timeline.PrevBatch)
		require.True(t, jr.Timeline.Limited)
		require.Equal(t, []string{visibleMessage.EventID()}, clientEventIDs(jr.Timeline.Events),
			"events before the hidden gap are paginated instead of replayed")
		require.True(t, containsCreateEvent(jr.State.Events))
		require.Contains(t, clientEventIDs(jr.State.Events), join.EventID(), "the removed join remains in state")

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
		ids := make([]string, 0, len(events))
		for _, ev := range events {
			ids = append(ids, ev.EventID())
		}
		require.Contains(t, ids, hiddenMessage.EventID(),
			"the hidden event between the join and the delivered message is reachable")
		require.Contains(t, ids, join.EventID(), "the removed join is reached through pagination")
		require.NotContains(t, ids, visibleMessage.EventID(), "the delivered event is not replayed")
		require.Equal(t, create.EventID(), ids[len(ids)-1], "create is the oldest event")
	})
}

// A topology-later retained event can have an earlier stream position than
// the hidden event between it and the create. Since prev_batch is a stream
// cursor, that retained event must be left for pagination instead of being
// replayed from the timeline.
func TestCompleteSyncPrevBatchDoesNotReplayRetainedEventBeforeGapStreamPosition(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		create, join := room.Events()[0], room.Events()[1]
		hidden := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "hidden"}, test.WithDepth(join.Depth()+1), test.WithPrevEvents(join.EventID()))
		visible := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "visible"}, test.WithDepth(hidden.Depth()+1), test.WithPrevEvents(hidden.EventID()))

		write := func(ev *rstypes.HeaderedEvent, excluded bool) types.StreamPosition {
			var state []*rstypes.HeaderedEvent
			var addState []string
			if ev.StateKey() != nil {
				state = []*rstypes.HeaderedEvent{ev}
				addState = []string{ev.EventID()}
			}
			pos, writeErr := db.WriteEvent(t.Context(), ev, state, addState, nil, nil, excluded, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, writeErr)
			return pos
		}
		write(create, false)
		write(join, false)
		visiblePos := write(visible, false)
		hiddenPos := write(hidden, true)
		require.Less(t, visiblePos, hiddenPos, "the retained event is persisted before the hidden gap")

		jr := completeSyncRoom(t, db, alice.ID, room.ID)
		require.NotNil(t, jr.Timeline.PrevBatch)
		require.NotContains(t, clientEventIDs(jr.Timeline.Events), visible.EventID(),
			"an event before the stream cursor must not remain in the delivered timeline")

		tok, err := types.NewStreamTokenFromString(*jr.Timeline.PrevBatch)
		require.NoError(t, err)
		require.Equal(t, hiddenPos, tok.PDUPosition, "the inclusive cursor must include the hidden event")
		snapshot, err := db.NewDatabaseSnapshot(t.Context())
		require.NoError(t, err)
		defer snapshot.Rollback() //nolint:errcheck
		page, _, _, err := snapshot.GetEventsBeforeStreamPosition(t.Context(), tok.PDUPosition, &types.TopologyToken{}, room.ID,
			&synctypes.RoomEventFilter{Limit: 100}, true)
		require.NoError(t, err)
		pageIDs := make([]string, 0, len(page))
		for _, ev := range page {
			pageIDs = append(pageIDs, ev.EventID())
		}
		require.Contains(t, pageIDs, hidden.EventID())
		require.Contains(t, pageIDs, visible.EventID(), "the trimmed retained event remains reachable through pagination")
		require.NotContains(t, clientEventIDs(jr.Timeline.Events), visible.EventID(), "pagination does not replay a delivered event")
	})
}

// A visible event on a topology branch that starts before the hidden gap can
// still be persisted after it. The prev_batch cursor is an inclusive stream
// cursor at the gap, so backward pagination cannot return such an event. It
// must stay in the timeline rather than being discarded by a topology-only
// check.
func TestCompleteSyncKeepsEventOnEarlierBranchPersistedAfterGap(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		create, join := room.Events()[0], room.Events()[1]

		write := func(ev *rstypes.HeaderedEvent, excluded bool) types.StreamPosition {
			var state []*rstypes.HeaderedEvent
			var addState []string
			if ev.StateKey() != nil {
				state = []*rstypes.HeaderedEvent{ev}
				addState = []string{ev.EventID()}
			}
			pos, writeErr := db.WriteEvent(t.Context(), ev, state, addState, nil, nil, excluded, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, writeErr)
			return pos
		}
		write(create, false)
		write(join, false)

		// The hidden branch is deeper, but persisted first.
		hidden := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "hidden"},
			test.WithDepth(join.Depth()+8), test.WithPrevEvents(join.EventID()))
		hiddenPos := write(hidden, true)

		// A sibling branch at an earlier depth is persisted after the gap.
		early := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "early branch"},
			test.WithDepth(join.Depth()+2), test.WithPrevEvents(join.EventID()))
		earlyPos := write(early, false)

		// Another visible event on the hidden branch keeps the gap inside the
		// selected window.
		tip := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "tip"},
			test.WithDepth(hidden.Depth()+2), test.WithPrevEvents(hidden.EventID()))
		write(tip, false)

		require.Less(t, hiddenPos, earlyPos,
			"the earlier-branch event is persisted after the hidden gap")

		jr := completeSyncRoom(t, db, alice.ID, room.ID)
		ids := clientEventIDs(jr.Timeline.Events)
		require.Contains(t, ids, early.EventID(),
			"an event beyond the stream cursor must stay in the timeline")
		require.Contains(t, ids, tip.EventID())
		require.NotContains(t, ids, hidden.EventID(), "the gap event is not delivered")

		snapshot, err := db.NewDatabaseSnapshot(t.Context())
		require.NoError(t, err)
		defer snapshot.Rollback() //nolint:errcheck
		tok, err := types.NewStreamTokenFromString(*jr.Timeline.PrevBatch)
		require.NoError(t, err)
		require.Equal(t, hiddenPos, tok.PDUPosition)
		page, _, _, err := snapshot.GetEventsBeforeStreamPosition(t.Context(), tok.PDUPosition, &types.TopologyToken{}, room.ID,
			&synctypes.RoomEventFilter{Limit: 100}, true)
		require.NoError(t, err)
		for _, ev := range page {
			require.NotEqual(t, early.EventID(), ev.EventID(),
				"pagination must not replay the event retained in the timeline")
		}
	})
}

// A window whose only visible event is the create, followed by hidden
// history, still leaves the create to state and anchors prev_batch at the
// hidden history so pagination can reach it.
func TestCompleteSyncCreateOnlyWindowWithHiddenHistory(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		create := room.Events()[0]
		storeRoomEvents(t, db, room, func(ev *rstypes.HeaderedEvent) bool {
			return ev.EventID() != create.EventID()
		})

		jr := completeSyncRoom(t, db, alice.ID, room.ID)
		require.Empty(t, jr.Timeline.Events, "the sole create is left for backward pagination")
		require.NotNil(t, jr.Timeline.PrevBatch)
		require.True(t, jr.Timeline.Limited)
		require.True(t, containsCreateEvent(jr.State.Events))

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
		require.NotEmpty(t, events)
		require.Equal(t, spec.MRoomCreate, events[len(events)-1].Type())
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
		require.True(t, containsCreateEvent(jr.Timeline.Events),
			"with no hidden gap the create stays in the window")
		require.False(t, jr.Timeline.Limited)
	})
}

func TestCompleteSyncKeepsCreateWhenExcludedEventFollowsWindow(t *testing.T) {
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

		require.False(t, jr.Timeline.Limited, "excluded events after the delivered window do not split it")
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
	})
}

func TestCompleteSyncPrevBatchCrossesHiddenMessageAfterVisibleJoin(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)

		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		var join *rstypes.HeaderedEvent
		for _, ev := range room.Events() {
			if ev.Type() == spec.MRoomMember && ev.StateKeyEquals(alice.ID) {
				join = ev
				break
			}
		}
		require.NotNil(t, join)
		hidden := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "backfilled"})
		visible := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "visible after gap"})
		storeRoomEvents(t, db, room, func(ev *rstypes.HeaderedEvent) bool {
			return ev.EventID() == hidden.EventID()
		})

		jr := completeSyncRoom(t, db, alice.ID, room.ID, 100)
		require.NotNil(t, jr.Timeline.PrevBatch)
		require.Equal(t, []string{visible.EventID()}, clientEventIDs(jr.Timeline.Events),
			"events before the hidden gap are paginated instead of replayed")
		require.Contains(t, clientEventIDs(jr.State.Events), room.Events()[0].EventID(), "the removed create remains in state")
		require.Contains(t, clientEventIDs(jr.State.Events), join.EventID(), "the removed join remains in state")

		snapshot, err := db.NewDatabaseSnapshot(t.Context())
		require.NoError(t, err)
		defer snapshot.Rollback() //nolint:errcheck
		tok, err := types.NewStreamTokenFromString(*jr.Timeline.PrevBatch)
		require.NoError(t, err)
		page, _, _, err := snapshot.GetEventsBeforeStreamPosition(
			t.Context(), tok.PDUPosition, &types.TopologyToken{}, room.ID,
			&synctypes.RoomEventFilter{Limit: 100}, true,
		)
		require.NoError(t, err)
		require.True(t, slices.ContainsFunc(page, func(ev types.StreamEvent) bool {
			return ev.EventID() == hidden.EventID()
		}), "prev_batch reaches the excluded message between the join and visible message")
		require.True(t, slices.ContainsFunc(page, func(ev types.StreamEvent) bool {
			return ev.EventID() == join.EventID()
		}), "the join removed from the timeline is reached through pagination")
		require.False(t, slices.ContainsFunc(page, func(ev types.StreamEvent) bool {
			return ev.EventID() == visible.EventID()
		}), "the delivered event is not replayed")
	})
}

func clientEventIDs(events []synctypes.ClientEvent) []string {
	result := make([]string, 0, len(events))
	for _, ev := range events {
		result = append(result, ev.EventID)
	}
	return result
}

func clientEventTypes(events []synctypes.ClientEvent) []string {
	result := make([]string, 0, len(events))
	for _, ev := range events {
		result = append(result, ev.Type)
	}
	return result
}
