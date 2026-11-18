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
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// boundarySyncEnv wires the database and the small operations the forward
// boundary tests share: storing events, reading the stream position, and
// running an incremental sync.
type boundarySyncEnv struct {
	t   *testing.T
	db  storage.Database
	ctx context.Context
}

func newBoundarySyncEnv(t *testing.T, dbType test.DBType) *boundarySyncEnv {
	t.Helper()
	cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
	t.Cleanup(closeDB)
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
	require.NoError(t, err)
	return &boundarySyncEnv{t: t, db: db, ctx: processCtx.Context()}
}

func (e *boundarySyncEnv) write(ev *rstypes.HeaderedEvent) {
	e.t.Helper()
	var state []*rstypes.HeaderedEvent
	var addState []string
	if ev.StateKey() != nil {
		state, addState = []*rstypes.HeaderedEvent{ev}, []string{ev.EventID()}
	}
	_, err := e.db.WriteEvent(e.ctx, ev, state, addState, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
	require.NoError(e.t, err)
}

// writeAllExcept stores every event but the named ones, which lets a test
// store a backfill last while keeping its place in the room's DAG.
func (e *boundarySyncEnv) writeAllExcept(events []*rstypes.HeaderedEvent, skip ...*rstypes.HeaderedEvent) {
	e.t.Helper()
	skipped := make(map[string]struct{}, len(skip))
	for _, ev := range skip {
		skipped[ev.EventID()] = struct{}{}
	}
	for _, ev := range events {
		if _, ok := skipped[ev.EventID()]; ok {
			continue
		}
		e.write(ev)
	}
}

func (e *boundarySyncEnv) position() types.StreamPosition {
	e.t.Helper()
	snapshot, err := e.db.NewDatabaseSnapshot(e.ctx)
	require.NoError(e.t, err)
	defer snapshot.Rollback() //nolint:errcheck
	pos, err := snapshot.MaxStreamPositionForPDUs(e.ctx)
	require.NoError(e.t, err)
	return pos
}

func (e *boundarySyncEnv) sync(userID string, since, to types.StreamPosition) (*types.SyncRequest, types.StreamPosition) {
	e.t.Helper()
	snapshot, err := e.db.NewDatabaseSnapshot(e.ctx)
	require.NoError(e.t, err)
	defer snapshot.Rollback() //nolint:errcheck
	req := &types.SyncRequest{
		Context:           e.ctx,
		Device:            &userapi.Device{UserID: userID},
		Rooms:             map[string]string{},
		Response:          types.NewResponse(),
		Filter:            synctypes.DefaultFilter(),
		Log:               logrus.NewEntry(logrus.StandardLogger()),
		MembershipChanges: map[string]struct{}{},
		NewlyJoinedRooms:  map[string]struct{}{},
	}
	provider := &PDUStreamProvider{rsAPI: leaveTestRoomserver{}}
	return req, provider.IncrementalSync(e.ctx, snapshot, req, since, to)
}

func timelineEventIDs(jr *types.JoinResponse) []string {
	ids := make([]string, 0, len(jr.Timeline.Events))
	for _, ev := range jr.Timeline.Events {
		ids = append(ids, ev.EventID)
	}
	return ids
}

// A backfilled state event stored after the room's normal activity is the last
// row before the token, but it is not the furthest the client has been sent.
// The boundary is the highest delivered topological position, so a later
// backfill that sits below that frontier is still history. A message from the
// same fork is delivered, because it may be new information.
func TestIncrementalSyncBoundaryIgnoresLateStoredHistory(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		env := newBoundarySyncEnv(t, dbType)
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "a"})
		backfilled := room.CreateAndInsert(t, alice, "m.room.topic", map[string]any{"topic": "backfilled"}, test.WithStateKey(""))
		forked := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "fork"})
		mid := room.CreateAndInsert(t, alice, "m.room.topic", map[string]any{"topic": "mid"}, test.WithStateKey(""))
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "c"})
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "d"})

		// The backfills are held back while the rest of the room arrives.
		env.writeAllExcept(room.Events(), backfilled, forked, mid)
		// The first is stored last before the token, at a low topological
		// position. It must not lower the boundary for the events after it.
		env.write(backfilled)
		since := env.position()

		// These arrive after the token but still below the room's frontier. The
		// state event is history; the message is new and must be delivered.
		env.write(forked)
		env.write(mid)
		live := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "live"})
		env.write(live)
		to := env.position()
		require.Greater(t, to, since)

		req, pos := env.sync(alice.ID, since, to)
		require.Equal(t, to, pos)

		jr, ok := req.Response.Rooms.Join[room.ID]
		require.True(t, ok)
		timeline := timelineEventIDs(jr)
		require.NotContains(t, timeline, mid.EventID(), "a late state event below the frontier is history")
		require.Contains(t, timeline, forked.EventID(), "a late message is still new information")
		require.Contains(t, timeline, live.EventID(), "new activity is still delivered")
	})
}

// A late member event is filtered from the timeline, but the state block still
// carries it. The room must still be delivered, and the filtered membership
// must not be reported as new activity.
func TestIncrementalSyncFilteredMemberStillArrivesInState(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		env := newBoundarySyncEnv(t, dbType)
		alice, bob := test.NewUser(t), test.NewUser(t)
		room := test.NewRoom(t, alice)
		bobJoin := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "later"})

		// The member event is stored last, behind the room's frontier.
		env.writeAllExcept(room.Events(), bobJoin)
		since := env.position()
		env.write(bobJoin)
		to := env.position()
		require.Greater(t, to, since)

		req, pos := env.sync(alice.ID, since, to)
		require.Equal(t, to, pos)

		jr, ok := req.Response.Rooms.Join[room.ID]
		require.True(t, ok, "the state block carries the late member event")
		require.NotContains(t, timelineEventIDs(jr), bobJoin.EventID(), "the timeline copy is history")
		stateIDs := make([]string, 0, len(jr.State.Events))
		for _, ev := range jr.State.Events {
			stateIDs = append(stateIDs, ev.EventID)
		}
		require.Contains(t, stateIDs, bobJoin.EventID(), "state still carries the membership")
		require.NotContains(t, req.MembershipChanges, bob.ID, "a filtered member event is not new activity")
	})
}

// A create stored after the rest of its room has a new stream position but the
// first topological position. A forward incremental sync must not deliver it as
// new activity, while a genuinely later event still arrives.
func TestIncrementalSyncDropsLateEventBeforeDeliveryBoundary(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		env := newBoundarySyncEnv(t, dbType)
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		events := room.Events()
		require.Equal(t, spec.MRoomCreate, events[0].Type())

		// Everything but the create arrives normally, so the client's token is
		// already past the room's live state.
		env.writeAllExcept(events, events[0])
		since := env.position()

		// The create is stored late, as a backfilled event, and gets a stream
		// position newer than anything the client has seen while its place in
		// the room stays at the start.
		env.write(events[0])

		// Activity that really is new must still be delivered.
		live := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "after"})
		env.write(live)
		to := env.position()
		require.Greater(t, to, since)

		req, pos := env.sync(alice.ID, since, to)
		require.Equal(t, to, pos)

		jr, ok := req.Response.Rooms.Join[room.ID]
		require.True(t, ok, "the room still carries the new activity")
		timeline := timelineEventIDs(jr)
		require.NotContains(t, timeline, events[0].EventID(), "a late event behind the frontier is history")
		require.Contains(t, timeline, live.EventID(), "new activity is still delivered")
	})
}

// When every state event in the timeline is behind the frontier, the timeline
// is empty but the state block still carries the change. The sync must advance
// to the end of the range so the event is not fetched again on every sync.
func TestIncrementalSyncAdvancesPastAFullyFilteredTimeline(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		env := newBoundarySyncEnv(t, dbType)
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		withheld := room.CreateAndInsert(t, alice, "m.room.topic", map[string]any{"topic": "withheld"}, test.WithStateKey(""))
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "later"})

		env.writeAllExcept(room.Events(), withheld)
		since := env.position()
		env.write(withheld)
		to := env.position()
		require.Greater(t, to, since)

		req, pos := env.sync(alice.ID, since, to)
		require.Equal(t, to, pos, "the token advances even though the timeline had nothing to deliver")

		jr, ok := req.Response.Rooms.Join[room.ID]
		require.True(t, ok, "the state change still arrives")
		require.NotContains(t, timelineEventIDs(jr), withheld.EventID(), "the timeline is filtered")
		stateIDs := make([]string, 0, len(jr.State.Events))
		for _, ev := range jr.State.Events {
			stateIDs = append(stateIDs, ev.EventID)
		}
		require.Contains(t, stateIDs, withheld.EventID(), "state carries the event")
	})
}
