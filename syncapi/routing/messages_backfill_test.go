// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
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

type backfillOnlyAPI struct {
	api.SyncRoomserverAPI
	events []*rstypes.HeaderedEvent
}

func (f *backfillOnlyAPI) PerformBackfill(_ context.Context, _ *api.PerformBackfillRequest, res *api.PerformBackfillResponse) error {
	res.Events = f.events
	res.HistoryVisibility = gomatrixserverlib.HistoryVisibilityShared
	return nil
}

type sequencedBackfillAPI struct {
	api.SyncRoomserverAPI
	batches [][]*rstypes.HeaderedEvent
	calls   int
}

func (f *sequencedBackfillAPI) PerformBackfill(_ context.Context, _ *api.PerformBackfillRequest, res *api.PerformBackfillResponse) error {
	if f.calls < len(f.batches) {
		res.Events = f.batches[f.calls]
	}
	f.calls++
	res.HistoryVisibility = gomatrixserverlib.HistoryVisibilityShared
	return nil
}

func TestIsTerminalBackfillFailure(t *testing.T) {
	terminal := &api.BackfillUnavailableError{Retryable: false, Err: errors.New("not found")}
	transient := &api.BackfillUnavailableError{Retryable: true, Err: errors.New("timeout")}

	assert.True(t, isTerminalBackfillFailure(terminal))
	assert.True(t, isTerminalBackfillFailure(fmt.Errorf("PerformBackfill failed: %w", terminal)))
	assert.False(t, isTerminalBackfillFailure(transient))
	assert.False(t, isTerminalBackfillFailure(fmt.Errorf("PerformBackfill failed: %w", transient)))
	assert.False(t, isTerminalBackfillFailure(errors.New("database unavailable")))
	assert.False(t, isTerminalBackfillFailure(nil))
}

func TestBackfillEventsPurgesFilledBackwardExtremities(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		older := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "older"})
		newer := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "newer"})

		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewSyncServerDatasource(context.Background(), cm, &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		})
		require.NoError(t, err)

		ctx := context.Background()
		// Store the newer event without its prev event, as happens when a
		// re-join arrives before the history behind it.
		_, err = db.WriteEvent(ctx, newer, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)

		snapshot, err := db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		extremities, err := snapshot.BackwardExtremitiesForRoom(ctx, room.ID)
		require.NoError(t, err)
		_ = snapshot.Rollback()
		require.Contains(t, extremities, newer.EventID())

		// Backfilling the prev event makes the newer event's marker stale.
		_, err = backfillEvents(ctx, &backfillOnlyAPI{events: []*rstypes.HeaderedEvent{older}},
			db, "hs2", "hs2", room.ID,
			map[string][]string{newer.EventID(): {older.EventID()}}, 10)
		require.NoError(t, err)

		snapshot, err = db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		defer func() { _ = snapshot.Rollback() }()
		extremities, err = snapshot.BackwardExtremitiesForRoom(ctx, room.ID)
		require.NoError(t, err)
		assert.NotContains(t, extremities, newer.EventID(), "filled extremity must be cleared")
		assert.Contains(t, extremities, older.EventID(), "the new boundary extremity must remain")
	})
}

func TestBackwardBackfillContinuesUntilGapClosesOrRoundLimit(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewSyncServerDatasource(context.Background(), cm, &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		})
		require.NoError(t, err)
		for _, tc := range []struct {
			name       string
			batchCount int
			noAnchor   bool
			wantOpen   bool
			wantCalls  int
		}{
			{name: "closes in second round", batchCount: 2, wantCalls: 2},
			{name: "remains open at round limit", batchCount: maxBackwardBackfillRounds + 1, wantOpen: true, wantCalls: maxBackwardBackfillRounds},
			// A chain that has not reached stored history strands nothing: the
			// next page backfills onward, so the page is complete after one batch.
			{name: "unfetched chain with nothing stored below", batchCount: maxBackwardBackfillRounds + 1, noAnchor: true, wantCalls: 1},
		} {
			t.Run(tc.name, func(t *testing.T) {
				alice := test.NewUser(t)
				room := test.NewRoom(t, alice)
				anchor := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "stored anchor"})
				chain := make([]*rstypes.HeaderedEvent, tc.batchCount)
				for i := range chain {
					prevEvents := []string(nil)
					if i > 0 {
						prevEvents = []string{chain[i-1].EventID()}
					}
					chain[i] = room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": fmt.Sprintf("chain %d", i)}, test.WithPrevEvents(prevEvents...))
				}
				child := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "stored child"}, test.WithPrevEvents(chain[len(chain)-1].EventID()))

				ctx := context.Background()
				if !tc.noAnchor {
					_, err = db.WriteEvent(ctx, anchor, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
					require.NoError(t, err)
				}
				_, err = db.WriteEvent(ctx, child, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
				require.NoError(t, err)
				snapshot, err := db.NewDatabaseTransaction(ctx)
				require.NoError(t, err)
				defer func() { _ = snapshot.Rollback() }()
				extremities, err := snapshot.BackwardExtremitiesForRoom(ctx, room.ID)
				require.NoError(t, err)
				require.Contains(t, extremities, child.EventID())

				batches := make([][]*rstypes.HeaderedEvent, 0, len(chain))
				for i := len(chain) - 1; i >= 0; i-- {
					batches = append(batches, []*rstypes.HeaderedEvent{chain[i]})
				}
				federation := &sequencedBackfillAPI{batches: batches}
				req := &messagesReq{
					ctx:              ctx,
					db:               db,
					snapshot:         snapshot,
					rsAPI:            federation,
					cfg:              &config.SyncAPI{Matrix: &config.Global{ServerName: "test"}},
					roomID:           room.ID,
					device:           &userapi.Device{UserID: "@alice:test"},
					backwardOrdering: true,
					from:             &types.TopologyToken{Depth: 0},
				}
				fetched, incomplete, err := req.backfillBackwardUntilComplete(extremities, nil, 10)
				require.NoError(t, err)
				assert.Equal(t, tc.wantOpen, incomplete)
				assert.Equal(t, tc.wantCalls, federation.calls)
				assert.Len(t, fetched, tc.wantCalls)
			})
		}
	})
}

func TestBackwardBackfillMissingParentWithoutOlderStoredHistoryIsComplete(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		missingParent := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "not fetched"})
		backfilled := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "backfilled"},
			test.WithPrevEvents(missingParent.EventID()))
		child := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "stored child"},
			test.WithPrevEvents(backfilled.EventID()))

		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewSyncServerDatasource(context.Background(), cm, &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		})
		require.NoError(t, err)

		ctx := context.Background()
		_, err = db.WriteEvent(ctx, child, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)
		snapshot, err := db.NewDatabaseTransaction(ctx)
		require.NoError(t, err)
		defer func() { _ = snapshot.Rollback() }()
		extremities, err := snapshot.BackwardExtremitiesForRoom(ctx, room.ID)
		require.NoError(t, err)

		req := &messagesReq{
			ctx:              ctx,
			db:               db,
			snapshot:         snapshot,
			rsAPI:            &sequencedBackfillAPI{batches: [][]*rstypes.HeaderedEvent{{backfilled}}},
			cfg:              &config.SyncAPI{Matrix: &config.Global{ServerName: "test"}},
			roomID:           room.ID,
			device:           &userapi.Device{UserID: "@alice:test"},
			backwardOrdering: true,
			from:             &types.TopologyToken{Depth: 0},
		}
		fetched, incomplete, err := req.backfillBackwardUntilComplete(extremities, nil, 10)
		require.NoError(t, err)
		require.Len(t, fetched, 1)
		assert.False(t, incomplete, "an unfetched parent with no older stored history does not block pagination")
	})
}

// Events backfilled after a client's sync token get stream positions above that
// token. Paginating back from the token must still return them, otherwise every
// refresh finds nothing locally and backfills over federation again.
func TestBackfilledEventsVisibleFromEarlierStreamToken(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		room := test.NewRoom(t, alice)
		older := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "older"})
		newer := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "newer"})
		after := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "after the token"})

		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewSyncServerDatasource(context.Background(), cm, &config.DatabaseOptions{
			ConnectionString: config.DataSource(connStr),
		})
		require.NoError(t, err)

		ctx := context.Background()
		tokenPos, err := db.WriteEvent(ctx, newer, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)

		_, err = db.WriteEvent(ctx, after, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)

		// Backfill happens after the client obtained its sync token.
		_, err = backfillEvents(ctx, &backfillOnlyAPI{events: []*rstypes.HeaderedEvent{older}},
			db, "hs2", "hs2", room.ID,
			map[string][]string{newer.EventID(): {older.EventID()}}, 10)
		require.NoError(t, err)

		// The same event can be written again by the normal sync API path, for
		// example when a gap is filled with /get_missing_events and the event is
		// replayed as an old event with exclude_from_sync=false. That must not
		// clear the backfill marker, or the history becomes unreachable again.
		_, err = db.WriteEvent(ctx, older, nil, nil, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
		require.NoError(t, err)

		snapshot, err := db.NewDatabaseSnapshot(ctx)
		require.NoError(t, err)
		defer func() { _ = snapshot.Rollback() }()
		events, _, _, err := snapshot.GetEventsBeforeStreamPosition(ctx, tokenPos, &types.TopologyToken{}, room.ID,
			&synctypes.RoomEventFilter{Limit: 10}, true)
		require.NoError(t, err)
		ids := make([]string, 0, len(events))
		for _, ev := range events {
			ids = append(ids, ev.EventID())
		}
		assert.Contains(t, ids, older.EventID(), "backfilled event must not be hidden by the stream-position cap")
		assert.NotContains(t, ids, after.EventID(), "ordinary events after the token must stay capped")
	})
}

func TestForwardStreamTokenEndStaysAtLastIncludedEvent(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	backfilled := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "backfilled"}, test.WithDepth(1386))
	live := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "live low-depth event"}, test.WithDepth(79))

	livePosition := types.TopologyToken{Depth: 79, PDUPosition: 127272}
	snapshot := &extremitySnapshot{positions: map[string]types.TopologyToken{
		live.EventID(): livePosition,
	}}
	req := messagesReq{
		snapshot:   snapshot,
		fromStream: &types.StreamingToken{PDUPosition: 127253},
	}

	filtered, end, err := req.filterByStreamToken(context.Background(), []types.StreamEvent{
		{HeaderedEvent: live, StreamPosition: 127272},
		{HeaderedEvent: backfilled, StreamPosition: 127092, ExcludeFromSync: true},
	}, types.TopologyToken{Depth: 1386, PDUPosition: 127092})
	require.NoError(t, err)
	require.Len(t, filtered, 1)
	assert.Equal(t, live.EventID(), filtered[0].EventID())
	assert.Equal(t, livePosition, end, "the end cursor must not advance over filtered backfill")
}

func TestForwardStreamTokenKeepsCursorPastAlreadySyncedEvents(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	live := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "already synced"})
	req := messagesReq{
		snapshot:   &extremitySnapshot{},
		fromStream: &types.StreamingToken{PDUPosition: 100},
	}
	originalEnd := types.TopologyToken{Depth: 20, PDUPosition: 120}
	filtered, end, err := req.filterByStreamToken(context.Background(), []types.StreamEvent{
		{HeaderedEvent: live, StreamPosition: 90},
	}, originalEnd)
	require.NoError(t, err)
	assert.Empty(t, filtered)
	assert.Equal(t, originalEnd, end, "already-synced events must not pull the cursor backward and be replayed")
}

// extremitySnapshot stubs the topology lookups used to decide which backward
// extremities can hold events older than a request.
type extremitySnapshot struct {
	storage.DatabaseTransaction
	positions   map[string]types.TopologyToken
	extremities map[string][]string
}

func (s *extremitySnapshot) EventPositionInTopology(_ context.Context, eventID string) (types.TopologyToken, error) {
	pos, ok := s.positions[eventID]
	if !ok {
		return types.TopologyToken{}, errors.New("event not in topology")
	}
	return pos, nil
}

func (s *extremitySnapshot) BackwardExtremitiesForRoom(context.Context, string) (map[string][]string, error) {
	return s.extremities, nil
}

// backfillProbe records whether a backfill was attempted at all.
type backfillProbe struct {
	api.SyncRoomserverAPI
	calls int
}

func (p *backfillProbe) PerformBackfill(context.Context, *api.PerformBackfillRequest, *api.PerformBackfillResponse) error {
	p.calls++
	return nil
}

// emptyExtremityDB makes the backward-extremity cleanup a no-op.
type emptyExtremityDB struct{ storage.Database }

func (emptyExtremityDB) Events(context.Context, []string) ([]*rstypes.HeaderedEvent, error) {
	return nil, nil
}

// Backfilling only from extremities at or below the requested depth keeps a
// backward page from returning events newer than the token it starts from.
// Extremities elsewhere in the room belong to different gaps.
func TestReachableBackwardExtremities(t *testing.T) {
	extremities := map[string][]string{
		"$at-cursor":        {"$a"},
		"$same-depth-newer": {"$same-depth-prev"},
		"$older":            {"$b"},
		"$newer":            {"$c"},
		"$unknown":          {"$d"},
	}
	snapshot := &extremitySnapshot{
		positions: map[string]types.TopologyToken{
			"$at-cursor":        {Depth: 2900},
			"$same-depth-newer": {Depth: 2900, PDUPosition: 101},
			"$older":            {Depth: 2800},
			"$newer":            {Depth: 2901},
		},
	}
	r := &messagesReq{
		ctx:              context.Background(),
		snapshot:         snapshot,
		backwardOrdering: true,
		from:             &types.TopologyToken{Depth: 2900, PDUPosition: 100},
	}

	assert.Equal(t, map[string][]string{"$at-cursor": {"$a"}},
		r.reachableBackwardExtremities(extremities),
		"only gaps strictly before the full topology cursor can hold older events, and the nearest one is filled first")

	// A cursor at stream position 0 is the prev_batch of a room joined over
	// federation: it carries the depth of the first stored event, which is the
	// extremity itself and sits at a later stream position. Its parents are older.
	r.from = &types.TopologyToken{Depth: 2900, PDUPosition: 0}
	assert.Equal(t, map[string][]string{"$at-cursor": {"$a"}, "$same-depth-newer": {"$same-depth-prev"}},
		r.reachableBackwardExtremities(extremities),
		"extremities at the anchored depth are reachable from a stream-position-0 cursor; deeper ones are not")

	r.from = &types.TopologyToken{Depth: 2, PDUPosition: 0}
	assert.Equal(t, map[string][]string{"$unknown": {"$d"}}, r.reachableBackwardExtremities(extremities),
		"a stream-position-0 cursor at depth 2 still reaches no deeper gap")

	// A stream-token cursor (a /sync prev_batch) keeps the token's own stream
	// position, which on a database whose first positions went to other data is
	// not 0 but still precedes the join.
	r.from = &types.TopologyToken{Depth: 2900, PDUPosition: 2}
	r.fromStream = &types.StreamingToken{PDUPosition: 2}
	assert.Equal(t, map[string][]string{"$at-cursor": {"$a"}, "$same-depth-newer": {"$same-depth-prev"}},
		r.reachableBackwardExtremities(extremities),
		"a stream-token cursor reaches same-depth extremities at later stream positions")
	r.fromStream = nil

	r.from = &types.TopologyToken{Depth: 2, PDUPosition: 100}
	assert.Equal(t, map[string][]string{"$unknown": {"$d"}}, r.reachableBackwardExtremities(extremities),
		"a depth-2 request reaches no known gap; an extremity with no readable position is kept")

	r.from = &types.TopologyToken{Depth: 0, PDUPosition: 100}
	assert.Equal(t, map[string][]string{"$newer": {"$c"}}, r.reachableBackwardExtremities(extremities),
		"depth 0 is before every stored event (a prev_batch of a federated join): every gap is older, and the deepest is filled first")

	r.backwardOrdering = false
	assert.Equal(t, extremities, r.reachableBackwardExtremities(extremities), "forward requests are unaffected")
}

// A backward request at the room's start must end the history instead of
// backfilling a newer gap and returning its events as if they were older.
func TestHandleEmptyEventsSliceDoesNotBackfillNewerGaps(t *testing.T) {
	snapshot := &extremitySnapshot{
		positions:   map[string]types.TopologyToken{"$recent-gap": {Depth: 2800}},
		extremities: map[string][]string{"$recent-gap": {"$prev"}},
	}
	for _, depth := range []types.StreamPosition{1, 2} {
		probe := &backfillProbe{}
		r := &messagesReq{
			ctx:              context.Background(),
			db:               emptyExtremityDB{},
			snapshot:         snapshot,
			rsAPI:            probe,
			roomID:           "!room:test",
			device:           &userapi.Device{UserID: "@alice:test"},
			backwardOrdering: true,
			from:             &types.TopologyToken{Depth: depth, PDUPosition: 10},
			filter:           &synctypes.RoomEventFilter{Limit: 10},
		}

		events, err := r.handleEmptyEventsSlice()
		require.NoError(t, err)
		assert.Empty(t, events)
		assert.Zero(t, probe.calls, "a depth-%d request must not backfill a depth-2800 gap", depth)
	}
}

// fullPageSnapshot stubs the lookups handleNonEmptyEventsSlice makes for a
// page that already contains a backward extremity.
type fullPageSnapshot struct {
	storage.DatabaseTransaction
	extremities map[string][]string
	positions   map[string]types.TopologyToken
}

func (s *fullPageSnapshot) BackwardExtremitiesForRoom(context.Context, string) (map[string][]string, error) {
	return s.extremities, nil
}

func (s *fullPageSnapshot) EventPositionInTopology(_ context.Context, eventID string) (types.TopologyToken, error) {
	pos, ok := s.positions[eventID]
	if !ok {
		return types.TopologyToken{}, errors.New("event not in topology")
	}
	return pos, nil
}

func (s *fullPageSnapshot) StreamEventsToEvents(_ context.Context, _ *userapi.Device, in []types.StreamEvent, _ api.SyncRoomserverAPI) []*rstypes.HeaderedEvent {
	out := make([]*rstypes.HeaderedEvent, 0, len(in))
	for _, ev := range in {
		out = append(out, ev.HeaderedEvent)
	}
	return out
}

// stubWriteDB accepts the events backfill stores without persisting them.
type stubWriteDB struct{ storage.Database }

func (stubWriteDB) Events(context.Context, []string) ([]*rstypes.HeaderedEvent, error) {
	return nil, nil
}

func (stubWriteDB) WriteEvent(context.Context, *rstypes.HeaderedEvent, []*rstypes.HeaderedEvent,
	[]string, []string, *api.TransactionID, bool, gomatrixserverlib.HistoryVisibility, ...string,
) (types.StreamPosition, error) {
	return 0, nil
}

// A full page that contains the extremity can still have a gap just below it.
// Backfilled events at or above the page's oldest event would be newer than the
// page's end token, so a later page would start below them and skip them. They
// must be returned with this page instead. Events older than the page come back
// on a later page and are left out here.
func TestFullPageBackfillKeepsGapEventsAboveThePageEnd(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	pageOldest := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "page oldest"})
	pageMiddle := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "page middle"})
	extremity := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "extremity"},
		test.WithPrevEvents(pageMiddle.EventID()))
	aboveGap := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "gap at the page end"})
	belowGap := room.Events()[0]

	page := []types.StreamEvent{
		{HeaderedEvent: extremity},
		{HeaderedEvent: pageMiddle},
		{HeaderedEvent: pageOldest},
	}
	positions := make(map[string]types.TopologyToken, len(page)+2)
	for i, ev := range page {
		positions[ev.EventID()] = types.TopologyToken{
			Depth:       types.StreamPosition(ev.Depth()),
			PDUPosition: types.StreamPosition(100 - i),
		}
	}
	positions[aboveGap.EventID()] = types.TopologyToken{Depth: types.StreamPosition(aboveGap.Depth()), PDUPosition: 60}
	positions[belowGap.EventID()] = types.TopologyToken{Depth: types.StreamPosition(belowGap.Depth()), PDUPosition: 50}
	newReq := func(pdus ...*rstypes.HeaderedEvent) *messagesReq {
		return &messagesReq{
			ctx: context.Background(),
			db:  stubWriteDB{},
			snapshot: &fullPageSnapshot{
				extremities: map[string][]string{extremity.EventID(): {"$missing"}},
				positions:   positions,
			},
			rsAPI:            &backfillOnlyAPI{events: pdus},
			cfg:              &config.SyncAPI{Matrix: &config.Global{ServerName: "test"}},
			roomID:           room.ID,
			from:             &types.TopologyToken{Depth: 0, PDUPosition: 100},
			device:           &userapi.Device{UserID: "@alice:test"},
			backwardOrdering: true,
			filter:           &synctypes.RoomEventFilter{Limit: 3},
		}
	}

	r := newReq(belowGap, aboveGap)
	events, err := r.handleNonEmptyEventsSlice(page)
	require.NoError(t, err)
	ids := make([]string, 0, len(events))
	for _, ev := range events {
		ids = append(ids, ev.EventID())
	}
	assert.Contains(t, ids, aboveGap.EventID(), "a gap event above the page end must not be skipped")
	assert.NotContains(t, ids, belowGap.EventID(), "events older than the page belong to a later page")
	assert.Contains(t, ids, extremity.EventID())
	assert.Contains(t, ids, pageOldest.EventID())
	assert.True(t, r.didBackfill)

	r = newReq(belowGap)
	events, err = r.handleNonEmptyEventsSlice(page)
	require.NoError(t, err)
	assert.Len(t, events, len(page), "only the page is returned when the gap is entirely older than it")
	assert.False(t, r.didBackfill)
}
