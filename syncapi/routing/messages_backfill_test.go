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
		"$at-cursor": {"$a"},
		"$older":     {"$b"},
		"$newer":     {"$c"},
		"$unknown":   {"$d"},
	}
	snapshot := &extremitySnapshot{
		positions: map[string]types.TopologyToken{
			"$at-cursor": {Depth: 2900},
			"$older":     {Depth: 2800},
			"$newer":     {Depth: 2901},
		},
	}
	r := &messagesReq{
		ctx:              context.Background(),
		snapshot:         snapshot,
		backwardOrdering: true,
		from:             &types.TopologyToken{Depth: 2900, PDUPosition: 100},
	}

	assert.Equal(t, map[string][]string{
		"$at-cursor": {"$a"},
		"$older":     {"$b"},
		"$unknown":   {"$d"},
	}, r.reachableBackwardExtremities(extremities), "only gaps at or below the cursor can hold older events")

	for _, depth := range []types.StreamPosition{0, 2} {
		r.from = &types.TopologyToken{Depth: depth, PDUPosition: 100}
		assert.Equal(t, map[string][]string{"$unknown": {"$d"}}, r.reachableBackwardExtremities(extremities),
			"a depth-%d request reaches no known gap; an extremity with no readable position is kept", depth)
	}

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
