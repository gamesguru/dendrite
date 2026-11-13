// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full copyright details.

package routing

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// forwardGapState is an in-memory stand-in for the stored part of a room: the
// events that are present, their topology positions and the backwards
// extremities implied by the events whose prev events are absent.
type forwardGapState struct {
	storage.DatabaseTransaction
	unstored    []*rstypes.HeaderedEvent // the hole, oldest first
	stored      map[string]*rstypes.HeaderedEvent
	positions   map[string]types.TopologyToken
	extremities map[string][]string
	batches     int
}

func newForwardGapState(stored, unstored []*rstypes.HeaderedEvent) *forwardGapState {
	state := &forwardGapState{
		unstored:  unstored,
		stored:    make(map[string]*rstypes.HeaderedEvent),
		positions: make(map[string]types.TopologyToken),
	}
	for _, ev := range stored {
		state.store(ev)
	}
	state.recomputeExtremities()
	return state
}

func (s *forwardGapState) store(ev *rstypes.HeaderedEvent) {
	s.stored[ev.EventID()] = ev
	s.positions[ev.EventID()] = types.TopologyToken{
		Depth:       types.StreamPosition(ev.Depth()),
		PDUPosition: types.StreamPosition(ev.Depth()),
	}
}

// recomputeExtremities mirrors what WriteEvent does while storing a batch: an
// event whose prev events are not all present becomes a backwards extremity.
func (s *forwardGapState) recomputeExtremities() {
	s.extremities = make(map[string][]string)
	for eventID := range s.stored {
		ev := s.stored[eventID]
		var missing []string
		for _, prevID := range ev.PrevEventIDs() {
			if _, ok := s.stored[prevID]; !ok {
				missing = append(missing, prevID)
			}
		}
		if len(missing) > 0 {
			s.extremities[eventID] = missing
		}
	}
}

func (s *forwardGapState) Events(_ context.Context, eventIDs []string) ([]*rstypes.HeaderedEvent, error) {
	events := make([]*rstypes.HeaderedEvent, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		if ev, ok := s.stored[eventID]; ok {
			events = append(events, ev)
		}
	}
	return events, nil
}

func (s *forwardGapState) EventPositionInTopology(_ context.Context, eventID string) (types.TopologyToken, error) {
	pos, ok := s.positions[eventID]
	if !ok {
		return types.TopologyToken{}, errors.New("event not in topology")
	}
	return pos, nil
}

func (s *forwardGapState) BackwardExtremitiesForRoom(context.Context, string) (map[string][]string, error) {
	return s.extremities, nil
}

// takeBatch returns the newest events still missing from the hole, as a remote
// server answering a backfill for the current extremity would.
func (s *forwardGapState) takeBatch(limit int) []*rstypes.HeaderedEvent {
	if limit > len(s.unstored) {
		limit = len(s.unstored)
	}
	if limit <= 0 {
		return nil
	}
	batch := append([]*rstypes.HeaderedEvent(nil), s.unstored[len(s.unstored)-limit:]...)
	s.batches++
	return batch
}

type forwardGapDB struct {
	storage.Database
	state *forwardGapState
}

func (d forwardGapDB) Events(ctx context.Context, eventIDs []string) ([]*rstypes.HeaderedEvent, error) {
	return d.state.Events(ctx, eventIDs)
}

func (d forwardGapDB) WriteEvent(_ context.Context, ev *rstypes.HeaderedEvent, _ []*rstypes.HeaderedEvent,
	_ []string, _ []string, _ *api.TransactionID, _ bool, _ gomatrixserverlib.HistoryVisibility, _ ...string,
) (types.StreamPosition, error) {
	d.state.store(ev)
	for i, pending := range d.state.unstored {
		if pending.EventID() == ev.EventID() {
			d.state.unstored = append(d.state.unstored[:i], d.state.unstored[i+1:]...)
			break
		}
	}
	d.state.recomputeExtremities()
	return 0, nil
}

func (d forwardGapDB) PurgeBackwardExtremitiesForEvents(context.Context, string, []string) error {
	return nil
}

type forwardGapAPI struct {
	api.SyncRoomserverAPI
	state *forwardGapState
}

func (a *forwardGapAPI) PerformBackfill(_ context.Context, req *api.PerformBackfillRequest, res *api.PerformBackfillResponse) error {
	res.Events = a.state.takeBatch(req.Limit)
	res.HistoryVisibility = gomatrixserverlib.HistoryVisibilityShared
	return nil
}

func newForwardGapReq(state *forwardGapState) *messagesReq {
	return &messagesReq{
		ctx:              context.Background(),
		db:               forwardGapDB{state: state},
		snapshot:         state,
		rsAPI:            &forwardGapAPI{state: state},
		cfg:              &config.SyncAPI{Matrix: &config.Global{ServerName: "test"}},
		roomID:           "!room:test",
		from:             &types.TopologyToken{},
		device:           &userapi.Device{UserID: "@alice:test"},
		backwardOrdering: false,
		filter:           &synctypes.RoomEventFilter{Limit: 10},
	}
}

// A forward page that would jump over a hole must fill that hole before it is
// served, because the forward cursor can never come back to it.
func TestFillForwardGapClosesHoleAheadOfCursor(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	for i := 0; i < 320; i++ {
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "message"})
	}

	all := room.Events()
	// Only the newest events have been fetched; the rest is the hole a forward
	// page from the start of the room would jump over.
	split := len(all) - 3
	state := newForwardGapState(all[split:], all[:split])
	hole := len(state.unstored)
	require.NotEmpty(t, state.extremities, "the first stored event has a missing prev event")

	r := newForwardGapReq(state)
	position := types.TopologyToken{Depth: types.StreamPosition(all[split].Depth()), PDUPosition: types.StreamPosition(all[split].Depth())}
	filled, err := r.fillForwardGap(r.ctx, position)
	require.NoError(t, err)
	assert.True(t, filled)
	assert.Empty(t, state.unstored, "the whole hole is filled")
	assert.Empty(t, state.extremities, "the hole is closed down to the room's creation")
	assert.Equal(t, (hole+forwardBackfillBatch-1)/forwardBackfillBatch, state.batches,
		"the hole is filled one federation batch at a time")
	assert.LessOrEqual(t, r.forwardBackfillRounds, maxForwardBackfillRounds)
}

// A gap that is still open after the per-request bound is an error, not a short
// page: returning events would move the cursor past the unfilled events.
func TestFillForwardGapReportsIncompleteGap(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	for i := 0; i < maxForwardBackfillRounds*forwardBackfillBatch+150; i++ {
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "message"})
	}

	all := room.Events()
	split := len(all) - 3
	state := newForwardGapState(all[split:], all[:split])
	hole := len(state.unstored)

	r := newForwardGapReq(state)
	position := types.TopologyToken{Depth: types.StreamPosition(all[split].Depth()), PDUPosition: types.StreamPosition(all[split].Depth())}
	filled, err := r.fillForwardGap(r.ctx, position)
	assert.ErrorIs(t, err, errForwardBackfillIncomplete)
	assert.True(t, filled, "the batches fetched before the bound are kept")
	assert.Equal(t, maxForwardBackfillRounds, state.batches)
	assert.Len(t, state.unstored, hole-maxForwardBackfillRounds*forwardBackfillBatch,
		"the part of the hole beyond the bound is left for the retry")
	assert.NotEmpty(t, state.extremities, "an open gap is still ahead of the cursor")
}

// Only a gap between the cursor and the first event on the page can be filled;
// deeper markers belong to a part of the walk that has not been reached, and
// markers behind the cursor were already passed.
func TestForwardGapExtremityOnlyTakesGapsAheadOfTheCursor(t *testing.T) {
	state := &forwardGapState{
		stored: map[string]*rstypes.HeaderedEvent{},
		positions: map[string]types.TopologyToken{
			"$behind": {Depth: 5},
			"$ahead":  {Depth: 50},
			"$far":    {Depth: 500},
		},
		extremities: map[string][]string{
			"$behind": {"$b"},
			"$ahead":  {"$a"},
			"$far":    {"$f"},
		},
	}
	r := &messagesReq{
		ctx:      context.Background(),
		snapshot: state,
		roomID:   "!room:test",
		from:     &types.TopologyToken{Depth: 10},
	}

	extremityID, prevs, err := r.forwardGapExtremity(r.ctx, types.TopologyToken{Depth: 100})
	require.NoError(t, err)
	assert.Equal(t, "$ahead", extremityID, "the shallowest gap ahead wins")
	assert.Equal(t, []string{"$a"}, prevs)

	extremityID, _, err = r.forwardGapExtremity(r.ctx, types.TopologyToken{Depth: 40})
	require.NoError(t, err)
	assert.Empty(t, extremityID, "a marker beyond the page's first event is a later gap")

	extremityID, _, err = r.forwardGapExtremity(r.ctx, types.TopologyToken{Depth: 50})
	require.NoError(t, err)
	assert.Equal(t, "$ahead", extremityID, "a gap at the first event's depth is the one being jumped over")
}

// A hole inside the page must not be walked over: the page is cut before the
// extremity and the end token stays on the last event before the hole, so the
// next request starts at the extremity and fills it.
func TestTruncateForwardPageAtGapCutsBeforeInPageExtremity(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	for i := 0; i < 10; i++ {
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "message"})
	}
	all := room.Events()
	// Drop events 5..7 so all[8] has missing prev events.
	stored := append(append([]*rstypes.HeaderedEvent{}, all[:5]...), all[8:]...)
	state := newForwardGapState(stored, all[5:8])
	require.Contains(t, state.extremities, all[8].EventID())

	page := make([]types.StreamEvent, 0, len(stored))
	for _, ev := range stored {
		page = append(page, types.StreamEvent{HeaderedEvent: ev})
	}
	r := newForwardGapReq(state)
	got, end, err := r.truncateForwardPageAtGap(r.ctx, page, types.TopologyToken{Depth: 99})
	require.NoError(t, err)
	require.Len(t, got, 5, "events before the hole are served")
	assert.Equal(t, got[4].EventID(), all[4].EventID())
	assert.Equal(t, state.positions[all[4].EventID()], end, "the cursor stays before the hole")

	// The first event is never cut, so the page always makes progress.
	got, _, err = r.truncateForwardPageAtGap(r.ctx, page[5:], types.TopologyToken{Depth: 99})
	require.NoError(t, err)
	assert.Len(t, got, len(page[5:]))
}

// countingTopology counts the topology lookups a page costs.
type countingTopology struct {
	*forwardGapState
	lookups int
}

func (c *countingTopology) EventPositionInTopology(ctx context.Context, eventID string) (types.TopologyToken, error) {
	c.lookups++
	return c.forwardGapState.EventPositionInTopology(ctx, eventID)
}

// Each open gap is looked up once for the page, not once per event.
func TestTruncateForwardPageAtGapLooksUpEachGapOnce(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	for i := 0; i < 14; i++ {
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "message"})
	}
	all := room.Events()
	stored := append(append([]*rstypes.HeaderedEvent{}, all[:11]...), all[12:]...)
	state := newForwardGapState(stored, all[11:12])
	require.Len(t, state.extremities, 1)

	page := make([]types.StreamEvent, 0, len(stored))
	for _, ev := range stored {
		page = append(page, types.StreamEvent{HeaderedEvent: ev})
	}
	counting := &countingTopology{forwardGapState: state}
	r := newForwardGapReq(state)
	r.snapshot = counting
	_, _, err := r.truncateForwardPageAtGap(r.ctx, page, types.TopologyToken{Depth: 99})
	require.NoError(t, err)
	assert.LessOrEqual(t, counting.lookups, len(page)+len(state.extremities),
		"one lookup per page event plus one per open gap")
}

type anchorSnapshot struct {
	storage.DatabaseTransaction
	anchor types.TopologyToken
	err    error
}

func (s anchorSnapshot) EventPositionByStreamPosition(context.Context, string, types.StreamPosition) (types.TopologyToken, error) {
	return s.anchor, s.err
}

// A repair only raises positions, so re-anchoring a backward cursor may raise
// its depth but must never lower it.
func TestReanchorBackwardCursorOnlyRaisesDepth(t *testing.T) {
	newReq := func(depth types.StreamPosition, snapshot storage.DatabaseTransaction) *messagesReq {
		return &messagesReq{
			roomID: "!r:test", snapshot: snapshot, backwardOrdering: true, fromTopologyToken: true,
			from: &types.TopologyToken{Depth: depth, PDUPosition: 10},
		}
	}
	ctx := context.Background()

	r := newReq(5, anchorSnapshot{anchor: types.TopologyToken{Depth: 9, PDUPosition: 11}})
	require.NoError(t, r.reanchorBackwardCursor(ctx))
	assert.Equal(t, types.StreamPosition(9), r.from.Depth, "a lifted anchor raises the cursor")

	r = newReq(9, anchorSnapshot{anchor: types.TopologyToken{Depth: 5, PDUPosition: 11}})
	require.NoError(t, r.reanchorBackwardCursor(ctx))
	assert.Equal(t, types.StreamPosition(9), r.from.Depth, "a shallower event is not the anchor of a lifted cursor")

	r = newReq(5, anchorSnapshot{err: sql.ErrNoRows})
	require.NoError(t, r.reanchorBackwardCursor(ctx))
	assert.Equal(t, types.StreamPosition(5), r.from.Depth)

	r = newReq(5, anchorSnapshot{anchor: types.TopologyToken{Depth: 9, PDUPosition: 11}})
	r.fromTopologyToken = false
	require.NoError(t, r.reanchorBackwardCursor(ctx))
	assert.Equal(t, types.StreamPosition(5), r.from.Depth, "only topology tokens are re-anchored")

	r = newReq(5, anchorSnapshot{err: errors.New("db down")})
	require.Error(t, r.reanchorBackwardCursor(ctx))

	// A cursor before every stream position stays before every stored event,
	// even though the first stored event sits deeper (a federated join's
	// history is backfilled below its join).
	r = newReq(1, anchorSnapshot{anchor: types.TopologyToken{Depth: 35, PDUPosition: 1}})
	r.from.PDUPosition = 0
	require.NoError(t, r.reanchorBackwardCursor(ctx))
	assert.Equal(t, types.StreamPosition(1), r.from.Depth, "a cursor at stream position 0 is not re-anchored")
}
