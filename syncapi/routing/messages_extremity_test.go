// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

type eventMapSnapshot struct {
	storage.DatabaseTransaction
	events map[string]*rstypes.HeaderedEvent
	// positions overrides the topology depth of an event. Events without an
	// entry sit at their wire depth.
	positions map[string]types.StreamPosition
}

func (s *eventMapSnapshot) EventPositionInTopology(_ context.Context, eventID string) (types.TopologyToken, error) {
	if depth, ok := s.positions[eventID]; ok {
		return types.TopologyToken{Depth: depth}, nil
	}
	if event, ok := s.events[eventID]; ok {
		return types.TopologyToken{Depth: types.StreamPosition(event.Depth())}, nil
	}
	return types.TopologyToken{}, context.Canceled
}

func (s *eventMapSnapshot) Events(_ context.Context, eventIDs []string) ([]*rstypes.HeaderedEvent, error) {
	events := make([]*rstypes.HeaderedEvent, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		if event, ok := s.events[eventID]; ok {
			events = append(events, event)
		}
	}
	return events, nil
}

func TestExtremityBranchEventsOnlyExcludesAncestors(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	branchPoint := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "shared"})
	mainA := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "main A"})
	mainB := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "main B"})
	fork := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "fork"}, test.WithPrevEvents(branchPoint.EventID()))

	snapshot := &eventMapSnapshot{events: map[string]*rstypes.HeaderedEvent{}}
	for _, event := range room.Events() {
		snapshot.events[event.EventID()] = event
	}
	req := messagesReq{snapshot: snapshot}

	page := []types.StreamEvent{
		{HeaderedEvent: mainB},
		{HeaderedEvent: mainA},
		{HeaderedEvent: branchPoint},
	}
	excluded, err := req.extremityBranchEvents(context.Background(), fork.EventID(), page, fork.Depth())
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{branchPoint.EventID(): {}}, excluded,
		"only ancestors on the extremity's branch are older than the extremity")
	assert.NotContains(t, excluded, mainA.EventID())
	assert.NotContains(t, excluded, mainB.EventID())
}

// TestExtremityBranchEventsWithStoredAndMissingPrev models a merge extremity
// whose prev events are one stored event on the main line and one event that
// was never backfilled. When the page reaches the extremity, the older events
// on the main line must be returned with it: they sit above the missing
// branch, so withholding them loses them from the walk.
func TestExtremityBranchEventsWithStoredAndMissingPrev(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	branchPoint := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "shared"})
	mainA := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "main A"})
	mainB := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "main B"})
	missing := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "hidden"},
		test.WithPrevEvents(branchPoint.EventID()))
	extremity := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "merge"},
		test.WithPrevEvents(mainB.EventID(), missing.EventID()))

	snapshot := &eventMapSnapshot{events: map[string]*rstypes.HeaderedEvent{}}
	for _, event := range room.Events() {
		snapshot.events[event.EventID()] = event
	}
	// The hidden branch was fetched for state but never stored locally.
	delete(snapshot.events, missing.EventID())
	req := messagesReq{snapshot: snapshot}

	withExtremity := []types.StreamEvent{
		{HeaderedEvent: extremity},
		{HeaderedEvent: mainB},
		{HeaderedEvent: mainA},
		{HeaderedEvent: branchPoint},
	}
	excluded, err := req.extremityBranchEvents(context.Background(), extremity.EventID(), withExtremity, extremity.Depth())
	require.NoError(t, err)
	assert.Empty(t, excluded, "a page that reaches the extremity keeps the main line with it")

	withoutExtremity := withExtremity[1:]
	excluded, err = req.extremityBranchEvents(context.Background(), extremity.EventID(), withoutExtremity, extremity.Depth())
	require.NoError(t, err)
	assert.Contains(t, excluded, mainB.EventID(), "below the extremity the main line is still held back")
	assert.Contains(t, excluded, mainA.EventID())
}

func TestExtremityBranchEventsWithoutOlderPageEvents(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	event := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "only"})
	req := messagesReq{snapshot: &eventMapSnapshot{events: map[string]*rstypes.HeaderedEvent{event.EventID(): event}}}

	excluded, err := req.extremityBranchEvents(context.Background(), event.EventID(), []types.StreamEvent{
		{HeaderedEvent: event},
	}, event.Depth())
	require.NoError(t, err)
	assert.Empty(t, excluded)
}

func TestOpenBackwardExtremities(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	stored := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "stored"})
	missing := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "missing"})
	req := messagesReq{snapshot: &eventMapSnapshot{events: map[string]*rstypes.HeaderedEvent{stored.EventID(): stored}}}

	open, err := req.openBackwardExtremities(context.Background(), map[string][]string{
		"$closed":  {stored.EventID()},
		"$partial": {stored.EventID(), missing.EventID()},
		"$absent":  {missing.EventID()},
		"$empty":   nil,
	})
	require.NoError(t, err)
	assert.NotContains(t, open, "$closed", "a marker whose prev events are all stored has no gap left")
	assert.Contains(t, open, "$partial", "one absent prev event keeps the gap open")
	assert.Contains(t, open, "$absent")
	assert.Contains(t, open, "$empty", "a marker with no recorded prev events is kept")
}

// The extremity's depth is a topology position. An event whose wire depth is
// lower but which is stored above the extremity (its server restarted its depth
// counter, or it arrived after its child) is newer than the gap and must not be
// held back with the extremity's ancestors.
func TestExtremityBranchEventsComparesTopologyDepth(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	branchPoint := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "shared"})
	lifted := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "wire depth low, stored high"})
	fork := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "fork"}, test.WithPrevEvents(lifted.EventID()))

	snapshot := &eventMapSnapshot{
		events: map[string]*rstypes.HeaderedEvent{},
		positions: map[string]types.StreamPosition{
			// The repaired topology stores this event far above the fork.
			lifted.EventID(): types.StreamPosition(fork.Depth()) + 1000,
		},
	}
	for _, event := range room.Events() {
		snapshot.events[event.EventID()] = event
	}
	req := messagesReq{snapshot: snapshot}

	page := []types.StreamEvent{{HeaderedEvent: lifted}, {HeaderedEvent: branchPoint}}
	excluded, err := req.extremityBranchEvents(context.Background(), fork.EventID(), page, fork.Depth())
	require.NoError(t, err)
	assert.Equal(t, map[string]struct{}{branchPoint.EventID(): {}}, excluded)
	assert.NotContains(t, excluded, lifted.EventID(),
		"an event stored above the extremity is not on the gap's branch, whatever its wire depth")
}
