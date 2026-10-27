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
		{HeaderedEvent: fork},
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
