// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

func TestForwardTimelineDropsStateEventsBehindDeliveryBoundary(t *testing.T) {
	alice := test.NewUser(t)
	r := test.NewRoom(t, alice)
	create := r.Events()[0]
	require.Equal(t, spec.MRoomCreate, create.Type())
	oldTopic := r.CreateAndInsert(t, alice, "m.room.topic", map[string]any{"topic": "old"}, test.WithStateKey(""))
	// A message from a lagging fork is below the frontier but still new.
	forkedMessage := r.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "fork"})
	liveName := r.CreateAndInsert(t, alice, "m.room.name", map[string]any{"name": "live"}, test.WithStateKey(""))

	positions := map[string]types.TopologyToken{
		create.EventID():        {Depth: types.StreamPosition(create.Depth())},
		oldTopic.EventID():      {Depth: types.StreamPosition(oldTopic.Depth())},
		forkedMessage.EventID(): {Depth: types.StreamPosition(forkedMessage.Depth())},
		liveName.EventID():      {Depth: types.StreamPosition(liveName.Depth())},
	}
	got := forwardTimelineEvents(
		[]*rstypes.HeaderedEvent{create, oldTopic, forkedMessage, liveName},
		types.StreamPosition(liveName.Depth()),
		positions,
	)
	require.Equal(t, []*rstypes.HeaderedEvent{forkedMessage, liveName}, got,
		"state behind the frontier stays in state, while a forked message is still delivered")

	// An event at exactly the boundary is concurrent with the frontier, not
	// behind it, so it is delivered.
	atBoundary := forwardTimelineEvents(
		[]*rstypes.HeaderedEvent{oldTopic},
		types.StreamPosition(oldTopic.Depth()),
		positions,
	)
	require.Equal(t, []*rstypes.HeaderedEvent{oldTopic}, atBoundary)
}

func TestBackwardTimelineKeepsLateCreateAtStart(t *testing.T) {
	alice := test.NewUser(t)
	r := test.NewRoom(t, alice)
	create := r.Events()[0]
	msg := r.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hi"})

	// A backfilled create can arrive after newer messages in stream order, but
	// the historical timeline keeps it first by ordering on the room DAG.
	got := orderRoomTimelineEvents([]*rstypes.HeaderedEvent{msg, create}, true)
	require.Len(t, got, 2)
	require.Equal(t, create.EventID(), got[0].EventID())
	require.Equal(t, msg.EventID(), got[1].EventID())
}
