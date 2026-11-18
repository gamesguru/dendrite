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

func TestForwardTimelineDropsEventsBehindDeliveryBoundary(t *testing.T) {
	alice := test.NewUser(t)
	r := test.NewRoom(t, alice)
	create := r.Events()[0]
	require.Equal(t, spec.MRoomCreate, create.Type())
	oldMessage := r.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "old"})
	liveMessage := r.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "live"})

	positions := map[string]types.TopologyToken{
		create.EventID():      {Depth: types.StreamPosition(create.Depth())},
		oldMessage.EventID():  {Depth: types.StreamPosition(oldMessage.Depth())},
		liveMessage.EventID(): {Depth: types.StreamPosition(liveMessage.Depth())},
	}
	got := forwardTimelineEvents(
		[]*rstypes.HeaderedEvent{create, oldMessage, liveMessage},
		types.StreamPosition(liveMessage.Depth()),
		positions,
	)
	require.Equal(t, []*rstypes.HeaderedEvent{liveMessage}, got,
		"events behind the frontier stay in history instead of appearing as new activity")

	// An event at exactly the boundary is concurrent with the frontier, not
	// behind it, so it is delivered.
	atBoundary := forwardTimelineEvents(
		[]*rstypes.HeaderedEvent{oldMessage},
		types.StreamPosition(oldMessage.Depth()),
		positions,
	)
	require.Equal(t, []*rstypes.HeaderedEvent{oldMessage}, atBoundary)
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
