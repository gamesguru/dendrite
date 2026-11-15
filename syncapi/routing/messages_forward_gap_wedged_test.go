// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

// wedgedLowGapAPI serves one low gap with a batch that never contains its
// missing parent (a hole no reachable server can fill, such as a state event
// the remote cannot supply), and fills every other gap normally.
type wedgedLowGapAPI struct {
	forwardGapAPI
	wedgedExtremity string
	wedged          []*rstypes.HeaderedEvent
	wedgedCalls     int
}

func (a *wedgedLowGapAPI) PerformBackfill(ctx context.Context, req *api.PerformBackfillRequest, res *api.PerformBackfillResponse) error {
	if _, ok := req.BackwardsExtremities[a.wedgedExtremity]; ok {
		a.wedgedCalls++
		res.Events = a.wedged
		res.HistoryVisibility = gomatrixserverlib.HistoryVisibilityShared
		return nil
	}
	return a.forwardGapAPI.PerformBackfill(ctx, req, res)
}

// A room can hold several open gaps. One that cannot be filled must not stop
// the others from being filled: forward filling takes the lowest gap first, so
// a wedged low gap would otherwise starve every newer gap, for good, because
// the failure is remembered and the low gap is picked first on every request.
// This is how a joined room kept a hole between a June event and the join.
func TestFillForwardGapDoesNotStarveNewerGapBehindWedgedOne(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	for i := 0; i < 60; i++ {
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "message"})
	}
	all := room.Events()
	n := len(all)

	// Stored: a low island whose parent exists nowhere (the wedged gap), and the
	// newest events. The hole between them is fetchable.
	lowStart, lowEnd := 10, 15
	state := newForwardGapState(append(append([]*rstypes.HeaderedEvent(nil), all[lowStart:lowEnd]...), all[n-3:]...), all[lowEnd:n-3])
	require.Len(t, state.extremities, 2, "one wedged low gap and one fillable newer gap")
	wedgedID := all[lowStart].EventID()
	require.Contains(t, state.extremities, wedgedID)

	api := &wedgedLowGapAPI{
		forwardGapAPI:   forwardGapAPI{state: state},
		wedgedExtremity: wedgedID,
		wedged:          all[lowStart:lowEnd], // already stored, and without the missing parent
	}
	r := newForwardGapReq(state)
	r.rsAPI = api
	key := terminalBackfillKey(r.cfg.Matrix.ServerName, r.device.UserDomain(), r.roomID, map[string][]string{wedgedID: state.extremities[wedgedID]})
	defaultTerminalBackfillCache.forget(key)
	defer defaultTerminalBackfillCache.forget(key)

	newest := all[n-3]
	position := types.TopologyToken{Depth: types.StreamPosition(newest.Depth()), PDUPosition: types.StreamPosition(newest.Depth())}
	// Two requests: the failure of the first is remembered by the second.
	for i := 0; i < 2; i++ {
		rr := newForwardGapReq(state)
		rr.rsAPI = api
		_, _ = rr.fillForwardGap(rr.ctx, position)
	}

	assert.Empty(t, state.unstored,
		"the newer gap was never filled: a wedged lower gap blocked it (%d events still missing)", len(state.unstored))
}
