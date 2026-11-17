// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"errors"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

// fakeOldEventState answers the lookups excludeOldEventFromSync makes.
type fakeOldEventState struct {
	current          *rstypes.HeaderedEvent
	err              error
	liveStart        types.StreamPosition
	hasLiveStart     bool
	liveStartErr     error
	lift             types.StreamPosition
	prospectiveErr   error
	asked            int
	askedLive        int
	askedProspective int
}

func (f *fakeOldEventState) GetStateEvent(context.Context, string, string, string) (*rstypes.HeaderedEvent, error) {
	f.asked++
	return f.current, f.err
}

func (f *fakeOldEventState) RoomLiveStartPosition(context.Context, string) (types.StreamPosition, bool, error) {
	f.askedLive++
	return f.liveStart, f.hasLiveStart, f.liveStartErr
}

func (f *fakeOldEventState) ProspectiveEventPosition(_ context.Context, ev *rstypes.HeaderedEvent) (types.StreamPosition, error) {
	f.askedProspective++
	if f.prospectiveErr != nil {
		return 0, f.prospectiveErr
	}
	return types.StreamPosition(ev.Depth()) + f.lift, nil
}

// An old state event is history only while it sits below the room's live
// start. Anything at or above it happened while we were in the room, however
// it arrived, so it must reach clients, unless the current state for its key
// is already at least as deep. Arrival path does not change this classification.
func TestExcludeOldEventFromSync(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	room := test.NewRoom(t, alice)
	join := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
	leave := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Leave}, test.WithStateKey(bob.ID))
	message := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hi", "msgtype": "m.text"})
	create := room.Events()[0]
	require.Greater(t, leave.Depth(), join.Depth(), "the test needs the leave to be deeper than the join")
	require.Equal(t, spec.MRoomCreate, create.Type())

	// liveAt makes the room's live start a given event's depth.
	liveAt := func(ev *rstypes.HeaderedEvent, current *rstypes.HeaderedEvent) *fakeOldEventState {
		return &fakeOldEventState{current: current, liveStart: types.StreamPosition(ev.Depth()), hasLiveStart: true}
	}
	pastLeave := &fakeOldEventState{liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true}

	for _, tc := range []struct {
		name    string
		ev      *rstypes.HeaderedEvent
		state   *fakeOldEventState
		exclude bool
	}{
		{"a timeline event is delivered", message, &fakeOldEventState{}, false},
		{"state below the live start is history", leave, pastLeave, true},
		{"state below the live start stays out even when it is the current state", leave,
			&fakeOldEventState{current: leave, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true}, true},
		{"state at the live start is delivered", leave, liveAt(leave, join), false},
		{"state above the live start is delivered when the current state is older", leave, liveAt(join, join), false},
		{"state above the live start is delivered when it is already the current state", leave, liveAt(join, leave), false},
		{"state above the live start is delivered when nothing holds its key", leave, liveAt(join, nil), false},
		{"a superseded state event above the live start stays out", join, liveAt(join, leave), true},
		{"a late parent lifts an event above the live start", leave,
			&fakeOldEventState{current: join, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true,
				lift: 2}, false},
		{"stored parents below the live start leave the event as history", leave,
			&fakeOldEventState{current: leave, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true}, true},
		{"an unreadable parent lookup keeps the event out", leave,
			&fakeOldEventState{liveStart: types.StreamPosition(leave.Depth() - 1), hasLiveStart: true,
				prospectiveErr: errors.New("db down")}, true},
		{"a room with nothing delivered yet keeps old state out", leave, &fakeOldEventState{}, true},
		{"an unreadable live start keeps the event out", leave,
			&fakeOldEventState{liveStartErr: errors.New("db down"), hasLiveStart: true}, true},
		{"an unreadable current state keeps the event out", leave,
			&fakeOldEventState{err: errors.New("db down"), liveStart: types.StreamPosition(join.Depth()), hasLiveStart: true}, true},
		{"a state fetched to fill a gap below the live start is still history", leave, pastLeave, true},
		{"m.room.create stays out", create, liveAt(create, nil), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.exclude, excludeOldEventFromSync(context.Background(), tc.state, tc.ev))
		})
	}

	t.Run("history never reads the current state", func(t *testing.T) {
		state := &fakeOldEventState{current: join, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true}
		require.True(t, excludeOldEventFromSync(context.Background(), state, leave))
		require.Zero(t, state.asked)
	})

	t.Run("without a store every old state event stays out", func(t *testing.T) {
		require.True(t, excludeOldEventFromSync(context.Background(), nil, leave))
	})
}
