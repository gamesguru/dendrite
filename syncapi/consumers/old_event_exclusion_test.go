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
// is already at least as deep. A gap-fill is never history.
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
		gapFill bool
		state   *fakeOldEventState
		exclude bool
	}{
		{"a timeline event is delivered, as before", message, false, &fakeOldEventState{}, false},
		{"a timeline event is delivered when gap-filled", message, true, &fakeOldEventState{}, false},
		{"state below the live start is history", leave, false, pastLeave, true},
		{"state below the live start stays out even when it is the current state", leave, false,
			&fakeOldEventState{current: leave, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true}, true},
		{"state at the live start is delivered", leave, false, liveAt(leave, join), false},
		{"state above the live start is delivered when the current state is older", leave, false, liveAt(join, join), false},
		{"state above the live start is delivered when it is already the current state", leave, false, liveAt(join, leave), false},
		{"state above the live start is delivered when nothing holds its key", leave, false, liveAt(join, nil), false},
		{"a superseded state event above the live start stays out", join, false, liveAt(join, leave), true},
		{"a late parent lifts an event above the live start", leave, false,
			&fakeOldEventState{current: join, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true,
				lift: 2}, false},
		{"stored parents below the live start leave the event as history", leave, false,
			&fakeOldEventState{current: leave, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true}, true},
		{"an unreadable parent lookup keeps the event out", leave, false,
			&fakeOldEventState{liveStart: types.StreamPosition(leave.Depth() - 1), hasLiveStart: true,
				prospectiveErr: errors.New("db down")}, true},
		{"a room with nothing delivered yet keeps old state out", leave, false, &fakeOldEventState{}, true},
		{"an unreadable live start keeps the event out", leave, false,
			&fakeOldEventState{liveStartErr: errors.New("db down"), hasLiveStart: true}, true},
		{"an unreadable current state keeps the event out", leave, false,
			&fakeOldEventState{err: errors.New("db down"), liveStart: types.StreamPosition(join.Depth()), hasLiveStart: true}, true},
		{"m.room.create stays out", create, false, liveAt(create, nil), true},
		{"a gap-filled create event stays out", create, true, &fakeOldEventState{}, true},
		{"a gap-filled leave is delivered when the current state is older", leave, true, &fakeOldEventState{current: join}, false},
		{"a gap-filled event is delivered when it is already the current state", leave, true, &fakeOldEventState{current: leave}, false},
		{"a gap-filled event is delivered when nothing holds its key yet", leave, true, &fakeOldEventState{}, false},
		{"a superseded gap-filled join stays out", join, true, &fakeOldEventState{current: leave}, true},
		{"an unreadable current state keeps a gap-filled event out", leave, true, &fakeOldEventState{err: errors.New("db down")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.exclude, excludeOldEventFromSync(context.Background(), tc.state, tc.ev, tc.gapFill))
		})
	}

	t.Run("a gap-fill never reads the live start", func(t *testing.T) {
		state := &fakeOldEventState{current: join}
		require.False(t, excludeOldEventFromSync(context.Background(), state, leave, true))
		require.Zero(t, state.askedLive)
		require.Zero(t, state.askedProspective)
	})

	t.Run("history never reads the current state", func(t *testing.T) {
		state := &fakeOldEventState{current: join, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true}
		require.True(t, excludeOldEventFromSync(context.Background(), state, leave, false))
		require.Zero(t, state.asked)
	})

	t.Run("without a store every old state event stays out", func(t *testing.T) {
		require.True(t, excludeOldEventFromSync(context.Background(), nil, leave, false))
		require.True(t, excludeOldEventFromSync(context.Background(), nil, leave, true))
	})
}
