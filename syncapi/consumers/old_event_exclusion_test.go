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
	rejoin := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
	message := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hi", "msgtype": "m.text"})
	create := room.Events()[0]
	require.Greater(t, leave.Depth(), join.Depth(), "the test needs the leave to be deeper than the join")
	require.Greater(t, rejoin.Depth(), leave.Depth(), "the test needs a later state event to supersede the leave")
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
		wantErr bool
	}{
		{name: "a timeline event is delivered", ev: message},
		{name: "state below the live start is history", ev: leave, state: pastLeave, exclude: true},
		{
			name: "state below the live start stays out even when it is the current state", ev: leave,
			state: &fakeOldEventState{current: leave, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true}, exclude: true,
		},
		{name: "state at the live start is delivered", ev: leave, state: liveAt(leave, join)},
		{name: "state above the live start is delivered when the current state is older", ev: leave, state: liveAt(join, join)},
		{name: "state above the live start is delivered when it is already the current state", ev: leave, state: liveAt(join, leave)},
		{name: "state above the live start is delivered when nothing holds its key", ev: leave, state: liveAt(join, nil)},
		{name: "a superseded state event above the live start stays out", ev: join, state: liveAt(join, leave), exclude: true},
		{
			// Depth is a proxy, not state resolution: a concurrent branch can
			// land at the same position, and suppression is the safe choice.
			name: "a concurrent event at the same position stays out", ev: join,
			state: &fakeOldEventState{
				current:      leave,
				liveStart:    types.StreamPosition(join.Depth()),
				hasLiveStart: true,
				lift:         types.StreamPosition(leave.Depth() - join.Depth()),
			},
			exclude: true,
		},
		{
			name: "a late parent lifts an event above the live start", ev: leave,
			state: &fakeOldEventState{current: join, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true, lift: 2},
		},
		{
			name: "stored parents below the live start leave the event as history", ev: leave,
			state: &fakeOldEventState{current: leave, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true}, exclude: true,
		},
		{
			name: "an unreadable parent lookup returns an error", ev: leave,
			state: &fakeOldEventState{liveStart: types.StreamPosition(leave.Depth() - 1), hasLiveStart: true, prospectiveErr: errors.New("db down")}, wantErr: true,
		},
		{name: "a room with nothing delivered yet keeps old state out", ev: leave, state: &fakeOldEventState{}, exclude: true},
		{
			name: "an unreadable live start returns an error", ev: leave,
			state: &fakeOldEventState{liveStartErr: errors.New("db down"), hasLiveStart: true}, wantErr: true,
		},
		{
			name: "an unreadable current state returns an error", ev: leave,
			state: &fakeOldEventState{err: errors.New("db down"), liveStart: types.StreamPosition(join.Depth()), hasLiveStart: true}, wantErr: true,
		},
		{name: "a state fetched to fill a gap below the live start is still history", ev: leave, state: pastLeave, exclude: true},
		{name: "m.room.create stays out", ev: create, state: liveAt(create, nil), exclude: true},
		{name: "gap-filled state that won current state is delivered", ev: leave, state: liveAt(join, leave)},
		{name: "gap-filled state is not suppressed by its older current-state predecessor", ev: leave, state: liveAt(join, join)},
		{name: "gap-filled state below an already newer current event is excluded", ev: leave, state: liveAt(join, rejoin), exclude: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := excludeOldEventFromSync(context.Background(), tc.state, tc.ev)
			if tc.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, tc.exclude, got)
		})
	}

	t.Run("history never reads the current state", func(t *testing.T) {
		state := &fakeOldEventState{current: join, liveStart: types.StreamPosition(leave.Depth() + 1), hasLiveStart: true}
		got, err := excludeOldEventFromSync(context.Background(), state, leave)
		require.NoError(t, err)
		require.True(t, got)
		require.Zero(t, state.asked)
	})

	t.Run("without a store every old state event stays out", func(t *testing.T) {
		got, err := excludeOldEventFromSync(context.Background(), nil, leave)
		require.NoError(t, err)
		require.True(t, got)
	})
}
