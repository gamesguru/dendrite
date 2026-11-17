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
	"codefloe.com/pat-s/zendrite/test"
)

// fakeCurrentState answers the one lookup excludeOldEventFromSync makes.
type fakeCurrentState struct {
	current *rstypes.HeaderedEvent
	err     error
	asked   int
}

func (f *fakeCurrentState) GetStateEvent(context.Context, string, string, string) (*rstypes.HeaderedEvent, error) {
	f.asked++
	return f.current, f.err
}

// A state event fetched to fill a gap while processing a live event happened
// while we were in the room, so it must reach clients, unless the current state
// for its key is already at least as deep. Anything that is not a gap-fill
// (resynced state, history) stays out of the timeline as before.
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

	for _, tc := range []struct {
		name    string
		ev      *rstypes.HeaderedEvent
		gapFill bool
		state   *fakeCurrentState
		exclude bool
	}{
		{"a timeline event is delivered, as before", message, false, &fakeCurrentState{}, false},
		{"a timeline event is delivered when gap-filled", message, true, &fakeCurrentState{}, false},
		{"resynced or historical state stays out", leave, false, &fakeCurrentState{}, true},
		{"a gap-filled create event stays out", create, true, &fakeCurrentState{}, true},
		{"a gap-filled leave is delivered when the current state is older", leave, true, &fakeCurrentState{current: join}, false},
		{"a gap-filled event is delivered when it is already the current state", leave, true, &fakeCurrentState{current: leave}, false},
		{"a gap-filled event is delivered when nothing holds its key yet", leave, true, &fakeCurrentState{}, false},
		{"a superseded gap-filled join stays out", join, true, &fakeCurrentState{current: leave}, true},
		{"an unreadable current state keeps the event out", leave, true, &fakeCurrentState{err: errors.New("db down")}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.exclude, excludeOldEventFromSync(context.Background(), tc.state, tc.ev, tc.gapFill))
		})
	}

	t.Run("a non-gap-fill never reads the current state", func(t *testing.T) {
		state := &fakeCurrentState{current: join}
		require.True(t, excludeOldEventFromSync(context.Background(), state, leave, false))
		require.Zero(t, state.asked)
	})
}
