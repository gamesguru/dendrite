// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"

	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
)

func memberStateKeys(state []*types.HeaderedEvent) []string {
	var keys []string
	for _, ev := range state {
		if ev.Type() == spec.MRoomMember {
			keys = append(keys, *ev.StateKey())
		}
	}
	return keys
}

func hasNonMemberState(state []*types.HeaderedEvent) bool {
	for _, ev := range state {
		if ev.Type() == spec.MRoomCreate {
			return true
		}
	}
	return false
}

func joinUser(t *testing.T, room *test.Room, user *test.User) {
	room.CreateAndInsert(t, user, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(user.ID))
}

func TestPartialSendJoinStateUnnamedRoomKeepsOldestHeroes(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
	users := []*test.User{alice}
	for range 6 {
		u := test.NewUser(t)
		joinUser(t, room, u)
		users = append(users, u)
	}
	invitee := test.NewUser(t)
	room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Invite}, test.WithStateKey(invitee.ID))
	joiner := test.NewUser(t)
	joinUser(t, room, joiner)

	state := partialSendJoinState(room.CurrentState(), joiner.ID)

	want := []string{joiner.ID}
	for _, u := range users[:sendJoinHeroLimit] {
		want = append(want, u.ID)
	}
	assert.ElementsMatch(t, want, memberStateKeys(state))
	assert.True(t, hasNonMemberState(state), "non-membership state must be kept")
}

func TestPartialSendJoinStateNamedRoomOmitsHeroes(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
	room.CreateAndInsert(t, alice, spec.MRoomName, map[string]any{"name": "named"}, test.WithStateKey(""))
	joiner := test.NewUser(t)
	joinUser(t, room, joiner)

	state := partialSendJoinState(room.CurrentState(), joiner.ID)

	assert.Equal(t, []string{joiner.ID}, memberStateKeys(state))
	assert.True(t, hasNonMemberState(state))
}

func TestPartialSendJoinStateIncludesInvitedMembers(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
	invitee := test.NewUser(t)
	room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Invite}, test.WithStateKey(invitee.ID))
	joiner := test.NewUser(t)
	joinUser(t, room, joiner)

	state := partialSendJoinState(room.CurrentState(), joiner.ID)

	assert.ElementsMatch(t, []string{joiner.ID, alice.ID, invitee.ID}, memberStateKeys(state))
}

func TestPartialSendJoinStateFallsBackToDepartedMembers(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
	bob := test.NewUser(t)
	joinUser(t, room, bob)
	room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Leave}, test.WithStateKey(bob.ID))
	joiner := test.NewUser(t)
	joinUser(t, room, joiner)
	room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Leave}, test.WithStateKey(alice.ID))

	state := partialSendJoinState(room.CurrentState(), joiner.ID)

	assert.ElementsMatch(t, []string{joiner.ID, alice.ID, bob.ID}, memberStateKeys(state))
}

func TestPartialSendJoinStateKeepsJoinAuthoriser(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
	room.CreateAndInsert(t, alice, spec.MRoomName, map[string]any{"name": "named"}, test.WithStateKey(""))
	joiner := test.NewUser(t)
	joinUser(t, room, joiner)

	state := partialSendJoinState(room.CurrentState(), joiner.ID, alice.ID)

	assert.ElementsMatch(t, []string{joiner.ID, alice.ID}, memberStateKeys(state))
}
