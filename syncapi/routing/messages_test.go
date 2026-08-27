// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"

	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/test"
)

func TestFilterHeaderedEvents(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "msg 1"})
	room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "msg 2"})
	events := room.Events()
	// room.Events() contains: m.room.create, m.room.member, m.room.power_levels,
	// m.room.join_rules, m.room.history_visibility, m.room.message x2 = 7 events

	// Simulate what the DB would store for backfilled events from federation:
	// UserID is set on the HeaderedEvent to the actual Matrix user ID.
	aliceID := spec.NewUserIDOrPanic(alice.ID, true)
	for _, ev := range events {
		ev.UserID = aliceID
	}

	t.Run("nil filter returns all events", func(t *testing.T) {
		got := filterHeaderedEvents(events, nil)
		assert.Len(t, got, 7)
	})

	t.Run("empty filter returns all events", func(t *testing.T) {
		got := filterHeaderedEvents(events, &synctypes.RoomEventFilter{})
		assert.Len(t, got, 7)
	})

	t.Run("sender filter matching alice returns all events", func(t *testing.T) {
		senders := []string{alice.ID}
		got := filterHeaderedEvents(events, &synctypes.RoomEventFilter{Senders: &senders})
		assert.Len(t, got, 7)
	})

	t.Run("sender filter for unknown user returns no events", func(t *testing.T) {
		senders := []string{"@bob:example.com"}
		got := filterHeaderedEvents(events, &synctypes.RoomEventFilter{Senders: &senders})
		assert.Empty(t, got)
	})

	t.Run("not_senders filter excluding alice returns no events", func(t *testing.T) {
		notSenders := []string{alice.ID}
		got := filterHeaderedEvents(events, &synctypes.RoomEventFilter{NotSenders: &notSenders})
		assert.Empty(t, got)
	})

	t.Run("types filter exact match returns matching events", func(t *testing.T) {
		types := []string{"m.room.message"}
		got := filterHeaderedEvents(events, &synctypes.RoomEventFilter{Types: &types})
		assert.Len(t, got, 2)
		for _, ev := range got {
			assert.Equal(t, "m.room.message", ev.Type())
		}
	})

	t.Run("types filter wildcard m.room.* matches all events", func(t *testing.T) {
		types := []string{"m.room.*"}
		got := filterHeaderedEvents(events, &synctypes.RoomEventFilter{Types: &types})
		assert.Len(t, got, 7)
	})

	t.Run("types filter * matches all events", func(t *testing.T) {
		types := []string{"*"}
		got := filterHeaderedEvents(events, &synctypes.RoomEventFilter{Types: &types})
		assert.Len(t, got, 7)
	})

	t.Run("types filter no match returns no events", func(t *testing.T) {
		types := []string{"m.custom.event"}
		got := filterHeaderedEvents(events, &synctypes.RoomEventFilter{Types: &types})
		assert.Empty(t, got)
	})

	t.Run("not_types filter excludes state events, keeps messages", func(t *testing.T) {
		notTypes := []string{
			"m.room.create",
			"m.room.member",
			"m.room.power_levels",
			"m.room.join_rules",
			"m.room.history_visibility",
		}
		got := filterHeaderedEvents(events, &synctypes.RoomEventFilter{NotTypes: &notTypes})
		assert.Len(t, got, 2)
		for _, ev := range got {
			assert.Equal(t, "m.room.message", ev.Type())
		}
	})

	t.Run("combined sender and type filter", func(t *testing.T) {
		senders := []string{alice.ID}
		types := []string{"m.room.message"}
		got := filterHeaderedEvents(events, &synctypes.RoomEventFilter{
			Senders: &senders,
			Types:   &types,
		})
		assert.Len(t, got, 2)
	})

	t.Run("empty events slice returns empty", func(t *testing.T) {
		senders := []string{alice.ID}
		got := filterHeaderedEvents([]*rstypes.HeaderedEvent{}, &synctypes.RoomEventFilter{Senders: &senders})
		assert.Empty(t, got)
	})
}
