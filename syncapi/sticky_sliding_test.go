// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package syncapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

// slidingSync makes a sliding sync request with the sticky events extension
// and returns the response.
func (e *stickyEnv) slidingSync(user *test.User, pos string, lists map[string]any, sticky map[string]any) gjson.Result {
	e.t.Helper()
	body, err := json.Marshal(map[string]any{
		"lists":      lists,
		"extensions": map[string]any{"org.matrix.msc4354.sticky_events": sticky},
	})
	require.NoError(e.t, err)
	params := map[string]string{"access_token": e.devices[user.ID].AccessToken, "timeout": "0"}
	if pos != "" {
		params["pos"] = pos
	}
	w := httptest.NewRecorder()
	e.routers.Client.ServeHTTP(w, test.NewRequest(e.t, "POST", "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync",
		test.WithQueryParams(params), test.WithJSONBody(e.t, json.RawMessage(body))))
	require.Equal(e.t, 200, w.Code, w.Body.String())
	return gjson.Parse(w.Body.String())
}

// dummyLists matches every room but lets none into the window, so the
// sticky events extension is the only way sticky events arrive.
var dummyLists = map[string]any{"main": map[string]any{"ranges": [][]int{{0, 0}}, "required_state": []any{}, "timeline_limit": 0}}

func extensionStickyIDs(res gjson.Result, roomID string) []string {
	var ids []string
	for _, ev := range res.Get(`extensions.org\.matrix\.msc4354\.sticky_events.rooms.` + gjson.Escape(roomID) + ".events").Array() {
		ids = append(ids, ev.Get("event_id").Str)
	}
	return ids
}

func stickyExtension(res gjson.Result) gjson.Result {
	return res.Get(`extensions.org\.matrix\.msc4354\.sticky_events`)
}

// The sliding sync sticky events extension (MSC4480) delivers each unexpired
// sticky event once along its own since token, with its TTL, and is omitted
// when it has nothing to deliver.
func TestStickyEventsSlidingSync(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		env := newStickyEnv(t, dbType, alice, bob)
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(bob.ID))
		env.send(room.Events()...)
		env.waitFor(alice, room.ID, room.Events()[len(room.Events())-1])
		enabled := map[string]any{"enabled": true}

		res := env.slidingSync(alice, "", dummyLists, enabled)
		require.False(t, stickyExtension(res).Exists(), "no sticky events yet: %s", res.Raw)
		res = env.slidingSync(alice, res.Get("pos").Str, dummyLists, enabled)
		require.False(t, stickyExtension(res).Exists(), res.Raw)

		first := sendSticky(room, bob, "first", 5*time.Minute)(t)
		env.send(first)
		env.waitFor(alice, room.ID, first)
		res = env.slidingSync(alice, "", dummyLists, enabled)
		require.Equal(t, []string{first.EventID()}, extensionStickyIDs(res, room.ID))
		ev := stickyExtension(res).Get("rooms." + gjson.Escape(room.ID) + ".events.0")
		require.Greater(t, ttlOf(ev).Int(), int64(0), ev.Raw)
		require.Equal(t, int64(300000), ev.Get("msc4354_sticky.duration_ms").Int(), ev.Raw)
		nextBatch := stickyExtension(res).Get("next_batch")
		require.Equal(t, gjson.String, nextBatch.Type, res.Raw)

		since := map[string]any{"enabled": true, "since": nextBatch.Str}
		res = env.slidingSync(alice, "", dummyLists, since)
		require.False(t, stickyExtension(res).Exists(), "delivered once: %s", res.Raw)

		second := sendSticky(room, alice, "second", 5*time.Minute)(t)
		env.send(second)
		env.waitFor(alice, room.ID, second)
		res = env.slidingSync(alice, "", dummyLists, since)
		require.Equal(t, []string{second.EventID()}, extensionStickyIDs(res, room.ID))

		// Ignored users' sticky events are left out.
		require.NoError(t, env.db.UpdateIgnoresForUser(context.Background(), alice.ID, &types.IgnoredUsers{List: map[string]any{bob.ID: map[string]any{}}}))
		res = env.slidingSync(alice, "", dummyLists, enabled)
		require.Equal(t, []string{second.EventID()}, extensionStickyIDs(res, room.ID))
	})
}

// Sticky events in the timeline of the response are left out of the
// extension, and carry their TTL in the timeline instead.
func TestStickyEventsSlidingSyncTimelineDedup(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		env := newStickyEnv(t, dbType, alice)
		room := test.NewRoom(t, alice)
		env.send(room.Events()...)

		s0 := sendSticky(room, alice, "0", 5*time.Minute)(t)
		s1 := sendSticky(room, alice, "1", 5*time.Minute)(t)
		regular := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "regular"})
		env.send(s0, s1, regular)
		env.waitFor(alice, room.ID, regular)

		lists := map[string]any{"main": map[string]any{"ranges": [][]int{{0, 10}}, "required_state": []any{}, "timeline_limit": 2}}
		res := env.slidingSync(alice, "", lists, map[string]any{"enabled": true, "limit": 2})
		require.Equal(t, []string{s0.EventID()}, extensionStickyIDs(res, room.ID))
		var timeline []string
		for _, ev := range res.Get("rooms." + gjson.Escape(room.ID) + ".timeline").Array() {
			timeline = append(timeline, ev.Get("event_id").Str)
		}
		require.Equal(t, []string{s1.EventID(), regular.EventID()}, timeline)
		inTimeline := res.Get("rooms." + gjson.Escape(room.ID) + ".timeline.0")
		require.Greater(t, ttlOf(inTimeline).Int(), int64(0), inTimeline.Raw)
	})
}

// The limit spreads sticky events over responses in stream order, and a
// limit of zero returns nothing.
func TestStickyEventsSlidingSyncPagination(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		env := newStickyEnv(t, dbType, alice)
		room := test.NewRoom(t, alice)
		env.send(room.Events()...)

		var sent []string
		var events []*rstypes.HeaderedEvent
		for i := range 4 {
			ev := sendSticky(room, alice, fmt.Sprint(i), 5*time.Minute)(t)
			events = append(events, ev)
			sent = append(sent, ev.EventID())
		}
		env.send(events...)
		env.waitFor(alice, room.ID, events[len(events)-1])

		res := env.slidingSync(alice, "", dummyLists, map[string]any{"enabled": true, "limit": 2})
		require.Equal(t, sent[:2], extensionStickyIDs(res, room.ID))
		nextBatch := stickyExtension(res).Get("next_batch").Str

		res = env.slidingSync(alice, "", dummyLists, map[string]any{"enabled": true, "since": nextBatch, "limit": 0})
		require.False(t, stickyExtension(res).Exists(), res.Raw)

		res = env.slidingSync(alice, "", dummyLists, map[string]any{"enabled": true, "since": nextBatch, "limit": 3})
		require.Equal(t, sent[2:], extensionStickyIDs(res, room.ID))
	})
}

// Joined users receive sticky events from before they joined, whatever the
// history visibility, also beyond the timeline.
func TestStickyEventsSlidingSyncHistoryVisibility(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		env := newStickyEnv(t, dbType, alice, bob)
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat), test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityJoined))
		env.send(room.Events()...)

		sticky := sendSticky(room, alice, "sticky", 5*time.Minute)(t)
		regular := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "regular"})
		join := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(bob.ID))
		env.send(sticky, regular, join)
		env.waitFor(bob, room.ID, join)

		// History visibility hides both earlier events from the timeline, so
		// the sticky one arrives through the extension instead.
		lists := map[string]any{"main": map[string]any{"ranges": [][]int{{0, 10}}, "required_state": []any{}, "timeline_limit": 10}}
		res := env.slidingSync(bob, "", lists, map[string]any{"enabled": true})
		timeline := res.Get("rooms." + gjson.Escape(room.ID) + ".timeline")
		require.True(t, timeline.Get(fmt.Sprintf(`#(event_id=="%s")`, join.EventID())).Exists(), timeline.Raw)
		require.False(t, timeline.Get(fmt.Sprintf(`#(event_id=="%s")`, regular.EventID())).Exists(), timeline.Raw)
		require.False(t, timeline.Get(fmt.Sprintf(`#(event_id=="%s")`, sticky.EventID())).Exists(), timeline.Raw)
		require.Equal(t, []string{sticky.EventID()}, extensionStickyIDs(res, room.ID))
	})
}
