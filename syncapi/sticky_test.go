// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package syncapi

import (
	"context"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// excludeMessages keeps messages out of the timeline, so that sticky messages
// can only arrive in the sticky section.
const excludeMessages = `{"room":{"timeline":{"types":["io.element.example"]}}}`

type stickyEnv struct {
	t       *testing.T
	routers httputil.Routers
	rsAPI   rsapi.RoomserverInternalAPI
	db      storage.Database
	devices map[string]userapi.Device
}

func newStickyEnv(t *testing.T, dbType test.DBType, users ...*test.User) *stickyEnv {
	t.Helper()
	cfg, processCtx, closeRig := testrig.CreateConfig(t, dbType)
	t.Cleanup(closeRig)
	cfg.ClientAPI.RateLimiting = config.RateLimiting{Enabled: false}
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	t.Cleanup(func() { jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream) })

	env := &stickyEnv{t: t, routers: httputil.NewRouters(), devices: make(map[string]userapi.Device)}
	accounts := make([]userapi.Device, 0, len(users))
	for i, user := range users {
		dev := userapi.Device{
			ID:          fmt.Sprintf("DEVICE%d", i),
			UserID:      user.ID,
			AccessToken: fmt.Sprintf("TOKEN%d", i),
			AccountType: userapi.AccountTypeUser,
		}
		env.devices[user.ID] = dev
		accounts = append(accounts, dev)
	}
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	env.rsAPI = rsAPI
	AddPublicRoutes(processCtx, env.routers, cfg, cm, &natsInstance, accountDataUserAPI{&syncUserAPI{accounts: accounts}}, rsAPI, caches, caching.DisableMetrics, nil, nil)

	db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
	require.NoError(t, err)
	env.db = db
	return env
}

func (e *stickyEnv) send(events ...*rstypes.HeaderedEvent) {
	e.t.Helper()
	require.NoError(e.t, rsapi.SendEvents(context.Background(), e.rsAPI, rsapi.KindNew, events, "test", "test", "test", nil, false))
}

func (e *stickyEnv) sync(user *test.User, since, filter string) gjson.Result {
	e.t.Helper()
	params := map[string]string{
		"access_token": e.devices[user.ID].AccessToken,
		"timeout":      "0",
	}
	if since != "" {
		params["since"] = since
	}
	if filter != "" {
		params["filter"] = filter
	}
	w := httptest.NewRecorder()
	e.routers.Client.ServeHTTP(w, test.NewRequest(e.t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(params)))
	require.Equal(e.t, 200, w.Code, w.Body.String())
	return gjson.Parse(w.Body.String())
}

// waitFor syncs from scratch until the event reaches the user's timeline,
// which means the sync API has processed it and everything before it.
func (e *stickyEnv) waitFor(user *test.User, roomID string, ev *rstypes.HeaderedEvent) {
	e.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		res := e.sync(user, "", `{"room":{"timeline":{"limit":1}}}`)
		if res.Get(fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id==%q)`, gjson.Escape(roomID), ev.EventID())).Exists() {
			return
		}
		if time.Now().After(deadline) {
			e.t.Fatalf("timed out waiting for %s: %s", ev.EventID(), res.Raw)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func stickyIDs(res gjson.Result, roomID string) []string {
	var ids []string
	for _, ev := range res.Get("rooms.join." + gjson.Escape(roomID) + ".msc4354_sticky.events").Array() {
		ids = append(ids, ev.Get("event_id").Str)
	}
	return ids
}

func timelineIDs(res gjson.Result, roomID string) []string {
	var ids []string
	for _, ev := range res.Get("rooms.join." + gjson.Escape(roomID) + ".timeline.events").Array() {
		ids = append(ids, ev.Get("event_id").Str)
	}
	return ids
}

func ttlOf(ev gjson.Result) gjson.Result {
	return ev.Get("unsigned." + gjson.Escape(synctypes.UnsignedFieldMSC4354StickyTTL))
}

func sendSticky(room *test.Room, sender *test.User, body string, d time.Duration) func(t *testing.T) *rstypes.HeaderedEvent {
	return func(t *testing.T) *rstypes.HeaderedEvent {
		return room.CreateAndInsert(t, sender, "m.room.message", map[string]any{"body": body, "msgtype": "m.text"}, test.WithStickyDuration(d))
	}
}

// Sticky events beyond the timeline arrive in their own section, with how
// long they remain sticky, and only once. A sticky event the timeline carries
// is not repeated in the sticky section.
func TestStickyEventsSyncDelivery(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		env := newStickyEnv(t, dbType, alice)
		room := test.NewRoom(t, alice)
		env.send(room.Events()...)

		sticky := sendSticky(room, alice, "sticky", 5*time.Minute)(t)
		events := []*rstypes.HeaderedEvent{sticky}
		for i := range 3 {
			events = append(events, room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": fmt.Sprint(i)}))
		}
		env.send(events...)
		env.waitFor(alice, room.ID, events[len(events)-1])

		const limit2 = `{"room":{"timeline":{"limit":2}}}`
		res := env.sync(alice, "", limit2)
		require.NotContains(t, timelineIDs(res, room.ID), sticky.EventID())
		require.Equal(t, []string{sticky.EventID()}, stickyIDs(res, room.ID))
		ev := res.Get("rooms.join." + gjson.Escape(room.ID) + ".msc4354_sticky.events.0")
		require.Equal(t, int64(300000), ev.Get("msc4354_sticky.duration_ms").Int(), ev.Raw)
		require.Greater(t, ttlOf(ev).Int(), int64(0), ev.Raw)
		require.LessOrEqual(t, ttlOf(ev).Int(), int64(300000), ev.Raw)

		// A new sticky event within the timeline comes with its TTL there,
		// and the sticky section leaves it out.
		inTimeline := sendSticky(room, alice, "in timeline", time.Minute)(t)
		env.send(inTimeline)
		env.waitFor(alice, room.ID, inTimeline)
		next := env.sync(alice, res.Get("next_batch").Str, limit2)
		require.Contains(t, timelineIDs(next, room.ID), inTimeline.EventID())
		require.Empty(t, stickyIDs(next, room.ID))
		timelineEv := next.Get(fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id==%q)`, gjson.Escape(room.ID), inTimeline.EventID()))
		require.Greater(t, ttlOf(timelineEv).Int(), int64(0), timelineEv.Raw)
		require.Equal(t, int64(60000), timelineEv.Get("msc4354_sticky.duration_ms").Int(), timelineEv.Raw)

		// Neither is delivered again.
		again := env.sync(alice, next.Get("next_batch").Str, excludeMessages)
		require.Empty(t, stickyIDs(again, room.ID))

		// A regular event has no TTL.
		plain := next.Get(fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id==%q)`, gjson.Escape(room.ID), events[len(events)-1].EventID()))
		if plain.Exists() {
			require.False(t, ttlOf(plain).Exists(), plain.Raw)
		}
	})
}

// A sticky event that the timeline filter leaves out arrives in the sticky
// section, and an expired one is no longer delivered.
func TestStickyEventsSyncFilteredAndExpired(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		env := newStickyEnv(t, dbType, alice)
		room := test.NewRoom(t, alice)
		env.send(room.Events()...)

		short := sendSticky(room, alice, "short", 1500*time.Millisecond)(t)
		long := sendSticky(room, alice, "long", time.Hour)(t)
		env.send(short, long)
		env.waitFor(alice, room.ID, long)

		res := env.sync(alice, "", excludeMessages)
		require.Equal(t, []string{short.EventID(), long.EventID()}, stickyIDs(res, room.ID))

		time.Sleep(time.Until(short.OriginServerTS().Time().Add(1600 * time.Millisecond)))
		res = env.sync(alice, "", excludeMessages)
		require.Equal(t, []string{long.EventID()}, stickyIDs(res, room.ID))

		// An event that had already expired when it arrived never enters
		// the stream.
		expired := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "old"},
			test.WithStickyDuration(time.Minute), test.WithTimestamp(time.Now().Add(-2*time.Minute)))
		marker := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "marker"})
		env.send(expired, marker)
		env.waitFor(alice, room.ID, marker)
		res = env.sync(alice, "", excludeMessages)
		require.Equal(t, []string{long.EventID()}, stickyIDs(res, room.ID))
	})
}

// Sticky events of ignored users are left out.
func TestStickyEventsSyncIgnoredUsers(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		env := newStickyEnv(t, dbType, alice, bob)
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(bob.ID))
		env.send(room.Events()...)
		require.NoError(t, env.db.UpdateIgnoresForUser(context.Background(), alice.ID, &types.IgnoredUsers{List: map[string]any{bob.ID: map[string]any{}}}))

		fromBob := sendSticky(room, bob, "ignored", time.Hour)(t)
		fromAlice := sendSticky(room, alice, "kept", time.Hour)(t)
		env.send(fromBob, fromAlice)
		env.waitFor(alice, room.ID, fromAlice)

		res := env.sync(alice, "", excludeMessages)
		require.Equal(t, []string{fromAlice.EventID()}, stickyIDs(res, room.ID))
		res = env.sync(bob, "", excludeMessages)
		require.Equal(t, []string{fromBob.EventID(), fromAlice.EventID()}, stickyIDs(res, room.ID))
	})
}

// Joining a room delivers its unexpired sticky events, even those sent before
// the joining user's sync position and hidden by history visibility, along
// with the memberships of their senders when members are lazily loaded.
// Changing a display name is not a join and delivers nothing again.
func TestStickyEventsSyncJoin(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		env := newStickyEnv(t, dbType, alice, bob)
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat), test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityJoined))
		env.send(room.Events()...)

		sticky := sendSticky(room, alice, "sticky", time.Hour)(t)
		regular := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "regular"})
		env.send(sticky, regular)
		env.waitFor(alice, room.ID, regular)

		// Bob's sync position is past the sticky event before he joins.
		bobRoom := test.NewRoom(t, bob)
		env.send(bobRoom.Events()...)
		env.waitFor(bob, bobRoom.ID, bobRoom.Events()[len(bobRoom.Events())-1])
		before := env.sync(bob, "", excludeMessages)
		require.False(t, before.Get("rooms.join."+gjson.Escape(room.ID)).Exists())

		join := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(bob.ID))
		env.send(join)
		env.waitFor(bob, room.ID, join)

		const lazy = `{"room":{"state":{"lazy_load_members":true},"timeline":{"types":["io.element.example"]}}}`
		joined := env.sync(bob, before.Get("next_batch").Str, lazy)
		require.Equal(t, []string{sticky.EventID()}, stickyIDs(joined, room.ID))
		require.True(t, joined.Get(fmt.Sprintf(`rooms.join.%s.state.events.#(state_key==%q)`, gjson.Escape(room.ID), alice.ID)).Exists(),
			"the sticky event's sender is lazily loaded: %s", joined.Get("rooms.join."+gjson.Escape(room.ID)).Raw)

		// History visibility hides the regular event, not the sticky one.
		initial := env.sync(bob, "", "")
		require.NotContains(t, timelineIDs(initial, room.ID), regular.EventID())
		require.NotContains(t, timelineIDs(initial, room.ID), sticky.EventID())
		require.Equal(t, []string{sticky.EventID()}, stickyIDs(initial, room.ID))

		rename := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "join", "displayname": "Bobby"}, test.WithStateKey(bob.ID))
		env.send(rename)
		env.waitFor(bob, room.ID, rename)
		renamed := env.sync(bob, joined.Get("next_batch").Str, `{"room":{"timeline":{"types":["m.room.member"]}}}`)
		require.Contains(t, timelineIDs(renamed, room.ID), rename.EventID())
		require.Empty(t, stickyIDs(renamed, room.ID))
	})
}

// More sticky events than one response carries are spread over consecutive
// responses, in stream order.
func TestStickyEventsSyncPagination(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		env := newStickyEnv(t, dbType, alice)
		room := test.NewRoom(t, alice)
		env.send(room.Events()...)

		const extra = 5
		var sent []string
		var events []*rstypes.HeaderedEvent
		for i := range types.MaxStickyEventsInSync + extra {
			ev := sendSticky(room, alice, fmt.Sprint(i), time.Hour)(t)
			events = append(events, ev)
			sent = append(sent, ev.EventID())
		}
		env.send(events...)
		env.waitFor(alice, room.ID, events[len(events)-1])

		first := env.sync(alice, "", excludeMessages)
		require.Equal(t, sent[:types.MaxStickyEventsInSync], stickyIDs(first, room.ID))
		second := env.sync(alice, first.Get("next_batch").Str, excludeMessages)
		require.Equal(t, sent[types.MaxStickyEventsInSync:], stickyIDs(second, room.ID))
		third := env.sync(alice, second.Get("next_batch").Str, excludeMessages)
		require.Empty(t, stickyIDs(third, room.ID))
	})
}
