package syncapi

import (
	"context"
	"fmt"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// TestSyncStateAfter checks MSC4222: a client that opts in with
// use_state_after gets the state at the end of the timeline in state_after,
// state events of the timeline included, instead of the state at its start.
func TestSyncStateAfter(t *testing.T) {
	test.WithAllDatabases(t, testSyncStateAfter)
}

// stateAfterRig syncs as alice against a sync API fed by a real roomserver.
type stateAfterRig struct {
	t       *testing.T
	routers httputil.Routers
	rsAPI   rsapi.RoomserverInternalAPI
	alice   *test.User
	tokens  map[*test.User]string
}

func (r *stateAfterRig) send(events ...*rstypes.HeaderedEvent) {
	r.t.Helper()
	if err := rsapi.SendEvents(context.Background(), r.rsAPI, rsapi.KindNew, events, "test", "test", "test", nil, false); err != nil {
		r.t.Fatalf("failed to send events: %v", err)
	}
}

// syncAs makes a sync request as the given user and returns the body.
func (r *stateAfterRig) syncAs(user *test.User, wantCode int, params map[string]string) string {
	r.t.Helper()
	query := map[string]string{"access_token": r.tokens[user], "timeout": "0"}
	for k, v := range params {
		query[k] = v
	}
	w := httptest.NewRecorder()
	r.routers.Client.ServeHTTP(w, test.NewRequest(r.t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(query)))
	if w.Code != wantCode {
		r.t.Fatalf("got HTTP %d want %d: %s", w.Code, wantCode, w.Body.String())
	}
	return w.Body.String()
}

func (r *stateAfterRig) sync(params map[string]string) string {
	r.t.Helper()
	return r.syncAs(r.alice, 200, params)
}

// syncUntil repeats a sync as the given user until done accepts its body.
func (r *stateAfterRig) syncUntil(user *test.User, params map[string]string, done func(string) bool) string {
	r.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		res := r.syncAs(user, 200, params)
		if done(res) {
			return res
		}
		if time.Now().After(deadline) {
			r.t.Fatalf("timed out waiting for sync with %v: %s", params, res)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// hasMessage reports whether the timeline of the room at path holds a message with the given body.
func hasMessage(res, path, body string) bool {
	return gjson.Get(res, fmt.Sprintf(`%s.timeline.events.#(content.body==%q)`, path, body)).Exists()
}

// eventIDs returns the IDs of the events in the given section of the room at path.
func eventIDs(res, path, section string) []string {
	var ids []string
	for _, ev := range gjson.Get(res, path+"."+gjson.Escape(section)+".events").Array() {
		ids = append(ids, ev.Get("event_id").Str)
	}
	return ids
}

// stateKeys returns "type|state_key" for the events in the given section of the room at path.
func stateKeys(res, path, section string) []string {
	var keys []string
	for _, ev := range gjson.Get(res, path+"."+gjson.Escape(section)+".events").Array() {
		keys = append(keys, ev.Get("type").Str+"|"+ev.Get("state_key").Str)
	}
	return keys
}

//nolint:gocyclo
func testSyncStateAfter(t *testing.T, dbType test.DBType) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	charlie := test.NewUser(t)
	aliceDev := userapi.Device{ID: "ALICEID", UserID: alice.ID, AccessToken: "ALICE_BEARER_TOKEN", AccountType: userapi.AccountTypeUser}
	bobDev := userapi.Device{ID: "BOBID", UserID: bob.ID, AccessToken: "BOB_BEARER_TOKEN", AccountType: userapi.AccountTypeUser}

	cfg, processCtx, closeRig := testrig.CreateConfig(t, dbType)
	defer closeRig()
	cfg.ClientAPI.RateLimiting = config.RateLimiting{Enabled: false}
	routers := httputil.NewRouters()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)

	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, &syncUserAPI{accounts: []userapi.Device{aliceDev, bobDev}}, rsAPI, caches, caching.DisableMetrics)

	r := &stateAfterRig{
		t: t, routers: routers, rsAPI: rsAPI, alice: alice,
		tokens: map[*test.User]string{alice: aliceDev.AccessToken, bob: bobDev.AccessToken},
	}

	// A room bob created and alice joined, with a topic and a message in the timeline.
	room := test.NewRoom(t, bob, test.RoomPreset(test.PresetPublicChat))
	path := "rooms.join." + gjson.Escape(room.ID)
	events := room.Events()
	events = append(events, room.CreateAndInsert(t, alice, "m.room.member", map[string]any{"membership": "join"}, test.WithStateKey(alice.ID)))
	topic := room.CreateAndInsert(t, bob, "m.room.topic", map[string]any{"topic": "first"}, test.WithStateKey(""))
	events = append(events, topic, room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "hello"}))
	r.send(events...)
	initial := r.syncUntil(alice, nil, func(res string) bool { return hasMessage(res, path, "hello") })
	since := gjson.Get(initial, "next_batch").Str

	t.Run("no opt-in keeps state", func(t *testing.T) {
		res := r.sync(map[string]string{"filter": `{"room":{"timeline":{"limit":2}}}`})
		room := gjson.Get(res, path)
		if room.Get("state_after").Exists() || room.Get(gjson.Escape("org.matrix.msc4222.state_after")).Exists() {
			t.Fatalf("state_after without opt-in: %s", room.Raw)
		}
		if !slices.Contains(stateKeys(res, path, "state"), "m.room.create|") {
			t.Fatalf("state lacks the create event: %s", room.Raw)
		}
		// The topic is in the timeline, so it is not repeated in state.
		if slices.Contains(eventIDs(res, path, "state"), topic.EventID()) {
			t.Fatalf("state repeats the timeline topic: %s", room.Raw)
		}
	})

	t.Run("initial sync", func(t *testing.T) {
		res := r.sync(map[string]string{"use_state_after": "true"})
		room := gjson.Get(res, path)
		if room.Get("state").Exists() {
			t.Fatalf("state sent with use_state_after: %s", room.Raw)
		}
		if !slices.Contains(eventIDs(res, path, "timeline"), topic.EventID()) {
			t.Fatalf("timeline lacks the topic: %s", room.Raw)
		}
		// The whole timeline fits, yet state_after holds the current state,
		// timeline state events included.
		keys := stateKeys(res, path, "state_after")
		for _, want := range []string{"m.room.create|", "m.room.member|" + alice.ID, "m.room.member|" + bob.ID} {
			if !slices.Contains(keys, want) {
				t.Fatalf("state_after lacks %s: %s", want, room.Raw)
			}
		}
		if !slices.Contains(eventIDs(res, path, "state_after"), topic.EventID()) {
			t.Fatalf("state_after lacks the topic: %s", room.Raw)
		}
	})

	t.Run("unstable parameter", func(t *testing.T) {
		res := r.sync(map[string]string{"org.matrix.msc4222.use_state_after": "true"})
		room := gjson.Get(res, path)
		if room.Get("state").Exists() || room.Get("state_after").Exists() {
			t.Fatalf("stable or legacy section with the unstable parameter: %s", room.Raw)
		}
		if !slices.Contains(eventIDs(res, path, "org.matrix.msc4222.state_after"), topic.EventID()) {
			t.Fatalf("unstable state_after lacks the topic: %s", room.Raw)
		}
	})

	t.Run("invalid parameter", func(t *testing.T) {
		for _, param := range []string{"use_state_after", "org.matrix.msc4222.use_state_after"} {
			res := r.syncAs(alice, 400, map[string]string{param: "yes"})
			if code := gjson.Get(res, "errcode").Str; code != "M_INVALID_PARAM" {
				t.Fatalf("%s=yes: got errcode %q want M_INVALID_PARAM", param, code)
			}
		}
	})

	// A topic change in the timeline of an incremental sync is part of the
	// state after it, but not of the state before it.
	topic2 := room.CreateAndInsert(t, bob, "m.room.topic", map[string]any{"topic": "second"}, test.WithStateKey(""))
	r.send(topic2, room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "m2"}))
	var afterTopic2 string

	t.Run("incremental sync", func(t *testing.T) {
		res := r.syncUntil(alice, map[string]string{"since": since, "use_state_after": "true"}, func(res string) bool { return hasMessage(res, path, "m2") })
		afterTopic2 = gjson.Get(res, "next_batch").Str
		room := gjson.Get(res, path)
		if room.Get("timeline.limited").Bool() {
			t.Fatalf("timeline unexpectedly limited: %s", room.Raw)
		}
		if !slices.Contains(eventIDs(res, path, "timeline"), topic2.EventID()) {
			t.Fatalf("timeline lacks the new topic: %s", room.Raw)
		}
		if ids := eventIDs(res, path, "state_after"); !slices.Equal(ids, []string{topic2.EventID()}) {
			t.Fatalf("state_after = %v, want only the new topic: %s", ids, room.Raw)
		}

		legacy := r.sync(map[string]string{"since": since})
		if slices.Contains(eventIDs(legacy, path, "state"), topic2.EventID()) {
			t.Fatalf("legacy state repeats the timeline topic: %s", gjson.Get(legacy, path).Raw)
		}
	})

	t.Run("empty state_after", func(t *testing.T) {
		r.send(room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "m3"}))
		res := r.syncUntil(alice, map[string]string{"since": afterTopic2, "use_state_after": "true"}, func(res string) bool { return hasMessage(res, path, "m3") })
		stateAfter := gjson.Get(res, path+".state_after.events")
		if !stateAfter.IsArray() || len(stateAfter.Array()) != 0 {
			t.Fatalf("want an empty state_after, got: %s", gjson.Get(res, path).Raw)
		}
	})

	t.Run("gappy incremental sync", func(t *testing.T) {
		topic3 := room.CreateAndInsert(t, bob, "m.room.topic", map[string]any{"topic": "third"}, test.WithStateKey(""))
		gap := []*rstypes.HeaderedEvent{topic3}
		for i := range 5 {
			gap = append(gap, room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": fmt.Sprintf("gap %d", i)}))
		}
		r.send(gap...)
		params := map[string]string{"since": afterTopic2, "use_state_after": "true", "filter": `{"room":{"timeline":{"limit":2}}}`}
		res := r.syncUntil(alice, params, func(res string) bool { return hasMessage(res, path, "gap 4") })
		room := gjson.Get(res, path)
		if !room.Get("timeline.limited").Bool() {
			t.Fatalf("timeline not limited: %s", room.Raw)
		}
		if slices.Contains(eventIDs(res, path, "timeline"), topic3.EventID()) {
			t.Fatalf("topic unexpectedly in the limited timeline: %s", room.Raw)
		}
		if !slices.Contains(eventIDs(res, path, "state_after"), topic3.EventID()) {
			t.Fatalf("state_after lacks the topic from the gap: %s", room.Raw)
		}
		if slices.Contains(eventIDs(res, path, "state_after"), topic2.EventID()) {
			t.Fatalf("state_after holds the replaced topic: %s", room.Raw)
		}
	})

	t.Run("full_state", func(t *testing.T) {
		res := r.sync(map[string]string{"since": afterTopic2, "use_state_after": "true", "full_state": "true"})
		room := gjson.Get(res, path)
		keys := stateKeys(res, path, "state_after")
		for _, want := range []string{"m.room.create|", "m.room.join_rules|", "m.room.topic|", "m.room.member|" + alice.ID} {
			if !slices.Contains(keys, want) {
				t.Fatalf("full state_after lacks %s: %s", want, room.Raw)
			}
		}
	})

	t.Run("state filter", func(t *testing.T) {
		res := r.sync(map[string]string{"use_state_after": "true", "filter": `{"room":{"state":{"types":["m.room.topic"]}}}`})
		if keys := stateKeys(res, path, "state_after"); !slices.Equal(keys, []string{"m.room.topic|"}) {
			t.Fatalf("filtered state_after = %v, want only the topic: %s", keys, gjson.Get(res, path).Raw)
		}
	})

	t.Run("lazy loading", func(t *testing.T) {
		llRoom := test.NewRoom(t, bob, test.RoomPreset(test.PresetPublicChat))
		llPath := "rooms.join." + gjson.Escape(llRoom.ID)
		events := llRoom.Events()
		events = append(events,
			llRoom.CreateAndInsert(t, charlie, "m.room.member", map[string]any{"membership": "join"}, test.WithStateKey(charlie.ID)),
			llRoom.CreateAndInsert(t, alice, "m.room.member", map[string]any{"membership": "join"}, test.WithStateKey(alice.ID)),
			llRoom.CreateAndInsert(t, charlie, "m.room.message", map[string]any{"body": "from charlie"}),
			llRoom.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "from bob"}),
		)
		r.send(events...)
		filter := `{"room":{"state":{"lazy_load_members":true},"timeline":{"limit":1}}}`
		res := r.syncUntil(alice, map[string]string{"use_state_after": "true", "filter": filter}, func(res string) bool { return hasMessage(res, llPath, "from bob") })
		keys := stateKeys(res, llPath, "state_after")
		// The timeline sender and alice herself, but not charlie, who only
		// sent an event before the timeline.
		for _, want := range []string{"m.room.create|", "m.room.member|" + bob.ID, "m.room.member|" + alice.ID} {
			if !slices.Contains(keys, want) {
				t.Fatalf("lazy state_after lacks %s: %s", want, gjson.Get(res, llPath).Raw)
			}
		}
		if slices.Contains(keys, "m.room.member|"+charlie.ID) {
			t.Fatalf("lazy state_after holds charlie's membership: %s", gjson.Get(res, llPath).Raw)
		}

		// Bob kicks charlie. Charlie sent nothing in the timeline, but the
		// membership change is part of the state after it.
		kick := llRoom.CreateAndInsert(t, bob, "m.room.member", map[string]any{"membership": "leave"}, test.WithStateKey(charlie.ID))
		r.send(kick, llRoom.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "kicked"}))
		params := map[string]string{"since": gjson.Get(res, "next_batch").Str, "use_state_after": "true", "filter": `{"room":{"state":{"lazy_load_members":true}}}`}
		res = r.syncUntil(alice, params, func(res string) bool { return hasMessage(res, llPath, "kicked") })
		if !slices.Contains(eventIDs(res, llPath, "state_after"), kick.EventID()) {
			t.Fatalf("lazy incremental state_after lacks the kick: %s", gjson.Get(res, llPath).Raw)
		}
	})

	t.Run("left room", func(t *testing.T) {
		leftRoom := test.NewRoom(t, bob, test.RoomPreset(test.PresetPublicChat))
		leftPath := "rooms.leave." + gjson.Escape(leftRoom.ID)
		events := leftRoom.Events()
		events = append(events, leftRoom.CreateAndInsert(t, alice, "m.room.member", map[string]any{"membership": "join"}, test.WithStateKey(alice.ID)))
		r.send(events...)
		res := r.syncUntil(alice, nil, func(res string) bool {
			return gjson.Get(res, "rooms.join."+gjson.Escape(leftRoom.ID)).Exists()
		})
		since := gjson.Get(res, "next_batch").Str

		beforeLeave := leftRoom.CreateAndInsert(t, bob, "m.room.name", map[string]any{"name": "before"}, test.WithStateKey(""))
		leave := leftRoom.CreateAndInsert(t, alice, "m.room.member", map[string]any{"membership": "leave"}, test.WithStateKey(alice.ID))
		afterLeave := leftRoom.CreateAndInsert(t, bob, "m.room.topic", map[string]any{"topic": "after leave"}, test.WithStateKey(""))
		r.send(beforeLeave, leave, afterLeave, leftRoom.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "after leave"}))
		// Wait until the sync API has the events after the leave as well.
		r.syncUntil(bob, nil, func(res string) bool {
			return hasMessage(res, "rooms.join."+gjson.Escape(leftRoom.ID), "after leave")
		})

		res = r.sync(map[string]string{"since": since, "use_state_after": "true"})
		ids := eventIDs(res, leftPath, "state_after")
		if !slices.Contains(ids, beforeLeave.EventID()) || !slices.Contains(ids, leave.EventID()) {
			t.Fatalf("left state_after lacks the state up to the leave: %s", gjson.Get(res, leftPath).Raw)
		}
		if slices.Contains(ids, afterLeave.EventID()) {
			t.Fatalf("left state_after holds state from after the leave: %s", gjson.Get(res, leftPath).Raw)
		}
		if gjson.Get(res, leftPath+".state").Exists() {
			t.Fatalf("left room has state with use_state_after: %s", gjson.Get(res, leftPath).Raw)
		}
	})
}
