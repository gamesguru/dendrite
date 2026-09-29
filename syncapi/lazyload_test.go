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

// TestLazyLoadRetriedSync checks that a sync retried from the same token
// sends the lazily loaded memberships again. A client that never received a
// response retries from its previous token, so memberships only count as sent
// once the client syncs from the response that carried them.
func TestLazyLoadRetriedSync(t *testing.T) {
	test.WithAllDatabases(t, testLazyLoadRetriedSync)
}

func testLazyLoadRetriedSync(t *testing.T, dbType test.DBType) {
	alice := test.NewUser(t)
	aliceDev := userapi.Device{
		ID:          "ALICEID",
		UserID:      alice.ID,
		AccessToken: "ALICE_BEARER_TOKEN",
		DisplayName: "ALICE",
		AccountType: userapi.AccountTypeUser,
	}
	bob := test.NewUser(t)

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
	AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, &syncUserAPI{accounts: []userapi.Device{aliceDev}}, rsAPI, caches, caching.DisableMetrics)

	ctx := context.Background()
	send := func(events ...*rstypes.HeaderedEvent) {
		t.Helper()
		if err := rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, events, "test", "test", "test", nil, false); err != nil {
			t.Fatalf("failed to send events: %v", err)
		}
	}
	const filter = `{"room":{"state":{"lazy_load_members":true},"timeline":{"limit":1}}}`
	sync := func(since string) string {
		t.Helper()
		w := httptest.NewRecorder()
		routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
			"access_token": aliceDev.AccessToken,
			"timeout":      "0",
			"since":        since,
			"filter":       filter,
		})))
		if w.Code != 200 {
			t.Fatalf("got HTTP %d want 200: %s", w.Code, w.Body.String())
		}
		return w.Body.String()
	}

	room := test.NewRoom(t, bob, test.RoomPreset(test.PresetPublicChat))
	roomPath := "rooms.join." + gjson.Escape(room.ID)
	syncUntilMessage := func(since, body string) string {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for {
			res := sync(since)
			if gjson.Get(res, fmt.Sprintf(`%s.timeline.events.#(content.body==%q)`, roomPath, body)).Exists() {
				return res
			}
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %q: %s", body, res)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}
	membersIn := func(res string) []string {
		var members []string
		for _, section := range []string{"state", "timeline"} {
			for _, ev := range gjson.Get(res, roomPath+"."+section+".events").Array() {
				if ev.Get("type").Str == "m.room.member" {
					members = append(members, ev.Get("state_key").Str)
				}
			}
		}
		return members
	}

	// Give alice a room of her own, so her sync has a position to resume from.
	aliceRoom := test.NewRoom(t, alice)
	send(aliceRoom.Events()...)
	syncUntil(t, routers, aliceDev.AccessToken, false, func(syncBody string) bool {
		return gjson.Get(syncBody, "rooms.join."+gjson.Escape(aliceRoom.ID)).Exists()
	})
	since := gjson.Get(sync(""), "next_batch").Str

	events := room.Events()
	events = append(events, room.CreateAndInsert(t, alice, "m.room.member", map[string]any{"membership": "join"}, test.WithStateKey(alice.ID)))
	events = append(events, room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "first"}))
	send(events...)

	// Sync twice from the same token, as a client does when the first
	// response is lost. Both responses must carry bob's membership.
	first := syncUntilMessage(since, "first")
	retried := sync(since)
	for name, res := range map[string]string{"first": first, "retried": retried} {
		if !slices.Contains(membersIn(res), bob.ID) {
			t.Fatalf("%s sync lacks bob's membership: %s", name, gjson.Get(res, roomPath).Raw)
		}
	}

	// Once the client syncs on from the retried response, bob's membership
	// counts as sent and is not repeated.
	send(room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "second"}))
	next := syncUntilMessage(gjson.Get(retried, "next_batch").Str, "second")
	if slices.Contains(membersIn(next), bob.ID) {
		t.Fatalf("resent bob's membership after the client received it: %s", gjson.Get(next, roomPath).Raw)
	}
}
