package syncapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// largeStateRoomserverAPI serves the state events of a large room to the sync
// API consumer, as the roomserver does when an output event adds state the
// sync API has never seen.
type largeStateRoomserverAPI struct {
	syncRoomserverAPI
	events map[string]*rstypes.HeaderedEvent
}

func (s *largeStateRoomserverAPI) QueryEventsByID(ctx context.Context, req *rsapi.QueryEventsByIDRequest, res *rsapi.QueryEventsByIDResponse) error {
	for _, id := range req.EventIDs {
		if ev, ok := s.events[id]; ok {
			res.Events = append(res.Events, ev)
		}
	}
	return nil
}

// TestLargeStateRewriteUnrelatedRoomLatency reproduces issue #470: one room
// event rewrites the sync API's current state of a room with tens of
// thousands of members, while another user keeps talking in a small room.
// The consumer is serial, so every small-room message waits behind the
// rewrite. The test reports how long, and checks that both rooms end up
// correct with every message delivered once.
//
// Set ZENDRITE_LARGE_STATE_ENTRIES=50000 for the size seen in production.
//
// The worst unrelated-room event-to-sync latency must stay within
// ZENDRITE_LARGE_STATE_MAX_LATENCY, 10s by default. With 50,000 entries and a
// 1ms round trip to PostgreSQL it measured 2m49s before state writes were
// batched and 3.2s after, so the target leaves headroom for slower machines
// while still catching a return to per-entry round trips.
func TestLargeStateRewriteUnrelatedRoomLatency(t *testing.T) {
	n := 2000
	if v := os.Getenv("ZENDRITE_LARGE_STATE_ENTRIES"); v != "" {
		var err error
		if n, err = strconv.Atoi(v); err != nil {
			t.Fatalf("ZENDRITE_LARGE_STATE_ENTRIES=%q: %s", v, err)
		}
	}
	maxLatency := 10 * time.Second
	if v := os.Getenv("ZENDRITE_LARGE_STATE_MAX_LATENCY"); v != "" {
		var err error
		if maxLatency, err = time.ParseDuration(v); err != nil {
			t.Fatalf("ZENDRITE_LARGE_STATE_MAX_LATENCY=%q: %s", v, err)
		}
	}

	aliceUser := test.NewUser(t)
	bobUser := test.NewUser(t)
	largeRoom := test.NewRoom(t, aliceUser)
	setupEvents := slices.Clone(largeRoom.Events())
	for i := range n {
		largeRoom.CreateAndInsert(t, aliceUser, spec.MRoomMember, map[string]any{"membership": spec.Invite},
			test.WithStateKey(fmt.Sprintf("@member%d:test", i)))
	}
	rewrite := largeRoom.CreateAndInsert(t, aliceUser, "m.room.topic", map[string]any{"topic": "large"}, test.WithStateKey(""))
	largeState := largeRoom.CurrentState()
	largeStateIDs := make([]string, len(largeState))
	stateByID := make(map[string]*rstypes.HeaderedEvent, len(largeState))
	for i, ev := range largeState {
		largeStateIDs[i] = ev.EventID()
		stateByID[ev.EventID()] = ev
	}

	smallRoom := test.NewRoom(t, bobUser)
	const smallMessages = 20
	messages := make([]*rstypes.HeaderedEvent, smallMessages)
	for i := range messages {
		messages[i] = smallRoom.CreateAndInsert(t, bobUser, "m.room.message", map[string]any{"body": fmt.Sprintf("message %d", i), "msgtype": "m.text"})
	}
	smallSetup := smallRoom.Events()[:len(smallRoom.Events())-smallMessages]

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := userapi.Device{ID: "ALICEID", UserID: aliceUser.ID, AccessToken: "ALICE_BEARER_TOKEN", AccountType: userapi.AccountTypeUser}
		bob := userapi.Device{ID: "BOBID", UserID: bobUser.ID, AccessToken: "BOB_BEARER_TOKEN", AccountType: userapi.AccountTypeUser}

		cfg, processCtx, closeRig := testrig.CreateConfig(t, dbType)
		defer closeRig()
		routers := httputil.NewRouters()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)

		rsAPI := &largeStateRoomserverAPI{
			syncRoomserverAPI: syncRoomserverAPI{rooms: []*test.Room{largeRoom, smallRoom}},
			events:            stateByID,
		}
		AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, &syncUserAPI{accounts: []userapi.Device{alice, bob}}, rsAPI, caches, caching.DisableMetrics)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		if err != nil {
			t.Fatalf("NewSyncServerDatasource: %s", err)
		}

		testrig.MustPublishMsgs(t, jsctx, toNATSMsgs(t, cfg, setupEvents...)...)
		testrig.MustPublishMsgs(t, jsctx, toNATSMsgs(t, cfg, smallSetup...)...)
		bobSince := syncUntilJoined(t, routers, bob.AccessToken, smallRoom.ID)
		syncUntilJoined(t, routers, alice.AccessToken, largeRoom.ID)

		rewriteMsg := testrig.NewOutputEventMsg(t, cfg, largeRoom.ID, rsapi.OutputEvent{
			Type: rsapi.OutputTypeNewRoomEvent,
			NewRoomEvent: &rsapi.OutputNewRoomEvent{
				Event:             rewrite,
				AddsStateEventIDs: largeStateIDs,
				RewritesState:     true,
				HistoryVisibility: rewrite.Visibility,
			},
		})
		start := time.Now()
		testrig.MustPublishMsgs(t, jsctx, rewriteMsg)

		// Keep the small room busy while the rewrite is processed.
		published := make(map[string]time.Time, smallMessages)
		publishDone := make(chan struct{})
		smallMsgs := toNATSMsgs(t, cfg, messages...)
		go func() {
			defer close(publishDone)
			for i, msg := range smallMsgs {
				published[messages[i].EventID()] = time.Now()
				if _, err := jsctx.PublishMsg(msg); err != nil {
					t.Errorf("publish small room message: %s", err)
					return
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()

		// Watch the small room through /sync, as bob's client would.
		isMessage := make(map[string]bool, smallMessages)
		for _, ev := range messages {
			isMessage[ev.EventID()] = true
		}
		seen := map[string]time.Time{}
		deliveries := map[string]int{}
		deadline := time.Now().Add(15 * time.Minute)
		for len(seen) < smallMessages {
			if time.Now().After(deadline) {
				t.Fatalf("saw %d of %d small room messages before the deadline", len(seen), smallMessages)
			}
			res := mustSync(t, routers, bob.AccessToken, bobSince, "5000")
			now := time.Now()
			bobSince = res.NextBatch.String()
			if res.Rooms == nil || res.Rooms.Join[smallRoom.ID] == nil {
				continue
			}
			for _, ev := range res.Rooms.Join[smallRoom.ID].Timeline.Events {
				if !isMessage[ev.EventID] {
					continue
				}
				deliveries[ev.EventID]++
				if _, ok := seen[ev.EventID]; !ok {
					seen[ev.EventID] = now
				}
			}
		}
		<-publishDone

		// The rewrite was committed before any later message was processed,
		// so its processing time is bounded by the first small message.
		var worst, total time.Duration
		for _, ev := range messages {
			latency := seen[ev.EventID()].Sub(published[ev.EventID()])
			total += latency
			worst = max(worst, latency)
			if deliveries[ev.EventID()] != 1 {
				t.Errorf("small room message %s delivered %d times, want 1", ev.EventID(), deliveries[ev.EventID()])
			}
		}
		t.Logf("%s: rewrite of %d state events; first unrelated message visible %s after the rewrite was published;"+
			" unrelated event-to-sync latency mean %s, max %s",
			dbType2String(dbType), len(largeState), seen[messages[0].EventID()].Sub(start),
			total/smallMessages, worst)
		if worst > maxLatency {
			t.Errorf("worst unrelated-room event-to-sync latency %s exceeds the %s target", worst, maxLatency)
		}

		snapshot, err := db.NewDatabaseSnapshot(processCtx.Context())
		if err != nil {
			t.Fatalf("NewDatabaseSnapshot: %s", err)
		}
		defer snapshot.Rollback() //nolint:errcheck
		filter := synctypes.DefaultStateFilter()
		filter.Limit = len(largeState) + 1
		got, err := snapshot.GetStateEventsForRoom(processCtx.Context(), largeRoom.ID, &filter)
		if err != nil {
			t.Fatalf("GetStateEventsForRoom: %s", err)
		}
		if len(got) != len(largeState) {
			t.Fatalf("large room current state has %d events, want %d", len(got), len(largeState))
		}
		for _, ev := range got {
			if _, ok := stateByID[ev.EventID()]; !ok {
				t.Fatalf("large room current state has unexpected event %s", ev.EventID())
			}
		}
	})
}

func dbType2String(dbType test.DBType) string {
	if dbType == test.DBTypePostgres {
		return "postgres"
	}
	return "sqlite"
}

// mustSync performs one /sync for the given token and returns the response.
func mustSync(t *testing.T, routers httputil.Routers, accessToken, since, timeout string) types.Response {
	t.Helper()
	// A timeline limit above the number of messages a test sends keeps a
	// burst from being cut short as a limited timeline.
	params := map[string]string{
		"access_token": accessToken, "timeout": timeout,
		"filter": `{"room":{"timeline":{"limit":100}}}`,
	}
	if since != "" {
		params["since"] = since
	}
	w := httptest.NewRecorder()
	routers.Client.ServeHTTP(w, test.NewRequest(t, "GET", "/_matrix/client/v3/sync", test.WithQueryParams(params)))
	if w.Code != 200 {
		t.Fatalf("/sync returned HTTP %d: %s", w.Code, w.Body.String())
	}
	var res types.Response
	if err := json.NewDecoder(w.Body).Decode(&res); err != nil {
		t.Fatalf("decode /sync response: %s", err)
	}
	return res
}

// syncUntilJoined syncs until the room appears in the joined section and
// returns the next batch token.
func syncUntilJoined(t *testing.T, routers httputil.Routers, accessToken, roomID string) string {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		res := mustSync(t, routers, accessToken, "", "0")
		if res.Rooms == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		if _, ok := res.Rooms.Join[roomID]; ok {
			return res.NextBatch.String()
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("room %s never appeared in /sync", roomID)
	return ""
}
