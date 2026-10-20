package syncapi

import (
	"fmt"
	"slices"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// A state rewrite for a room the sync API already has state for lists the
// room's whole state, nearly all of it unchanged. Stored as it came, every
// incremental sync over the rewrite loaded and sent the whole state of the
// room, which in a large room took longer than clients wait for a sync, so a
// client behind such rewrites retried the same sync forever. Only the state
// that changed may reach the sync, and the room's current state must still
// end up as the rewrite says.
func TestStateRewriteOfKnownRoomSyncsOnlyTheDifference(t *testing.T) {
	const members = 200
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		aliceUser := test.NewUser(t)
		room := test.NewRoom(t, aliceUser)
		for i := range members {
			room.CreateAndInsert(t, aliceUser, spec.MRoomMember, map[string]any{"membership": spec.Invite},
				test.WithStateKey(fmt.Sprintf("@member%d:test", i)))
		}
		known := slices.Clone(room.Events())
		// The rewrite changes one member and adds a topic. The rest of the
		// state it lists is what the sync API already has.
		changed := room.CreateAndInsert(t, aliceUser, spec.MRoomMember, map[string]any{"membership": spec.Leave},
			test.WithStateKey("@member0:test"))
		rewrite := room.CreateAndInsert(t, aliceUser, "m.room.topic", map[string]any{"topic": "rewritten"}, test.WithStateKey(""))
		state := room.CurrentState()
		stateIDs := make([]string, len(state))
		stateByID := make(map[string]*rstypes.HeaderedEvent, len(state))
		for i, ev := range state {
			stateIDs[i] = ev.EventID()
			stateByID[ev.EventID()] = ev
		}

		alice := userapi.Device{ID: "ALICEID", UserID: aliceUser.ID, AccessToken: "ALICE_BEARER_TOKEN", AccountType: userapi.AccountTypeUser}
		cfg, processCtx, closeRig := testrig.CreateConfig(t, dbType)
		defer closeRig()
		routers := httputil.NewRouters()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)

		rsAPI := &largeStateRoomserverAPI{
			syncRoomserverAPI: syncRoomserverAPI{rooms: []*test.Room{room}},
			events:            stateByID,
		}
		AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, &syncUserAPI{accounts: []userapi.Device{alice}}, rsAPI, caches, caching.DisableMetrics, nil, nil)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		if err != nil {
			t.Fatalf("NewSyncServerDatasource: %s", err)
		}

		deadline := time.Now().Add(3 * time.Minute)
		testrig.MustPublishMsgs(t, jsctx, toNATSMsgs(t, cfg, known...)...)
		since := syncUntilJoined(t, routers, alice.AccessToken, room.ID)
		// The room can show up before the consumer has written all of its
		// events, so catch up with the last one first.
		last := known[len(known)-1].EventID()
		for caughtUp := false; !caughtUp; {
			if time.Now().After(deadline) {
				t.Fatalf("the room's events never reached /sync")
			}
			res := mustSync(t, routers, alice.AccessToken, since, "5000")
			since = res.NextBatch.String()
			if res.Rooms == nil || res.Rooms.Join[room.ID] == nil {
				continue
			}
			caughtUp = slices.ContainsFunc(res.Rooms.Join[room.ID].Timeline.Events, func(ev synctypes.ClientEvent) bool {
				return ev.EventID == last
			})
		}

		testrig.MustPublishMsgs(t, jsctx, testrig.NewOutputEventMsg(t, cfg, room.ID, rsapi.OutputEvent{
			Type: rsapi.OutputTypeNewRoomEvent,
			NewRoomEvent: &rsapi.OutputNewRoomEvent{
				Event:             rewrite,
				AddsStateEventIDs: stateIDs,
				RewritesState:     true,
				HistoryVisibility: rewrite.Visibility,
			},
		}))

		var synced []string
		for !slices.Contains(synced, rewrite.EventID()) {
			if time.Now().After(deadline) {
				t.Fatalf("the rewrite never reached /sync")
			}
			res := mustSync(t, routers, alice.AccessToken, since, "5000")
			since = res.NextBatch.String()
			if res.Rooms == nil || res.Rooms.Join[room.ID] == nil {
				continue
			}
			join := res.Rooms.Join[room.ID]
			if join.State != nil {
				for _, ev := range join.State.Events {
					synced = append(synced, ev.EventID)
				}
			}
			if join.Timeline != nil {
				for _, ev := range join.Timeline.Events {
					synced = append(synced, ev.EventID)
				}
			}
		}
		if !slices.Contains(synced, changed.EventID()) {
			t.Errorf("the changed membership %s did not reach /sync", changed.EventID())
		}
		if len(synced) > 2 {
			t.Errorf("/sync sent %d state and timeline events for a rewrite that changed 2", len(synced))
		}

		snapshot, err := db.NewDatabaseSnapshot(processCtx.Context())
		if err != nil {
			t.Fatalf("NewDatabaseSnapshot: %s", err)
		}
		defer snapshot.Rollback() //nolint:errcheck
		filter := synctypes.DefaultStateFilter()
		filter.Limit = len(state) + 1
		got, err := snapshot.GetStateEventsForRoom(processCtx.Context(), room.ID, &filter)
		if err != nil {
			t.Fatalf("GetStateEventsForRoom: %s", err)
		}
		gotIDs := make([]string, len(got))
		for i, ev := range got {
			gotIDs[i] = ev.EventID()
		}
		slices.Sort(gotIDs)
		wantIDs := slices.Clone(stateIDs)
		slices.Sort(wantIDs)
		if !slices.Equal(gotIDs, wantIDs) {
			t.Errorf("current state has %d events, want the %d of the rewrite", len(gotIDs), len(wantIDs))
		}
	})
}
