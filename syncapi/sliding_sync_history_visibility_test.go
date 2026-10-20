package syncapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// TestSlidingSyncHistoryVisibility checks that sliding sync timelines apply
// m.room.history_visibility like sync v2 and /messages do.
func TestSlidingSyncHistoryVisibility(t *testing.T) {
	test.WithAllDatabases(t, testSlidingSyncHistoryVisibility)
}

func testSlidingSyncHistoryVisibility(t *testing.T, dbType test.DBType) {
	alice, bob := test.NewUser(t), test.NewUser(t)
	aliceDev := userapi.Device{ID: "ALICEID", UserID: alice.ID, AccessToken: "ALICE_BEARER_TOKEN", AccountType: userapi.AccountTypeUser}
	bobDev := userapi.Device{ID: "BOBID", UserID: bob.ID, AccessToken: "BOB_BEARER_TOKEN", AccountType: userapi.AccountTypeUser}

	cfg, processCtx, closeRig := testrig.CreateConfig(t, dbType)
	defer closeRig()
	routers := httputil.NewRouters()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, accountDataUserAPI{&syncUserAPI{accounts: []userapi.Device{aliceDev, bobDev}}}, rsAPI, caches, caching.DisableMetrics, nil, nil)
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)

	slidingSync := func(t *testing.T, roomID string) gjson.Result {
		t.Helper()
		body := map[string]any{"lists": map[string]any{"all": map[string]any{"ranges": [][]int{{0, 50}}, "timeline_limit": 20}}}
		w := httptest.NewRecorder()
		routers.Client.ServeHTTP(w, test.NewRequest(t, http.MethodPost, "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync",
			test.WithQueryParams(map[string]string{"access_token": bobDev.AccessToken}), test.WithJSONBody(t, body)))
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		return gjson.ParseBytes(w.Body.Bytes()).Get("rooms." + gjson.Escape(roomID))
	}
	inTimeline := func(roomData gjson.Result, eventID string) bool {
		return roomData.Get(fmt.Sprintf(`timeline.#(event_id=="%s")`, eventID)).Exists()
	}
	for _, tc := range []struct {
		visibility                  gomatrixserverlib.HistoryVisibility
		seeBeforeInvite, seeInvited bool
	}{
		{gomatrixserverlib.HistoryVisibilityWorldReadable, true, true},
		{gomatrixserverlib.HistoryVisibilityShared, true, true},
		{gomatrixserverlib.HistoryVisibilityInvited, false, true},
		{gomatrixserverlib.HistoryVisibilityJoined, false, false},
	} {
		t.Run(string(tc.visibility), func(t *testing.T) {
			room := test.NewRoom(t, alice, test.RoomHistoryVisibility(tc.visibility))
			beforeInvite := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"msgtype": "m.text", "body": "before invite"})
			room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Invite}, test.WithStateKey(bob.ID))
			invited := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"msgtype": "m.text", "body": "while invited"})
			room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
			joined := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"msgtype": "m.text", "body": "after join"})
			require.NoError(t, rsapi.SendEvents(context.Background(), rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false))
			syncUntil(t, routers, aliceDev.AccessToken, false, func(syncBody string) bool {
				path := fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id=="%s")`, gjson.Escape(room.ID), joined.EventID())
				return gjson.Get(syncBody, path).Exists()
			})

			roomData := slidingSync(t, room.ID)
			require.True(t, roomData.Exists(), "room missing from sliding sync")
			assert.Equal(t, tc.seeBeforeInvite, inTimeline(roomData, beforeInvite.EventID()), "message before the invite")
			assert.Equal(t, tc.seeInvited, inTimeline(roomData, invited.EventID()), "message while invited")
			assert.True(t, inTimeline(roomData, joined.EventID()), "message after the join")
		})
	}
}
