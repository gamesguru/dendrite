package syncapi

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/clientapi"
	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/internal/transactions"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi"
	uapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// TestMessagesPaginateAfterRejoin checks that a user who left a room and
// re-joined it can page back through every message sent while away. The room
// is built through the client API, as a real client would.
func TestMessagesPaginateAfterRejoin(t *testing.T) {
	test.WithAllDatabases(t, testMessagesPaginateAfterRejoin)
}

func testMessagesPaginateAfterRejoin(t *testing.T, dbType test.DBType) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)

	cfg, processCtx, closeRig := testrig.CreateConfig(t, dbType)
	defer closeRig()
	cfg.ClientAPI.RateLimiting = config.RateLimiting{Enabled: false}
	routers := httputil.NewRouters()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := &jetstream.NATSInstance{}
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)

	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	usrAPI := userapi.NewInternalAPI(processCtx, cfg, cm, natsInstance, rsAPI, nil, caching.DisableMetrics, func(spec.ServerName) (*statistics.ServerStatistics, error) {
		return &statistics.ServerStatistics{}, nil
	})
	clientapi.AddPublicRoutes(processCtx, routers, cfg, natsInstance, nil, rsAPI, nil, transactions.New(processCtx.Context()), nil, usrAPI, nil, nil, caches, caching.DisableMetrics)
	AddPublicRoutes(processCtx, routers, cfg, cm, natsInstance, usrAPI, rsAPI, caches, caching.DisableMetrics, nil, nil)

	login := func(u *test.User) string {
		localpart, serverName, _ := gomatrixserverlib.SplitID('@', u.ID)
		password := util.RandomString(8)
		if err := usrAPI.PerformAccountCreation(processCtx.Context(), &uapi.PerformAccountCreationRequest{
			AccountType: u.AccountType, Localpart: localpart, ServerName: serverName, Password: password,
		}, &uapi.PerformAccountCreationResponse{}); err != nil {
			t.Fatalf("failed to create account: %s", err)
		}
		rec := httptest.NewRecorder()
		routers.Client.ServeHTTP(rec, test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/login", test.WithJSONBody(t, map[string]any{
			"type":       authtypes.LoginTypePassword,
			"identifier": map[string]any{"type": "m.id.user", "user": u.ID},
			"password":   password,
		})))
		if rec.Code != http.StatusOK {
			t.Fatalf("failed to login: %s", rec.Body.String())
		}
		return gjson.GetBytes(rec.Body.Bytes(), "access_token").Str
	}
	aliceToken := login(alice)
	bobToken := login(bob)

	do := func(token, method, path string, body any) string {
		t.Helper()
		opts := []test.HTTPRequestOpt{test.WithQueryParams(map[string]string{"access_token": token})}
		if body != nil {
			opts = append(opts, test.WithJSONBody(t, body))
		}
		rec := httptest.NewRecorder()
		routers.Client.ServeHTTP(rec, test.NewRequest(t, method, "/_matrix/client/v3"+path, opts...))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s %s: HTTP %d: %s", method, path, rec.Code, rec.Body.String())
		}
		return rec.Body.String()
	}
	txn := 0
	send := func(token, roomID, evType string, content map[string]any) string {
		txn++
		return gjson.Get(do(token, http.MethodPut, fmt.Sprintf("/rooms/%s/send/%s/t%d", roomID, evType, txn), content), "event_id").Str
	}

	roomID := gjson.Get(do(aliceToken, http.MethodPost, "/createRoom", map[string]any{"preset": "public_chat"}), "room_id").Str
	do(bobToken, http.MethodPost, "/join/"+roomID, map[string]any{})
	do(bobToken, http.MethodPost, "/rooms/"+roomID+"/leave", map[string]any{})

	var sent []string
	msgs := func(n int) {
		for i := 0; i < n; i++ {
			sent = append(sent, send(aliceToken, roomID, "m.room.message", map[string]any{"msgtype": "m.text", "body": fmt.Sprintf("m%d", len(sent))}))
		}
	}
	topic := func(text string) {
		do(aliceToken, http.MethodPut, "/rooms/"+roomID+"/state/m.room.topic", map[string]any{"topic": text})
	}
	msgs(30)
	topic("Bob missed this topic change")
	msgs(30)
	topic("And this one too")
	msgs(40)
	do(bobToken, http.MethodPost, "/join/"+roomID, map[string]any{})

	messages := func(limit int, from string) string {
		t.Helper()
		params := map[string]string{"access_token": bobToken, "dir": "b", "limit": strconv.Itoa(limit)}
		if from != "" {
			params["from"] = from
		}
		rec := httptest.NewRecorder()
		routers.Client.ServeHTTP(rec, test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/rooms/"+roomID+"/messages", test.WithQueryParams(params)))
		if rec.Code != http.StatusOK {
			t.Fatalf("got HTTP %d want 200: %s", rec.Code, rec.Body.String())
		}
		return rec.Body.String()
	}

	for _, limit := range []int{1, 3, 7, 50} {
		t.Run(fmt.Sprintf("limit=%d", limit), func(t *testing.T) {
			seen := map[string]int{}
			var pages []int
			from := ""
			for requests := 0; ; requests++ {
				if requests > 500 {
					t.Fatal("pagination did not terminate")
				}
				res := messages(limit, from)
				chunk := gjson.Get(res, "chunk").Array()
				pages = append(pages, len(chunk))
				for _, ev := range chunk {
					seen[ev.Get("event_id").Str]++
				}
				end := gjson.Get(res, "end")
				if !end.Exists() || end.Str == from {
					break
				}
				from = end.Str
			}
			var missing []int
			for i, id := range sent {
				if seen[id] == 0 {
					missing = append(missing, i)
				}
			}
			if len(missing) > 0 {
				t.Errorf("missing messages %v; pages %v", missing, pages)
			}
			for id, n := range seen {
				if n > 1 {
					t.Errorf("event %s returned %d times", id, n)
				}
			}
		})
	}
}
