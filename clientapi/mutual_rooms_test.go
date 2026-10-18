package clientapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"sort"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
)

func TestMutualRooms(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cfg.ClientAPI.RateLimiting.Enabled = false
		routers := httputil.NewRouters()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, nil, nil, userAPI, nil, nil, nil, caching.DisableMetrics)
		alice, bob, charlie := test.NewUser(t), test.NewUser(t), test.NewUser(t)
		accessTokens := map[*test.User]userDevice{alice: {}, bob: {}, charlie: {}}
		createAccessTokens(t, accessTokens, userAPI, processCtx.Context(), routers)
		ctx := processCtx.Context()
		rooms := make(map[string]*test.Room)
		shared := make([]string, 0, 101)
		for range 101 {
			room := test.NewRoom(t, alice)
			room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]string{"membership": spec.Join}, test.WithStateKey(bob.ID))
			require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))
			shared = append(shared, room.ID)
			rooms[room.ID] = room
		}
		sort.Strings(shared)
		// Public rooms and invitations do not qualify without both users joined.
		for _, owner := range []*test.User{alice, bob} {
			room := test.NewRoom(t, owner)
			room.CreateAndInsert(t, owner, spec.MRoomMember, map[string]string{"membership": spec.Invite}, test.WithStateKey(charlie.ID))
			require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))
		}
		request := func(t *testing.T, user *test.User, query string, status int) gjson.Result {
			t.Helper()
			req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v1/mutual_rooms?"+query, nil)
			if user != nil {
				req.Header.Set("Authorization", "Bearer "+accessTokens[user].accessToken)
			}
			w := httptest.NewRecorder()
			routers.Client.ServeHTTP(w, req)
			require.Equal(t, status, w.Code, w.Body.String())
			return gjson.Parse(w.Body.String())
		}
		query := "user_id=" + url.QueryEscape(bob.ID)
		t.Run("authentication", func(t *testing.T) {
			request(t, nil, query, http.StatusUnauthorized)
		})
		for _, target := range []string{"", "invalid", "@user:", "@:test", "@a b:test", "@é:test", alice.ID} {
			t.Run("invalid user "+target, func(t *testing.T) {
				result := request(t, alice, "user_id="+url.QueryEscape(target), http.StatusBadRequest)
				require.Equal(t, "M_INVALID_PARAM", result.Get("errcode").String())
			})
		}
		t.Run("missing user", func(t *testing.T) {
			require.Equal(t, "M_INVALID_PARAM", request(t, alice, "", http.StatusBadRequest).Get("errcode").String())
		})
		for _, target := range []string{"@unknown:test", "@unknown:elsewhere.test", "@Historical:test", charlie.ID} {
			t.Run("empty "+target, func(t *testing.T) {
				result := request(t, alice, "user_id="+url.QueryEscape(target), http.StatusOK)
				require.Equal(t, "[]", result.Get("joined").Raw)
				require.EqualValues(t, 0, result.Get("count").Int())
				require.False(t, result.Get("next_batch").Exists())
			})
		}
		first := request(t, alice, query, http.StatusOK)
		require.EqualValues(t, 101, first.Get("count").Int())
		require.Len(t, first.Get("joined").Array(), 100)
		token := first.Get("next_batch").String()
		require.NotEmpty(t, token)
		next := query + "&from=" + url.QueryEscape(token)
		last := request(t, alice, next, http.StatusOK)
		require.Equal(t, shared[100], last.Get("joined.0").String())
		require.Len(t, last.Get("joined").Array(), 1)
		require.EqualValues(t, 101, last.Get("count").Int())
		require.False(t, last.Get("next_batch").Exists())
		for i, room := range first.Get("joined").Array() {
			require.Equal(t, shared[i], room.String())
		}
		t.Run("tokens cannot cross users or targets", func(t *testing.T) {
			request(t, charlie, next, http.StatusBadRequest)
			request(t, alice, "user_id="+url.QueryEscape(charlie.ID)+"&from="+url.QueryEscape(token), http.StatusBadRequest)
			for _, bad := range []string{"", "garbage", token + "x"} {
				require.Equal(t, "M_INVALID_PARAM", request(t, alice, query+"&from="+url.QueryEscape(bad), http.StatusBadRequest).Get("errcode").String())
			}
		})
		// Leaving an earlier room must not shift the last page past a result.
		changeMembership := func(room *test.Room, sender, target *test.User, membership string) {
			ev := room.CreateAndInsert(t, sender, spec.MRoomMember, map[string]string{"membership": membership}, test.WithStateKey(target.ID))
			require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, []*types.HeaderedEvent{ev}, "test", "test", "test", nil, false))
		}
		changeMembership(rooms[shared[0]], bob, bob, spec.Leave)
		last = request(t, alice, next, http.StatusOK)
		require.Equal(t, shared[100], last.Get("joined.0").String())
		require.EqualValues(t, 100, last.Get("count").Int())
		// A ban on a later page must immediately stop exposing that room.
		changeMembership(rooms[shared[100]], alice, bob, spec.Ban)
		last = request(t, alice, next, http.StatusOK)
		require.Equal(t, "[]", last.Get("joined").Raw)
		require.EqualValues(t, 99, last.Get("count").Int())
		require.False(t, last.Get("next_batch").Exists())
		changeMembership(rooms[shared[0]], bob, bob, spec.Join)
		require.EqualValues(t, 100, request(t, alice, query, http.StatusOK).Get("count").Int())
		changeMembership(rooms[shared[0]], alice, alice, spec.Leave)
		require.EqualValues(t, 99, request(t, alice, query, http.StatusOK).Get("count").Int())
	})
}
