// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package clientapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
	uapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// TestReportRoom covers reporting rooms (MSC4151) and listing the reports
// through the Synapse admin API.
func TestReportRoom(t *testing.T) {
	admin := test.NewUser(t, test.WithAccountType(uapi.AccountTypeAdmin))
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	room := test.NewRoom(t, alice)
	room.CreateAndInsert(t, alice, spec.MRoomName, map[string]any{"name": "Reported room"}, test.WithStateKey(""))

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		cfg.ClientAPI.RateLimiting.Enabled = false
		defer closeDB()
		routers := httputil.NewRouters()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		require.NoError(t, api.SendEvents(context.Background(), rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))
		AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, nil, nil, userAPI, nil, nil, nil, caching.DisableMetrics)

		accessTokens := map[*test.User]userDevice{admin: {}, alice: {}, bob: {}}
		createAccessTokens(t, accessTokens, userAPI, processCtx.Context(), routers)

		do := func(method, path string, u *test.User, body any) (int, gjson.Result) {
			t.Helper()
			opts := []test.HTTPRequestOpt{}
			if body != nil {
				opts = append(opts, test.WithJSONBody(t, body))
			}
			req := test.NewRequest(t, method, path, opts...)
			req.Header.Set("Authorization", "Bearer "+accessTokens[u].accessToken)
			rec := httptest.NewRecorder()
			if strings.HasPrefix(path, "/_synapse/") {
				routers.SynapseAdmin.ServeHTTP(rec, req)
			} else {
				routers.Client.ServeHTTP(rec, req)
			}
			return rec.Code, gjson.ParseBytes(rec.Body.Bytes())
		}
		report := "/_matrix/client/v3/rooms/" + room.ID + "/report"

		// Bob is not in the room, which does not matter.
		code, res := do(http.MethodPost, report, bob, map[string]any{"reason": "spam"})
		require.Equal(t, http.StatusOK, code, res.Raw)
		code, res = do(http.MethodPost, report, alice, map[string]any{"reason": ""})
		require.Equal(t, http.StatusOK, code, res.Raw)

		code, res = do(http.MethodPost, report, bob, map[string]any{})
		require.Equal(t, http.StatusBadRequest, code, res.Raw)
		require.Equal(t, "M_MISSING_PARAM", res.Get("errcode").Str)
		code, res = do(http.MethodPost, "/_matrix/client/v3/rooms/!unknown:test/report", bob, map[string]any{"reason": "spam"})
		require.Equal(t, http.StatusNotFound, code, res.Raw)
		require.Equal(t, "M_NOT_FOUND", res.Get("errcode").Str)

		code, res = do(http.MethodGet, "/_synapse/admin/v1/room_reports", bob, nil)
		require.Equal(t, http.StatusForbidden, code, res.Raw)

		code, res = do(http.MethodGet, "/_synapse/admin/v1/room_reports", admin, nil)
		require.Equal(t, http.StatusOK, code, res.Raw)
		require.EqualValues(t, 2, res.Get("total").Int(), res.Raw)
		require.Len(t, res.Get("room_reports").Array(), 2, res.Raw)
		newest := res.Get("room_reports.0")
		require.Equal(t, alice.ID, newest.Get("user_id").Str, res.Raw)
		require.Equal(t, room.ID, newest.Get("room_id").Str)
		require.Equal(t, "Reported room", newest.Get("name").Str)
		require.True(t, newest.Get("received_ts").Exists())

		code, res = do(http.MethodGet, "/_synapse/admin/v1/room_reports?user_id="+bob.ID+"&dir=f", admin, nil)
		require.Equal(t, http.StatusOK, code, res.Raw)
		require.EqualValues(t, 1, res.Get("total").Int(), res.Raw)
		require.Equal(t, "spam", res.Get("room_reports.0.reason").Str)

		code, res = do(http.MethodGet, "/_synapse/admin/v1/room_reports?limit=1", admin, nil)
		require.Equal(t, http.StatusOK, code, res.Raw)
		require.Len(t, res.Get("room_reports").Array(), 1, res.Raw)
		require.EqualValues(t, 1, res.Get("next_token").Int(), res.Raw)
	})
}
