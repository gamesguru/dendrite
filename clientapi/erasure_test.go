// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package clientapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
	uapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// TestServerManagedAccountData checks MSC4010: m.push_rules and m.fully_read
// cannot be set through the account data API, globally or per room, and
// fail with 405 M_BAD_JSON. Other types can still be set.
func TestServerManagedAccountData(t *testing.T) {
	alice := test.NewUser(t)
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		routers := httputil.NewRouters()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, nil, nil, userAPI, nil, nil, nil, caching.DisableMetrics)
		accessTokens := map[*test.User]userDevice{alice: {}}
		createAccessTokens(t, accessTokens, userAPI, processCtx.Context(), routers)

		put := func(path string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodPut, path, strings.NewReader(`{"global":{}}`))
			req.Header.Set("Authorization", "Bearer "+accessTokens[alice].accessToken)
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			return rec
		}
		base := "/_matrix/client/v3/user/" + url.PathEscape(alice.ID)
		for _, path := range []string{
			base + "/account_data/m.push_rules",
			base + "/account_data/m.fully_read",
			base + "/rooms/" + url.PathEscape("!room:test") + "/account_data/m.push_rules",
			base + "/rooms/" + url.PathEscape("!room:test") + "/account_data/m.fully_read",
		} {
			rec := put(path)
			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code, path)
			assert.Equal(t, "M_BAD_JSON", gjson.GetBytes(rec.Body.Bytes(), "errcode").Str, path)
		}
		rec := put(base + "/account_data/com.example.settings")
		assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	})
}

// TestDeactivateErase checks MSC4025 on /account/deactivate: with erase the
// user is marked erased in the roomserver and their account data and 3PIDs
// are deleted; without it they are kept. Both report the unbind result.
func TestDeactivateErase(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		ctx := processCtx.Context()
		routers := httputil.NewRouters()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, nil, nil, userAPI, nil, nil, nil, caching.DisableMetrics)

		for _, erase := range []bool{true, false} {
			user := test.NewUser(t)
			accessTokens := map[*test.User]userDevice{user: {}}
			createAccessTokens(t, accessTokens, userAPI, ctx, routers)
			localpart, serverName, err := gomatrixserverlib.SplitID('@', user.ID)
			require.NoError(t, err)

			require.NoError(t, userAPI.InputAccountData(ctx, &uapi.InputAccountDataRequest{
				UserID: user.ID, DataType: "com.example.settings", AccountData: json.RawMessage(`{"secret":true}`),
			}, &uapi.InputAccountDataResponse{}))
			require.NoError(t, userAPI.InputAccountData(ctx, &uapi.InputAccountDataRequest{
				UserID: user.ID, RoomID: "!room:test", DataType: "com.example.room", AccountData: json.RawMessage(`{"secret":true}`),
			}, &uapi.InputAccountDataResponse{}))
			require.NoError(t, userAPI.PerformSaveThreePIDAssociation(ctx, &uapi.PerformSaveThreePIDAssociationRequest{
				ThreePID: localpart + "@example.org", Localpart: localpart, ServerName: serverName, Medium: "email",
			}, &struct{}{}))

			deactivate := func(body map[string]any) *httptest.ResponseRecorder {
				var buf bytes.Buffer
				require.NoError(t, json.NewEncoder(&buf).Encode(body))
				req := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/account/deactivate", &buf)
				req.Header.Set("Authorization", "Bearer "+accessTokens[user].accessToken)
				rec := httptest.NewRecorder()
				routers.Client.ServeHTTP(rec, req)
				return rec
			}
			rec := deactivate(map[string]any{"erase": erase})
			require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
			rec = deactivate(map[string]any{
				"erase": erase,
				"auth": map[string]string{
					"session":  gjson.GetBytes(rec.Body.Bytes(), "session").Str,
					"type":     authtypes.LoginTypePassword,
					"user":     user.ID,
					"password": accessTokens[user].password,
				},
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.JSONEq(t, `{"id_server_unbind_result":"success"}`, rec.Body.String())

			erased, err := rsAPI.QueryErasedUsers(ctx, []string{user.ID})
			require.NoError(t, err)
			assert.Equal(t, erase, len(erased) == 1, "erased users: %v", erased)

			var data uapi.QueryAccountDataResponse
			require.NoError(t, userAPI.QueryAccountData(ctx, &uapi.QueryAccountDataRequest{UserID: user.ID}, &data))
			var threepids uapi.QueryThreePIDsForLocalpartResponse
			require.NoError(t, userAPI.QueryThreePIDsForLocalpart(ctx, &uapi.QueryThreePIDsForLocalpartRequest{Localpart: localpart, ServerName: serverName}, &threepids))
			if erase {
				assert.NotContains(t, data.GlobalAccountData, "com.example.settings")
				assert.Empty(t, data.RoomAccountData)
				assert.Empty(t, threepids.ThreePIDs)
			} else {
				assert.Contains(t, data.GlobalAccountData, "com.example.settings")
				assert.Contains(t, data.RoomAccountData, "!room:test")
				assert.Len(t, threepids.ThreePIDs, 1)
			}
		}
	})
}
