// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package clientapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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
)

// TestRefreshTokens covers refresh tokens on /login and /refresh (MSC2918).
func TestRefreshTokens(t *testing.T) {
	alice := test.NewUser(t)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		cfg.ClientAPI.RateLimiting.Enabled = false
		cfg.ClientAPI.RefreshableAccessTokenLifetime = time.Hour
		defer closeDB()
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		routers := httputil.NewRouters()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, nil, nil, userAPI, nil, nil, nil, caching.DisableMetrics)

		accessTokens := map[*test.User]userDevice{alice: {}}
		createAccessTokens(t, accessTokens, userAPI, processCtx.Context(), routers)

		do := func(method, path, accessToken string, body any) *httptest.ResponseRecorder {
			t.Helper()
			opts := []test.HTTPRequestOpt{}
			if body != nil {
				opts = append(opts, test.WithJSONBody(t, body))
			}
			req := test.NewRequest(t, method, path, opts...)
			if accessToken != "" {
				req.Header.Set("Authorization", "Bearer "+accessToken)
			}
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			return rec
		}
		login := func(refreshToken bool) gjson.Result {
			t.Helper()
			rec := do(http.MethodPost, "/_matrix/client/v3/login", "", map[string]any{
				"type":          authtypes.LoginTypePassword,
				"identifier":    map[string]any{"type": "m.id.user", "user": alice.ID},
				"password":      accessTokens[alice].password,
				"refresh_token": refreshToken,
			})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			return gjson.ParseBytes(rec.Body.Bytes())
		}
		refresh := func(refreshToken string) (int, gjson.Result) {
			t.Helper()
			rec := do(http.MethodPost, "/_matrix/client/v3/refresh", "", map[string]any{"refresh_token": refreshToken})
			return rec.Code, gjson.ParseBytes(rec.Body.Bytes())
		}
		mustRefresh := func(refreshToken string) (string, string) {
			t.Helper()
			code, res := refresh(refreshToken)
			require.Equal(t, http.StatusOK, code, res.Raw)
			require.NotEmpty(t, res.Get("access_token").Str)
			require.NotEmpty(t, res.Get("refresh_token").Str)
			return res.Get("access_token").Str, res.Get("refresh_token").Str
		}
		whoami := func(accessToken string) (int, gjson.Result) {
			t.Helper()
			rec := do(http.MethodGet, "/_matrix/client/v3/account/whoami", accessToken, nil)
			return rec.Code, gjson.ParseBytes(rec.Body.Bytes())
		}
		requireRejected := func(refreshToken string) {
			t.Helper()
			code, res := refresh(refreshToken)
			require.Equal(t, http.StatusUnauthorized, code, res.Raw)
			require.Equal(t, "M_UNKNOWN_TOKEN", res.Get("errcode").Str)
		}

		// Clients that do not ask for a refresh token keep non-expiring tokens.
		plain := login(false)
		require.False(t, plain.Get("refresh_token").Exists())
		require.False(t, plain.Get("expires_in_ms").Exists())

		res := login(true)
		require.EqualValues(t, time.Hour.Milliseconds(), res.Get("expires_in_ms").Int())
		access1, refresh1 := res.Get("access_token").Str, res.Get("refresh_token").Str
		deviceID := res.Get("device_id").Str
		require.NotEmpty(t, refresh1)
		code, _ := whoami(access1)
		require.Equal(t, http.StatusOK, code)

		// Refreshing replaces the access token of the same device.
		access2, _ := mustRefresh(refresh1)
		code, _ = whoami(access1)
		require.Equal(t, http.StatusUnauthorized, code)

		// The client lost the response: the old refresh token still works
		// until the new access token is used.
		access3, refresh3 := mustRefresh(refresh1)
		code, _ = whoami(access2)
		require.Equal(t, http.StatusUnauthorized, code)
		code, who := whoami(access3)
		require.Equal(t, http.StatusOK, code)
		require.Equal(t, deviceID, who.Get("device_id").Str)
		requireRejected(refresh1)

		// Using the new refresh token revokes the one before it.
		_, refresh4 := mustRefresh(refresh3)
		_, refresh5 := mustRefresh(refresh4)
		requireRejected(refresh3)

		// An expired access token is a soft logout, and refreshing recovers.
		cfg.ClientAPI.RefreshableAccessTokenLifetime = time.Millisecond
		access6, refresh6 := mustRefresh(refresh5)
		time.Sleep(10 * time.Millisecond)
		code, who = whoami(access6)
		require.Equal(t, http.StatusUnauthorized, code)
		require.Equal(t, "M_UNKNOWN_TOKEN", who.Get("errcode").Str)
		require.True(t, who.Get("soft_logout").Bool())
		cfg.ClientAPI.RefreshableAccessTokenLifetime = time.Hour
		access7, refresh7 := mustRefresh(refresh6)
		code, _ = whoami(access7)
		require.Equal(t, http.StatusOK, code)

		// Logging out deletes the device and so its refresh token.
		require.Equal(t, http.StatusOK, do(http.MethodPost, "/_matrix/client/v3/logout", access7, map[string]any{}).Code)
		requireRejected(refresh7)
		requireRejected("unknown")

		// With refresh tokens disabled, clients asking for one get none.
		cfg.ClientAPI.RefreshableAccessTokenLifetime = 0
		res = login(true)
		require.False(t, res.Get("refresh_token").Exists())
		require.False(t, res.Get("expires_in_ms").Exists())
	})
}
