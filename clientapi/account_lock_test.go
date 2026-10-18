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
	uapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// TestAccountLocking covers account locking (MSC3939) through the admin lock
// endpoints (MSC4323).
func TestAccountLocking(t *testing.T) {
	admin := test.NewUser(t, test.WithAccountType(uapi.AccountTypeAdmin))
	otherAdmin := test.NewUser(t, test.WithAccountType(uapi.AccountTypeAdmin))
	alice := test.NewUser(t)
	bob := test.NewUser(t)

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

		accessTokens := map[*test.User]userDevice{admin: {}, otherAdmin: {}, alice: {}, bob: {}}
		createAccessTokens(t, accessTokens, userAPI, processCtx.Context(), routers)

		do := func(method, path, accessToken string, body any) (int, gjson.Result) {
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
			return rec.Code, gjson.ParseBytes(rec.Body.Bytes())
		}
		lockPath := func(userID string) string { return "/_matrix/client/v1/admin/lock/" + userID }
		setLock := func(userID string, locked bool) (int, gjson.Result) {
			t.Helper()
			return do(http.MethodPut, lockPath(userID), accessTokens[admin].accessToken, map[string]any{"locked": locked})
		}
		requireErr := func(code int, res gjson.Result, wantCode int, wantErr string) {
			t.Helper()
			require.Equal(t, wantCode, code, res.Raw)
			require.Equal(t, wantErr, res.Get("errcode").Str, res.Raw)
		}
		requireLocked := func(code int, res gjson.Result) {
			t.Helper()
			requireErr(code, res, http.StatusUnauthorized, "M_USER_LOCKED")
			require.True(t, res.Get("soft_logout").Bool(), res.Raw)
		}
		login := func(u *test.User) (int, gjson.Result) {
			t.Helper()
			return do(http.MethodPost, "/_matrix/client/v3/login", "", map[string]any{
				"type":          authtypes.LoginTypePassword,
				"identifier":    map[string]any{"type": "m.id.user", "user": u.ID},
				"password":      accessTokens[u].password,
				"refresh_token": true,
			})
		}
		aliceToken := accessTokens[alice].accessToken

		// Only administrators see the capability and may use the endpoints.
		_, caps := do(http.MethodGet, "/_matrix/client/v3/capabilities", accessTokens[admin].accessToken, nil)
		require.True(t, caps.Get(`capabilities.m\.account_moderation.lock`).Bool(), caps.Raw)
		require.False(t, caps.Get(`capabilities.m\.account_moderation.suspend`).Bool(), caps.Raw)
		_, caps = do(http.MethodGet, "/_matrix/client/v3/capabilities", aliceToken, nil)
		require.False(t, caps.Get(`capabilities.m\.account_moderation`).Exists(), caps.Raw)
		code, res := do(http.MethodGet, lockPath(bob.ID), aliceToken, nil)
		requireErr(code, res, http.StatusForbidden, "M_FORBIDDEN")

		// Invalid targets.
		code, res = setLock("@someone:remote.example", true)
		requireErr(code, res, http.StatusBadRequest, "M_INVALID_PARAM")
		code, res = setLock("@nobody:test", true)
		requireErr(code, res, http.StatusNotFound, "M_NOT_FOUND")
		code, res = setLock(admin.ID, true)
		requireErr(code, res, http.StatusForbidden, "M_FORBIDDEN")
		code, res = setLock(otherAdmin.ID, true)
		requireErr(code, res, http.StatusForbidden, "M_FORBIDDEN")
		code, res = do(http.MethodPut, lockPath(alice.ID), accessTokens[admin].accessToken, map[string]any{})
		requireErr(code, res, http.StatusBadRequest, "M_MISSING_PARAM")

		// Sessions obtained before the lock.
		code, res = login(alice)
		require.Equal(t, http.StatusOK, code, res.Raw)
		refreshToken := res.Get("refresh_token").Str
		code, res = login(alice)
		require.Equal(t, http.StatusOK, code, res.Raw)
		logoutToken := res.Get("access_token").Str

		code, res = do(http.MethodGet, lockPath(alice.ID), accessTokens[admin].accessToken, nil)
		require.Equal(t, http.StatusOK, code, res.Raw)
		require.False(t, res.Get("locked").Bool(), res.Raw)
		code, res = setLock(alice.ID, true)
		require.Equal(t, http.StatusOK, code, res.Raw)
		require.True(t, res.Get("locked").Bool(), res.Raw)
		code, res = do(http.MethodGet, lockPath(alice.ID), accessTokens[admin].accessToken, nil)
		require.Equal(t, http.StatusOK, code, res.Raw)
		require.True(t, res.Get("locked").Bool(), res.Raw)

		// A locked account can neither use nor obtain access tokens.
		requireLocked(do(http.MethodGet, "/_matrix/client/v3/account/whoami", aliceToken, nil))
		requireLocked(do(http.MethodGet, "/_matrix/client/v3/capabilities", aliceToken, nil))
		requireLocked(login(alice))
		requireLocked(do(http.MethodPost, "/_matrix/client/v3/refresh", "", map[string]any{"refresh_token": refreshToken}))
		code, res = do(http.MethodGet, "/_matrix/client/v3/account/whoami", accessTokens[bob].accessToken, nil)
		require.Equal(t, http.StatusOK, code, res.Raw)

		// It can still log out.
		code, res = do(http.MethodPost, "/_matrix/client/v3/logout", logoutToken, map[string]any{})
		require.Equal(t, http.StatusOK, code, res.Raw)

		// Unlocking keeps the remaining sessions.
		code, res = setLock(alice.ID, false)
		require.Equal(t, http.StatusOK, code, res.Raw)
		require.False(t, res.Get("locked").Bool(), res.Raw)
		code, res = do(http.MethodGet, "/_matrix/client/v3/account/whoami", aliceToken, nil)
		require.Equal(t, http.StatusOK, code, res.Raw)
		require.Equal(t, alice.ID, res.Get("user_id").Str)
		code, res = login(alice)
		require.Equal(t, http.StatusOK, code, res.Raw)
		code, res = do(http.MethodPost, "/_matrix/client/v3/refresh", "", map[string]any{"refresh_token": refreshToken})
		require.Equal(t, http.StatusOK, code, res.Raw)
		code, res = do(http.MethodGet, "/_matrix/client/v3/account/whoami", logoutToken, nil)
		requireErr(code, res, http.StatusUnauthorized, "M_UNKNOWN_TOKEN")

		// Deactivated accounts are not found.
		code, res = do(http.MethodPost, "/_matrix/client/v3/account/deactivate", accessTokens[bob].accessToken, map[string]any{
			"auth": map[string]any{
				"type":     authtypes.LoginTypePassword,
				"user":     bob.ID,
				"password": accessTokens[bob].password,
			},
		})
		require.Equal(t, http.StatusOK, code, res.Raw)
		code, res = do(http.MethodGet, lockPath(bob.ID), accessTokens[admin].accessToken, nil)
		requireErr(code, res, http.StatusNotFound, "M_NOT_FOUND")
	})
}
