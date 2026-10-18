// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package clientapi

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
)

// TestDeleteBackupKeys covers the DELETE /room_keys/keys endpoints of MSC1219.
func TestDeleteBackupKeys(t *testing.T) {
	alice := test.NewUser(t)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		cfg.ClientAPI.RateLimiting.Enabled = false
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

		do := func(method, path string, params map[string]string, body any) *httptest.ResponseRecorder {
			t.Helper()
			opts := []test.HTTPRequestOpt{}
			if params != nil {
				opts = append(opts, test.WithQueryParams(params))
			}
			if body != nil {
				opts = append(opts, test.WithJSONBody(t, body))
			}
			req := test.NewRequest(t, method, path, opts...)
			req.Header.Set("Authorization", "Bearer "+accessTokens[alice].accessToken)
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			return rec
		}
		v1 := map[string]string{"version": "1"}
		session := map[string]any{"first_message_index": 1, "forwarded_count": 0, "is_verified": true, "session_data": map[string]any{}}
		const keysPath = "/_matrix/client/v3/room_keys/keys"

		rec := do(http.MethodPost, "/_matrix/client/v3/room_keys/version", nil, map[string]any{
			"algorithm": "m.megolm_backup.v1.curve25519-aes-sha2",
			"auth_data": map[string]any{"public_key": "abcdefg"},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.Equal(t, "1", gjson.GetBytes(rec.Body.Bytes(), "version").Str)

		rec = do(http.MethodPut, keysPath, v1, map[string]any{"rooms": map[string]any{
			"!a:test": map[string]any{"sessions": map[string]any{"s1": session, "s2": session}},
			"!b:test": map[string]any{"sessions": map[string]any{"s1": session, "s2": session}},
		}})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		require.EqualValues(t, 4, gjson.GetBytes(rec.Body.Bytes(), "count").Int())
		etag := gjson.GetBytes(rec.Body.Bytes(), "etag").Str

		// deleteKeys deletes keys and checks the remaining count, and whether
		// the etag changed.
		deleteKeys := func(path string, wantCount int64, wantChanged bool) {
			t.Helper()
			rec := do(http.MethodDelete, path, v1, nil)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			res := gjson.ParseBytes(rec.Body.Bytes())
			assert.Equal(t, wantCount, res.Get("count").Int(), res.Raw)
			if wantChanged {
				assert.NotEqual(t, etag, res.Get("etag").Str, res.Raw)
			} else {
				assert.Equal(t, etag, res.Get("etag").Str, res.Raw)
			}
			etag = res.Get("etag").Str
		}

		deleteKeys(keysPath+"/!a:test/s1", 3, true)
		rec = do(http.MethodGet, keysPath+"/!a:test", v1, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, []string{"s2"}, mapKeys(gjson.GetBytes(rec.Body.Bytes(), "sessions")))

		// Deleting what is not there succeeds and leaves the etag alone.
		deleteKeys(keysPath+"/!a:test/s1", 3, false)
		deleteKeys(keysPath+"/!c:test", 3, false)

		deleteKeys(keysPath+"/!b:test", 1, true)
		rec = do(http.MethodGet, keysPath, v1, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, []string{"!a:test"}, mapKeys(gjson.GetBytes(rec.Body.Bytes(), "rooms")))

		deleteKeys(keysPath, 0, true)
		rec = do(http.MethodGet, keysPath, v1, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Empty(t, mapKeys(gjson.GetBytes(rec.Body.Bytes(), "rooms")))

		// The version's own count and etag follow the deletions.
		rec = do(http.MethodGet, "/_matrix/client/v3/room_keys/version/1", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.EqualValues(t, 0, gjson.GetBytes(rec.Body.Bytes(), "count").Int())
		assert.Equal(t, etag, gjson.GetBytes(rec.Body.Bytes(), "etag").Str)

		for name, tc := range map[string]struct {
			path     string
			params   map[string]string
			wantCode int
			wantErr  string
		}{
			"missing version":     {path: keysPath, wantCode: http.StatusBadRequest, wantErr: "M_MISSING_PARAM"},
			"unknown version":     {path: keysPath, params: map[string]string{"version": "7"}, wantCode: http.StatusNotFound, wantErr: "M_NOT_FOUND"},
			"non-numeric version": {path: keysPath + "/!a:test", params: map[string]string{"version": "x"}, wantCode: http.StatusNotFound, wantErr: "M_NOT_FOUND"},
		} {
			t.Run(name, func(t *testing.T) {
				rec := do(http.MethodDelete, tc.path, tc.params, nil)
				assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())
				assert.Equal(t, tc.wantErr, gjson.GetBytes(rec.Body.Bytes(), "errcode").Str)
			})
		}

		// A deleted version has no keys left to delete.
		rec = do(http.MethodDelete, "/_matrix/client/v3/room_keys/version/1", nil, nil)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		rec = do(http.MethodDelete, keysPath, v1, nil)
		assert.Equal(t, http.StatusNotFound, rec.Code, rec.Body.String())
	})
}

func mapKeys(obj gjson.Result) []string {
	var keys []string
	obj.ForEach(func(key, _ gjson.Result) bool {
		keys = append(keys, key.Str)
		return true
	})
	return keys
}
