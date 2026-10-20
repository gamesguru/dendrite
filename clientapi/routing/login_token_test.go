// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/matrix-org/util"

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

func TestGetLoginToken(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)

	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, close := testrig.CreateConfig(t, dbType)
		defer close()
		cfg.ClientAPI.RateLimiting.Enabled = false
		natsInstance := jetstream.NATSInstance{}

		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		routers := httputil.NewRouters()
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		Setup(routers, cfg, nil, nil, userAPI, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics) //nolint:contextcheck

		password := util.RandomString(8)
		for _, u := range []*test.User{alice, bob} {
			localpart, serverName, _ := gomatrixserverlib.SplitID('@', u.ID)
			if err := userAPI.PerformAccountCreation(ctx, &uapi.PerformAccountCreationRequest{
				AccountType: uapi.AccountTypeUser,
				Localpart:   localpart,
				ServerName:  serverName,
				Password:    password,
			}, &uapi.PerformAccountCreationResponse{}); err != nil {
				t.Fatalf("failed to create account: %s", err)
			}
		}

		login := func(t *testing.T, body map[string]any) (int, loginResponse) {
			t.Helper()
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/login", test.WithJSONBody(t, body)))
			var resp loginResponse
			_ = json.Unmarshal(rec.Body.Bytes(), &resp)
			return rec.Code, resp
		}
		code, aliceLogin := login(t, map[string]any{
			"type":       authtypes.LoginTypePassword,
			"identifier": map[string]any{"type": "m.id.user", "user": alice.ID},
			"password":   password,
		})
		if code != http.StatusOK {
			t.Fatalf("password login failed: %d", code)
		}

		getToken := func(t *testing.T, body map[string]any) *httptest.ResponseRecorder {
			t.Helper()
			rec := httptest.NewRecorder()
			req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v1/login/get_token", test.WithJSONBody(t, body))
			req.Header.Set("Authorization", "Bearer "+aliceLogin.AccessToken)
			routers.Client.ServeHTTP(rec, req)
			return rec
		}

		t.Run("without auth the password is requested", func(t *testing.T) {
			rec := getToken(t, map[string]any{})
			if rec.Code != http.StatusUnauthorized {
				t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
			}
		})

		t.Run("another user's password is refused", func(t *testing.T) {
			rec := getToken(t, map[string]any{"auth": map[string]any{
				"type":       authtypes.LoginTypePassword,
				"identifier": map[string]any{"type": "m.id.user", "user": bob.ID},
				"password":   password,
			}})
			if rec.Code != http.StatusForbidden {
				t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
			}
		})

		t.Run("the token logs in once", func(t *testing.T) {
			rec := getToken(t, map[string]any{"auth": map[string]any{
				"type":       authtypes.LoginTypePassword,
				"identifier": map[string]any{"type": "m.id.user", "user": alice.ID},
				"password":   password,
			}})
			if rec.Code != http.StatusOK {
				t.Fatalf("got %d: %s", rec.Code, rec.Body.String())
			}
			var resp getLoginTokenResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			if resp.LoginToken == "" || resp.ExpiresInMS <= 0 || resp.ExpiresInMS > uapi.DefaultLoginTokenLifetime.Milliseconds() {
				t.Fatalf("unexpected response: %+v", resp)
			}

			body := map[string]any{"type": authtypes.LoginTypeToken, "token": resp.LoginToken}
			code, newLogin := login(t, body)
			if code != http.StatusOK || newLogin.UserID != alice.ID || newLogin.DeviceID == aliceLogin.DeviceID {
				t.Fatalf("token login failed: %d %+v", code, newLogin)
			}
			if code, _ = login(t, body); code != http.StatusForbidden {
				t.Fatalf("token was accepted twice: %d", code)
			}
		})
	})
}
