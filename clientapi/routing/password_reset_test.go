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
	"strings"
	"sync"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

// fakeIdentityServer answers requestToken and getValidated3pid for the
// configured identity server and records which hosts were contacted.
type fakeIdentityServer struct {
	mu        sync.Mutex
	validated map[string]string // sid -> validated email
	hosts     []string
	requested []string // emails asked to validate
}

func (f *fakeIdentityServer) client(t *testing.T) *fclient.Client {
	t.Helper()
	return fclient.NewClient(fclient.WithTransport(threePIDTransport(func(req *http.Request) (*http.Response, error) {
		f.mu.Lock()
		defer f.mu.Unlock()
		f.hosts = append(f.hosts, req.URL.Host)
		switch req.URL.Path {
		case "/_matrix/identity/v2/validate/email/requestToken":
			var body struct {
				Email string `json:"email"`
			}
			_ = json.NewDecoder(req.Body).Decode(&body)
			f.requested = append(f.requested, body.Email)
			return identityResponse(200, `{"sid":"sid-1"}`), nil
		case "/_matrix/identity/v2/3pid/getValidated3pid":
			sid := req.URL.Query().Get("sid")
			if req.URL.Query().Get("client_secret") != "secret" {
				return identityResponse(400, `{"errcode":"M_NO_VALID_SESSION"}`), nil
			}
			email, ok := f.validated[sid]
			if !ok {
				return identityResponse(400, `{"errcode":"M_SESSION_NOT_VALIDATED"}`), nil
			}
			return identityResponse(200, `{"medium":"email","address":"`+email+`","validated_at":1}`), nil
		}
		return identityResponse(404, `{"errcode":"M_UNRECOGNIZED"}`), nil
	})))
}

func passwordResetBody(t *testing.T, creds map[string]string, extra map[string]any) *http.Request {
	t.Helper()
	body := map[string]any{"new_password": "a new long password"}
	if creds != nil {
		body["auth"] = map[string]any{"type": "m.login.email.identity", "session": "uia", "threepid_creds": creds}
	}
	for k, v := range extra {
		body[k] = v
	}
	data, err := json.Marshal(body)
	require.NoError(t, err)
	return httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/account/password", strings.NewReader(string(data)))
}

func responseErrcode(t *testing.T, res any) string {
	t.Helper()
	data, err := json.Marshal(res)
	require.NoError(t, err)
	return gjson.GetBytes(data, "errcode").Str
}

func TestRequestPasswordResetEmailToken(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		userAPI, ctx, cfg := passwordResetUserAPI(t, dbType)
		createPasswordResetUser(t, userAPI, ctx, "alice", "Alice@Example.org")
		is := &fakeIdentityServer{}

		// Not configured: the medium is unsupported.
		res := RequestPasswordResetEmailToken(threePIDRequest(`{"email":"alice@example.org","client_secret":"secret","send_attempt":1}`), userAPI, &cfg.ClientAPI, is.client(t))
		assert.Equal(t, http.StatusBadRequest, res.Code)
		assert.Equal(t, "M_THREEPID_MEDIUM_NOT_SUPPORTED", responseErrcode(t, res.JSON))

		cfg.ClientAPI.EmailValidationServer = "id.example"
		cfg.Global.TrustedIDServers = []string{"id.example"}

		res = RequestPasswordResetEmailToken(threePIDRequest(`{"email":"nobody@example.org","client_secret":"secret","send_attempt":1}`), userAPI, &cfg.ClientAPI, is.client(t))
		assert.Equal(t, http.StatusBadRequest, res.Code)
		assert.Equal(t, "M_THREEPID_NOT_FOUND", responseErrcode(t, res.JSON))

		// A client-chosen identity server is ignored; the address is
		// case-folded before the lookup.
		res = RequestPasswordResetEmailToken(threePIDRequest(`{"email":"ALICE@example.org","client_secret":"secret","send_attempt":1,"id_server":"evil.example"}`), userAPI, &cfg.ClientAPI, is.client(t))
		require.Equal(t, http.StatusOK, res.Code, "%+v", res.JSON)
		assert.Equal(t, reqTokenResponse{SID: "sid-1"}, res.JSON)
		assert.NotContains(t, is.hosts, "evil.example")
		assert.Equal(t, []string{"alice@example.org"}, is.requested)

		res = RequestPasswordResetMSISDNToken()
		assert.Equal(t, "M_THREEPID_MEDIUM_NOT_SUPPORTED", responseErrcode(t, res.JSON))
	})
}

// TestPasswordReset changes the password of the account owning the validated
// email address, and logs out its devices unless asked not to.
func TestPasswordReset(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		userAPI, ctx, cfg := passwordResetUserAPI(t, dbType)
		cfg.ClientAPI.EmailValidationServer = "id.example"
		alice := createPasswordResetUser(t, userAPI, ctx, "alice", "alice@example.org")
		bob := createPasswordResetUser(t, userAPI, ctx, "bob", "bob@example.org")
		is := &fakeIdentityServer{validated: map[string]string{"alice-sid": "Alice@Example.org", "stranger-sid": "stranger@example.org"}}
		reset := func(creds map[string]string, extra map[string]any) (int, string, []byte) {
			res := PasswordReset(passwordResetBody(t, creds, extra), userAPI, &cfg.ClientAPI, is.client(t))
			data, err := json.Marshal(res.JSON)
			require.NoError(t, err)
			return res.Code, gjson.GetBytes(data, "errcode").Str, data
		}

		// Without the stage, the flow is offered.
		code, _, body := reset(nil, nil)
		require.Equal(t, http.StatusUnauthorized, code)
		assert.Equal(t, `[{"stages":["m.login.email.identity"]}]`, gjson.GetBytes(body, "flows").Raw)

		// Not validated yet: clients keep polling.
		code, errcode, body := reset(map[string]string{"sid": "pending-sid", "client_secret": "secret"}, nil)
		assert.Equal(t, http.StatusUnauthorized, code)
		assert.Equal(t, "M_UNAUTHORIZED", errcode)
		assert.True(t, gjson.GetBytes(body, "flows").Exists())

		// Another identity server cannot vouch for an address.
		code, errcode, _ = reset(map[string]string{"sid": "alice-sid", "client_secret": "secret", "id_server": "evil.example"}, nil)
		assert.Equal(t, http.StatusUnauthorized, code)
		assert.Equal(t, "M_THREEPID_AUTH_FAILED", errcode)
		assert.NotContains(t, is.hosts, "evil.example")

		// A wrong client secret fails without changing anything.
		code, _, _ = reset(map[string]string{"sid": "alice-sid", "client_secret": "wrong"}, nil)
		assert.Equal(t, http.StatusUnauthorized, code)

		// A validated address without an account.
		code, errcode, _ = reset(map[string]string{"sid": "stranger-sid", "client_secret": "secret"}, nil)
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "M_THREEPID_NOT_FOUND", errcode)

		// A weak password is refused before anything changes.
		code, errcode, _ = reset(map[string]string{"sid": "alice-sid", "client_secret": "secret"}, map[string]any{"new_password": "short"})
		assert.Equal(t, http.StatusBadRequest, code)
		assert.Equal(t, "M_WEAK_PASSWORD", errcode)
		assertPassword(t, userAPI, ctx, "alice", "old password long enough", true)

		// The legacy threepidCreds key works too; devices stay when asked.
		res := PasswordReset(httptest.NewRequest(http.MethodPost, "/", strings.NewReader(
			`{"new_password":"first new password","logout_devices":false,"auth":{"type":"m.login.email.identity","threepidCreds":{"sid":"alice-sid","client_secret":"secret"}}}`,
		)), userAPI, &cfg.ClientAPI, is.client(t))
		require.Equal(t, http.StatusOK, res.Code, "%+v", res.JSON)
		assertPassword(t, userAPI, ctx, "alice", "first new password", true)
		assert.Len(t, devicesOf(t, userAPI, ctx, alice), 1)

		code, _, body = reset(map[string]string{"sid": "alice-sid", "client_secret": "secret"}, nil)
		require.Equal(t, http.StatusOK, code, string(body))
		assertPassword(t, userAPI, ctx, "alice", "a new long password", true)
		assertPassword(t, userAPI, ctx, "alice", "first new password", false)
		assert.Empty(t, devicesOf(t, userAPI, ctx, alice), "logout_devices defaults to true")

		// Bob is untouched.
		assertPassword(t, userAPI, ctx, "bob", "old password long enough", true)
		assert.Len(t, devicesOf(t, userAPI, ctx, bob), 1)
	})
}

// TestPasswordRouting keeps the access token flow for signed-in users and
// sends requests without a token to the reset flow.
func TestPasswordRouting(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		for _, validationServer := range []string{"", "id.example"} {
			cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
			cfg.ClientAPI.RateLimiting.Enabled = false
			cfg.ClientAPI.EmailValidationServer = validationServer
			cfg.Global.TrustedIDServers = []string{"id.example"}
			routers := httputil.NewRouters()
			cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
			caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
			natsInstance := jetstream.NATSInstance{}
			rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
			rsAPI.SetFederationAPI(nil, nil)
			userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
			Setup(routers, cfg, rsAPI, nil, userAPI, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

			serve := func(token string) *httptest.ResponseRecorder {
				req := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/account/password", strings.NewReader(`{"new_password":"whatever long enough"}`))
				if token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
				}
				rec := httptest.NewRecorder()
				routers.Client.ServeHTTP(rec, req)
				return rec
			}
			rec := serve("")
			require.Equal(t, http.StatusUnauthorized, rec.Code, rec.Body.String())
			if validationServer == "" {
				assert.Equal(t, "M_MISSING_TOKEN", gjson.Get(rec.Body.String(), "errcode").Str)
			} else {
				assert.Equal(t, `[{"stages":["m.login.email.identity"]}]`, gjson.Get(rec.Body.String(), "flows").Raw)
			}
			// An invalid token still goes through access token auth.
			rec = serve("not-a-token")
			assert.Equal(t, http.StatusUnauthorized, rec.Code)
			assert.Equal(t, "M_UNKNOWN_TOKEN", gjson.Get(rec.Body.String(), "errcode").Str)
			closeDB()
		}
	})
}

func passwordResetUserAPI(t *testing.T, dbType test.DBType) (api.UserInternalAPI, context.Context, *config.Zendrite) {
	t.Helper()
	cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
	t.Cleanup(closeDB)
	cfg.Global.ServerName = "test"
	cfg.Global.TrustedIDServers = []string{"id.example"}
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
	return userAPI, processCtx.Context(), cfg
}

// createPasswordResetUser creates an account with one device and an email
// address, and returns its user ID.
func createPasswordResetUser(t *testing.T, userAPI api.UserInternalAPI, ctx context.Context, localpart, email string) string {
	t.Helper()
	require.NoError(t, userAPI.PerformAccountCreation(ctx, &api.PerformAccountCreationRequest{
		AccountType: api.AccountTypeUser, Localpart: localpart, ServerName: "test", Password: "old password long enough",
	}, &api.PerformAccountCreationResponse{}))
	require.NoError(t, userAPI.PerformDeviceCreation(ctx, &api.PerformDeviceCreationRequest{
		Localpart: localpart, ServerName: "test", AccessToken: "token-" + localpart, NoDeviceListUpdate: true,
	}, &api.PerformDeviceCreationResponse{}))
	require.NoError(t, userAPI.PerformSaveThreePIDAssociation(ctx, &api.PerformSaveThreePIDAssociationRequest{
		ThreePID: strings.ToLower(email), Localpart: localpart, ServerName: "test", Medium: "email",
	}, &struct{}{}))
	return "@" + localpart + ":test"
}

func assertPassword(t *testing.T, userAPI api.UserInternalAPI, ctx context.Context, localpart, password string, want bool) {
	t.Helper()
	var res api.QueryAccountByPasswordResponse
	require.NoError(t, userAPI.QueryAccountByPassword(ctx, &api.QueryAccountByPasswordRequest{
		Localpart: localpart, ServerName: "test", PlaintextPassword: password,
	}, &res))
	assert.Equal(t, want, res.Exists, "password %q of %s", password, localpart)
}

func devicesOf(t *testing.T, userAPI api.UserInternalAPI, ctx context.Context, userID string) []api.Device {
	t.Helper()
	var res api.QueryDevicesResponse
	require.NoError(t, userAPI.QueryDevices(ctx, &api.QueryDevicesRequest{UserID: userID}, &res))
	return res.Devices
}
