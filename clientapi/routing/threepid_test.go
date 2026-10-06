// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	"codefloe.com/pat-s/zendrite/clientapi/threepid"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/storage"
)

type threePIDUserAPI struct {
	api.ClientUserAPI
	saved     []api.PerformSaveThreePIDAssociationRequest
	forgotten []api.PerformForgetThreePIDRequest
	saveErr   error
	data      map[string]json.RawMessage
}

func (a *threePIDUserAPI) PerformSaveThreePIDAssociation(_ context.Context, req *api.PerformSaveThreePIDAssociationRequest, _ *struct{}) error {
	if a.saveErr != nil {
		return a.saveErr
	}
	a.saved = append(a.saved, *req)
	return nil
}

func (a *threePIDUserAPI) PerformForgetThreePID(_ context.Context, req *api.PerformForgetThreePIDRequest, _ *struct{}) error {
	a.forgotten = append(a.forgotten, *req)
	return nil
}

func (a *threePIDUserAPI) InputAccountData(_ context.Context, req *api.InputAccountDataRequest, _ *api.InputAccountDataResponse) error {
	a.data[req.UserID+req.DataType] = req.AccountData
	return nil
}

func (a *threePIDUserAPI) QueryAccountData(_ context.Context, req *api.QueryAccountDataRequest, res *api.QueryAccountDataResponse) error {
	res.GlobalAccountData = map[string]json.RawMessage{req.DataType: a.data[req.UserID+req.DataType]}
	return nil
}

func (a *threePIDUserAPI) QueryAccountByPassword(_ context.Context, req *api.QueryAccountByPasswordRequest, res *api.QueryAccountByPasswordResponse) error {
	res.Exists = req.PlaintextPassword == "password"
	res.Account = &api.Account{UserID: "@" + req.Localpart + ":" + string(req.ServerName)}
	return nil
}

type threePIDTransport func(*http.Request) (*http.Response, error)

// RoundTrip answers the homeserver's registration with the identity server,
// which it makes before its first request without a client token.
func (f threePIDTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.URL.Path == "/_matrix/identity/v2/account/register" {
		return identityResponse(200, `{"token":"server-token"}`), nil
	}
	return f(req)
}

func (a *threePIDUserAPI) PerformOpenIDTokenCreation(_ context.Context, req *api.PerformOpenIDTokenCreationRequest, res *api.PerformOpenIDTokenCreationResponse) error {
	res.Token = api.OpenIDToken{Token: "openid", UserID: req.UserID, ExpiresAtMS: time.Now().Add(time.Hour).UnixMilli()}
	return nil
}

func identityResponse(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

func threePIDConfig() *config.ClientAPI {
	cfg := &config.Zendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	cfg.Global.ServerName = "test"
	cfg.Global.TrustedIDServers = []string{"id.example"}
	return &cfg.ClientAPI
}

func threePIDRequest(body string) *http.Request {
	return httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
}

func requireThreePIDResponse(t *testing.T, res util.JSONResponse, code int, errcode string) {
	t.Helper()
	require.Equal(t, code, res.Code, "%+v", res.JSON)
	if errcode != "" {
		data, err := json.Marshal(res.JSON)
		require.NoError(t, err)
		var result spec.MatrixError
		require.NoError(t, json.Unmarshal(data, &result))
		require.Equal(t, errcode, string(result.ErrCode))
	}
}

func TestAdd3PID(t *testing.T) {
	cfg := threePIDConfig()
	device := &api.Device{UserID: "@alice:test"}
	for _, tc := range []struct {
		name, body, validation, errcode string
		code                            int
		saveErr                         error
		saves                           int
	}{
		{name: "challenge", body: `{"sid":"sid","client_secret":"secret"}`, code: 401},
		{name: "missing", body: `{}`, code: 400, errcode: "M_MISSING_PARAM"},
		{name: "malformed", body: `{`, code: 400, errcode: "M_BAD_JSON"},
		{name: "wrong user", body: `{"sid":"sid","client_secret":"secret","auth":{"type":"m.login.password","user":"bob","password":"password"}}`, code: 403, errcode: "M_FORBIDDEN"},
		{name: "wrong password", body: `{"sid":"sid","client_secret":"secret","auth":{"type":"m.login.password","user":"alice","password":"wrong"}}`, code: 401, errcode: "M_FORBIDDEN"},
		{name: "unknown session", body: `{"sid":"unknown","client_secret":"secret","auth":{"type":"m.login.password","user":"alice","password":"password"}}`, code: 400, errcode: "M_THREEPID_AUTH_FAILED"},
		{name: "unvalidated", validation: `{"errcode":"M_SESSION_NOT_VALIDATED"}`, code: 400, errcode: "M_THREEPID_AUTH_FAILED"},
		{name: "duplicate", code: 400, errcode: "M_THREEPID_IN_USE", saveErr: storage.Err3PIDInUse},
		{name: "database failure", code: 500, errcode: "M_UNKNOWN", saveErr: errors.New("database unavailable")},
		{name: "success", code: 200, saves: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			userAPI := &threePIDUserAPI{saveErr: tc.saveErr}
			checks := 0
			client := fclient.NewClient(fclient.WithTransport(threePIDTransport(func(req *http.Request) (*http.Response, error) {
				if strings.HasSuffix(req.URL.Path, "requestToken") {
					return identityResponse(200, `{"sid":"sid"}`), nil
				}
				require.Contains(t, req.URL.Path, "getValidated3pid")
				checks++
				if tc.validation != "" {
					return identityResponse(400, tc.validation), nil
				}
				return identityResponse(200, `{"address":"alice@example.com","medium":"email","validated_at":1}`), nil
			})))
			identity := threepid.IdentityAuth{UserAPI: userAPI, Config: cfg, Client: client}
			_, err := threepid.CreateSession(t.Context(), threepid.EmailAssociationRequest{IDServer: "id.example", Secret: "secret", Email: "alice@example.com"}, identity)
			require.NoError(t, err)
			body := tc.body
			if body == "" {
				body = `{"sid":"sid","client_secret":"secret","auth":{"type":"m.login.password","user":"alice","password":"password"}}`
			}
			res := Add3PID(threePIDRequest(body), userAPI, device, cfg, client, auth.NewUserInteractive(userAPI, cfg))
			requireThreePIDResponse(t, res, tc.code, tc.errcode)
			require.Len(t, userAPI.saved, tc.saves)
			if tc.saves > 0 {
				require.Equal(t, "alice", userAPI.saved[0].Localpart)
				require.Equal(t, spec.ServerName("test"), userAPI.saved[0].ServerName)
			}
			if tc.body != "" {
				require.Zero(t, checks)
			}
		})
	}
}

func TestBindAndUnbind3PID(t *testing.T) {
	cfg := threePIDConfig()
	device := &api.Device{UserID: "@alice:test"}
	userAPI := &threePIDUserAPI{data: make(map[string]json.RawMessage), saveErr: storage.Err3PIDInUse}
	binds, unbinds := 0, 0
	client := fclient.NewClient(fclient.WithTransport(threePIDTransport(func(req *http.Request) (*http.Response, error) {
		var body map[string]any
		require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
		require.Equal(t, device.UserID, body["mxid"])
		switch req.URL.Path {
		case "/_matrix/identity/v2/3pid/bind":
			binds++
			require.Equal(t, "Bearer token", req.Header.Get("Authorization"))
			require.Equal(t, "secret", body["client_secret"])
			return identityResponse(200, `{"address":"alice@example.com","medium":"email"}`), nil
		case "/_matrix/identity/v2/3pid/unbind":
			unbinds++
			require.Contains(t, req.Header.Get("Authorization"), "X-Matrix")
			require.Equal(t, map[string]any{"address": "alice@example.com", "medium": "email"}, body["threepid"])
			return identityResponse(200, `{}`), nil
		default:
			t.Fatalf("unexpected request: %s", req.URL)
			return nil, errors.New("unexpected request")
		}
	})))
	bind := `{"sid":"sid","client_secret":"secret","id_server":"id.example","id_access_token":"token"}`
	requireThreePIDResponse(t, Bind3PID(threePIDRequest(bind), userAPI, device, cfg, client), 200, "")
	require.Equal(t, 1, binds)
	require.Empty(t, userAPI.saved)
	// Resolve the identity server from persisted binding metadata, without a local association.
	unbind := `{"medium":"email","address":"alice@example.com"}`
	res := Unbind3PID(threePIDRequest(unbind), userAPI, device, cfg, client)
	requireThreePIDResponse(t, res, 200, "")
	require.Equal(t, map[string]string{"id_server_unbind_result": "success"}, res.JSON)
	require.Equal(t, 1, unbinds)
	require.Empty(t, userAPI.forgotten)
	// A second unbind has no known server and must not contact one.
	res = Unbind3PID(threePIDRequest(unbind), userAPI, device, cfg, client)
	require.Equal(t, map[string]string{"id_server_unbind_result": "no-support"}, res.JSON)
	require.Equal(t, 1, unbinds)
	res = Forget3PID(threePIDRequest(unbind), userAPI, device, cfg, client)
	requireThreePIDResponse(t, res, 200, "")
	require.Len(t, userAPI.forgotten, 1)
	require.Equal(t, "alice", userAPI.forgotten[0].Localpart)
	require.Equal(t, spec.ServerName("test"), userAPI.forgotten[0].ServerName)
}

func Test3PIDIdentityFailures(t *testing.T) {
	for _, tc := range []struct {
		name              string
		status            int
		response, errcode string
		code              int
		transportErr      error
	}{
		{name: "forbidden", status: 403, response: `{"errcode":"M_FORBIDDEN","error":"denied"}`, code: 403, errcode: "M_FORBIDDEN"},
		{name: "unvalidated", status: 400, response: `{"errcode":"M_SESSION_NOT_VALIDATED","error":"not validated"}`, code: 400, errcode: "M_SESSION_NOT_VALIDATED"},
		{name: "rate limit", status: 429, response: `{"errcode":"M_LIMIT_EXCEEDED","error":"slow down"}`, code: 429, errcode: "M_LIMIT_EXCEEDED"},
		{name: "invalid response", status: 200, response: `{}`, code: 502, errcode: "M_UNKNOWN"},
		{name: "unavailable", status: 502, response: `not json`, code: 502, errcode: "M_UNKNOWN"},
		{name: "network", transportErr: errors.New("unavailable"), code: 502, errcode: "M_UNKNOWN"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			userAPI := &threePIDUserAPI{data: make(map[string]json.RawMessage)}
			client := fclient.NewClient(fclient.WithTransport(threePIDTransport(func(*http.Request) (*http.Response, error) {
				if tc.transportErr != nil {
					return nil, tc.transportErr
				}
				return identityResponse(tc.status, tc.response), nil
			})))
			body := `{"sid":"sid","client_secret":"secret","id_server":"id.example","id_access_token":"token"}`
			res := Bind3PID(threePIDRequest(body), userAPI, &api.Device{UserID: "@alice:test"}, threePIDConfig(), client)
			requireThreePIDResponse(t, res, tc.code, tc.errcode)
			require.Empty(t, userAPI.saved)
			require.Empty(t, userAPI.data)
		})
	}
}

func Test3PIDValidation(t *testing.T) {
	for _, operation := range []string{"bind", "unbind", "delete"} {
		for _, tc := range []struct{ body, errcode string }{
			{`{}`, "M_MISSING_PARAM"},
			{`{"sid":4}`, "M_BAD_JSON"},
			{`{"sid":"s","client_secret":"c","id_server":"untrusted","id_access_token":"t","address":"a","medium":"email"}`, "M_SERVER_NOT_TRUSTED"},
		} {
			t.Run(operation+tc.body, func(t *testing.T) {
				userAPI := &threePIDUserAPI{data: make(map[string]json.RawMessage)}
				handler := Bind3PID
				if operation == "unbind" {
					handler = Unbind3PID
				}
				if operation == "delete" {
					handler = Forget3PID
				}
				// The sid type is not a field of unbind/delete requests.
				if operation != "bind" && tc.errcode == "M_BAD_JSON" {
					tc.body = `{"address":4}`
				}
				res := handler(threePIDRequest(tc.body), userAPI, &api.Device{UserID: "@alice:test"}, threePIDConfig(), nil)
				requireThreePIDResponse(t, res, 400, tc.errcode)
				require.Empty(t, userAPI.forgotten)
			})
		}
	}
}

func TestUnbind3PIDFailures(t *testing.T) {
	for _, tc := range []struct {
		status, code int
		body, result string
	}{
		{200, 200, `{}`, "success"},
		{400, 200, `{}`, "no-support"},
		{404, 200, `{}`, "no-support"},
		{501, 200, `{}`, "no-support"},
		{500, 502, `not json`, ""},
		{403, 403, `{"errcode":"M_FORBIDDEN","error":"denied"}`, ""},
	} {
		for _, forget := range []bool{false, true} {
			t.Run(fmt.Sprintf("%d/delete=%t", tc.status, forget), func(t *testing.T) {
				userAPI := &threePIDUserAPI{data: make(map[string]json.RawMessage)}
				client := fclient.NewClient(fclient.WithTransport(threePIDTransport(func(*http.Request) (*http.Response, error) { return identityResponse(tc.status, tc.body), nil })))
				req := threePIDRequest(`{"medium":"email","address":"alice@example.com","id_server":"id.example"}`)
				res := remove3PID(req, userAPI, &api.Device{UserID: "@alice:test"}, threePIDConfig(), client, forget)
				requireThreePIDResponse(t, res, tc.code, "")
				if tc.result != "" {
					require.Equal(t, map[string]string{"id_server_unbind_result": tc.result}, res.JSON)
				}
				if forget && tc.code == 200 {
					require.Len(t, userAPI.forgotten, 1)
				} else {
					require.Empty(t, userAPI.forgotten)
				}
			})
		}
	}
}

func TestLegacy3PID(t *testing.T) {
	for _, bind := range []bool{false, true} {
		for _, duplicate := range []bool{false, true} {
			t.Run(fmt.Sprintf("bind=%t/duplicate=%t", bind, duplicate), func(t *testing.T) {
				userAPI := &threePIDUserAPI{data: make(map[string]json.RawMessage)}
				if duplicate {
					userAPI.saveErr = storage.Err3PIDInUse
				}
				binds := 0
				client := fclient.NewClient(fclient.WithTransport(threePIDTransport(func(req *http.Request) (*http.Response, error) {
					if strings.HasSuffix(req.URL.Path, "/bind") {
						binds++
						require.Equal(t, "Bearer token", req.Header.Get("Authorization"))
						return identityResponse(200, `{"address":"alice@example.com","medium":"email"}`), nil
					}
					require.Equal(t, "/_matrix/identity/v2/3pid/getValidated3pid", req.URL.Path)
					require.Equal(t, "Bearer token", req.Header.Get("Authorization"))
					require.Equal(t, "s&sid=injected", req.URL.Query().Get("sid"))
					require.Equal(t, "c&client_secret=injected", req.URL.Query().Get("client_secret"))
					return identityResponse(200, `{"address":"alice@example.com","medium":"email"}`), nil
				})))
				body := fmt.Sprintf(`{"three_pid_creds":{"sid":"s&sid=injected","client_secret":"c&client_secret=injected","id_server":"id.example","id_access_token":"token"},"bind":%t}`, bind)
				res := CheckAndSave3PIDAssociation(threePIDRequest(body), userAPI, &api.Device{UserID: "@alice:test"}, threePIDConfig(), client)
				if duplicate {
					requireThreePIDResponse(t, res, 400, "M_THREEPID_IN_USE")
					require.Zero(t, binds)
				} else {
					requireThreePIDResponse(t, res, 200, "")
					require.Len(t, userAPI.saved, 1)
					if bind {
						require.Equal(t, 1, binds)
						require.Len(t, userAPI.data, 1)
					} else {
						require.Zero(t, binds)
					}
				}
			})
		}
	}
}

func TestNative3PIDAuthentication(t *testing.T) {
	routers, _, _, cleanup := setupMSC3861Stack(t, test.DBTypeSQLite, func(cfg *config.Zendrite) { cfg.MSCs.MSCs = nil })
	defer cleanup()
	for _, operation := range []string{"add", "bind", "unbind"} {
		t.Run(operation, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/_matrix/client/v3/account/3pid/"+operation, strings.NewReader(`{}`))
			recorder := httptest.NewRecorder()
			routers.Client.ServeHTTP(recorder, req)
			require.Equal(t, http.StatusUnauthorized, recorder.Code, recorder.Body.String())
			require.Contains(t, recorder.Body.String(), "M_MISSING_TOKEN")
		})
	}
}

func Test3PIDBindingMetadata(t *testing.T) {
	userAPI := &threePIDUserAPI{data: make(map[string]json.RawMessage)}
	req := threePIDRequest(`{}`)
	userID := "@alice:test"
	require.NoError(t, saveBinding(req, userAPI, userID, "email", "alice@example.com", "first.example", true))
	require.NoError(t, saveBinding(req, userAPI, userID, "email", "alice@example.com", "second.example", true))
	require.NoError(t, saveBinding(req, userAPI, userID, "email", "alice@example.com", "first.example", false))
	servers, err := bindingServers(req, userAPI, userID, "email", "alice@example.com")
	require.NoError(t, err)
	require.Equal(t, map[string]bool{"second.example": true}, servers)
	servers, err = bindingServers(req, userAPI, "@bob:test", "email", "alice@example.com")
	require.NoError(t, err)
	require.Empty(t, servers)
	key := bindingDataType("email", "alice@example.com")
	res := SaveAccountData(threePIDRequest(`{}`), userAPI, &api.Device{UserID: userID}, userID, "", key, nil)
	requireThreePIDResponse(t, res, 403, "M_FORBIDDEN")
}

func (a *threePIDUserAPI) QueryLocalpartForThreePID(_ context.Context, _ *api.QueryLocalpartForThreePIDRequest, _ *api.QueryLocalpartForThreePIDResponse) error {
	return nil
}

func TestRequestEmailTokenWithoutIdentityServer(t *testing.T) {
	for _, tc := range []struct {
		name, configured, requested, expected, errcode string
	}{
		{name: "Element request", configured: "id.example", expected: "id.example"},
		{name: "not configured", errcode: "M_THREEPID_MEDIUM_NOT_SUPPORTED"},
		{name: "untrusted configured server", configured: "untrusted.example", errcode: "M_SERVER_NOT_TRUSTED"},
		{name: "legacy explicit server", requested: "id.example", expected: "id.example"},
		{name: "explicit server takes precedence", configured: "default.example", requested: "id.example", expected: "id.example"},
		{name: "untrusted explicit server", configured: "id.example", requested: "untrusted.example", errcode: "M_SERVER_NOT_TRUSTED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := threePIDConfig()
			cfg.EmailValidationServer = tc.configured
			cfg.Matrix.TrustedIDServers = append(cfg.Matrix.TrustedIDServers, "default.example")
			userAPI := &threePIDUserAPI{}
			requests := 0
			client := fclient.NewClient(fclient.WithTransport(threePIDTransport(func(req *http.Request) (*http.Response, error) {
				requests++
				require.Equal(t, "https", req.URL.Scheme)
				require.Equal(t, tc.expected, req.URL.Host)
				switch req.URL.Path {
				case "/_matrix/identity/v2/validate/email/requestToken":
					var body map[string]any
					require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
					require.Equal(t, map[string]any{"email": "alice@example.com", "client_secret": tc.name, "send_attempt": float64(1)}, body)
					require.Equal(t, "Bearer server-token", req.Header.Get("Authorization"))
					return identityResponse(200, `{"sid":"email-session"}`), nil
				case "/_matrix/identity/v2/3pid/getValidated3pid":
					require.Equal(t, "Bearer server-token", req.Header.Get("Authorization"))
					require.Equal(t, "email-session", req.URL.Query().Get("sid"))
					require.Equal(t, tc.name, req.URL.Query().Get("client_secret"))
					return identityResponse(200, `{"address":"alice@example.com","medium":"email","validated_at":1}`), nil
				default:
					t.Fatalf("unexpected request: %s", req.URL)
					return nil, errors.New("unexpected request")
				}
			})))
			body := map[string]any{"email": "alice@example.com", "client_secret": tc.name, "send_attempt": 1}
			if tc.requested != "" {
				body["id_server"] = tc.requested
			}
			encoded, err := json.Marshal(body)
			require.NoError(t, err)
			res := RequestEmailToken(threePIDRequest(string(encoded)), userAPI, cfg, client)
			if tc.errcode != "" {
				requireThreePIDResponse(t, res, 400, tc.errcode)
				require.Zero(t, requests)
				return
			}
			requireThreePIDResponse(t, res, 200, "")
			require.Equal(t, reqTokenResponse{SID: "email-session"}, res.JSON)
			require.Equal(t, tc.expected, threepid.SessionServer("email-session", tc.name))
			// Element's final add request also omits id_server; use the remembered server.
			add := fmt.Sprintf(`{"sid":"email-session","client_secret":%q,"auth":{"type":"m.login.password","user":"alice","password":"password"}}`, tc.name)
			res = Add3PID(threePIDRequest(add), userAPI, &api.Device{UserID: "@alice:test"}, cfg, client, auth.NewUserInteractive(userAPI, cfg))
			requireThreePIDResponse(t, res, 200, "")
			require.Len(t, userAPI.saved, 1)
			require.Equal(t, "alice@example.com", userAPI.saved[0].ThreePID)
			require.Equal(t, 2, requests)
		})
	}
}

func TestRequestEmailTokenIdentityErrors(t *testing.T) {
	for _, tc := range []struct {
		name, body, errcode string
		upstream, code      int
	}{
		{"invalid email", `{"errcode":"M_INVALID_EMAIL","error":"Invalid email address"}`, "M_INVALID_EMAIL", 400, 400},
		{"forbidden", `{"errcode":"M_FORBIDDEN","error":"Email verification is disabled"}`, "M_FORBIDDEN", 403, 403},
		{"rate limited", `{"errcode":"M_LIMIT_EXCEEDED","error":"Try later","retry_after_ms":60000}`, "M_LIMIT_EXCEEDED", 429, 429},
		{"non JSON", `<html>Upstream unavailable</html>`, "M_UNKNOWN", 503, 502},
		{"unstructured JSON", `{"message":"Upstream unavailable"}`, "M_UNKNOWN", 503, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := threePIDConfig()
			cfg.EmailValidationServer = "id.example"
			client := fclient.NewClient(fclient.WithTransport(threePIDTransport(func(*http.Request) (*http.Response, error) {
				return identityResponse(tc.upstream, tc.body), nil
			})))
			res := RequestEmailToken(threePIDRequest(`{"email":"alice@example.com","client_secret":"secret","send_attempt":1}`), &threePIDUserAPI{}, cfg, client)
			requireThreePIDResponse(t, res, tc.code, tc.errcode)
			if tc.code == tc.upstream {
				encoded, err := json.Marshal(res.JSON)
				require.NoError(t, err)
				require.JSONEq(t, tc.body, string(encoded))
			}
		})
	}
}

type nonMemberRoomserverAPI struct {
	roomserverAPI.ClientRoomserverAPI
}

func (nonMemberRoomserverAPI) QueryMembershipForUser(_ context.Context, _ *roomserverAPI.QueryMembershipForUserRequest, res *roomserverAPI.QueryMembershipForUserResponse) error {
	res.IsInRoom = false
	return nil
}

// TestThreePIDInviteNeedsMembership checks that a 3PID invite from someone
// who is not in the room never reaches the identity server, which would
// otherwise learn the room's details and email them to the invitee.
func TestThreePIDInviteNeedsMembership(t *testing.T) {
	client := fclient.NewClient(fclient.WithTransport(threePIDTransport(func(req *http.Request) (*http.Response, error) {
		t.Fatalf("identity server contacted: %s", req.URL)
		return nil, errors.New("unexpected request")
	})))
	body := `{"id_server":"id.example","id_access_token":"token","medium":"email","address":"bob@example.com"}`
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	res := SendInvite(req, &threePIDUserAPI{}, &api.Device{UserID: "@alice:test"}, "!room:test", threePIDConfig(), nonMemberRoomserverAPI{}, nil, client)
	requireThreePIDResponse(t, res, http.StatusForbidden, "M_FORBIDDEN")
}

// TestThreePIDEmailCaseFolding covers MSC2265 for the requests Zendrite makes
// on behalf of the client: identity servers see case-folded email addresses,
// and a binding is found whatever the case of the address given to unbind it.
func TestThreePIDEmailCaseFolding(t *testing.T) {
	cfg := threePIDConfig()
	cfg.EmailValidationServer = "id.example"
	device := &api.Device{UserID: "@alice:test"}
	userAPI := &threePIDUserAPI{data: make(map[string]json.RawMessage)}
	var requested, unbound []string
	client := fclient.NewClient(fclient.WithTransport(threePIDTransport(func(req *http.Request) (*http.Response, error) {
		switch req.URL.Path {
		case "/_matrix/identity/v2/validate/email/requestToken":
			var body struct {
				Email string `json:"email"`
			}
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			requested = append(requested, body.Email)
			return identityResponse(200, `{"sid":"casefold-session"}`), nil
		case "/_matrix/identity/v2/3pid/bind":
			return identityResponse(200, `{"address":"Strauß@Example.com","medium":"email"}`), nil
		case "/_matrix/identity/v2/3pid/unbind":
			var body struct {
				ThreePID struct {
					Address string `json:"address"`
				} `json:"threepid"`
			}
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			unbound = append(unbound, body.ThreePID.Address)
			return identityResponse(200, `{}`), nil
		default:
			t.Fatalf("unexpected request: %s", req.URL)
			return nil, errors.New("unexpected request")
		}
	})))

	res := RequestEmailToken(threePIDRequest(`{"email":" Strauß@Example.COM","client_secret":"casefold","send_attempt":1}`), userAPI, cfg, client)
	requireThreePIDResponse(t, res, 200, "")
	require.Equal(t, []string{"strauss@example.com"}, requested)

	bind := `{"sid":"sid","client_secret":"secret","id_server":"id.example","id_access_token":"token"}`
	requireThreePIDResponse(t, Bind3PID(threePIDRequest(bind), userAPI, device, cfg, client), 200, "")
	res = Unbind3PID(threePIDRequest(`{"medium":"email","address":"STRAUSS@example.com"}`), userAPI, device, cfg, client)
	requireThreePIDResponse(t, res, 200, "")
	require.Equal(t, map[string]string{"id_server_unbind_result": "success"}, res.JSON)
	require.Equal(t, []string{"strauss@example.com"}, unbound)
}
