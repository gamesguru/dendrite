// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
	userapiAPI "codefloe.com/pat-s/zendrite/userapi/api"
)

// setupMSC3861SSORouters creates a full router stack with MSC3861 enabled and
// the provided OIDC config. It is used by the legacy SSO compat tests because
// those need to point the homeserver at a mock OIDC provider.
func setupMSC3861SSORouters(t *testing.T, msc3861 config.MSC3861Config) (httputil.Routers, *config.Zendrite, func()) {
	t.Helper()
	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	cfg.ClientAPI.RateLimiting.Enabled = false
	cfg.MSCs.MSCs = []string{"msc3861"}
	cfg.MSCs.MSC3861 = msc3861
	cfg.Global.WellKnownClientName = "https://matrix.example.com"

	natsInstance := jetstream.NATSInstance{}
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	uapi := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)

	routers := httputil.NewRouters()
	Setup(routers, cfg, nil, nil, uapi, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

	return routers, cfg, closeDB
}

// startSSOFlow performs GET /login/sso/redirect like a browser would and
// returns the resulting state parameter and the browser-binding cookie, which
// must be presented on the callback.
func startSSOFlow(t *testing.T, routers httputil.Routers, redirectURL string) (string, *http.Cookie) {
	t.Helper()
	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/redirect", test.WithQueryParams(map[string]string{
		"redirectUrl": redirectURL,
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("expected redirect 302, got %d: %s", rec.Code, rec.Body.String())
	}
	location, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatalf("failed to parse redirect location: %v", err)
	}
	state := location.Query().Get("state")
	if state == "" {
		t.Fatal("expected state in redirect location")
	}
	var cookie *http.Cookie
	for _, c := range (&http.Response{Header: rec.Header()}).Cookies() {
		if c.Name == ssoCookieName {
			cookie = c
		}
	}
	if cookie == nil {
		t.Fatal("expected SSO browser-binding cookie on the redirect response")
	}
	if !cookie.HttpOnly {
		t.Error("expected SSO browser-binding cookie to be HttpOnly")
	}
	if cookie.SameSite != http.SameSiteLaxMode {
		t.Errorf("expected SSO browser-binding cookie SameSite=Lax, got %v", cookie.SameSite)
	}
	if cookie.Path != ssoCookiePath {
		t.Errorf("expected SSO browser-binding cookie path %q, got %q", ssoCookiePath, cookie.Path)
	}
	return state, cookie
}

func TestMSC3861SSOCompat_LoginFlowsAdvertiseSSOAndToken(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:       "https://auth.example.com",
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login")
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp flows
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if len(resp.Flows) != 2 {
		t.Fatalf("expected 2 flows, got %d: %+v", len(resp.Flows), resp.Flows)
	}
	got := make(map[string]bool)
	for _, f := range resp.Flows {
		got[f.Type] = true
		if f.Type == authtypes.LoginTypeSSO && !f.OAuthAwarePreferred {
			t.Errorf("expected SSO flow to be marked oauth_aware_preferred")
		}
	}
	if !got[authtypes.LoginTypeSSO] {
		t.Errorf("expected %s flow", authtypes.LoginTypeSSO)
	}
	if !got[authtypes.LoginTypeToken] {
		t.Errorf("expected %s flow", authtypes.LoginTypeToken)
	}
}

func TestMSC3861SSOCompat_LoginPOSTPasswordForbidden(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:       "https://auth.example.com",
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/login", test.WithJSONBody(t, map[string]any{
		"type":     authtypes.LoginTypePassword,
		"user":     "alice",
		"password": "secret",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for password login, got %d: %s", rec.Code, rec.Body.String())
	}
}

// A login token that was not issued by the SSO callback must be rejected:
// redeeming it through the legacy login path would mint a homeserver-local
// access token that can never validate against the OIDC provider.
func TestMSC3861SSOCompat_LoginPOSTTokenWithoutSSORejected(t *testing.T) {
	t.Parallel()
	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cfg.ClientAPI.RateLimiting.Enabled = false
	cfg.MSCs.MSCs = []string{"msc3861"}
	cfg.MSCs.MSC3861 = config.MSC3861Config{
		Issuer:                "https://auth.example.com",
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		IntrospectionEndpoint: "https://auth.example.com/introspect",
	}
	cfg.Global.WellKnownClientName = "https://matrix.example.com"

	natsInstance := jetstream.NATSInstance{}
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	uapi := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)

	// Create the account first, because QueryLoginToken validates it exists.
	var createRes userapiAPI.PerformAccountCreationResponse
	if err := uapi.PerformAccountCreation(context.Background(), &userapiAPI.PerformAccountCreationRequest{
		Localpart:  "alice",
		ServerName: cfg.Global.ServerName,
	}, &createRes); err != nil {
		t.Fatalf("failed to create account: %v", err)
	}

	// Create a login token manually, bypassing the SSO callback.
	var tokenRes userapiAPI.PerformLoginTokenCreationResponse
	if err := uapi.PerformLoginTokenCreation(context.Background(), &userapiAPI.PerformLoginTokenCreationRequest{
		Data: userapiAPI.LoginTokenData{UserID: "@alice:" + string(cfg.Global.ServerName)},
	}, &tokenRes); err != nil {
		t.Fatalf("failed to create login token: %v", err)
	}

	routers := httputil.NewRouters()
	Setup(routers, cfg, nil, nil, uapi, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

	req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/login", test.WithJSONBody(t, map[string]any{
		"type":  authtypes.LoginTypeToken,
		"token": tokenRes.Metadata.Token,
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 for a login token not issued via SSO, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMSC3861SSOCompat_RefreshSuccess(t *testing.T) {
	t.Parallel()

	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("failed to parse token form: %v", err)
		}
		if r.Form.Get("grant_type") != "refresh_token" {
			t.Errorf("expected grant_type refresh_token, got %s", r.Form.Get("grant_type"))
		}
		if r.Form.Get("refresh_token") != "old-refresh-token" {
			t.Errorf("unexpected refresh_token: %s", r.Form.Get("refresh_token"))
		}
		user, pass, ok := r.BasicAuth()
		if !ok || user != "test-client-id" || pass != "test-client-secret" {
			t.Errorf("expected basic auth test-client-id:test-client-secret, got ok=%v user=%s pass=%s", ok, user, pass)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token":  "new-access-token",
			"refresh_token": "rotated-refresh-token",
			"token_type":    "Bearer",
			"expires_in":    3600,
		})
	}))
	defer mockProvider.Close()

	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                mockProvider.URL,
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: mockProvider.URL + "/auth",
		TokenEndpoint:         mockProvider.URL + "/token",
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/refresh", test.WithJSONBody(t, map[string]any{
		"refresh_token": "old-refresh-token",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp refreshResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse refresh response: %v", err)
	}
	if resp.AccessToken != "new-access-token" {
		t.Errorf("expected new-access-token, got %q", resp.AccessToken)
	}
	if resp.RefreshToken != "rotated-refresh-token" {
		t.Errorf("expected rotated refresh token, got %q", resp.RefreshToken)
	}
	if resp.ExpiresInMs != 3600*1000 {
		t.Errorf("expected expires_in_ms 3600000, got %d", resp.ExpiresInMs)
	}
}

func TestMSC3861SSOCompat_RefreshKeepsUnrotatedToken(t *testing.T) {
	t.Parallel()

	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "new-access-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	defer mockProvider.Close()

	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                mockProvider.URL,
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: mockProvider.URL + "/auth",
		TokenEndpoint:         mockProvider.URL + "/token",
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/refresh", test.WithJSONBody(t, map[string]any{
		"refresh_token": "old-refresh-token",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp refreshResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse refresh response: %v", err)
	}
	if resp.RefreshToken != "old-refresh-token" {
		t.Errorf("expected unrotated refresh token to be handed back, got %q", resp.RefreshToken)
	}
}

func TestMSC3861SSOCompat_RefreshRejectedByProvider(t *testing.T) {
	t.Parallel()

	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusBadRequest)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"error":             "invalid_grant",
			"error_description": "refresh token expired",
		})
	}))
	defer mockProvider.Close()

	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                mockProvider.URL,
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: mockProvider.URL + "/auth",
		TokenEndpoint:         mockProvider.URL + "/token",
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/refresh", test.WithJSONBody(t, map[string]any{
		"refresh_token": "dead-refresh-token",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401, got %d: %s", rec.Code, rec.Body.String())
	}
	var errResp map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
		t.Fatalf("failed to parse error response: %v", err)
	}
	if errResp["errcode"] != "M_UNKNOWN_TOKEN" {
		t.Errorf("expected M_UNKNOWN_TOKEN, got %v", errResp["errcode"])
	}
}

func TestMSC3861SSOCompat_RefreshMissingToken(t *testing.T) {
	t.Parallel()

	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:       "https://auth.example.com",
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/refresh", test.WithJSONBody(t, map[string]any{}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMSC3861SSOCompat_RedirectUsesConfiguredEndpoints(t *testing.T) {
	t.Parallel()
	authzEndpoint := "https://auth.example.com/custom/auth"
	tokenEndpoint := "https://auth.example.com/custom/token"

	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                "https://auth.example.com",
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: authzEndpoint,
		TokenEndpoint:         tokenEndpoint,
		SSOCallbackURL:        "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
		SSORedirectAllowlist:  []string{"element://"},
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/redirect", test.WithQueryParams(map[string]string{
		"redirectUrl": "element://vector/webapp",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d: %s", rec.Code, rec.Body.String())
	}

	location := rec.Header().Get("Location")
	if location == "" {
		t.Fatal("expected Location header")
	}
	if !strings.HasPrefix(location, authzEndpoint+"?") {
		t.Fatalf("expected redirect to configured authorization endpoint, got %s", location)
	}

	u, err := url.Parse(location)
	if err != nil {
		t.Fatalf("failed to parse Location: %v", err)
	}
	q := u.Query()
	if q.Get("client_id") != "test-client-id" {
		t.Errorf("expected client_id test-client-id, got %s", q.Get("client_id"))
	}
	if q.Get("response_type") != "code" {
		t.Errorf("expected response_type code, got %s", q.Get("response_type"))
	}
	if q.Get("redirect_uri") != "https://matrix.example.com/_matrix/client/v3/login/sso/callback" {
		t.Errorf("unexpected redirect_uri: %s", q.Get("redirect_uri"))
	}
	if q.Get("code_challenge_method") != "S256" {
		t.Errorf("expected PKCE S256, got %s", q.Get("code_challenge_method"))
	}
	if q.Get("code_challenge") == "" {
		t.Error("expected code_challenge")
	}
	if q.Get("state") == "" {
		t.Error("expected state")
	}
	// A device ID must be allocated and requested via the stable device scope,
	// alongside offline_access so the provider issues a refresh token, and
	// email/profile so the homeserver can derive a friendly localpart.
	scope := q.Get("scope")
	if !strings.HasPrefix(scope, "openid offline_access email profile urn:matrix:client:api:* urn:matrix:client:device:") {
		t.Errorf("expected scope to request offline_access, email, profile and a device ID, got %q", scope)
	}
	if strings.HasSuffix(scope, "urn:matrix:client:device:") {
		t.Error("expected a non-empty device ID in the scope")
	}
}

func TestMSC3861SSOCompat_RedirectActionRegisterSetsPromptCreate(t *testing.T) {
	t.Parallel()

	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                "https://auth.example.com",
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: "https://auth.example.com/custom/auth",
		TokenEndpoint:         "https://auth.example.com/custom/token",
		SSOCallbackURL:        "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
		SSORedirectAllowlist:  []string{"element://"},
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/redirect", test.WithQueryParams(map[string]string{
		"redirectUrl": "element://vector/webapp",
		"action":      "register",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d: %s", rec.Code, rec.Body.String())
	}

	u, err := url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatalf("failed to parse Location: %v", err)
	}
	if got := u.Query().Get("prompt"); got != "create" {
		t.Errorf("expected prompt=create for action=register, got %q", got)
	}

	// action=login (or no action) must not set prompt.
	req = test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/redirect", test.WithQueryParams(map[string]string{
		"redirectUrl": "element://vector/webapp",
		"action":      "login",
	}))
	rec = httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d: %s", rec.Code, rec.Body.String())
	}
	u, err = url.Parse(rec.Header().Get("Location"))
	if err != nil {
		t.Fatalf("failed to parse Location: %v", err)
	}
	if got := u.Query().Get("prompt"); got != "" {
		t.Errorf("expected no prompt for action=login, got %q", got)
	}
}

func TestMSC3861SSOCompat_RedirectDiscoversEndpoints(t *testing.T) {
	t.Parallel()

	var discoveryHits int32
	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			atomic.AddInt32(&discoveryHits, 1)
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"authorization_endpoint": "https://auth.example.com/discovery/auth",
				"token_endpoint":         "https://auth.example.com/discovery/token",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer mockProvider.Close()

	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:               mockProvider.URL,
		ClientID:             "test-client-id",
		ClientSecret:         "test-client-secret",
		SSOCallbackURL:       "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
		SSORedirectAllowlist: []string{"element://"},
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/redirect", test.WithQueryParams(map[string]string{
		"redirectUrl": "element://vector/webapp",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d: %s", rec.Code, rec.Body.String())
	}

	location := rec.Header().Get("Location")
	if !strings.HasPrefix(location, "https://auth.example.com/discovery/auth?") {
		t.Fatalf("expected redirect to discovered authorization endpoint, got %s", location)
	}
	if atomic.LoadInt32(&discoveryHits) != 1 {
		t.Fatalf("expected OIDC discovery to be hit once, got %d", discoveryHits)
	}
}

func TestMSC3861SSOCompat_RedirectRequiresRedirectUrl(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:       "https://auth.example.com",
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/redirect")
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMSC3861SSOCompat_CallbackSuccess(t *testing.T) {
	t.Parallel()

	accessToken := "oidc-access-token-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	refreshToken := "oidc-refresh-token-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	var tokenEndpointHits, introspectionHits int32

	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			atomic.AddInt32(&tokenEndpointHits, 1)
			if err := r.ParseForm(); err != nil {
				t.Fatalf("failed to parse token form: %v", err)
			}
			if r.Form.Get("grant_type") != "authorization_code" {
				t.Errorf("expected grant_type authorization_code, got %s", r.Form.Get("grant_type"))
			}
			if r.Form.Get("code") != "mock-auth-code" {
				t.Errorf("unexpected code: %s", r.Form.Get("code"))
			}
			if r.Form.Get("code_verifier") == "" {
				t.Error("expected code_verifier")
			}
			if r.Form.Get("redirect_uri") != "https://matrix.example.com/_matrix/client/v3/login/sso/callback" {
				t.Errorf("unexpected redirect_uri: %s", r.Form.Get("redirect_uri"))
			}

			user, pass, ok := r.BasicAuth()
			if !ok || user != "test-client-id" || pass != "test-client-secret" {
				t.Errorf("expected basic auth test-client-id:test-client-secret, got ok=%v user=%s pass=%s", ok, user, pass)
			}

			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token":  accessToken,
				"refresh_token": refreshToken,
				"token_type":    "Bearer",
				"expires_in":    3600,
			})
		case "/introspect":
			atomic.AddInt32(&introspectionHits, 1)
			if err := r.ParseForm(); err != nil {
				t.Fatalf("failed to parse introspect form: %v", err)
			}
			if r.Form.Get("token") != accessToken {
				t.Errorf("expected token %s, got %s", accessToken, r.Form.Get("token"))
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"active":   true,
				"sub":      "alice-oidc-sub",
				"username": "alice",
				"scope":    "openid urn:matrix:client:api:* urn:matrix:client:device:SSODEVDEVICE1",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer mockProvider.Close()

	routers, cfg, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                mockProvider.URL,
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: mockProvider.URL + "/auth",
		TokenEndpoint:         mockProvider.URL + "/token",
		IntrospectionEndpoint: mockProvider.URL + "/introspect",
		SSOCallbackURL:        "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
		SSORedirectAllowlist:  []string{"element://"},
	})
	defer closeDB()

	// Start the SSO flow to obtain a valid state and the browser-binding
	// cookie a real browser would carry through the flow.
	state, cookie := startSSOFlow(t, routers, "element://vector/webapp?element-desktop-ssoid=abc")

	// Invoke the callback as the OIDC provider would, presenting the cookie
	// issued by the redirect.
	callbackReq := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/callback", test.WithQueryParams(map[string]string{
		"state": state,
		"code":  "mock-auth-code",
	}))
	callbackReq.AddCookie(cookie)
	callbackRec := httptest.NewRecorder()
	routers.Client.ServeHTTP(callbackRec, callbackReq)
	if callbackRec.Code != http.StatusFound {
		t.Fatalf("expected callback 302, got %d: %s", callbackRec.Code, callbackRec.Body.String())
	}

	callbackLocation := callbackRec.Header().Get("Location")
	if callbackLocation == "" {
		t.Fatal("expected callback Location header")
	}
	u, err := url.Parse(callbackLocation)
	if err != nil {
		t.Fatalf("failed to parse callback location: %v", err)
	}
	if !strings.HasPrefix(u.String(), "element://vector/webapp") {
		t.Fatalf("expected redirect to element webapp, got %s", callbackLocation)
	}
	loginToken := u.Query().Get("loginToken")
	if loginToken == "" {
		t.Fatalf("expected loginToken in callback location, got %s", callbackLocation)
	}

	if atomic.LoadInt32(&tokenEndpointHits) != 1 {
		t.Fatalf("expected token endpoint hit once, got %d", tokenEndpointHits)
	}
	if atomic.LoadInt32(&introspectionHits) != 1 {
		t.Fatalf("expected introspection endpoint hit once, got %d", introspectionHits)
	}

	// The returned login token must be redeemable for a Matrix session.
	loginReq := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/login", test.WithJSONBody(t, map[string]any{
		"type":  authtypes.LoginTypeToken,
		"token": loginToken,
	}))
	loginRec := httptest.NewRecorder()
	routers.Client.ServeHTTP(loginRec, loginReq)
	if loginRec.Code != http.StatusOK {
		t.Fatalf("expected 200 redeeming login token, got %d: %s", loginRec.Code, loginRec.Body.String())
	}
	var loginResp loginResponse
	if err := json.Unmarshal(loginRec.Body.Bytes(), &loginResp); err != nil {
		t.Fatalf("failed to parse login response: %v", err)
	}
	expectedUserID := "@alice:" + string(cfg.Global.ServerName)
	if loginResp.UserID != expectedUserID {
		t.Errorf("expected user_id %s, got %s", expectedUserID, loginResp.UserID)
	}
	// The returned credentials must be the provider-issued ones: a
	// homeserver-local access token would never introspect under MSC3861.
	if loginResp.AccessToken != accessToken {
		t.Errorf("expected the OIDC access token %q, got %q", accessToken, loginResp.AccessToken)
	}
	if loginResp.DeviceID != "SSODEVDEVICE1" {
		t.Errorf("expected device_id SSODEVDEVICE1 from the token scope, got %q", loginResp.DeviceID)
	}
	// The provider-issued refresh token and expiry must be handed through so
	// the client can use POST /refresh when the access token expires.
	if loginResp.RefreshToken != refreshToken {
		t.Errorf("expected the OIDC refresh token %q, got %q", refreshToken, loginResp.RefreshToken)
	}
	if loginResp.ExpiresInMs != 3600*1000 {
		t.Errorf("expected expires_in_ms 3600000, got %d", loginResp.ExpiresInMs)
	}

	// The returned access token must actually authenticate API requests.
	whoamiReq := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/account/whoami")
	whoamiReq.Header.Set("Authorization", "Bearer "+loginResp.AccessToken)
	whoamiRec := httptest.NewRecorder()
	routers.Client.ServeHTTP(whoamiRec, whoamiReq)
	if whoamiRec.Code != http.StatusOK {
		t.Fatalf("expected 200 from whoami with the SSO-issued token, got %d: %s", whoamiRec.Code, whoamiRec.Body.String())
	}

	// Login tokens are single-use: replaying the redemption must fail.
	replayReq := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/login", test.WithJSONBody(t, map[string]any{
		"type":  authtypes.LoginTypeToken,
		"token": loginToken,
	}))
	replayRec := httptest.NewRecorder()
	routers.Client.ServeHTTP(replayRec, replayReq)
	if replayRec.Code != http.StatusForbidden {
		t.Fatalf("expected 403 when replaying a login token, got %d: %s", replayRec.Code, replayRec.Body.String())
	}
}

func TestMSC3861SSOCompat_CallbackInvalidState(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:       "https://auth.example.com",
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/callback", test.WithQueryParams(map[string]string{
		"state": "not-a-valid-state",
		"code":  "mock-auth-code",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMSC3861SSOCompat_CallbackProviderError(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:       "https://auth.example.com",
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/callback", test.WithQueryParams(map[string]string{
		"error":             "access_denied",
		"error_description": "user denied",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMSC3861SSOCompat_CallbackMissingCode(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:       "https://auth.example.com",
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/callback", test.WithQueryParams(map[string]string{
		"state": "anything",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMSC3861SSOCompat_CallbackTokenExchangeFailure(t *testing.T) {
	t.Parallel()

	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/token" {
			w.WriteHeader(http.StatusUnauthorized)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"error": "invalid_client",
			})
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer mockProvider.Close()

	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                mockProvider.URL,
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: mockProvider.URL + "/auth",
		TokenEndpoint:         mockProvider.URL + "/token",
		SSOCallbackURL:        "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
		SSORedirectAllowlist:  []string{"element://"},
	})
	defer closeDB()

	// Obtain a valid state and the browser-binding cookie.
	state, cookie := startSSOFlow(t, routers, "element://vector/webapp")

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/callback", test.WithQueryParams(map[string]string{
		"state": state,
		"code":  "bad-code",
	}))
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMSC3861SSOCompat_CallbackInactiveToken(t *testing.T) {
	t.Parallel()

	accessToken := "inactive-token-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": accessToken,
				"token_type":   "Bearer",
			})
		case "/introspect":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"active": false,
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer mockProvider.Close()

	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                mockProvider.URL,
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: mockProvider.URL + "/auth",
		TokenEndpoint:         mockProvider.URL + "/token",
		IntrospectionEndpoint: mockProvider.URL + "/introspect",
		SSOCallbackURL:        "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
		SSORedirectAllowlist:  []string{"element://"},
	})
	defer closeDB()

	state, cookie := startSSOFlow(t, routers, "element://vector/webapp")

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/callback", test.WithQueryParams(map[string]string{
		"state": state,
		"code":  "mock-code",
	}))
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("expected 401 for inactive token, got %d: %s", rec.Code, rec.Body.String())
	}
	body := rec.Body.String()
	if !strings.Contains(body, "M_UNKNOWN_TOKEN") {
		t.Fatalf("expected M_UNKNOWN_TOKEN in response body, got %s", body)
	}
}

func TestMSC3861SSOCompat_CallbackClientSecretPost(t *testing.T) {
	t.Parallel()

	accessToken := "post-token-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	var receivedClientSecret string
	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/token" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		if err := r.ParseForm(); err != nil {
			t.Fatalf("failed to parse form: %v", err)
		}
		receivedClientSecret = r.Form.Get("client_secret")
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": accessToken,
			"token_type":   "Bearer",
		})
	}))
	defer mockProvider.Close()

	mockIntrospect := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"active":   true,
			"sub":      "bob",
			"username": "bob",
		})
	}))
	defer mockIntrospect.Close()

	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                mockProvider.URL,
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		ClientAuthMethod:      "client_secret_post",
		AuthorizationEndpoint: mockProvider.URL + "/auth",
		TokenEndpoint:         mockProvider.URL + "/token",
		IntrospectionEndpoint: mockIntrospect.URL + "/introspect",
		SSOCallbackURL:        "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
		SSORedirectAllowlist:  []string{"element://"},
	})
	defer closeDB()

	state, cookie := startSSOFlow(t, routers, "element://vector/webapp")

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/callback", test.WithQueryParams(map[string]string{
		"state": state,
		"code":  "mock-code",
	}))
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d: %s", rec.Code, rec.Body.String())
	}
	if receivedClientSecret != "test-client-secret" {
		t.Errorf("expected client_secret in token request body, got %q", receivedClientSecret)
	}
}

func TestMSC3861SSOCompat_CallbackReusesExistingUser(t *testing.T) {
	t.Parallel()

	accessToken := "reuses-token-" + strconv.FormatInt(time.Now().UnixNano(), 10)
	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/token":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"access_token": accessToken,
				"token_type":   "Bearer",
			})
		case "/introspect":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(map[string]any{
				"active":   true,
				"sub":      "existing-user-sub",
				"username": "existinguser",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer mockProvider.Close()

	routers, cfg, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                mockProvider.URL,
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: mockProvider.URL + "/auth",
		TokenEndpoint:         mockProvider.URL + "/token",
		IntrospectionEndpoint: mockProvider.URL + "/introspect",
		SSOCallbackURL:        "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
		SSORedirectAllowlist:  []string{"element://"},
	})
	defer closeDB()

	// Pre-create the user account.
	expectedUserID := "@existinguser:" + string(cfg.Global.ServerName)
	// We can't directly create an account here easily, but the first SSO login
	// will auto-provision it. We just verify the callback succeeds and the
	// resulting login token is for the expected user.
	state, cookie := startSSOFlow(t, routers, "element://vector/webapp")

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/callback", test.WithQueryParams(map[string]string{
		"state": state,
		"code":  "mock-code",
	}))
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusFound {
		t.Fatalf("expected 302, got %d: %s", rec.Code, rec.Body.String())
	}

	u, _ := url.Parse(rec.Header().Get("Location"))
	loginToken := u.Query().Get("loginToken")

	loginReq := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/login", test.WithJSONBody(t, map[string]any{
		"type":  authtypes.LoginTypeToken,
		"token": loginToken,
	}))
	loginRec := httptest.NewRecorder()
	routers.Client.ServeHTTP(loginRec, loginReq)
	if loginRec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", loginRec.Code, loginRec.Body.String())
	}
	var loginResp loginResponse
	if err := json.Unmarshal(loginRec.Body.Bytes(), &loginResp); err != nil {
		t.Fatalf("failed to parse login response: %v", err)
	}
	if loginResp.UserID != expectedUserID {
		t.Errorf("expected user_id %s, got %s", expectedUserID, loginResp.UserID)
	}
}

// The callback must be rejected when the browser does not present the
// browser-binding cookie issued by /login/sso/redirect: without it an
// attacker could start a flow with a malicious redirectUrl and trick a victim
// into completing it, leaking the resulting login token.
func TestMSC3861SSOCompat_CallbackWithoutCookieRejected(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                "https://auth.example.com",
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: "https://auth.example.com/auth",
		TokenEndpoint:         "https://auth.example.com/token",
		SSOCallbackURL:        "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
		SSORedirectAllowlist:  []string{"element://"},
	})
	defer closeDB()

	state, _ := startSSOFlow(t, routers, "element://vector/webapp")

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/callback", test.WithQueryParams(map[string]string{
		"state": state,
		"code":  "mock-auth-code",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a callback without the browser-binding cookie, got %d: %s", rec.Code, rec.Body.String())
	}
}

// A callback presenting a cookie whose nonce does not match the one stored
// with the session must be rejected.
func TestMSC3861SSOCompat_CallbackMismatchedCookieRejected(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                "https://auth.example.com",
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: "https://auth.example.com/auth",
		TokenEndpoint:         "https://auth.example.com/token",
		SSOCallbackURL:        "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
		SSORedirectAllowlist:  []string{"element://"},
	})
	defer closeDB()

	state, cookie := startSSOFlow(t, routers, "element://vector/webapp")
	cookie.Value = "tampered-nonce"

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/callback", test.WithQueryParams(map[string]string{
		"state": state,
		"code":  "mock-auth-code",
	}))
	req.AddCookie(cookie)
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 for a callback with a mismatched cookie, got %d: %s", rec.Code, rec.Body.String())
	}
}

// When sso_redirect_allowlist is configured, only redirectUrl values matching
// one of the allowed prefixes are accepted.
func TestMSC3861SSOCompat_RedirectURLAllowlist(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                "https://auth.example.com",
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: "https://auth.example.com/auth",
		TokenEndpoint:         "https://auth.example.com/token",
		SSOCallbackURL:        "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
		SSORedirectAllowlist:  []string{"https://app.example.com/", "element://"},
	})
	defer closeDB()

	for _, tc := range []struct {
		name        string
		redirectURL string
		wantCode    int
	}{
		{name: "allowed https prefix", redirectURL: "https://app.example.com/welcome?x=1", wantCode: http.StatusFound},
		{name: "allowed scheme prefix", redirectURL: "element://vector/webapp", wantCode: http.StatusFound},
		{name: "rejected host", redirectURL: "https://evil.example.com/", wantCode: http.StatusBadRequest},
		{name: "rejected prefix lookalike", redirectURL: "https://app.example.com.evil.com/", wantCode: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/redirect", test.WithQueryParams(map[string]string{
				"redirectUrl": tc.redirectURL,
			}))
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("expected %d for redirectUrl %q, got %d: %s", tc.wantCode, tc.redirectURL, rec.Code, rec.Body.String())
			}
		})
	}
}

// takeSession consumes the session atomically: the same state can only be
// used once, and expired sessions are rejected.
func TestSSOCompatHandlerTakeSessionSingleConsumption(t *testing.T) {
	t.Parallel()
	h := newSSOCompatHandler(&config.MSC3861Config{}, nil, nil, nil)
	defer h.Close()

	state, _, err := h.registerRedirect("element://vector/webapp")
	if err != nil {
		t.Fatalf("registerRedirect failed: %v", err)
	}
	if s := h.takeSession(state); s == nil {
		t.Fatal("expected takeSession to return the session")
	}
	if s := h.takeSession(state); s != nil {
		t.Error("expected takeSession to consume the session, but it was returned twice")
	}

	// Expired sessions must be rejected and removed.
	expiredState, expiredSession, err := h.registerRedirect("element://vector/webapp")
	if err != nil {
		t.Fatalf("registerRedirect failed: %v", err)
	}
	expiredSession.expiresAt = time.Now().Add(-time.Minute)
	if s := h.takeSession(expiredState); s != nil {
		t.Error("expected takeSession to reject an expired session")
	}
	if s := h.takeSession(expiredState); s != nil {
		t.Error("expected the expired session to be removed")
	}
}

func TestGeneratePKCEVerifier(t *testing.T) {
	t.Parallel()
	v, err := generatePKCEVerifier()
	if err != nil {
		t.Fatalf("generatePKCEVerifier failed: %v", err)
	}
	if len(v) != 43 {
		t.Errorf("expected verifier length 43, got %d", len(v))
	}
	if strings.Contains(v, "+") || strings.Contains(v, "/") || strings.Contains(v, "=") {
		t.Errorf("verifier is not URL-safe: %s", v)
	}
}

func TestPKCEChallenge(t *testing.T) {
	t.Parallel()
	verifier := "test-verifier"
	ch1 := pkceChallenge(verifier)
	ch2 := pkceChallenge(verifier)
	if ch1 != ch2 {
		t.Errorf("PKCE challenge not deterministic: %s vs %s", ch1, ch2)
	}
	if ch1 == "" {
		t.Error("expected non-empty challenge")
	}
	if strings.Contains(ch1, "+") || strings.Contains(ch1, "/") || strings.Contains(ch1, "=") {
		t.Errorf("challenge is not URL-safe: %s", ch1)
	}
}

func TestRequestScheme(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		req      *http.Request
		expected string
	}{
		{
			name: "forwarded proto https",
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "http://localhost/foo", nil)
				r.Header.Set("X-Forwarded-Proto", "https")
				return r
			}(),
			expected: "https",
		},
		{
			name: "TLS request",
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "https://localhost/foo", nil)
				return r
			}(),
			expected: "https",
		},
		{
			name: "plain HTTP",
			req: func() *http.Request {
				r := httptest.NewRequest(http.MethodGet, "http://localhost/foo", nil)
				return r
			}(),
			expected: "http",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := requestScheme(tt.req); got != tt.expected {
				t.Errorf("requestScheme() = %s, want %s", got, tt.expected)
			}
		})
	}
}

func TestSSOCompatHandlerCallbackURL(t *testing.T) {
	t.Parallel()
	h := newSSOCompatHandler(&config.MSC3861Config{
		SSOCallbackURL: "https://custom.example.com/callback",
	}, nil, nil, nil)
	defer h.Close()
	req := httptest.NewRequest(http.MethodGet, "http://localhost/_matrix/client/v3/login/sso/redirect", nil)
	if got := h.callbackURL(req); got != "https://custom.example.com/callback" {
		t.Errorf("expected custom callback URL, got %s", got)
	}

	h2 := newSSOCompatHandler(&config.MSC3861Config{}, nil, nil, nil)
	defer h2.Close()
	req2 := httptest.NewRequest(http.MethodGet, "http://example.com/_matrix/client/v3/login/sso/redirect", nil)
	req2.Header.Set("X-Forwarded-Proto", "https")
	if got := h2.callbackURL(req2); got != "https://example.com/_matrix/client/v3/login/sso/callback" {
		t.Errorf("expected derived callback URL, got %s", got)
	}

	// public_base_url takes precedence over the request-derived URL.
	h3 := newSSOCompatHandler(&config.MSC3861Config{
		PublicBaseURL: "https://matrix.example.com/",
	}, nil, nil, nil)
	defer h3.Close()
	req3 := httptest.NewRequest(http.MethodGet, "http://internal:8008/_matrix/client/v3/login/sso/redirect", nil)
	if got := h3.callbackURL(req3); got != "https://matrix.example.com/_matrix/client/v3/login/sso/callback" {
		t.Errorf("expected public_base_url-derived callback URL, got %s", got)
	}
}

// With no sso_redirect_allowlist configured the redirect endpoint is
// default-deny: only targets on the homeserver's own origin are accepted.
// Anything else would let an attacker's link walk the victim's own browser
// through the flow and collect the resulting login token, which POST /login
// redeems into the provider-issued access and refresh tokens.
func TestMSC3861SSOCompat_RedirectDefaultDeniesCrossOrigin(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                "https://auth.example.com",
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: "https://auth.example.com/auth",
		TokenEndpoint:         "https://auth.example.com/token",
		SSOCallbackURL:        "https://matrix.example.com/_matrix/client/v3/login/sso/callback",
	})
	defer closeDB()

	// test.NewRequest builds requests for http://localhost, so that is the
	// origin the homeserver sees.
	for _, tc := range []struct {
		name        string
		redirectURL string
		wantCode    int
	}{
		{name: "same origin", redirectURL: "http://localhost/element/#/welcome", wantCode: http.StatusFound},
		{name: "same origin case insensitive", redirectURL: "HTTP://LOCALHOST/element", wantCode: http.StatusFound},
		{name: "attacker origin", redirectURL: "https://evil.example.com/", wantCode: http.StatusBadRequest},
		{name: "same host different scheme", redirectURL: "https://localhost/element", wantCode: http.StatusBadRequest},
		{name: "protocol relative", redirectURL: "//evil.example.com/", wantCode: http.StatusBadRequest},
		{name: "deep link without allowlist", redirectURL: "element://vector/webapp", wantCode: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/redirect", test.WithQueryParams(map[string]string{
				"redirectUrl": tc.redirectURL,
			}))
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("expected %d for redirectUrl %q, got %d: %s", tc.wantCode, tc.redirectURL, rec.Code, rec.Body.String())
			}
			if tc.wantCode != http.StatusBadRequest {
				return
			}
			// The rejection must tell the operator how to allow the target.
			var errResp map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &errResp); err != nil {
				t.Fatalf("failed to parse error response: %v", err)
			}
			if errResp["errcode"] != "M_INVALID_PARAM" {
				t.Errorf("expected M_INVALID_PARAM, got %v", errResp["errcode"])
			}
			if msg, _ := errResp["error"].(string); !strings.Contains(msg, "sso_redirect_allowlist") {
				t.Errorf("expected the error to point at sso_redirect_allowlist, got %q", msg)
			}
		})
	}
}

// When public_base_url is configured it, rather than the request's Host
// header, defines the origin accepted by the default-deny check.
func TestMSC3861SSOCompat_RedirectDefaultDenyUsesPublicBaseURL(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                "https://auth.example.com",
		ClientID:              "test-client-id",
		ClientSecret:          "test-client-secret",
		AuthorizationEndpoint: "https://auth.example.com/auth",
		TokenEndpoint:         "https://auth.example.com/token",
		PublicBaseURL:         "https://matrix.example.com",
	})
	defer closeDB()

	for _, tc := range []struct {
		name        string
		redirectURL string
		wantCode    int
	}{
		{name: "public base url origin", redirectURL: "https://matrix.example.com/element/", wantCode: http.StatusFound},
		{name: "request origin no longer trusted", redirectURL: "http://localhost/element/", wantCode: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/login/sso/redirect", test.WithQueryParams(map[string]string{
				"redirectUrl": tc.redirectURL,
			}))
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			if rec.Code != tc.wantCode {
				t.Fatalf("expected %d for redirectUrl %q, got %d: %s", tc.wantCode, tc.redirectURL, rec.Code, rec.Body.String())
			}
		})
	}
}

// redirectURLAllowed matches http(s) allowlist entries on the parsed origin,
// so a lookalike host cannot pass by sharing a string prefix with an entry.
func TestSSOCompatHandlerRedirectURLAllowed(t *testing.T) {
	t.Parallel()
	req := httptest.NewRequest(http.MethodGet, "http://matrix.example.com/_matrix/client/v3/login/sso/redirect", nil)

	for _, tc := range []struct {
		name        string
		allowlist   []string
		publicBase  string
		redirectURL string
		want        bool
	}{
		{name: "empty allowlist allows request origin", redirectURL: "http://matrix.example.com/element", want: true},
		{name: "empty allowlist denies other origin", redirectURL: "https://evil.example.com/", want: false},
		{name: "empty allowlist denies relative target", redirectURL: "/element/#/welcome", want: false},
		{name: "empty allowlist honors public_base_url", publicBase: "https://public.example.com", redirectURL: "https://public.example.com/element", want: true},
		{
			name:        "allowlist entry without trailing slash allows the origin",
			allowlist:   []string{"https://app.example.com"},
			redirectURL: "https://app.example.com/welcome?x=1",
			want:        true,
		},
		{
			name:        "allowlist entry without trailing slash rejects a lookalike host",
			allowlist:   []string{"https://app.example.com"},
			redirectURL: "https://app.example.com.evil.com/",
			want:        false,
		},
		{
			name:        "allowlist entry rejects an evil userinfo host",
			allowlist:   []string{"https://app.example.com"},
			redirectURL: "https://app.example.com@evil.example.com/",
			want:        false,
		},
		{
			name:        "allowlist entry rejects a different port",
			allowlist:   []string{"https://app.example.com"},
			redirectURL: "https://app.example.com:8443/",
			want:        false,
		},
		{
			name:        "allowlist path scopes the target",
			allowlist:   []string{"https://app.example.com/element/"},
			redirectURL: "https://app.example.com/element/welcome",
			want:        true,
		},
		{
			name:        "allowlist path rejects a sibling path",
			allowlist:   []string{"https://app.example.com/element/"},
			redirectURL: "https://app.example.com/elementary",
			want:        false,
		},
		{
			name:        "non-http entries keep prefix matching",
			allowlist:   []string{"element://"},
			redirectURL: "element://vector/webapp",
			want:        true,
		},
		{
			name:        "non-http entries do not match other schemes",
			allowlist:   []string{"element://"},
			redirectURL: "elementx://vector/webapp",
			want:        false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			h := newSSOCompatHandler(&config.MSC3861Config{
				SSORedirectAllowlist: tc.allowlist,
				PublicBaseURL:        tc.publicBase,
			}, nil, nil, nil)
			defer h.Close()
			if got := h.redirectURLAllowed(req, tc.redirectURL); got != tc.want {
				t.Errorf("redirectURLAllowed(%q) = %v, want %v", tc.redirectURL, got, tc.want)
			}
		})
	}
}

// Discovery results are cached per issuer, so that /login/sso/redirect and the
// unauthenticated POST /refresh do not fetch .well-known/openid-configuration
// on every request.
func TestSSOCompatHandlerDiscoveryCache(t *testing.T) {
	t.Parallel()

	var discoveryHits int32
	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/.well-known/openid-configuration" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		atomic.AddInt32(&discoveryHits, 1)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"authorization_endpoint": "https://auth.example.com/discovery/auth",
			"token_endpoint":         "https://auth.example.com/discovery/token",
		})
	}))
	defer mockProvider.Close()

	h := newSSOCompatHandler(&config.MSC3861Config{
		Issuer:       mockProvider.URL,
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	}, nil, mockProvider.Client(), nil)
	defer h.Close()

	// A zero TTL makes every entry stale as soon as it is written, so each
	// call refetches. This is what the injected TTL is for.
	for i := range 3 {
		endpoints, err := h.discoverOIDCEndpointsWithTTL(context.Background(), 0, 0)
		if err != nil {
			t.Fatalf("discoverOIDCEndpointsWithTTL call %d failed: %v", i, err)
		}
		if endpoints.authorizationEndpoint != "https://auth.example.com/discovery/auth" {
			t.Errorf("unexpected authorization endpoint: %s", endpoints.authorizationEndpoint)
		}
		if endpoints.tokenEndpoint != "https://auth.example.com/discovery/token" {
			t.Errorf("unexpected token endpoint: %s", endpoints.tokenEndpoint)
		}
	}
	if got := atomic.LoadInt32(&discoveryHits); got != 3 {
		t.Fatalf("expected an expired cache entry to be refetched every time, got %d fetches", got)
	}

	// With the production TTL the document is fetched once and then served
	// from the cache.
	for i := range 3 {
		if _, err := h.discoverOIDCEndpoints(context.Background()); err != nil {
			t.Fatalf("discoverOIDCEndpoints call %d failed: %v", i, err)
		}
	}
	if got := atomic.LoadInt32(&discoveryHits); got != 4 {
		t.Fatalf("expected a single further discovery fetch, got %d in total", got)
	}
}

// Failed discoveries are negative-cached, so a provider outage cannot be
// amplified into one outbound request per client request.
func TestSSOCompatHandlerDiscoveryCacheNegativeCaching(t *testing.T) {
	t.Parallel()

	var discoveryHits int32
	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&discoveryHits, 1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer mockProvider.Close()

	h := newSSOCompatHandler(&config.MSC3861Config{
		Issuer:       mockProvider.URL,
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	}, nil, mockProvider.Client(), nil)
	defer h.Close()

	for i := range 3 {
		if _, err := h.discoverOIDCEndpoints(context.Background()); err == nil {
			t.Fatalf("expected discovery call %d to fail", i)
		}
	}
	if got := atomic.LoadInt32(&discoveryHits); got != 1 {
		t.Fatalf("expected the failure to be negative-cached after one fetch, got %d", got)
	}
}

// Client credentials are sent verbatim in the Authorization header: MAS,
// Keycloak and Hydra compare the raw values, so form-encoding them would break
// every secret containing '+', '/', '=' or '%'.
func TestMSC3861SSOCompat_ClientSecretBasicSendsRawCredentials(t *testing.T) {
	t.Parallel()

	const clientSecret = "s3cr3t+with/special=chars%21"
	mockProvider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, pass, ok := r.BasicAuth()
		if !ok || user != "test-client-id" || pass != clientSecret {
			t.Errorf("expected raw basic auth credentials, got ok=%v user=%q pass=%q", ok, user, pass)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"access_token": "new-access-token",
			"token_type":   "Bearer",
			"expires_in":   3600,
		})
	}))
	defer mockProvider.Close()

	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:                mockProvider.URL,
		ClientID:              "test-client-id",
		ClientSecret:          clientSecret,
		AuthorizationEndpoint: mockProvider.URL + "/auth",
		TokenEndpoint:         mockProvider.URL + "/token",
	})
	defer closeDB()

	req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/refresh", test.WithJSONBody(t, map[string]any{
		"refresh_token": "old-refresh-token",
	}))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
}

// Close stops the cleanup goroutine and is safe to call repeatedly.
func TestSSOCompatHandlerCloseIsIdempotent(t *testing.T) {
	t.Parallel()
	h := newSSOCompatHandler(&config.MSC3861Config{}, nil, nil, nil)
	h.Close()
	h.Close()
}
