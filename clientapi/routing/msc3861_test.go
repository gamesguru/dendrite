// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sync/atomic"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

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
)

// setupMSC3861Routers creates a full router stack with MSC3861 enabled for testing.
// These tests only verify HTTP routing behavior (status codes, response bodies),
// not database semantics, so we always use SQLite to avoid the shared-postgres
// schema race that occurs when multiple parallel subtests DROP SCHEMA on the
// same CI database.
func setupMSC3861Routers(t *testing.T) (httputil.Routers, *config.Zendrite, func()) {
	t.Helper()
	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	cfg.ClientAPI.RateLimiting.Enabled = false
	cfg.MSCs.MSCs = []string{"msc3861"}
	cfg.MSCs.MSC3861 = config.MSC3861Config{
		Issuer:               "https://auth.example.com",
		ClientID:             "test-client-id",
		ClientSecret:         "test-client-secret",
		AdminToken:           "test-admin-token",
		AccountManagementURL: "https://auth.example.com/account",
	}
	// WellKnownClientName needed for well-known endpoint registration
	cfg.Global.WellKnownClientName = "https://matrix.example.com"

	// Register a dummy application service so we can verify that
	// m.login.application_service registrations remain available.
	asRegex := regexp.MustCompile(`@_appservice_.*:test`)
	cfg.ClientAPI.Derived.ApplicationServices = []config.ApplicationService{
		{
			ID:              "fake-as",
			ASToken:         "fake-as-token",
			HSToken:         "fake-hs-token",
			URL:             "null",
			SenderLocalpart: "_appservice_bot",
			NamespaceMap: map[string][]config.ApplicationServiceNamespace{
				"users": {
					{
						Exclusive:    true,
						Regex:        asRegex.String(),
						RegexpObject: asRegex,
					},
				},
			},
		},
	}

	natsInstance := jetstream.NATSInstance{}
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)

	routers := httputil.NewRouters()
	Setup(routers, cfg, nil, nil, userAPI, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

	return routers, cfg, closeDB
}

func TestMSC3861_DisabledEndpoints(t *testing.T) {
	t.Parallel()

	type endpointTest struct {
		name   string
		method string
		path   string
	}

	endpoints := []endpointTest{
		{"register", http.MethodPost, "/_matrix/client/v3/register"},
		{"register available", http.MethodGet, "/_matrix/client/v3/register/available"},
		{"password", http.MethodPost, "/_matrix/client/v3/account/password"},
		{"deactivate", http.MethodPost, "/_matrix/client/v3/account/deactivate"},
		{"3pid POST", http.MethodPost, "/_matrix/client/v3/account/3pid"},
		{"3pid delete", http.MethodPost, "/_matrix/client/v3/account/3pid/delete"},
		{"3pid email requestToken", http.MethodPost, "/_matrix/client/v3/account/3pid/email/requestToken"},
		{"register email requestToken", http.MethodPost, "/_matrix/client/v3/register/email/requestToken"},
		{"auth fallback web", http.MethodGet, "/_matrix/client/v3/auth/m.login.sso/fallback/web"},
		{"device PUT", http.MethodPut, "/_matrix/client/v3/devices/ABCDEF"},
		{"device DELETE", http.MethodDelete, "/_matrix/client/v3/devices/ABCDEF"},
		{"delete_devices", http.MethodPost, "/_matrix/client/v3/delete_devices"},
		{"logout", http.MethodPost, "/_matrix/client/v3/logout"},
		{"logout all", http.MethodPost, "/_matrix/client/v3/logout/all"},
		{"login POST", http.MethodPost, "/_matrix/client/v3/login"},
	}

	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			req := test.NewRequest(t, ep.method, ep.path)
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			if rec.Code != http.StatusForbidden {
				t.Errorf("%s %s: expected 403, got %d: %s", ep.method, ep.path, rec.Code, rec.Body.String())
			}

			var body map[string]any
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("failed to parse response: %v", err)
			}
			if errcode, ok := body["errcode"].(string); !ok || errcode != string(spec.ErrorForbidden) {
				t.Errorf("expected errcode %s, got %v", spec.ErrorForbidden, body["errcode"])
			}
		})
	}
}

func TestMSC3861_LoginFlowsGET(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
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
		t.Fatalf("expected 2 flows (sso, token), got %d: %+v", len(resp.Flows), resp.Flows)
	}
	gotTypes := make(map[string]bool)
	for _, f := range resp.Flows {
		gotTypes[f.Type] = true
	}
	if !gotTypes[authtypes.LoginTypeSSO] {
		t.Errorf("expected %s flow to be advertised", authtypes.LoginTypeSSO)
	}
	if !gotTypes[authtypes.LoginTypeToken] {
		t.Errorf("expected %s flow to be advertised", authtypes.LoginTypeToken)
	}
}

func TestMSC3861_LoginPOST(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/login")
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d: %s", rec.Code, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if errcode, ok := body["errcode"].(string); !ok || errcode != string(spec.ErrorForbidden) {
		t.Errorf("expected errcode %s, got %v", spec.ErrorForbidden, body["errcode"])
	}
}

func TestMSC3861_WellKnown(t *testing.T) {
	t.Parallel()
	routers, cfg, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/.well-known/matrix/client")
	rec := httptest.NewRecorder()
	routers.WellKnown.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp WellKnownClientResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp.Authentication == nil {
		t.Fatal("expected m.authentication in well-known response")
	}
	if resp.Authentication.Issuer != cfg.MSCs.MSC3861.Issuer {
		t.Errorf("expected issuer %s, got %s", cfg.MSCs.MSC3861.Issuer, resp.Authentication.Issuer)
	}
	if resp.Authentication.Account != cfg.MSCs.MSC3861.AccountManagementURL {
		t.Errorf("expected account %s, got %s", cfg.MSCs.MSC3861.AccountManagementURL, resp.Authentication.Account)
	}
}

func TestMSC3861_AuthMetadata(t *testing.T) {
	t.Parallel()
	routers, cfg, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v1/auth_metadata")
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp AuthMetadataResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp.Issuer != cfg.MSCs.MSC3861.Issuer {
		t.Errorf("expected issuer %s, got %s", cfg.MSCs.MSC3861.Issuer, resp.Issuer)
	}
	if resp.AuthorizationEndpoint == "" {
		t.Error("expected non-empty authorization_endpoint")
	}
	if resp.TokenEndpoint == "" {
		t.Error("expected non-empty token_endpoint")
	}
	if resp.RegistrationEndpoint == "" {
		t.Error("expected non-empty registration_endpoint")
	}
	if resp.RevocationEndpoint == "" {
		t.Error("expected non-empty revocation_endpoint")
	}
	if resp.JWKSURI == "" {
		t.Error("expected non-empty jwks_uri")
	}
}

func TestMSC3861_AuthMetadataDiscoversEndpoints(t *testing.T) {
	t.Parallel()

	var discoveryHits int
	discoverySrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			discoveryHits++
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"issuer":                 "http://" + r.Host,
				"jwks_uri":               "http://" + r.Host + "/.well-known/jwks.json",
				"authorization_endpoint": "http://" + r.Host + "/oauth2/auth",
				"token_endpoint":         "http://" + r.Host + "/oauth2/token",
				"registration_endpoint":  "http://" + r.Host + "/oauth2/register",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer discoverySrv.Close()

	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cfg.ClientAPI.RateLimiting.Enabled = false
	cfg.MSCs.MSCs = []string{"msc3861"}
	cfg.MSCs.MSC3861 = config.MSC3861Config{
		Issuer:       discoverySrv.URL,
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	}
	cfg.Global.WellKnownClientName = "https://matrix.example.com"

	natsInstance := jetstream.NATSInstance{}
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)

	routers := httputil.NewRouters()
	Setup(routers, cfg, nil, nil, userAPI, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v1/auth_metadata")
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp AuthMetadataResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp.JWKSURI != "http://localhost/_matrix/client/v1/auth_metadata/jwks" {
		t.Errorf("expected jwks_uri to be the homeserver JWKS proxy, got %s", resp.JWKSURI)
	}
	if resp.RegistrationEndpoint != discoverySrv.URL+"/oauth2/register" {
		t.Errorf("expected registration_endpoint from discovery, got %s", resp.RegistrationEndpoint)
	}
	if resp.AuthorizationEndpoint != discoverySrv.URL+"/oauth2/auth" {
		t.Errorf("expected authorization_endpoint from discovery, got %s", resp.AuthorizationEndpoint)
	}
	if resp.TokenEndpoint != discoverySrv.URL+"/oauth2/token" {
		t.Errorf("expected token_endpoint from discovery, got %s", resp.TokenEndpoint)
	}
	if discoveryHits != 1 {
		t.Errorf("expected exactly one discovery request, got %d", discoveryHits)
	}

	// A second request should be served from cache.
	rec2 := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec2, req)
	if rec2.Code != http.StatusOK {
		t.Fatalf("expected 200 on second request, got %d: %s", rec2.Code, rec2.Body.String())
	}
	if discoveryHits != 1 {
		t.Errorf("expected discovery to be cached, got %d hits", discoveryHits)
	}
}

func TestMSC3861_MASAdminRoutesRequireAdminToken(t *testing.T) {
	t.Parallel()

	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cfg.ClientAPI.RateLimiting.Enabled = false
	cfg.MSCs.MSCs = []string{"msc3861"}
	cfg.MSCs.MSC3861 = config.MSC3861Config{
		Issuer:       "https://auth.example.com",
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
		// AdminToken deliberately left empty.
	}
	cfg.Global.WellKnownClientName = "https://matrix.example.com"

	natsInstance := jetstream.NATSInstance{}
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)

	routers := httputil.NewRouters()
	Setup(routers, cfg, nil, nil, userAPI, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

	req := test.NewRequest(t, http.MethodGet, "/_synapse/admin/v1/username_available?username=alice")
	rec := httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Errorf("expected 404 when admin_token is empty, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMSC3861_DevicesGETStillWorks(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	// GET /devices should NOT be blocked by MSC3861 (only PUT/DELETE are blocked).
	// Without auth it returns 401, not 403 — that proves the endpoint is still
	// routed to the real handler rather than the forbidden handler.
	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/devices")
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code == http.StatusForbidden {
		t.Error("GET /devices should not be forbidden when MSC3861 is enabled")
	}
}

func TestMSC3861_ApplicationServiceRegisterStillWorks(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	body, err := json.Marshal(map[string]any{
		"type":     "m.login.application_service",
		"username": "_appservice_alice",
	})
	if err != nil {
		t.Fatalf("failed to marshal request body: %v", err)
	}

	req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/register?access_token=fake-as-token")
	req.Body = io.NopCloser(bytes.NewReader(body))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)

	// A 200/401 response means the request reached the real registration handler
	// and was NOT rejected by the MSC3861 forbidden handler.
	if rec.Code == http.StatusForbidden {
		t.Fatalf("application service registration should not be forbidden under MSC3861: %s", rec.Body.String())
	}
	if rec.Code != http.StatusOK && rec.Code != http.StatusUnauthorized {
		t.Errorf("expected 200 or 401, got %d: %s", rec.Code, rec.Body.String())
	}
}

// TestMSC3861_RegisterBodyTooLarge verifies that the unauthenticated register
// shim bounds the body it buffers instead of reading it in full.
func TestMSC3861_RegisterBodyTooLarge(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	// One byte over the limit is enough to be rejected; the handler must not
	// buffer the whole body first.
	oversized := append([]byte(`{"padding":"`), bytes.Repeat([]byte("a"), msc3861RegisterBodyMaxBytes)...)
	oversized = append(oversized, []byte(`"}`)...)

	req := test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/register")
	req.Body = io.NopCloser(bytes.NewReader(oversized))
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)

	if rec.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("expected 413 for an oversized register body, got %d: %s", rec.Code, rec.Body.String())
	}
}

func TestMSC3861_VersionsAdvertisesMSC2965(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/versions")
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp struct {
		UnstableFeatures map[string]bool `json:"unstable_features"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if !resp.UnstableFeatures["org.matrix.msc2965"] {
		t.Error("expected org.matrix.msc2965 unstable feature to be advertised")
	}
	if !resp.UnstableFeatures["org.matrix.msc2967"] {
		t.Error("expected org.matrix.msc2967 unstable feature to be advertised")
	}
}

func TestMSC3861_AuthMetadataUnstable(t *testing.T) {
	t.Parallel()
	routers, cfg, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/unstable/org.matrix.msc2965/auth_metadata")
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp AuthMetadataResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp.Issuer != cfg.MSCs.MSC3861.Issuer {
		t.Errorf("expected issuer %s, got %s", cfg.MSCs.MSC3861.Issuer, resp.Issuer)
	}
}

func TestMSC3861_AuthMetadataDisabled(t *testing.T) {
	t.Parallel()

	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cfg.ClientAPI.RateLimiting.Enabled = false
	// MSC3861 intentionally not enabled.
	cfg.Global.WellKnownClientName = "https://matrix.example.com"

	natsInstance := jetstream.NATSInstance{}
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)

	routers := httputil.NewRouters()
	Setup(routers, cfg, nil, nil, userAPI, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

	req := test.NewRequest(t, http.MethodGet, "/_matrix/client/v1/auth_metadata")
	rec := httptest.NewRecorder()
	routers.Client.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 when MSC3861 is disabled, got %d: %s", rec.Code, rec.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if errcode, ok := body["errcode"].(string); !ok || errcode != string(spec.ErrorUnrecognized) {
		t.Errorf("expected errcode %s, got %v", spec.ErrorUnrecognized, body["errcode"])
	}
}

func TestMSC3861_AuthIssuer(t *testing.T) {
	t.Parallel()

	for _, path := range []string{
		"/_matrix/client/v1/auth_issuer",
		"/_matrix/client/unstable/org.matrix.msc2965/auth_issuer",
	} {
		t.Run(path, func(t *testing.T) {
			routers, cfg, closeDB := setupMSC3861Routers(t)
			defer closeDB()

			req := test.NewRequest(t, http.MethodGet, path)
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
			}

			var resp authIssuerResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatalf("failed to parse response: %v", err)
			}
			if resp.Issuer != cfg.MSCs.MSC3861.Issuer {
				t.Errorf("expected issuer %s, got %s", cfg.MSCs.MSC3861.Issuer, resp.Issuer)
			}
		})
	}
}

// TestMSC3861_AuthMetadataPublicBaseURL verifies that the advertised jwks_uri
// is built from the configured public base URL when set, and only falls back
// to the request Host otherwise.
func TestMSC3861_AuthMetadataPublicBaseURL(t *testing.T) {
	t.Parallel()

	mscCfg := &config.MSCs{
		MSCs: []string{"msc3861"},
		MSC3861: config.MSC3861Config{
			Issuer:        "https://auth.example.com",
			PublicBaseURL: "https://matrix.example.com/",
		},
	}

	req := httptest.NewRequest(http.MethodGet, "http://forged-host.example.org/_matrix/client/v1/auth_metadata", nil)
	res := msc3861AuthMetadata(req, mscCfg)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}
	resp, ok := res.JSON.(AuthMetadataResponse)
	if !ok {
		t.Fatalf("expected AuthMetadataResponse, got %T", res.JSON)
	}
	want := "https://matrix.example.com/_matrix/client/v1/auth_metadata/jwks"
	if resp.JWKSURI != want {
		t.Errorf("expected jwks_uri %q, got %q", want, resp.JWKSURI)
	}

	// Without a configured public base URL the request Host is used.
	mscCfg.MSC3861.PublicBaseURL = ""
	res = msc3861AuthMetadata(req, mscCfg)
	resp, ok = res.JSON.(AuthMetadataResponse)
	if !ok {
		t.Fatalf("expected AuthMetadataResponse, got %T", res.JSON)
	}
	want = "http://forged-host.example.org/_matrix/client/v1/auth_metadata/jwks"
	if resp.JWKSURI != want {
		t.Errorf("expected jwks_uri %q, got %q", want, resp.JWKSURI)
	}
}

// TestMSC3861_DiscoveryCacheExpiry verifies that cached discovery documents
// expire instead of being pinned for the process lifetime.
func TestMSC3861_DiscoveryCacheExpiry(t *testing.T) {
	t.Parallel()

	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"issuer":         "http://" + r.Host,
			"token_endpoint": "http://" + r.Host + "/oauth2/token",
		})
	}))
	defer srv.Close()

	// Entries stored with a zero TTL expire immediately, so every lookup
	// revalidates instead of being pinned for the process lifetime.
	_ = discoverOIDCAuthServerMetadataWithTTL(context.Background(), srv.URL, 0)
	_ = discoverOIDCAuthServerMetadataWithTTL(context.Background(), srv.URL, 0)
	if hits != 2 {
		t.Fatalf("expected expired cache entries to be revalidated, got %d hits", hits)
	}

	// A long-lived entry is served from cache on subsequent lookups.
	doc := discoverOIDCAuthServerMetadataWithTTL(context.Background(), srv.URL, time.Hour)
	if doc.TokenEndpoint == "" {
		t.Fatal("expected discovery document to be used")
	}
	doc = discoverOIDCAuthServerMetadataWithTTL(context.Background(), srv.URL, time.Hour)
	if doc.TokenEndpoint == "" {
		t.Fatal("expected cached discovery document to be used")
	}
	if hits != 3 {
		t.Fatalf("expected the second lookup to be served from cache, got %d hits", hits)
	}
}

// TestMSC3861_DiscoveryIssuerMismatch verifies that a discovery document whose
// issuer does not match the configured issuer is neither cached nor used.
func TestMSC3861_DiscoveryIssuerMismatch(t *testing.T) {
	t.Parallel()

	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"issuer":         "https://attacker.example.org",
			"token_endpoint": "https://attacker.example.org/oauth2/token",
		})
	}))
	defer srv.Close()

	doc := discoverOIDCAuthServerMetadataWithTTL(context.Background(), srv.URL, time.Hour)
	if doc.TokenEndpoint != "" {
		t.Fatalf("expected mismatched issuer document to be ignored, got %+v", doc)
	}
	// Not cached: a second lookup hits the server again.
	_ = discoverOIDCAuthServerMetadataWithTTL(context.Background(), srv.URL, time.Hour)
	if hits != 2 {
		t.Fatalf("expected mismatched issuer document to not be cached, got %d hits", hits)
	}
}

// TestMSC3861_DiscoveryIssuerTrailingSlash verifies that a configured issuer
// with a trailing slash still matches the discovery document and is cached,
// instead of silently revalidating on every request.
func TestMSC3861_DiscoveryIssuerTrailingSlash(t *testing.T) {
	t.Parallel()

	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits++
		if r.URL.Path != "/.well-known/openid-configuration" {
			t.Errorf("unexpected discovery path %q", r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"issuer":         "http://" + r.Host,
			"token_endpoint": "http://" + r.Host + "/oauth2/token",
		})
	}))
	defer srv.Close()

	doc := discoverOIDCAuthServerMetadataWithTTL(context.Background(), srv.URL+"/", time.Hour)
	if doc.TokenEndpoint == "" {
		t.Fatal("expected a trailing-slash issuer to match the discovery document")
	}

	// Cached under the normalised issuer: a second lookup does not hit the
	// provider again, whether or not the caller passes the trailing slash.
	if doc = discoverOIDCAuthServerMetadataWithTTL(context.Background(), srv.URL+"/", time.Hour); doc.TokenEndpoint == "" {
		t.Fatal("expected cached discovery document to be used")
	}
	if doc = discoverOIDCAuthServerMetadataWithTTL(context.Background(), srv.URL, time.Hour); doc.TokenEndpoint == "" {
		t.Fatal("expected the normalised issuer to share the cache entry")
	}
	if hits != 1 {
		t.Fatalf("expected the trailing-slash issuer to be cached, got %d hits", hits)
	}
}

// TestMSC3861_DiscoveryIssuerDocTrailingSlash verifies that the issuer
// comparison also tolerates a trailing slash on the provider's side.
func TestMSC3861_DiscoveryIssuerDocTrailingSlash(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"issuer":         "http://" + r.Host + "/",
			"token_endpoint": "http://" + r.Host + "/oauth2/token",
		})
	}))
	defer srv.Close()

	doc := discoverOIDCAuthServerMetadataWithTTL(context.Background(), srv.URL, time.Hour)
	if doc.TokenEndpoint == "" {
		t.Fatal("expected a trailing-slash discovered issuer to match the configured issuer")
	}
}

func jwksProxyTestConfig(jwksURI string) *config.MSCs {
	return &config.MSCs{
		MSCs: []string{"msc3861"},
		MSC3861: config.MSC3861Config{
			Issuer:  "https://auth.example.com",
			JWKSURI: jwksURI,
		},
	}
}

// TestMSC3861_JWKSProxyRejectsInvalidPayload verifies that an upstream payload
// that is not a valid JWKS document yields a 502 and is not cached.
func TestMSC3861_JWKSProxyRejectsInvalidPayload(t *testing.T) {
	t.Parallel()

	jwksBody := []byte(`{"keys":[{"kty":"RSA","kid":"test","n":"abc","e":"AQAB"}]}`)
	var serveValid atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if serveValid.Load() {
			_, _ = w.Write(jwksBody)
			return
		}
		_, _ = w.Write([]byte(`<html>error page</html>`))
	}))
	defer srv.Close()

	mscCfg := jwksProxyTestConfig(srv.URL + "/jwks")
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	res := jwksProxyWithTTL(req, mscCfg, time.Minute)
	if res.Code != http.StatusBadGateway {
		t.Fatalf("expected 502 for an invalid JWKS payload, got %d", res.Code)
	}

	// The invalid payload must not have been cached: once the upstream
	// recovers, the proxy serves the valid document.
	serveValid.Store(true)
	res = jwksProxyWithTTL(req, mscCfg, time.Minute)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200 after upstream recovery, got %d", res.Code)
	}
	raw, ok := res.JSON.(jsonRaw)
	if !ok {
		t.Fatalf("expected jsonRaw, got %T", res.JSON)
	}
	if !bytes.Equal([]byte(raw), jwksBody) {
		t.Errorf("unexpected JWKS body: %s", string(raw))
	}
}

// TestMSC3861_JWKSProxyServesStaleOnError verifies that a failed revalidation
// serves the expired cache entry with a short max-age instead of a 502.
func TestMSC3861_JWKSProxyServesStaleOnError(t *testing.T) {
	t.Parallel()

	jwksBody := []byte(`{"keys":[{"kty":"RSA","kid":"test","n":"abc","e":"AQAB"}]}`)
	var fail atomic.Bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if fail.Load() {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(jwksBody)
	}))
	defer srv.Close()

	mscCfg := jwksProxyTestConfig(srv.URL + "/jwks")
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	// Prime the cache with an entry that expires immediately.
	res := jwksProxyWithTTL(req, mscCfg, -time.Second)
	if res.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", res.Code)
	}

	// Revalidation fails; the stale entry is served with a short max-age.
	fail.Store(true)
	res = jwksProxyWithTTL(req, mscCfg, -time.Second)
	if res.Code != http.StatusOK {
		t.Fatalf("expected stale entry to be served, got %d", res.Code)
	}
	if cc := res.Headers["Cache-Control"]; cc != "public, max-age=60" {
		t.Errorf("expected short stale max-age, got %q", cc)
	}
	raw, ok := res.JSON.(jsonRaw)
	if !ok {
		t.Fatalf("expected jsonRaw, got %T", res.JSON)
	}
	if !bytes.Equal([]byte(raw), jwksBody) {
		t.Errorf("unexpected stale JWKS body: %s", string(raw))
	}
}

// TestMSC3861_MASAdminUserLifecycle exercises user creation and retrieval
// through the MAS admin endpoints, including the admin flag mapping.
func TestMSC3861_MASAdminUserLifecycle(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	// An unknown user maps to 404.
	req := test_masAdminRequest(t, http.MethodGet, "/_synapse/admin/v1/users/@alice:test", "test-admin-token", "")
	rec := httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for unknown user, got %d: %s", rec.Code, rec.Body.String())
	}

	// Create a regular user.
	req = test_masAdminRequest(t, http.MethodPut, "/_synapse/admin/v1/users/@alice:test", "test-admin-token", `{"displayname": "Alice"}`)
	rec = httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 creating user, got %d: %s", rec.Code, rec.Body.String())
	}

	// Retrieval reports the account record.
	req = test_masAdminRequest(t, http.MethodGet, "/_synapse/admin/v1/users/@alice:test", "test-admin-token", "")
	rec = httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 getting user, got %d: %s", rec.Code, rec.Body.String())
	}
	var userResp struct {
		Name        string `json:"name"`
		DisplayName string `json:"displayname"`
		Admin       bool   `json:"admin"`
		Deactivated bool   `json:"deactivated"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &userResp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if userResp.Name != "@alice:test" {
		t.Errorf("expected name @alice:test, got %q", userResp.Name)
	}
	if userResp.Admin {
		t.Error("expected admin=false for a regular user")
	}

	// Create an admin user and verify the flag comes from the account record.
	req = test_masAdminRequest(t, http.MethodPut, "/_synapse/admin/v1/users/@admin-user:test", "test-admin-token", `{"admin": true}`)
	rec = httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 creating admin user, got %d: %s", rec.Code, rec.Body.String())
	}
	req = test_masAdminRequest(t, http.MethodGet, "/_synapse/admin/v1/users/@admin-user:test", "test-admin-token", "")
	rec = httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 getting admin user, got %d: %s", rec.Code, rec.Body.String())
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &userResp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if !userResp.Admin {
		t.Error("expected admin=true to be reported from the account record")
	}
}

// TestMSC3861_MASPlaceholderTokenIsRandom verifies that the placeholder device
// access token comes from the CSPRNG and is unpredictable: a guessable value
// would be a usable credential for the admin-created device.
func TestMSC3861_MASPlaceholderTokenIsRandom(t *testing.T) {
	t.Parallel()

	seen := map[string]struct{}{}
	for range 100 {
		token, err := generatePlaceholderToken()
		if err != nil {
			t.Fatalf("failed to generate placeholder token: %v", err)
		}
		// 32 random bytes hex-encoded, prefixed to mark the token's origin.
		if len(token) != len("mas_")+64 {
			t.Fatalf("unexpected placeholder token length: %q", token)
		}
		if _, dup := seen[token]; dup {
			t.Fatalf("placeholder token repeated: %q", token)
		}
		seen[token] = struct{}{}
	}
}

// TestMSC3861_MASAdminDeviceIdempotent verifies that creating an existing
// device returns it unchanged instead of clobbering it.
func TestMSC3861_MASAdminDeviceIdempotent(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	req := test_masAdminRequest(t, http.MethodPut, "/_synapse/admin/v1/users/@alice:test", "test-admin-token", `{}`)
	rec := httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 creating user, got %d: %s", rec.Code, rec.Body.String())
	}

	req = test_masAdminRequest(t, http.MethodPut, "/_synapse/admin/v1/users/@alice:test/devices/DEVICEONE", "test-admin-token", `{"display_name": "First name"}`)
	rec = httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 creating device, got %d: %s", rec.Code, rec.Body.String())
	}

	// A second PUT for the same device returns the existing device untouched.
	req = test_masAdminRequest(t, http.MethodPut, "/_synapse/admin/v1/users/@alice:test/devices/DEVICEONE", "test-admin-token", `{"display_name": "Second name"}`)
	rec = httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("expected 200 for idempotent device creation, got %d: %s", rec.Code, rec.Body.String())
	}
	var deviceResp struct {
		DeviceID    string `json:"device_id"`
		DisplayName string `json:"display_name"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &deviceResp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if deviceResp.DeviceID != "DEVICEONE" {
		t.Errorf("expected device_id DEVICEONE, got %q", deviceResp.DeviceID)
	}
	if deviceResp.DisplayName != "First name" {
		t.Errorf("expected existing display name to be preserved, got %q", deviceResp.DisplayName)
	}
}

func TestMSC3861_AuthJWKSProxy(t *testing.T) {
	t.Parallel()

	jwksBody := []byte(`{"keys":[{"kty":"RSA","kid":"test","n":"abc","e":"AQAB"}]}`)
	jwksSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openid-configuration":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"issuer":                 "http://" + r.Host,
				"jwks_uri":               "http://" + r.Host + "/.well-known/jwks.json",
				"authorization_endpoint": "http://" + r.Host + "/oauth2/auth",
				"token_endpoint":         "http://" + r.Host + "/oauth2/token",
			})
		case "/.well-known/jwks.json":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write(jwksBody)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer jwksSrv.Close()

	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cfg.ClientAPI.RateLimiting.Enabled = false
	cfg.MSCs.MSCs = []string{"msc3861"}
	cfg.MSCs.MSC3861 = config.MSC3861Config{
		Issuer:       jwksSrv.URL,
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	}
	cfg.Global.WellKnownClientName = "https://matrix.example.com"

	natsInstance := jetstream.NATSInstance{}
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)

	routers := httputil.NewRouters()
	Setup(routers, cfg, nil, nil, userAPI, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

	for _, path := range []string{
		"/_matrix/client/v1/auth_metadata/jwks",
		"/_matrix/client/unstable/org.matrix.msc2965/auth_metadata/jwks",
	} {
		t.Run(path, func(t *testing.T) {
			req := test.NewRequest(t, http.MethodGet, path)
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Errorf("expected Content-Type application/json, got %s", ct)
			}
			if !bytes.Equal(rec.Body.Bytes(), jwksBody) {
				t.Errorf("unexpected JWKS body: %s", rec.Body.String())
			}
		})
	}
}
