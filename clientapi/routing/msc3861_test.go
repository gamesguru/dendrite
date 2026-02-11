// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/dendrite/internal/caching"
	"codefloe.com/pat-s/dendrite/internal/httputil"
	"codefloe.com/pat-s/dendrite/internal/sqlutil"
	"codefloe.com/pat-s/dendrite/roomserver"
	"codefloe.com/pat-s/dendrite/setup/config"
	"codefloe.com/pat-s/dendrite/setup/jetstream"
	"codefloe.com/pat-s/dendrite/test"
	"codefloe.com/pat-s/dendrite/test/testrig"
	"codefloe.com/pat-s/dendrite/userapi"
)

// setupMSC3861Routers creates a full router stack with MSC3861 enabled for testing.
// These tests only verify HTTP routing behavior (status codes, response bodies),
// not database semantics, so we always use SQLite to avoid the shared-postgres
// schema race that occurs when multiple parallel subtests DROP SCHEMA on the
// same CI database.
func setupMSC3861Routers(t *testing.T) (httputil.Routers, *config.Dendrite, func()) {
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
		{"password", http.MethodPost, "/_matrix/client/v3/account/password"},
		{"deactivate", http.MethodPost, "/_matrix/client/v3/account/deactivate"},
		{"3pid POST", http.MethodPost, "/_matrix/client/v3/account/3pid"},
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

	var resp struct {
		Flows []map[string]string `json:"flows"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if len(resp.Flows) != 1 {
		t.Fatalf("expected 1 flow, got %d: %+v", len(resp.Flows), resp.Flows)
	}
	if resp.Flows[0]["type"] != "m.login.sso" {
		t.Errorf("expected m.login.sso flow, got %s", resp.Flows[0]["type"])
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
