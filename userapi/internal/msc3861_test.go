// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"codefloe.com/pat-s/dendrite/internal/caching"
	"codefloe.com/pat-s/dendrite/internal/sqlutil"
	"codefloe.com/pat-s/dendrite/roomserver"
	"codefloe.com/pat-s/dendrite/setup/config"
	"codefloe.com/pat-s/dendrite/setup/jetstream"
	"codefloe.com/pat-s/dendrite/test"
	"codefloe.com/pat-s/dendrite/test/testrig"
	"codefloe.com/pat-s/dendrite/userapi/api"
	"codefloe.com/pat-s/dendrite/userapi/producers"
	"codefloe.com/pat-s/dendrite/userapi/storage"
)

func TestIntrospectToken_Active(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
			"active":   true,
			"sub":      "opaque-uuid-alice",
			"scope":    "openid urn:matrix:org.matrix.msc2967.client:api:*",
			"username": "alice",
		})
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "client-id",
		ClientSecret:          "client-secret",
		IntrospectionEndpoint: srv.URL + "/introspect",
	}

	resp, err := introspectToken(context.Background(), msc3861, "test-token-active-"+t.Name(), srv.Client())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !resp.Active {
		t.Error("expected token to be active")
	}
	if resp.Sub != "opaque-uuid-alice" {
		t.Errorf("expected sub opaque-uuid-alice, got %s", resp.Sub)
	}
	if resp.Username != "alice" {
		t.Errorf("expected username alice, got %s", resp.Username)
	}
}

func TestIntrospectToken_Inactive(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"active": false}) //nolint:errcheck
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "client-id",
		ClientSecret:          "client-secret",
		IntrospectionEndpoint: srv.URL + "/introspect",
	}

	resp, err := introspectToken(context.Background(), msc3861, "bad-token-"+t.Name(), srv.Client())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Active {
		t.Error("expected token to be inactive")
	}
}

func TestIntrospectToken_ClientSecretPost(t *testing.T) {
	t.Parallel()
	var gotClientID, gotClientSecret string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		gotClientID = r.FormValue("client_id")
		gotClientSecret = r.FormValue("client_secret")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"active": true, "sub": "uuid-alice", "username": "alice"}) //nolint:errcheck
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "my-client-id",
		ClientSecret:          "my-client-secret",
		ClientAuthMethod:      "client_secret_post",
		IntrospectionEndpoint: srv.URL + "/introspect",
	}

	_, err := introspectToken(context.Background(), msc3861, "test-token-post-"+t.Name(), srv.Client())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if gotClientID != "my-client-id" {
		t.Errorf("expected client_id in form body, got %q", gotClientID)
	}
	if gotClientSecret != "my-client-secret" {
		t.Errorf("expected client_secret in form body, got %q", gotClientSecret)
	}
}

func TestIntrospectToken_ServerError(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "internal error", http.StatusInternalServerError)
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "client-id",
		ClientSecret:          "client-secret",
		IntrospectionEndpoint: srv.URL + "/introspect",
	}

	_, err := introspectToken(context.Background(), msc3861, "test-token-err-"+t.Name(), srv.Client())
	if err == nil {
		t.Fatal("expected error for 500 response")
	}
}

func TestIntrospectToken_DefaultEndpoint(t *testing.T) {
	// Not parallel: this test relies on the package-level discovery cache.
	discoveryMu.Lock()
	savedEndpoint := discoveredEndpoint
	savedTime := discoveredEndpointTime
	discoveredEndpoint = ""
	discoveredEndpointTime = time.Time{}
	discoveryMu.Unlock()
	defer func() {
		discoveryMu.Lock()
		discoveredEndpoint = savedEndpoint
		discoveredEndpointTime = savedTime
		discoveryMu.Unlock()
	}()

	var requestedPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"active": false}) //nolint:errcheck
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:       srv.URL,
		ClientID:     "client-id",
		ClientSecret: "client-secret",
		// IntrospectionEndpoint left empty to test default
	}

	_, err := introspectToken(context.Background(), msc3861, "test-token-default-"+t.Name(), srv.Client())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// Either the OIDC discovery or fallback path should be used.
	// Since the discovery will fail (test server doesn't serve /.well-known), it falls back.
	if requestedPath != "/oauth2/introspect" && requestedPath != "/.well-known/openid-configuration" {
		t.Errorf("expected default path /oauth2/introspect or discovery path, got %s", requestedPath)
	}
}

func TestIntrospectToken_BasicAuth(t *testing.T) {
	t.Parallel()
	var gotUser, gotPass string
	var gotBasicAuth bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotUser, gotPass, gotBasicAuth = r.BasicAuth()
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"active": false}) //nolint:errcheck
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "my-client",
		ClientSecret:          "my-secret",
		IntrospectionEndpoint: srv.URL + "/introspect",
		// Default auth method is client_secret_basic
	}

	_, err := introspectToken(context.Background(), msc3861, "test-token-basic-"+t.Name(), srv.Client())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gotBasicAuth {
		t.Fatal("expected Basic auth header")
	}
	// The values are URL-encoded in SetBasicAuth, so decode them
	decodedUser, _ := url.QueryUnescape(gotUser)
	decodedPass, _ := url.QueryUnescape(gotPass)
	if decodedUser != "my-client" {
		t.Errorf("expected basic auth user my-client, got %q", decodedUser)
	}
	if decodedPass != "my-secret" {
		t.Errorf("expected basic auth pass my-secret, got %q", decodedPass)
	}
}

func TestExtractDeviceIDFromScope(t *testing.T) {
	t.Parallel()
	tests := []struct {
		scope    string
		expected string
	}{
		{"openid urn:matrix:org.matrix.msc2967.client:device:ABCDEF", "ABCDEF"},
		{"openid urn:matrix:org.matrix.msc2967.client:api:*", ""},
		{"urn:matrix:org.matrix.msc2967.client:device:MyDevice openid", "MyDevice"},
		{"", ""},
		{"urn:matrix:org.matrix.msc2967.client:device:", ""},
	}

	for _, tc := range tests {
		got := extractDeviceIDFromScope(tc.scope)
		if got != tc.expected {
			t.Errorf("extractDeviceIDFromScope(%q) = %q, want %q", tc.scope, got, tc.expected)
		}
	}
}

func TestDiscoverIntrospectionEndpoint(t *testing.T) {
	// Not parallel: this test relies on the package-level discovery cache.
	discoveryMu.Lock()
	savedEndpoint := discoveredEndpoint
	savedTime := discoveredEndpointTime
	discoveredEndpoint = ""
	discoveredEndpointTime = time.Time{}
	discoveryMu.Unlock()
	defer func() {
		discoveryMu.Lock()
		discoveredEndpoint = savedEndpoint
		discoveredEndpointTime = savedTime
		discoveryMu.Unlock()
	}()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/openid-configuration" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"introspection_endpoint": "https://auth.example.com/oauth2/introspect",
			})
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	endpoint := discoverIntrospectionEndpoint(context.Background(), srv.URL, srv.Client())
	if endpoint != "https://auth.example.com/oauth2/introspect" {
		t.Errorf("expected discovered endpoint, got %q", endpoint)
	}
}

// TestQueryAccessTokenMSC3861 tests the queryAccessTokenMSC3861 method with various scenarios,
// following the WithAllDatabases pattern from existing tests.
func TestQueryAccessTokenMSC3861(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		t.Run("AdminToken", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				t.Fatal("introspection should not be called for admin token")
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()
			userAPI.Config.MSCs.MSC3861.AdminToken = "super-secret-admin"

			req := &api.QueryAccessTokenRequest{AccessToken: "super-secret-admin"}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device for admin token")
			}
			if res.Device.AccountType != api.AccountTypeAdmin {
				t.Errorf("expected admin account type, got %d", res.Device.AccountType)
			}
			expectedUserID := fmt.Sprintf("@admin:%s", userAPI.Config.Matrix.ServerName)
			if res.Device.UserID != expectedUserID {
				t.Errorf("expected user ID %s, got %s", expectedUserID, res.Device.UserID)
			}
		})

		t.Run("InactiveToken", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{"active": false}) //nolint:errcheck
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			req := &api.QueryAccessTokenRequest{AccessToken: "inactive-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device != nil {
				t.Errorf("expected nil device for inactive token, got %+v", res.Device)
			}
		})

		t.Run("ActiveToken", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      "opaque-uuid-alice",
					"scope":    "openid",
					"username": "alice",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			req := &api.QueryAccessTokenRequest{AccessToken: "valid-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device for active token")
			}
			if res.Device.UserID != "@alice:test" {
				t.Errorf("expected user ID @alice:test, got %s", res.Device.UserID)
			}
			if res.Device.AccountType != api.AccountTypeUser {
				t.Errorf("expected user account type, got %d", res.Device.AccountType)
			}
		})

		t.Run("AdminScope", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      "opaque-uuid-adminuser",
					"scope":    "openid urn:synapse:admin:*",
					"username": "adminuser",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			req := &api.QueryAccessTokenRequest{AccessToken: "admin-scope-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device for admin-scoped token")
			}
			if res.Device.AccountType != api.AccountTypeAdmin {
				t.Errorf("expected admin account type, got %d", res.Device.AccountType)
			}
		})

		t.Run("AutoProvision", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      "opaque-uuid-newuser",
					"scope":    "openid",
					"username": "newuser",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			// First call should auto-provision.
			req := &api.QueryAccessTokenRequest{AccessToken: "new-user-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device after auto-provisioning")
			}
			if res.Device.UserID != "@newuser:test" {
				t.Errorf("expected user ID @newuser:test, got %s", res.Device.UserID)
			}

			// Second call with same user should also succeed (ConflictUpdate).
			res2 := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res2); err != nil {
				t.Fatalf("unexpected error on second call: %v", err)
			}
			if res2.Device == nil {
				t.Fatal("expected device on second call")
			}
		})

		t.Run("DeviceIDFromScope", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      "opaque-uuid-devicetest",
					"scope":    "openid urn:matrix:org.matrix.msc2967.client:device:MYDEVICE",
					"username": "devicetest",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			req := &api.QueryAccessTokenRequest{AccessToken: "device-scope-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device")
			}
			if res.Device.ID != "MYDEVICE" {
				t.Errorf("expected device ID MYDEVICE, got %s", res.Device.ID)
			}
		})

		t.Run("ExternalIDMappingPersists", func(t *testing.T) {
			sub := "opaque-uuid-persist-test"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      sub,
					"scope":    "openid",
					"username": "persistuser",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			// First call creates the external ID mapping.
			req := &api.QueryAccessTokenRequest{AccessToken: "persist-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device")
			}

			// Verify the external ID mapping was created.
			issuer := userAPI.Config.MSCs.MSC3861.Issuer
			localpart, _, err := userAPI.DB.GetLocalpartByExternalID(context.Background(), issuer, sub)
			if err != nil {
				t.Fatalf("failed to look up external ID: %v", err)
			}
			if localpart != "persistuser" {
				t.Errorf("expected localpart persistuser, got %q", localpart)
			}
		})

		t.Run("EmptySub", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      "",
					"scope":    "openid",
					"username": "nosubuser",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			req := &api.QueryAccessTokenRequest{AccessToken: "empty-sub-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device != nil {
				t.Errorf("expected nil device for empty sub, got %+v", res.Device)
			}
		})

		t.Run("NoUsername", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active": true,
					"sub":    "opaque-uuid-nousername",
					"scope":  "openid",
					// No "username" field
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			req := &api.QueryAccessTokenRequest{AccessToken: "no-username-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device != nil {
				t.Errorf("expected nil device when no username, got %+v", res.Device)
			}
		})
	})
}

// makeTestUserAPI creates a UserInternalAPI backed by a real database with a mock
// introspection server for MSC3861 tests.
func makeTestUserAPI(t *testing.T, dbType test.DBType, introspectionServer *httptest.Server) (*UserInternalAPI, func()) {
	t.Helper()
	cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
	cfg.MSCs.MSCs = []string{"msc3861"}
	cfg.MSCs.MSC3861 = config.MSC3861Config{
		Issuer:                introspectionServer.URL,
		ClientID:              "client-id",
		ClientSecret:          "client-secret",
		IntrospectionEndpoint: introspectionServer.URL + "/introspect",
	}
	cfg.UserAPI.MSCs = &cfg.MSCs

	natsInstance := jetstream.NATSInstance{}
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)

	js, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)

	db, err := storage.NewUserDatabase(
		processCtx.Context(),
		cm,
		&cfg.UserAPI.AccountDatabase,
		cfg.Global.ServerName,
		cfg.UserAPI.BCryptCost,
		cfg.UserAPI.OpenIDTokenLifetimeMS,
		api.DefaultLoginTokenLifetime,
		cfg.UserAPI.Matrix.ServerNotices.LocalPart,
	)
	if err != nil {
		t.Fatalf("failed to create user DB: %v", err)
	}

	syncProducer := producers.NewSyncAPI(
		db, js,
		cfg.Global.JetStream.Prefixed(jetstream.OutputClientData),
		cfg.Global.JetStream.Prefixed(jetstream.OutputNotificationData),
	)

	userAPI := &UserInternalAPI{
		DB:           db,
		Config:       &cfg.UserAPI,
		RSAPI:        rsAPI,
		SyncProducer: syncProducer,
		HTTPClient:   introspectionServer.Client(),
	}

	return userAPI, closeDB
}
