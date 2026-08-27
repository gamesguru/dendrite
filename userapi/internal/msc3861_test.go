// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/producers"
	"codefloe.com/pat-s/zendrite/userapi/storage"
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
	discoveredEndpointsMu.Lock()
	saved := discoveredEndpoints
	discoveredEndpoints = make(map[string]*oidcDiscoveryCache)
	discoveredEndpointsMu.Unlock()
	defer func() {
		discoveredEndpointsMu.Lock()
		discoveredEndpoints = saved
		discoveredEndpointsMu.Unlock()
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

func TestIntrospectToken_UserinfoFallback(t *testing.T) {
	t.Parallel()

	var gotUserinfo bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/introspect":
			w.WriteHeader(http.StatusNotFound)
		case "/.well-known/openid-configuration":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"userinfo_endpoint": "http://" + r.Host + "/userinfo",
			})
		case "/userinfo":
			gotUserinfo = true
			auth := r.Header.Get("Authorization")
			if auth != "Bearer fallback-token" {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"sub":                "fallback-sub",
				"preferred_username": "fallback-user",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "client-id",
		ClientSecret:          "client-secret",
		IntrospectionEndpoint: srv.URL + "/introspect",
	}

	resp, err := introspectToken(context.Background(), msc3861, "fallback-token", srv.Client())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !gotUserinfo {
		t.Fatal("expected userinfo endpoint to be called")
	}
	if !resp.Active {
		t.Error("expected token to be active")
	}
	if resp.Sub != "fallback-sub" {
		t.Errorf("expected sub fallback-sub, got %s", resp.Sub)
	}
	if resp.Username != "fallback-user" {
		t.Errorf("expected username fallback-user, got %s", resp.Username)
	}
}

func TestIntrospectToken_UserinfoExpiredToken(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/introspect":
			w.WriteHeader(http.StatusNotFound)
		case "/userinfo":
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token", error_description="Token expired"`)
			w.WriteHeader(http.StatusUnauthorized)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "client-id",
		ClientSecret:          "client-secret",
		IntrospectionEndpoint: srv.URL + "/introspect",
		UserinfoEndpoint:      srv.URL + "/userinfo",
	}

	_, err := introspectToken(context.Background(), msc3861, "expired-token", srv.Client())
	if !errors.Is(err, errTokenExpired) {
		t.Fatalf("expected errTokenExpired, got %v", err)
	}
}

func TestIntrospectToken_UserinfoInvalidTokenNotExpired(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/introspect":
			w.WriteHeader(http.StatusNotFound)
		case "/userinfo":
			w.Header().Set("WWW-Authenticate", `Bearer error="invalid_token", error_description="Token revoked"`)
			w.WriteHeader(http.StatusUnauthorized)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "client-id",
		ClientSecret:          "client-secret",
		IntrospectionEndpoint: srv.URL + "/introspect",
		UserinfoEndpoint:      srv.URL + "/userinfo",
	}

	_, err := introspectToken(context.Background(), msc3861, "revoked-token", srv.Client())
	if err == nil {
		t.Fatal("expected an error")
	}
	if errors.Is(err, errTokenExpired) {
		t.Fatalf("expected a generic error, got errTokenExpired")
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

	// A secret full of characters that url.QueryEscape would mangle. MAS,
	// Keycloak and Hydra all compare the raw value, so it must arrive verbatim.
	const rawSecret = "s3cr+t/with=chars%and spaces"
	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "my client+id",
		ClientSecret:          rawSecret,
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
	if gotUser != "my client+id" {
		t.Errorf("expected basic auth user to be sent raw, got %q", gotUser)
	}
	if gotPass != rawSecret {
		t.Errorf("expected basic auth pass to be sent raw, got %q", gotPass)
	}
}

func TestIntrospectToken_UnsupportedAuthMethod(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("no request must be made for an unsupported auth method")
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "client-id",
		ClientSecret:          "client-secret",
		ClientAuthMethod:      "private_key_jwt",
		IntrospectionEndpoint: srv.URL + "/introspect",
	}

	_, err := introspectToken(context.Background(), msc3861, "unsupported-method-token-"+t.Name(), srv.Client())
	if err == nil {
		t.Fatal("expected an error for an unsupported client_auth_method")
	}
	if !strings.Contains(err.Error(), "private_key_jwt") {
		t.Errorf("expected the error to name the auth method, got %v", err)
	}
}

func TestIntrospectToken_ClientSecretPostSurvivesRedirect(t *testing.T) {
	t.Parallel()
	// Go's http client replays the body via req.GetBody on a 307, so a form
	// that was patched onto req.Body after the request was built would arrive
	// without the client credentials.
	var gotClientID, gotClientSecret string
	var redirected bool
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/introspect" {
			redirected = true
			http.Redirect(w, r, "/introspect-moved", http.StatusTemporaryRedirect)
			return
		}
		if err := r.ParseForm(); err != nil {
			http.Error(w, "bad form", http.StatusBadRequest)
			return
		}
		gotClientID = r.FormValue("client_id")
		gotClientSecret = r.FormValue("client_secret")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{"active": true, "sub": "uuid-redirect"}) //nolint:errcheck
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "my-client-id",
		ClientSecret:          "my-client-secret",
		ClientAuthMethod:      "client_secret_post",
		IntrospectionEndpoint: srv.URL + "/introspect",
	}

	if _, err := introspectToken(context.Background(), msc3861, "redirect-token-"+t.Name(), srv.Client()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !redirected {
		t.Fatal("expected the introspection endpoint to redirect")
	}
	if gotClientID != "my-client-id" {
		t.Errorf("expected client_id to survive the redirect, got %q", gotClientID)
	}
	if gotClientSecret != "my-client-secret" {
		t.Errorf("expected client_secret to survive the redirect, got %q", gotClientSecret)
	}
}

func TestIntrospectToken_RejectedCredentialsFallBackLoudly(t *testing.T) {
	t.Parallel()
	var hook loggerHook
	logrus.AddHook(&hook)
	defer func() { logrus.StandardLogger().ReplaceHooks(make(logrus.LevelHooks)) }()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/introspect":
			// The provider rejects our client credentials, not the user token.
			w.WriteHeader(http.StatusUnauthorized)
		case "/userinfo":
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
				"sub":                "rejected-creds-sub",
				"preferred_username": "rejecteduser",
			})
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	msc3861 := &config.MSC3861Config{
		Issuer:                srv.URL,
		ClientID:              "client-id",
		ClientSecret:          "wrong-secret",
		IntrospectionEndpoint: srv.URL + "/introspect",
		UserinfoEndpoint:      srv.URL + "/userinfo",
	}

	resp, err := introspectToken(context.Background(), msc3861, "rejected-creds-token-"+t.Name(), srv.Client())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// The fallback still authenticates the user, but without any scope.
	if resp.Scope != "" {
		t.Errorf("expected no scope from the userinfo fallback, got %q", resp.Scope)
	}
	if !hook.hasErrorContaining("rejected the configured client credentials") {
		t.Errorf("expected an Error-level log about rejected client credentials, got %v", hook.entries())
	}
}

// loggerHook collects logrus entries so tests can assert on log level and
// message, which is the only observable effect of a masked misconfiguration.
type loggerHook struct {
	mu      sync.Mutex
	records []*logrus.Entry
}

func (h *loggerHook) Levels() []logrus.Level {
	return logrus.AllLevels
}

func (h *loggerHook) Fire(entry *logrus.Entry) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, entry)
	return nil
}

func (h *loggerHook) hasErrorContaining(substr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, entry := range h.records {
		if entry.Level == logrus.ErrorLevel && strings.Contains(entry.Message, substr) {
			return true
		}
	}
	return false
}

func (h *loggerHook) entries() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	messages := make([]string, 0, len(h.records))
	for _, entry := range h.records {
		messages = append(messages, entry.Level.String()+": "+entry.Message)
	}
	return messages
}

func TestExtractDeviceIDFromScope(t *testing.T) {
	t.Parallel()
	tests := []struct {
		scope    string
		expected string
	}{
		{"openid urn:matrix:client:device:ABCDEF", "ABCDEF"},
		{"openid urn:matrix:client:api:* urn:matrix:client:device:ABCDEF", "ABCDEF"},
		{"urn:matrix:client:device:MyDevice openid", "MyDevice"},
		{"urn:matrix:client:device:", ""},
		{"openid urn:matrix:device:ABCDEF", "ABCDEF"},
		{"openid urn:matrix:org.matrix.msc2967.client:device:ABCDEF", "ABCDEF"},
		{"openid urn:matrix:org.matrix.msc2967.client:api:*", ""},
		{"openid urn:matrix:client:api:*", ""},
		{"urn:matrix:device:MyDevice openid", "MyDevice"},
		{"urn:matrix:org.matrix.msc2967.client:device:MyDevice openid", "MyDevice"},
		{"", ""},
		{"urn:matrix:device:", ""},
		{"urn:matrix:org.matrix.msc2967.client:device:", ""},
	}

	for _, tc := range tests {
		got := extractDeviceIDFromScope(tc.scope)
		if got != tc.expected {
			t.Errorf("extractDeviceIDFromScope(%q) = %q, want %q", tc.scope, got, tc.expected)
		}
	}
}

func TestIsExpiredBearerToken(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name            string
		wwwAuthenticate string
		body            string
		expected        bool
	}{
		{
			name:            "header invalid_token with expiry description",
			wwwAuthenticate: `Bearer error="invalid_token", error_description="The access token expired"`,
			expected:        true,
		},
		{
			name:            "header invalid_token revoked",
			wwwAuthenticate: `Bearer error="invalid_token", error_description="Token revoked"`,
			expected:        false,
		},
		{
			name:            "header case-insensitive",
			wwwAuthenticate: `bearer Error="invalid_token", error_description="Expired"`,
			expected:        true,
		},
		{
			name:            "header invalid_token without description",
			wwwAuthenticate: `Bearer realm="example", error="invalid_token"`,
			expected:        false,
		},
		{
			name:            "header takes precedence over body (revoked in header, expired in body)",
			wwwAuthenticate: `Bearer error="invalid_token", error_description="Token revoked"`,
			body:            `{"error":"invalid_token","error_description":"expired"}`,
			expected:        false,
		},
		{
			name:            "header insufficient_scope is not an expired token",
			wwwAuthenticate: `Bearer error="insufficient_scope"`,
			body:            `{"error":"invalid_token","error_description":"expired"}`,
			expected:        false,
		},
		{
			name:     "body fallback when header absent, expired",
			body:     `{"error":"invalid_token","error_description":"The token expired"}`,
			expected: true,
		},
		{
			name:     "body fallback when header absent, revoked",
			body:     `{"error":"invalid_token","error_description":"The token was revoked"}`,
			expected: false,
		},
		{
			name:            "non-bearer challenge",
			wwwAuthenticate: `Basic realm="example"`,
			expected:        false,
		},
		{
			name:            "bare Bearer scheme without params",
			wwwAuthenticate: `Bearer`,
			expected:        false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := isExpiredBearerToken(tc.wwwAuthenticate, []byte(tc.body))
			if got != tc.expected {
				t.Errorf("isExpiredBearerToken(%q, %q) = %v, want %v", tc.wwwAuthenticate, tc.body, got, tc.expected)
			}
		})
	}
}

func TestDeriveLocalpart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name          string
		email         string
		emailVerified bool
		username      string
		sub           string
		serverName    string
		expectedPart  string
	}{
		{"email matching server domain", "alice@example.com", true, "ignored", "sub-1", "example.com", "alice"},
		{"email matching server domain case-insensitively", "alice@Example.COM", true, "ignored", "sub-1", "example.com", "alice"},
		{"email on foreign domain kept whole", "alice@other.com", true, "ignored", "sub-1", "example.com", "alice_other.com"},
		{"username when no email", "", false, "bob", "sub-1", "example.com", "bob"},
		{"sub when no email or username", "", false, "", "sub-1", "example.com", "sub-1"},
		{"verified email preferred over username", "alice@example.com", true, "bob", "sub-1", "example.com", "alice"},
		{"unverified email ignored in favor of username", "alice@example.com", false, "bob", "sub-1", "example.com", "bob"},
		// The dangerous case: an unverified, self-asserted address on our own
		// domain must not hand out a localpart in our namespace.
		{"unverified email ignored in favor of sub", "admin@example.com", false, "", "sub-1", "example.com", "sub-1"},
		{"empty everything", "", false, "", "", "example.com", ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := deriveLocalpart(&IntrospectionResponse{
				Email:         tc.email,
				EmailVerified: tc.emailVerified,
				Username:      tc.username,
				Sub:           tc.sub,
			}, tc.serverName)
			if got != tc.expectedPart {
				t.Errorf("deriveLocalpart(email=%q, email_verified=%v, username=%q, sub=%q, server=%q) = %q, want %q",
					tc.email, tc.emailVerified, tc.username, tc.sub, tc.serverName, got, tc.expectedPart)
			}
		})
	}
}

func TestIntrospectToken_EmailVerified(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		emailVerified    any
		expectedVerified bool
	}{
		{"verified email", true, true},
		{"unverified email", false, false},
		{"absent email_verified claim", nil, false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body := map[string]any{
					"active": true,
					"sub":    "opaque-uuid-email",
					"email":  "alice@example.com",
				}
				if tc.emailVerified != nil {
					body["email_verified"] = tc.emailVerified
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(body) //nolint:errcheck
			}))
			defer srv.Close()

			msc3861 := &config.MSC3861Config{
				Issuer:                srv.URL,
				ClientID:              "client-id",
				ClientSecret:          "client-secret",
				IntrospectionEndpoint: srv.URL + "/introspect",
			}

			resp, err := introspectToken(context.Background(), msc3861, "email-verified-token-"+t.Name(), srv.Client())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.EmailVerified != tc.expectedVerified {
				t.Errorf("expected EmailVerified %v, got %v", tc.expectedVerified, resp.EmailVerified)
			}
		})
	}
}

func TestIntrospectToken_UserinfoEmailVerified(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name             string
		emailVerified    bool
		expectedUsername string
		expectedVerified bool
	}{
		// The userinfo username chain falls back to the email address, which
		// would otherwise smuggle an unverified address past deriveLocalpart.
		{"verified email used as username fallback", true, "alice@example.com", true},
		{"unverified email not used as username fallback", false, "", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch r.URL.Path {
				case "/introspect":
					w.WriteHeader(http.StatusNotFound)
				case "/userinfo":
					w.Header().Set("Content-Type", "application/json")
					json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
						"sub":            "userinfo-email-sub",
						"email":          "alice@example.com",
						"email_verified": tc.emailVerified,
					})
				default:
					w.WriteHeader(http.StatusNotFound)
				}
			}))
			defer srv.Close()

			msc3861 := &config.MSC3861Config{
				Issuer:                srv.URL,
				ClientID:              "client-id",
				ClientSecret:          "client-secret",
				IntrospectionEndpoint: srv.URL + "/introspect",
				UserinfoEndpoint:      srv.URL + "/userinfo",
			}

			resp, err := introspectToken(context.Background(), msc3861, "userinfo-email-token-"+t.Name(), srv.Client())
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if resp.Username != tc.expectedUsername {
				t.Errorf("expected username %q, got %q", tc.expectedUsername, resp.Username)
			}
			if resp.EmailVerified != tc.expectedVerified {
				t.Errorf("expected EmailVerified %v, got %v", tc.expectedVerified, resp.EmailVerified)
			}
		})
	}
}

func TestDiscoverIntrospectionEndpoint(t *testing.T) {
	// Not parallel: this test relies on the package-level discovery cache.
	discoveredEndpointsMu.Lock()
	saved := discoveredEndpoints
	discoveredEndpoints = make(map[string]*oidcDiscoveryCache)
	discoveredEndpointsMu.Unlock()
	defer func() {
		discoveredEndpointsMu.Lock()
		discoveredEndpoints = saved
		discoveredEndpointsMu.Unlock()
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

func TestDiscoverIntrospectionEndpoint_PerIssuerCache(t *testing.T) {
	// Not parallel: this test relies on the package-level discovery cache.
	discoveredEndpointsMu.Lock()
	saved := discoveredEndpoints
	discoveredEndpoints = make(map[string]*oidcDiscoveryCache)
	discoveredEndpointsMu.Unlock()
	defer func() {
		discoveredEndpointsMu.Lock()
		discoveredEndpoints = saved
		discoveredEndpointsMu.Unlock()
	}()

	a := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/openid-configuration" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"introspection_endpoint": "https://a.example.com/introspect"}) //nolint:errcheck
			return
		}
		http.NotFound(w, r)
	}))
	defer a.Close()

	b := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/.well-known/openid-configuration" {
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]any{"introspection_endpoint": "https://b.example.com/introspect"}) //nolint:errcheck
			return
		}
		http.NotFound(w, r)
	}))
	defer b.Close()

	if got := discoverIntrospectionEndpoint(context.Background(), a.URL, a.Client()); got != "https://a.example.com/introspect" {
		t.Errorf("issuer A: expected https://a.example.com/introspect, got %q", got)
	}
	if got := discoverIntrospectionEndpoint(context.Background(), b.URL, b.Client()); got != "https://b.example.com/introspect" {
		t.Errorf("issuer B: expected https://b.example.com/introspect, got %q", got)
	}
	// A second lookup for issuer A must still return A's endpoint, not B's.
	if got := discoverIntrospectionEndpoint(context.Background(), a.URL, a.Client()); got != "https://a.example.com/introspect" {
		t.Errorf("issuer A cached: expected https://a.example.com/introspect, got %q", got)
	}
}

func TestSanitizeLocalpart(t *testing.T) {
	t.Parallel()
	tests := []struct {
		input    string
		expected string
	}{
		{"alice", "alice"},
		{"Alice", "alice"},
		{"alice@example.com", "alice_example.com"},
		{"Alice.Smith_42", "alice.smith_42"},
		{"_underscore", "u_underscore"},
		{"", ""},
		{"   ", ""},
		{"foo!bar", "foo_bar"},
	}
	for _, tc := range tests {
		got := sanitizeLocalpart(tc.input)
		if got != tc.expected {
			t.Errorf("sanitizeLocalpart(%q) = %q, want %q", tc.input, got, tc.expected)
		}
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

		t.Run("EmailPreferredAndDomainStripped", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":         true,
					"sub":            "01HULID000000000000000000",
					"scope":          "openid",
					"username":       "uliduser",
					"email":          "alice@test",
					"email_verified": true,
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			req := &api.QueryAccessTokenRequest{AccessToken: "email-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device for active token")
			}
			// The verified email claim wins over the username/sub, and the
			// domain is stripped because it matches the server name.
			if res.Device.UserID != "@alice:test" {
				t.Errorf("expected user ID @alice:test, got %s", res.Device.UserID)
			}
		})

		t.Run("UnverifiedEmailDoesNotClaimLocalpart", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active": true,
					"sub":    "opaque-uuid-unverified",
					"scope":  "openid",
					// Self-asserted address on our own domain, with no
					// email_verified claim to back it up.
					"email": "serveradmin@test",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			req := &api.QueryAccessTokenRequest{AccessToken: "unverified-email-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device for active token")
			}
			// The unverified email must not decide the localpart; the subject
			// claim does.
			if res.Device.UserID != "@opaque-uuid-unverified:test" {
				t.Errorf("expected user ID @opaque-uuid-unverified:test, got %s", res.Device.UserID)
			}
		})

		t.Run("ExpiredToken", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      "opaque-uuid-expired",
					"scope":    "openid",
					"username": "expireduser",
					"exp":      time.Now().Add(-time.Hour).Unix(),
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			req := &api.QueryAccessTokenRequest{AccessToken: "expired-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device != nil {
				t.Errorf("expected nil device for expired token, got %+v", res.Device)
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

		t.Run("NoUsernameFallsBackToSub", func(t *testing.T) {
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
			if res.Device == nil {
				t.Fatal("expected device when falling back to sub")
			}
			expectedUserID := "@opaque-uuid-nousername:test"
			if res.Device.UserID != expectedUserID {
				t.Errorf("expected user ID %s, got %s", expectedUserID, res.Device.UserID)
			}
		})

		t.Run("LocalpartCollisionFallsBackToSub", func(t *testing.T) {
			sub := "opaque-uuid-collision"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      sub,
					"scope":    "openid",
					"username": "alice",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			// A pre-existing (e.g. password-registered) account already owns the
			// localpart the OIDC identity would derive from its username claim.
			ctx := context.Background()
			serverName := userAPI.Config.Matrix.ServerName
			if _, err := userAPI.DB.CreateAccount(ctx, "alice", serverName, "secret", "", api.AccountTypeUser); err != nil {
				t.Fatalf("failed to create colliding account: %v", err)
			}

			req := &api.QueryAccessTokenRequest{AccessToken: "collision-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(ctx, req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device when falling back to sub-derived localpart")
			}
			// The OIDC identity must not be attached to the existing @alice:test
			// account; it falls back to the subject claim instead.
			expectedUserID := "@" + sub + ":test"
			if res.Device.UserID != expectedUserID {
				t.Errorf("expected user ID %s, got %s", expectedUserID, res.Device.UserID)
			}

			issuer := userAPI.Config.MSCs.MSC3861.Issuer
			localpart, _, err := userAPI.DB.GetLocalpartByExternalID(ctx, issuer, sub)
			if err != nil {
				t.Fatalf("failed to look up external ID: %v", err)
			}
			if localpart != sub {
				t.Errorf("expected mapping to sub-derived localpart %q, got %q", sub, localpart)
			}
		})

		t.Run("LocalpartCollisionHardFail", func(t *testing.T) {
			sub := "opaque-uuid-taken"
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      sub,
					"scope":    "openid",
					"username": "alice",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			// Both the derived localpart and the subject-derived fallback are
			// already taken by different accounts.
			ctx := context.Background()
			serverName := userAPI.Config.Matrix.ServerName
			if _, err := userAPI.DB.CreateAccount(ctx, "alice", serverName, "secret", "", api.AccountTypeUser); err != nil {
				t.Fatalf("failed to create colliding account: %v", err)
			}
			if _, err := userAPI.DB.CreateAccount(ctx, sub, serverName, "secret", "", api.AccountTypeUser); err != nil {
				t.Fatalf("failed to create colliding sub account: %v", err)
			}

			req := &api.QueryAccessTokenRequest{AccessToken: "hardfail-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(ctx, req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device != nil {
				t.Errorf("expected nil device when all localparts collide, got %+v", res.Device)
			}

			// No mapping must have been persisted.
			issuer := userAPI.Config.MSCs.MSC3861.Issuer
			localpart, _, err := userAPI.DB.GetLocalpartByExternalID(ctx, issuer, sub)
			if err != nil {
				t.Fatalf("failed to look up external ID: %v", err)
			}
			if localpart != "" {
				t.Errorf("expected no external ID mapping, got %q", localpart)
			}
		})

		t.Run("DeactivatedAccountRejected", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      "opaque-uuid-deact",
					"scope":    "openid",
					"username": "deactuser",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			ctx := context.Background()
			req := &api.QueryAccessTokenRequest{AccessToken: "deact-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(ctx, req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device on first login")
			}

			serverName := userAPI.Config.Matrix.ServerName
			if err := userAPI.DB.DeactivateAccount(ctx, "deactuser", serverName); err != nil {
				t.Fatalf("failed to deactivate account: %v", err)
			}

			res2 := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(ctx, req, res2); err != nil {
				t.Fatalf("unexpected error on second call: %v", err)
			}
			if res2.Device != nil {
				t.Errorf("expected nil device for deactivated account, got %+v", res2.Device)
			}
		})

		t.Run("DeviceNotRecreatedForSameToken", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      "opaque-uuid-session",
					"scope":    "openid urn:matrix:org.matrix.msc2967.client:device:MYDEV",
					"username": "sessionuser",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			ctx := context.Background()
			req := &api.QueryAccessTokenRequest{AccessToken: "session-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(ctx, req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device on first login")
			}
			if res.Device.ID != "MYDEV" {
				t.Errorf("expected device ID MYDEV, got %s", res.Device.ID)
			}

			// Give the device a display name; it must survive subsequent
			// token validations.
			serverName := userAPI.Config.Matrix.ServerName
			displayName := "My Device"
			if err := userAPI.DB.UpdateDevice(ctx, "sessionuser", serverName, "MYDEV", &displayName); err != nil {
				t.Fatalf("failed to update device display name: %v", err)
			}

			res2 := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(ctx, req, res2); err != nil {
				t.Fatalf("unexpected error on second call: %v", err)
			}
			if res2.Device == nil {
				t.Fatal("expected device on second call")
			}
			if res2.Device.SessionID != res.Device.SessionID {
				t.Errorf("expected session ID to be preserved (%d), got %d", res.Device.SessionID, res2.Device.SessionID)
			}
			if res2.Device.DisplayName != displayName {
				t.Errorf("expected display name %q to be preserved, got %q", displayName, res2.Device.DisplayName)
			}

			stored, err := userAPI.DB.GetDeviceByID(ctx, "sessionuser", serverName, "MYDEV")
			if err != nil {
				t.Fatalf("failed to look up device: %v", err)
			}
			if stored.DisplayName != displayName {
				t.Errorf("expected stored display name %q, got %q", displayName, stored.DisplayName)
			}
		})

		t.Run("DeviceReusedAcrossTokenRotation", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      "opaque-uuid-rotation",
					"scope":    "openid urn:matrix:org.matrix.msc2967.client:device:ROTDEV",
					"username": "rotationuser",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			ctx := context.Background()
			serverName := userAPI.Config.Matrix.ServerName

			res := &api.QueryAccessTokenResponse{}
			first := &api.QueryAccessTokenRequest{AccessToken: "rotation-token-1-" + t.Name()}
			if err := userAPI.queryAccessTokenMSC3861(ctx, first, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device on first login")
			}

			displayName := "Rotating Device"
			if err := userAPI.DB.UpdateDevice(ctx, "rotationuser", serverName, "ROTDEV", &displayName); err != nil {
				t.Fatalf("failed to update device display name: %v", err)
			}

			// The provider refreshed the access token. The device is the same,
			// so the row must be updated in place rather than recreated.
			res2 := &api.QueryAccessTokenResponse{}
			second := &api.QueryAccessTokenRequest{AccessToken: "rotation-token-2-" + t.Name()}
			if err := userAPI.queryAccessTokenMSC3861(ctx, second, res2); err != nil {
				t.Fatalf("unexpected error after token rotation: %v", err)
			}
			if res2.Device == nil {
				t.Fatal("expected device after token rotation")
			}
			if res2.Device.AccessToken != second.AccessToken {
				t.Errorf("expected the rotated access token %q, got %q", second.AccessToken, res2.Device.AccessToken)
			}
			if res2.Device.SessionID != res.Device.SessionID {
				t.Errorf("expected session ID %d to survive rotation, got %d", res.Device.SessionID, res2.Device.SessionID)
			}
			if res2.Device.DisplayName != displayName {
				t.Errorf("expected display name %q to survive rotation, got %q", displayName, res2.Device.DisplayName)
			}

			stored, err := userAPI.DB.GetDeviceByID(ctx, "rotationuser", serverName, "ROTDEV")
			if err != nil {
				t.Fatalf("failed to look up device: %v", err)
			}
			if stored.DisplayName != displayName {
				t.Errorf("expected stored display name %q, got %q", displayName, stored.DisplayName)
			}

			// The superseded token must no longer resolve to a device.
			if _, err := userAPI.DB.GetDeviceByAccessToken(ctx, first.AccessToken); !errors.Is(err, sql.ErrNoRows) {
				t.Errorf("expected the old access token to be revoked, got %v", err)
			}
		})

		t.Run("TokenHeldByAnotherDeviceIsRevoked", func(t *testing.T) {
			// The device scope only appears on the second call, so the same
			// token is first bound to the default "OIDC" device row and then
			// has to move to the scoped one.
			var withDeviceScope bool
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				scope := "openid"
				if withDeviceScope {
					scope += " urn:matrix:org.matrix.msc2967.client:device:MOVEDDEV"
				}
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					"active":   true,
					"sub":      "opaque-uuid-moved",
					"scope":    scope,
					"username": "moveduser",
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			ctx := context.Background()
			serverName := userAPI.Config.Matrix.ServerName
			req := &api.QueryAccessTokenRequest{AccessToken: "moved-token-" + t.Name()}

			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(ctx, req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device == nil || res.Device.ID != defaultOIDCDeviceID {
				t.Fatalf("expected the default OIDC device on first login, got %+v", res.Device)
			}

			// Create the scoped device row so the token has to be moved off the
			// default row rather than simply inserted.
			newDeviceID := "MOVEDDEV"
			if _, err := userAPI.DB.CreateDevice(ctx, "moveduser", serverName, &newDeviceID, "other-token-"+t.Name(), nil, "", ""); err != nil {
				t.Fatalf("failed to create scoped device: %v", err)
			}

			withDeviceScope = true
			// Introspection results are cached by token hash, so drop the entry.
			getIntrospectionCache().Delete(hashToken(req.AccessToken))

			res2 := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(ctx, req, res2); err != nil {
				t.Fatalf("unexpected error on second call: %v", err)
			}
			if res2.Device == nil {
				t.Fatal("expected device on second call")
			}
			if res2.Device.ID != newDeviceID {
				t.Errorf("expected device ID %s, got %s", newDeviceID, res2.Device.ID)
			}
			// The stale row that used to hold the token must be gone.
			if _, err := userAPI.DB.GetDeviceByID(ctx, "moveduser", serverName, defaultOIDCDeviceID); !errors.Is(err, sql.ErrNoRows) {
				t.Errorf("expected the stale OIDC device to be revoked, got %v", err)
			}
		})

		t.Run("ExpiredActiveFalseSetsSoftLogout", func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				json.NewEncoder(w).Encode(map[string]any{ //nolint:errcheck
					// Providers following RFC 7662 answer active:false for
					// expired tokens but still return the exp claim.
					"active": false,
					"exp":    time.Now().Add(-time.Hour).Unix(),
				})
			}))
			defer srv.Close()

			userAPI, closeDB := makeTestUserAPI(t, dbType, srv)
			defer closeDB()

			req := &api.QueryAccessTokenRequest{AccessToken: "expired-inactive-token-" + t.Name()}
			res := &api.QueryAccessTokenResponse{}
			if err := userAPI.queryAccessTokenMSC3861(context.Background(), req, res); err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if res.Device != nil {
				t.Errorf("expected nil device for expired token, got %+v", res.Device)
			}
			if !res.SoftLogout {
				t.Error("expected SoftLogout for expired token with active:false")
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
