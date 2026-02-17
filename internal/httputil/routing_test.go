package httputil

import (
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
)

func TestNormalisePattern(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		expected string
	}{
		{
			name:     "no variables",
			pattern:  "/rooms/state",
			expected: "/rooms/state",
		},
		{
			name:     "variable without regex",
			pattern:  "/rooms/{roomID}/state",
			expected: "/rooms/{roomID}/state",
		},
		{
			name:     "variable with regex stripped",
			pattern:  "/rooms/{roomID}/state/{eventType:[^/]+}",
			expected: "/rooms/{roomID}/state/{eventType}",
		},
		{
			name:     "multiple variables with regex",
			pattern:  "/{scope:[^/]+}/{kind:[^/]+}",
			expected: "/{scope}/{kind}",
		},
		{
			name:     "wildcard variable converted",
			pattern:  "/static/{path...}",
			expected: "/static/{path:*}",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := normalisePattern(tc.pattern)
			if got != tc.expected {
				t.Errorf("expected %q, got %q", tc.expected, got)
			}
		})
	}
}

func TestExpandPattern(t *testing.T) {
	tests := []struct {
		name     string
		pattern  string
		expected []string
	}{
		{
			name:     "simple pattern unchanged",
			pattern:  "/rooms/{roomID}/state",
			expected: []string{"/rooms/{roomID}/state"},
		},
		{
			name:     "regex stripped",
			pattern:  "/rooms/{roomID}/state/{eventType:[^/]+}",
			expected: []string{"/rooms/{roomID}/state/{eventType}"},
		},
		{
			name:     "optional trailing slash produces two patterns",
			pattern:  "/rooms/{roomID}/state/{eventType:[^/]+/?}",
			expected: []string{"/rooms/{roomID}/state/{eventType}", "/rooms/{roomID}/state/{eventType}/"},
		},
		{
			name:     "alternatives produce multiple patterns",
			pattern:  "/{apiversion:(?:r0|v3)}/rooms/{roomID}",
			expected: []string{"/r0/rooms/{roomID}", "/v3/rooms/{roomID}"},
		},
		{
			name:     "pushrules scope pattern",
			pattern:  "/pushrules/{scope:[^/]+/?}",
			expected: []string{"/pushrules/{scope}", "/pushrules/{scope}/"},
		},
		{
			name:     "pushrules scope and kind pattern",
			pattern:  "/pushrules/{scope}/{kind:[^/]+/?}",
			expected: []string{"/pushrules/{scope}/{kind}", "/pushrules/{scope}/{kind}/"},
		},
		{
			name:     "alternative combined with other variables",
			pattern:  "/{path:(?:account/3pid|register)}/email/requestToken",
			expected: []string{"/account/3pid/email/requestToken", "/register/email/requestToken"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := expandPattern(tc.pattern)
			if len(got) != len(tc.expected) {
				t.Fatalf("expected %d patterns, got %d: %v", len(tc.expected), len(got), got)
			}
			for i := range tc.expected {
				if got[i] != tc.expected[i] {
					t.Errorf("pattern[%d]: expected %q, got %q", i, tc.expected[i], got[i])
				}
			}
		})
	}
}

func TestOptionalTrailingSlashRouting(t *testing.T) {
	router := NewRouter("/_matrix/client")
	var capturedVars map[string]string
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		capturedVars = Vars(r)
		w.WriteHeader(http.StatusOK)
	})

	// Register routes matching the real Dendrite patterns
	router.Handle("/rooms/{roomID}/state/{eventType:[^/]+/?}", handler).
		Methods(http.MethodGet, http.MethodPut, http.MethodOptions)
	router.Handle("/rooms/{roomID}/state/{eventType}/{stateKey}", handler).
		Methods(http.MethodGet, http.MethodPut, http.MethodOptions)
	router.Handle("/pushrules/{scope:[^/]+/?}", handler).
		Methods(http.MethodGet, http.MethodOptions)
	router.Handle("/pushrules/{scope}/{kind:[^/]+/?}", handler).
		Methods(http.MethodGet, http.MethodOptions)

	tests := []struct {
		name           string
		method         string
		path           string
		expectedStatus int
		expectedVars   map[string]string
	}{
		// Room state without trailing slash
		{
			name:           "PUT room state without trailing slash",
			method:         http.MethodPut,
			path:           "/_matrix/client/rooms/!foo:bar/state/m.room.avatar",
			expectedStatus: http.StatusOK,
			expectedVars:   map[string]string{"roomID": "!foo:bar", "eventType": "m.room.avatar"},
		},
		// Room state with trailing slash
		{
			name:           "PUT room state with trailing slash",
			method:         http.MethodPut,
			path:           "/_matrix/client/rooms/!foo:bar/state/m.room.avatar/",
			expectedStatus: http.StatusOK,
			expectedVars:   map[string]string{"roomID": "!foo:bar", "eventType": "m.room.avatar"},
		},
		// GET room state without trailing slash
		{
			name:           "GET room state without trailing slash",
			method:         http.MethodGet,
			path:           "/_matrix/client/rooms/!foo:bar/state/m.room.pinned_events",
			expectedStatus: http.StatusOK,
			expectedVars:   map[string]string{"roomID": "!foo:bar", "eventType": "m.room.pinned_events"},
		},
		// GET room state with trailing slash
		{
			name:           "GET room state with trailing slash",
			method:         http.MethodGet,
			path:           "/_matrix/client/rooms/!foo:bar/state/m.room.pinned_events/",
			expectedStatus: http.StatusOK,
			expectedVars:   map[string]string{"roomID": "!foo:bar", "eventType": "m.room.pinned_events"},
		},
		// OPTIONS (CORS preflight) with trailing slash
		{
			name:           "OPTIONS room state with trailing slash",
			method:         http.MethodOptions,
			path:           "/_matrix/client/rooms/!foo:bar/state/m.room.join_rules/",
			expectedStatus: http.StatusOK,
			expectedVars:   map[string]string{"roomID": "!foo:bar", "eventType": "m.room.join_rules"},
		},
		// Room state with explicit state key (should not match trailing-slash route)
		{
			name:           "GET room state with state key",
			method:         http.MethodGet,
			path:           "/_matrix/client/rooms/!foo:bar/state/m.room.member/@user:bar",
			expectedStatus: http.StatusOK,
			expectedVars:   map[string]string{"roomID": "!foo:bar", "eventType": "m.room.member", "stateKey": "@user:bar"},
		},
		// PUT room state with explicit state key
		{
			name:           "PUT room state with state key",
			method:         http.MethodPut,
			path:           "/_matrix/client/rooms/!foo:bar/state/m.room.power_levels/",
			expectedStatus: http.StatusOK,
			expectedVars:   map[string]string{"roomID": "!foo:bar", "eventType": "m.room.power_levels"},
		},
		// Pushrules scope without trailing slash
		{
			name:           "GET pushrules scope without trailing slash",
			method:         http.MethodGet,
			path:           "/_matrix/client/pushrules/global",
			expectedStatus: http.StatusOK,
			expectedVars:   map[string]string{"scope": "global"},
		},
		// Pushrules scope with trailing slash
		{
			name:           "GET pushrules scope with trailing slash",
			method:         http.MethodGet,
			path:           "/_matrix/client/pushrules/global/",
			expectedStatus: http.StatusOK,
			expectedVars:   map[string]string{"scope": "global"},
		},
		// Pushrules scope and kind without trailing slash
		{
			name:           "GET pushrules kind without trailing slash",
			method:         http.MethodGet,
			path:           "/_matrix/client/pushrules/global/override",
			expectedStatus: http.StatusOK,
			expectedVars:   map[string]string{"scope": "global", "kind": "override"},
		},
		// Pushrules scope and kind with trailing slash
		{
			name:           "GET pushrules kind with trailing slash",
			method:         http.MethodGet,
			path:           "/_matrix/client/pushrules/global/override/",
			expectedStatus: http.StatusOK,
			expectedVars:   map[string]string{"scope": "global", "kind": "override"},
		},
		// Unregistered method should 405
		{
			name:           "POST room state returns 405",
			method:         http.MethodPost,
			path:           "/_matrix/client/rooms/!foo:bar/state/m.room.avatar",
			expectedStatus: http.StatusMethodNotAllowed,
		},
		// Completely unknown path should 404
		{
			name:           "unknown path returns 404",
			method:         http.MethodGet,
			path:           "/_matrix/client/nonexistent",
			expectedStatus: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			capturedVars = nil
			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			router.ServeHTTP(rec, req)
			if rec.Code != tc.expectedStatus {
				t.Fatalf("expected status %d, got %d (body: %s)", tc.expectedStatus, rec.Code, rec.Body.String())
			}
			if tc.expectedVars != nil {
				if capturedVars == nil {
					t.Fatal("handler was not called, no vars captured")
				}
				for key, expected := range tc.expectedVars {
					if got := capturedVars[key]; got != expected {
						t.Errorf("var %q: expected %q, got %q", key, expected, got)
					}
				}
			}
		})
	}
}

func TestURLDecodeMapValues(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]string
		expected map[string]string
		wantErr  bool
	}{
		{
			name:     "plain values unchanged",
			input:    map[string]string{"roomID": "!foo:bar", "eventType": "m.room.avatar"},
			expected: map[string]string{"roomID": "!foo:bar", "eventType": "m.room.avatar"},
		},
		{
			name:     "percent-encoded colon decoded",
			input:    map[string]string{"roomID": "!foo%3Abar"},
			expected: map[string]string{"roomID": "!foo:bar"},
		},
		{
			name:     "percent-encoded room ID",
			input:    map[string]string{"roomID": "%21abc%3Aexample.com"},
			expected: map[string]string{"roomID": "!abc:example.com"},
		},
		{
			name:    "invalid encoding returns error",
			input:   map[string]string{"bad": "%zz"},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := URLDecodeMapValues(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			for key, expected := range tc.expected {
				if got[key] != expected {
					t.Errorf("key %q: expected %q, got %q", key, expected, got[key])
				}
			}
		})
	}
}

func TestRoutersError(t *testing.T) {
	r := NewRouters()

	// not found test
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, filepath.Join(PublicFederationPathPrefix, "doesnotexist"), nil)
	r.Federation.ServeHTTP(rec, req)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("unexpected status code: %d - %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Result().Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("unexpected content-type: %s", ct)
	}

	// not allowed test
	r.DendriteAdmin.
		Handle("/test", http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {})).
		Methods(http.MethodPost)

	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, filepath.Join(DendriteAdminPathPrefix, "test"), nil)
	r.DendriteAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("unexpected status code: %d - %s", rec.Code, rec.Body.String())
	}
	if ct := rec.Result().Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("unexpected content-type: %s", ct)
	}
}
