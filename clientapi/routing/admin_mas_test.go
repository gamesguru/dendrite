// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestVerifyMASAdminToken(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		token      string
		authHeader string
		wantCode   int
	}{
		{
			name:       "valid token",
			token:      "secret",
			authHeader: "Bearer secret",
			wantCode:   0, // nil response = success
		},
		{
			name:       "empty admin token config",
			token:      "",
			authHeader: "Bearer anything",
			wantCode:   http.StatusForbidden,
		},
		{
			name:       "missing auth header",
			token:      "secret",
			authHeader: "",
			wantCode:   http.StatusUnauthorized,
		},
		{
			name:       "invalid auth header format",
			token:      "secret",
			authHeader: "Basic dXNlcjpwYXNz",
			wantCode:   http.StatusUnauthorized,
		},
		{
			name:       "wrong token",
			token:      "secret",
			authHeader: "Bearer wrong",
			wantCode:   http.StatusForbidden,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			if tc.authHeader != "" {
				req.Header.Set("Authorization", tc.authHeader)
			}
			res := verifyMASAdminToken(req, tc.token)
			if tc.wantCode == 0 {
				if res != nil {
					t.Errorf("expected nil response (success), got %d", res.Code)
				}
			} else {
				if res == nil {
					t.Fatal("expected error response, got nil")
				}
				if res.Code != tc.wantCode {
					t.Errorf("expected status %d, got %d", tc.wantCode, res.Code)
				}
			}
		})
	}
}

func TestMASAdminUsernameAvailable(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	t.Run("missing username param", func(t *testing.T) {
		req := test_masAdminRequest(t, http.MethodGet, "/_synapse/admin/v1/username_available", "test-admin-token", "")
		rec := httptest.NewRecorder()
		routers.SynapseAdmin.ServeHTTP(rec, req)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("available username", func(t *testing.T) {
		req := test_masAdminRequest(t, http.MethodGet, "/_synapse/admin/v1/username_available?username=newuser123", "test-admin-token", "")
		rec := httptest.NewRecorder()
		routers.SynapseAdmin.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		var body struct {
			Available bool `json:"available"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if !body.Available {
			t.Error("expected username to be available")
		}
	})

	t.Run("unauthorized", func(t *testing.T) {
		req := test_masAdminRequest(t, http.MethodGet, "/_synapse/admin/v1/username_available?username=test", "wrong-token", "")
		rec := httptest.NewRecorder()
		routers.SynapseAdmin.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("expected 403, got %d: %s", rec.Code, rec.Body.String())
		}
	})
}

func TestMASAdminCreateUser(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	t.Run("create user", func(t *testing.T) {
		body := `{"displayname": "Test User"}`
		req := test_masAdminRequest(t, http.MethodPut, "/_synapse/admin/v1/users/@testmas:test", "test-admin-token", body)
		rec := httptest.NewRecorder()
		routers.SynapseAdmin.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		var resp struct {
			Name           string `json:"name"`
			AccountCreated bool   `json:"account_created"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if resp.Name != "@testmas:test" {
			t.Errorf("expected name @testmas:test, got %s", resp.Name)
		}
	})

	t.Run("get user", func(t *testing.T) {
		// First create the user.
		body := `{}`
		req := test_masAdminRequest(t, http.MethodPut, "/_synapse/admin/v1/users/@gettest:test", "test-admin-token", body)
		rec := httptest.NewRecorder()
		routers.SynapseAdmin.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("failed to create user: %d: %s", rec.Code, rec.Body.String())
		}

		// Now GET the user.
		req2 := test_masAdminRequest(t, http.MethodGet, "/_synapse/admin/v1/users/@gettest:test", "test-admin-token", "")
		rec2 := httptest.NewRecorder()
		routers.SynapseAdmin.ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusOK {
			t.Errorf("expected 200, got %d: %s", rec2.Code, rec2.Body.String())
		}
	})
}

func TestMASAdminDevices(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	// Create a user first.
	req := test_masAdminRequest(t, http.MethodPut, "/_synapse/admin/v1/users/@devtest:test", "test-admin-token", `{}`)
	rec := httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("failed to create user: %d: %s", rec.Code, rec.Body.String())
	}

	t.Run("create device", func(t *testing.T) {
		req := test_masAdminRequest(t, http.MethodPut, "/_synapse/admin/v1/users/@devtest:test/devices/TESTDEV", "test-admin-token", `{}`)
		rec := httptest.NewRecorder()
		routers.SynapseAdmin.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		var resp struct {
			DeviceID string `json:"device_id"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if resp.DeviceID != "TESTDEV" {
			t.Errorf("expected device_id TESTDEV, got %s", resp.DeviceID)
		}
	})

	t.Run("delete device", func(t *testing.T) {
		// Create a device to delete.
		req := test_masAdminRequest(t, http.MethodPut, "/_synapse/admin/v1/users/@devtest:test/devices/DELDEV", "test-admin-token", `{}`)
		rec := httptest.NewRecorder()
		routers.SynapseAdmin.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("failed to create device: %d: %s", rec.Code, rec.Body.String())
		}

		req2 := test_masAdminRequest(t, http.MethodDelete, "/_synapse/admin/v1/users/@devtest:test/devices/DELDEV", "test-admin-token", "")
		rec2 := httptest.NewRecorder()
		routers.SynapseAdmin.ServeHTTP(rec2, req2)
		if rec2.Code != http.StatusOK {
			t.Errorf("expected 200, got %d: %s", rec2.Code, rec2.Body.String())
		}
	})

	t.Run("delete all devices", func(t *testing.T) {
		req := test_masAdminRequest(t, http.MethodDelete, "/_synapse/admin/v1/users/@devtest:test/devices", "test-admin-token", "")
		rec := httptest.NewRecorder()
		routers.SynapseAdmin.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
	})
}

func TestMASAdminDeactivateUser(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	// Create user first.
	req := test_masAdminRequest(t, http.MethodPut, "/_synapse/admin/v1/users/@deactivatetest:test", "test-admin-token", `{}`)
	rec := httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("failed to create user: %d: %s", rec.Code, rec.Body.String())
	}

	req2 := test_masAdminRequest(t, http.MethodPost, "/_synapse/admin/v1/users/@deactivatetest:test/_deactivate", "test-admin-token", "")
	rec2 := httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec2, req2)
	if rec2.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rec2.Code, rec2.Body.String())
	}
}

func TestMASAdminAllowCrossSigningReplacement(t *testing.T) {
	t.Parallel()
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	req := test_masAdminRequest(t, http.MethodPost, "/_synapse/admin/v1/users/@cstest:test/_allow_cross_signing_replacement_without_uia", "test-admin-token", "")
	rec := httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp struct {
		UpdatedAt int64 `json:"updated_at"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to parse response: %v", err)
	}
	if resp.UpdatedAt == 0 {
		t.Error("expected non-zero updated_at")
	}
}

// test_masAdminRequest creates an HTTP request with the MAS admin token in the Authorization header.
func test_masAdminRequest(t *testing.T, method, path, token, body string) *http.Request {
	t.Helper()
	var req *http.Request
	if body != "" {
		req = httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
	} else {
		req = httptest.NewRequest(method, path, nil)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	return req
}
