// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"regexp"
	"strings"
	"testing"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// These tests exercise the /_synapse/mas provisioning endpoints through the
// real router stack (setupMSC3861Routers enables MSC3861 with the admin token
// "test-admin-token" and an exclusive appservice namespace
// @_appservice_*:test), asserting the wire contract MAS depends on:
// bearer-token auth, status codes (201/200/204/404) and Matrix errcodes.

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
			if res := verifyMASAdminToken(req, tc.token); res != nil {
				if res.Code != tc.wantCode {
					t.Errorf("verifyMASAdminToken code = %d, want %d", res.Code, tc.wantCode)
				}
			} else if tc.wantCode != 0 {
				t.Errorf("verifyMASAdminToken = nil, want code %d", tc.wantCode)
			}
		})
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

// masAdminCall sends a request through the SynapseAdmin router and returns the
// recorded response.
func masAdminCall(t *testing.T, routers httputil.Routers, method, path, token, body string) *httptest.ResponseRecorder {
	t.Helper()
	rec := httptest.NewRecorder()
	routers.SynapseAdmin.ServeHTTP(rec, test_masAdminRequest(t, method, path, token, body))
	return rec
}

// masErrcode extracts the errcode from a Matrix error response body.
func masErrcode(t *testing.T, rec *httptest.ResponseRecorder) string {
	t.Helper()
	var errBody struct {
		ErrCode string `json:"errcode"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &errBody); err != nil {
		t.Fatalf("failed to parse error body %q: %v", rec.Body.String(), err)
	}
	return errBody.ErrCode
}

func TestMASRoutesAuthentication(t *testing.T) {
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	t.Run("NoAuthHeaderIs401", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodGet, "/_synapse/mas/query_user?localpart=alice", "", "")
		if rec.Code != http.StatusUnauthorized {
			t.Errorf("expected 401, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("WrongTokenIs403", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodGet, "/_synapse/mas/query_user?localpart=alice", "wrong-token", "")
		if rec.Code != http.StatusForbidden {
			t.Errorf("expected 403, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	// Positive control: with the right token the endpoint responds with its
	// normal status (404 for an unknown user here), not 401/403.
	t.Run("ValidTokenIsAccepted", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodGet, "/_synapse/mas/query_user?localpart=nobody", "test-admin-token", "")
		if rec.Code != http.StatusNotFound {
			t.Errorf("expected 404 for unknown user with valid token, got %d", rec.Code)
		}
	})
}

func TestMASQueryUser(t *testing.T) {
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	t.Run("UnknownUserIs404NotFound", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodGet, "/_synapse/mas/query_user?localpart=nobody", "test-admin-token", "")
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
		}
		if code := masErrcode(t, rec); code != "M_NOT_FOUND" {
			t.Errorf("expected M_NOT_FOUND, got %s", code)
		}
	})

	t.Run("MissingLocalpart", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodGet, "/_synapse/mas/query_user", "test-admin-token", "")
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("ReturnsProfileState", func(t *testing.T) {
		masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/provision_user", "test-admin-token", `{"localpart":"queryme","set_displayname":"Query Me"}`)

		rec := masAdminCall(t, routers, http.MethodGet, "/_synapse/mas/query_user?localpart=queryme", "test-admin-token", "")
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		var body struct {
			UserID        string  `json:"user_id"`
			DisplayName   *string `json:"display_name"`
			AvatarURL     *string `json:"avatar_url"`
			IsSuspended   bool    `json:"is_suspended"`
			IsDeactivated bool    `json:"is_deactivated"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if body.UserID != "@queryme:test" {
			t.Errorf("user_id = %q, want @queryme:test", body.UserID)
		}
		if body.DisplayName == nil || *body.DisplayName != "Query Me" {
			t.Errorf("display_name = %v, want 'Query Me'", body.DisplayName)
		}
		if body.AvatarURL != nil {
			t.Errorf("avatar_url = %v, want nil", body.AvatarURL)
		}
		if body.IsSuspended {
			t.Error("is_suspended must be false")
		}
		if body.IsDeactivated {
			t.Error("is_deactivated must be false")
		}
	})
}

func TestMASProvisionUser(t *testing.T) {
	routers, cfg, closeDB := setupMSC3861Routers(t)
	defer closeDB()
	registerExclusiveNamespace(cfg)

	provision := func(body string) *httptest.ResponseRecorder {
		return masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/provision_user", "test-admin-token", body)
	}

	t.Run("CreateReturns201", func(t *testing.T) {
		rec := provision(`{"localpart":"provisionme"}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("SecondProvisionReturns200", func(t *testing.T) {
		provision(`{"localpart":"provisionme"}`)
		rec := provision(`{"localpart":"provisionme"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("MissingLocalpart", func(t *testing.T) {
		rec := provision(`{}`)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("MalformedJSON", func(t *testing.T) {
		rec := provision(`{"localpart": `)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("InvalidLocalpart", func(t *testing.T) {
		rec := provision(`{"localpart":"not a valid localpart!"}`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d", rec.Code)
		}
		if code := masErrcode(t, rec); code != "M_INVALID_USERNAME" {
			t.Errorf("expected M_INVALID_USERNAME, got %s", code)
		}
	})

	t.Run("ExclusiveNamespaceRejected", func(t *testing.T) {
		// MAS skips is_localpart_available for users it already knows about,
		// so provisioning must enforce appservice namespaces itself.
		rec := provision(`{"localpart":"exclusivetag"}`)
		if rec.Code != http.StatusBadRequest {
			t.Fatalf("expected 400, got %d: %s", rec.Code, rec.Body.String())
		}
		if code := masErrcode(t, rec); code != "M_EXCLUSIVE" {
			t.Errorf("expected M_EXCLUSIVE, got %s", code)
		}
	})

	t.Run("SetAndUnsetConflict", func(t *testing.T) {
		for _, body := range []string{
			`{"localpart":"conflict","set_displayname":"a","unset_displayname":true}`,
			`{"localpart":"conflict","set_avatar_url":"mxc://x/y","unset_avatar_url":true}`,
			`{"localpart":"conflict","set_emails":["a@b.c"],"unset_emails":true}`,
		} {
			rec := provision(body)
			if rec.Code != http.StatusBadRequest {
				t.Errorf("body %s: expected 400, got %d", body, rec.Code)
			}
		}
	})

	t.Run("UpdateAppliesProfileChanges", func(t *testing.T) {
		provision(`{"localpart":"updateme","set_displayname":"Before"}`)
		rec := provision(`{"localpart":"updateme","set_displayname":"After","set_avatar_url":"mxc://example/abc"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		q := masAdminCall(t, routers, http.MethodGet, "/_synapse/mas/query_user?localpart=updateme", "test-admin-token", "")
		var body struct {
			DisplayName *string `json:"display_name"`
			AvatarURL   *string `json:"avatar_url"`
		}
		if err := json.Unmarshal(q.Body.Bytes(), &body); err != nil {
			t.Fatalf("failed to parse query_user response: %v", err)
		}
		if body.DisplayName == nil || *body.DisplayName != "After" {
			t.Errorf("display_name = %v, want 'After'", body.DisplayName)
		}
		if body.AvatarURL == nil || *body.AvatarURL != "mxc://example/abc" {
			t.Errorf("avatar_url = %v, want mxc://example/abc", body.AvatarURL)
		}
	})
}

// registerExclusiveNamespace adds an exclusive appservice namespace whose
// localparts pass username validation, to reach the M_EXCLUSIVE branch (the
// shared harness's @_appservice_* namespace is unreachable because leading
// underscores fail validation first).
func registerExclusiveNamespace(cfg *config.Zendrite) {
	exclusiveRegex := regexp.MustCompile(`@exclusive.*:test`)
	cfg.ClientAPI.Derived.ApplicationServices = append(cfg.ClientAPI.Derived.ApplicationServices, config.ApplicationService{
		ID:              "exclusive-as",
		ASToken:         "exclusive-as-token",
		HSToken:         "exclusive-hs-token",
		URL:             "null",
		SenderLocalpart: "exclusive_bot",
		NamespaceMap: map[string][]config.ApplicationServiceNamespace{
			"users": {
				{
					Exclusive:    true,
					Regex:        exclusiveRegex.String(),
					RegexpObject: exclusiveRegex,
				},
			},
		},
	})
}

func TestMASIsLocalpartAvailable(t *testing.T) {
	routers, cfg, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	registerExclusiveNamespace(cfg)

	masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/provision_user", "test-admin-token", `{"localpart":"taken"}`)

	tests := []struct {
		name      string
		localpart string
		wantCode  int
		wantErr   string
	}{
		{"available", "freshtag", 200, ""},
		{"taken", "taken", 400, "M_USER_IN_USE"},
		{"invalid", "NOT VALID!", 400, "M_INVALID_USERNAME"},
		{"uppercase is not folded", "Alice", 400, "M_INVALID_USERNAME"},
		{"appservice underscore prefix", "_appservice_bot", 400, "M_INVALID_USERNAME"},
		{"appservice exclusive", "exclusivetag", 400, "M_EXCLUSIVE"},
		{"missing param", "", 400, "M_MISSING_PARAM"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := "/_synapse/mas/is_localpart_available"
			if tc.localpart != "" {
				path += "?localpart=" + url.QueryEscape(tc.localpart)
			}
			rec := masAdminCall(t, routers, http.MethodGet, path, "test-admin-token", "")
			if rec.Code != tc.wantCode {
				t.Fatalf("expected %d, got %d: %s", tc.wantCode, rec.Code, rec.Body.String())
			}
			if tc.wantErr != "" && masErrcode(t, rec) != tc.wantErr {
				t.Errorf("expected errcode %s, got %s", tc.wantErr, masErrcode(t, rec))
			}
		})
	}
}

func TestMASUpsertDevice(t *testing.T) {
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/provision_user", "test-admin-token", `{"localpart":"deviceuser"}`)

	t.Run("UnknownUserIs404", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"nobody","device_id":"DEV1"}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
		}
		if code := masErrcode(t, rec); code != "M_NOT_FOUND" {
			t.Errorf("expected M_NOT_FOUND, got %s", code)
		}
	})

	t.Run("CreateReturns201", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"deviceuser","device_id":"MASDEVICE1","display_name":"Element Web"}`)
		if rec.Code != http.StatusCreated {
			t.Fatalf("expected 201, got %d: %s", rec.Code, rec.Body.String())
		}
		var body struct {
			DeviceID    string  `json:"device_id"`
			DisplayName *string `json:"display_name"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("failed to parse response: %v", err)
		}
		if body.DeviceID != "MASDEVICE1" {
			t.Errorf("device_id = %q, want MASDEVICE1", body.DeviceID)
		}
		if body.DisplayName == nil || *body.DisplayName != "Element Web" {
			t.Errorf("display_name = %v, want 'Element Web'", body.DisplayName)
		}
	})

	t.Run("ExistingDeviceReturns200", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"deviceuser","device_id":"MASDEVICE1"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("MissingDeviceID", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"deviceuser"}`)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", rec.Code)
		}
	})
}

func TestMASUpdateDeviceDisplayName(t *testing.T) {
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/provision_user", "test-admin-token", `{"localpart":"renameuser"}`)
	masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"renameuser","device_id":"RENAME1"}`)

	t.Run("UnknownUserIs404", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/update_device_display_name", "test-admin-token", `{"localpart":"nobody","device_id":"RENAME1","display_name":"x"}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", rec.Code)
		}
	})

	t.Run("UnknownDeviceIs404", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/update_device_display_name", "test-admin-token", `{"localpart":"renameuser","device_id":"MISSING","display_name":"x"}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("RenameReturns200", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/update_device_display_name", "test-admin-token", `{"localpart":"renameuser","device_id":"RENAME1","display_name":"Renamed"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("MissingDisplayName", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/update_device_display_name", "test-admin-token", `{"localpart":"renameuser","device_id":"RENAME1","display_name":""}`)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", rec.Code)
		}
	})
}

func TestMASDeleteDevice(t *testing.T) {
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/provision_user", "test-admin-token", `{"localpart":"deleteme"}`)
	masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"deleteme","device_id":"DELETE1"}`)

	t.Run("UnknownUserIs404", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/delete_device", "test-admin-token", `{"localpart":"nobody","device_id":"DELETE1"}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", rec.Code)
		}
	})

	t.Run("DeleteReturns204", func(t *testing.T) {
		// Only the status is asserted: a real net/http server never writes a
		// body for 204, whereas httptest.ResponseRecorder records whatever
		// the handler wrote.
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/delete_device", "test-admin-token", `{"localpart":"deleteme","device_id":"DELETE1"}`)
		if rec.Code != http.StatusNoContent {
			t.Fatalf("expected 204, got %d: %s", rec.Code, rec.Body.String())
		}
	})
}

func TestMASSyncDevices(t *testing.T) {
	routers, cfg, userAPI, closeDB := setupMSC3861Stack(t, test.DBTypeSQLite, nil)
	defer closeDB()

	masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/provision_user", "test-admin-token", `{"localpart":"syncuser"}`)
	masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"syncuser","device_id":"KEEP1"}`)
	masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"syncuser","device_id":"DROP1"}`)

	t.Run("UnknownUserIs404", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/sync_devices", "test-admin-token", `{"localpart":"nobody","devices":[]}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d", rec.Code)
		}
	})

	t.Run("MissingDevicesField", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/sync_devices", "test-admin-token", `{"localpart":"syncuser"}`)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("SyncKeepsFallbackOIDCDevice", func(t *testing.T) {
		// Tokens without a device scope are bound to a synthetic device that
		// MAS never lists, so the sync must not delete it.
		token := "fallback-token"
		var createRes userapi.PerformDeviceCreationResponse
		fallbackID := userapi.MSC3861FallbackDeviceID
		if err := userAPI.PerformDeviceCreation(t.Context(), &userapi.PerformDeviceCreationRequest{
			Localpart:          "syncuser",
			ServerName:         cfg.Global.ServerName,
			AccessToken:        token,
			DeviceID:           &fallbackID,
			NoDeviceListUpdate: true,
		}, &createRes); err != nil {
			t.Fatalf("failed to create fallback device: %v", err)
		}

		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/sync_devices", "test-admin-token", `{"localpart":"syncuser","devices":["KEEP1"]}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var devices userapi.QueryDevicesResponse
		if err := userAPI.QueryDevices(t.Context(), &userapi.QueryDevicesRequest{UserID: "@syncuser:" + string(cfg.Global.ServerName)}, &devices); err != nil {
			t.Fatalf("QueryDevices failed: %v", err)
		}
		found := false
		for _, d := range devices.Devices {
			if d.ID == userapi.MSC3861FallbackDeviceID {
				found = true
			}
		}
		if !found {
			t.Errorf("fallback device %q must survive sync_devices, got %+v", userapi.MSC3861FallbackDeviceID, devices.Devices)
		}
	})

	t.Run("SyncReturns200AndReconciles", func(t *testing.T) {
		// Keep KEEP1, add NEW1, implicitly drop DROP1.
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/sync_devices", "test-admin-token", `{"localpart":"syncuser","devices":["KEEP1","NEW1"]}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		// Re-upserting a dropped device must recreate it (201), proving the
		// sync removed it; the kept device must still exist (200).
		if rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"syncuser","device_id":"DROP1"}`); rec.Code != http.StatusCreated {
			t.Errorf("expected DROP1 to be recreated with 201, got %d", rec.Code)
		}
		if rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"syncuser","device_id":"KEEP1"}`); rec.Code != http.StatusOK {
			t.Errorf("expected KEEP1 to survive with 200, got %d", rec.Code)
		}
		if rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"syncuser","device_id":"NEW1"}`); rec.Code != http.StatusOK {
			t.Errorf("expected NEW1 to have been created by sync (200), got %d", rec.Code)
		}
	})
}

func TestMASDeleteUserLifecycle(t *testing.T) {
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/provision_user", "test-admin-token", `{"localpart":"lifecycle"}`)

	queryDeactivated := func() bool {
		rec := masAdminCall(t, routers, http.MethodGet, "/_synapse/mas/query_user?localpart=lifecycle", "test-admin-token", "")
		var body struct {
			IsDeactivated bool `json:"is_deactivated"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("failed to parse query_user response: %v", err)
		}
		return body.IsDeactivated
	}

	t.Run("UnknownUserIs404", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/delete_user", "test-admin-token", `{"localpart":"nobody","erase":false}`)
		if rec.Code != http.StatusNotFound {
			t.Fatalf("expected 404, got %d: %s", rec.Code, rec.Body.String())
		}
	})

	t.Run("DeleteMarksDeactivated", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/delete_user", "test-admin-token", `{"localpart":"lifecycle","erase":true}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		if !queryDeactivated() {
			t.Error("expected is_deactivated=true after delete_user")
		}
	})

	t.Run("ReactivateRestoresAccount", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/reactivate_user", "test-admin-token", `{"localpart":"lifecycle"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		if queryDeactivated() {
			t.Error("expected is_deactivated=false after reactivate_user")
		}
	})

	t.Run("ReactivateUnknownUserIs404", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/reactivate_user", "test-admin-token", `{"localpart":"nobody"}`)
		if rec.Code != http.StatusNotFound {
			t.Errorf("expected 404, got %d", rec.Code)
		}
	})
}

func TestMASSetAndUnsetDisplayName(t *testing.T) {
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/provision_user", "test-admin-token", `{"localpart":"displayuser"}`)

	displayName := func() *string {
		rec := masAdminCall(t, routers, http.MethodGet, "/_synapse/mas/query_user?localpart=displayuser", "test-admin-token", "")
		var body struct {
			DisplayName *string `json:"display_name"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
			t.Fatalf("failed to parse query_user response: %v", err)
		}
		return body.DisplayName
	}

	t.Run("SetDisplayName", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/set_displayname", "test-admin-token", `{"localpart":"displayuser","displayname":"Display User"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		if name := displayName(); name == nil || *name != "Display User" {
			t.Errorf("display_name = %v, want 'Display User'", name)
		}
	})

	t.Run("SetDisplayNameUnknownUserIs404", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/set_displayname", "test-admin-token", `{"localpart":"nobody","displayname":"x"}`)
		if rec.Code != http.StatusNotFound {
			t.Errorf("expected 404, got %d", rec.Code)
		}
	})

	t.Run("SetDisplayNameEmptyIs400", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/set_displayname", "test-admin-token", `{"localpart":"displayuser","displayname":""}`)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", rec.Code)
		}
	})

	t.Run("UnsetDisplayName", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/unset_displayname", "test-admin-token", `{"localpart":"displayuser"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		if name := displayName(); name != nil {
			t.Errorf("display_name = %v, want nil after unset", name)
		}
	})

	t.Run("UnsetDisplayNameUnknownUserIs404", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/unset_displayname", "test-admin-token", `{"localpart":"nobody"}`)
		if rec.Code != http.StatusNotFound {
			t.Errorf("expected 404, got %d", rec.Code)
		}
	})
}

func TestMASAllowCrossSigningReset(t *testing.T) {
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	t.Run("RecordsConfirmation", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/allow_cross_signing_reset", "test-admin-token", `{"localpart":"crosssigner"}`)
		if rec.Code != http.StatusOK {
			t.Fatalf("expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		if !consumeMASCrossSigningConfirmation("@crosssigner:test") {
			t.Error("expected confirmation to be recorded for @crosssigner:test")
		}
		// The confirmation is consumed exactly once.
		if consumeMASCrossSigningConfirmation("@crosssigner:test") {
			t.Error("confirmation must be consumed exactly once")
		}
	})

	t.Run("MissingLocalpart", func(t *testing.T) {
		rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/allow_cross_signing_reset", "test-admin-token", `{}`)
		if rec.Code != http.StatusBadRequest {
			t.Errorf("expected 400, got %d", rec.Code)
		}
	})
}

func TestMASRoutesMethodNotAllowed(t *testing.T) {
	routers, _, closeDB := setupMSC3861Routers(t)
	defer closeDB()

	// query_user is GET-only; a POST must be rejected by the router.
	rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/query_user?localpart=alice", "test-admin-token", "")
	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("expected 405, got %d", rec.Code)
	}
}

func TestMASRoutesNotRegisteredWithoutAdminToken(t *testing.T) {
	// With MSC3861 enabled but no admin token configured, the provisioning
	// routes must not exist at all.
	routers, _, closeDB := setupMSC3861SSORouters(t, config.MSC3861Config{
		Issuer:       "https://auth.example.com",
		ClientID:     "test-client-id",
		ClientSecret: "test-client-secret",
	})
	defer closeDB()

	for _, path := range []string{
		"/_synapse/mas/query_user",
		"/_synapse/mas/provision_user",
		"/_synapse/mas/upsert_device",
	} {
		req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
		req.Header.Set("Authorization", "Bearer whatever")
		rec := httptest.NewRecorder()
		routers.SynapseAdmin.ServeHTTP(rec, req)
		if rec.Code != http.StatusNotFound {
			t.Errorf("%s: expected 404, got %d", path, rec.Code)
		}
	}
}

func TestMASRoutesAreRateLimited(t *testing.T) {
	routers, _, _, closeDB := setupMSC3861Stack(t, test.DBTypeSQLite, func(cfg *config.Zendrite) {
		cfg.ClientAPI.RateLimiting = config.RateLimiting{
			Enabled:   true,
			Threshold: 1,
			CooloffMS: 60_000,
		}
	})
	defer closeDB()

	// The limiter keys unauthenticated callers by remote address, so a
	// second request from the same client within the cooloff must be
	// rejected before the admin token is even checked.
	first := masAdminCall(t, routers, http.MethodGet, "/_synapse/mas/query_user?localpart=alice", "wrong-token", "")
	if first.Code != http.StatusForbidden {
		t.Fatalf("expected first call to reach token verification (403), got %d: %s", first.Code, first.Body.String())
	}
	second := masAdminCall(t, routers, http.MethodGet, "/_synapse/mas/query_user?localpart=alice", "wrong-token", "")
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("expected second call to be rate limited (429), got %d: %s", second.Code, second.Body.String())
	}
	if errcode := masErrcode(t, second); errcode != "M_LIMIT_EXCEEDED" {
		t.Errorf("expected M_LIMIT_EXCEEDED, got %s", errcode)
	}
}
