// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package routing

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
	userapiAPI "codefloe.com/pat-s/zendrite/userapi/api"
)

// These tests cover the interaction between the /_synapse/mas provisioning
// API and the MSC3861 token binding path, which the per-endpoint tests in
// admin_mas_test.go cannot observe: MAS provisions accounts and devices
// first, and the client's access token is bound into those rows on its first
// introspection. The introspection endpoint is a programmable fake MAS.

// fakeMASIntrospection is a programmable OIDC introspection endpoint. It
// answers for the tokens it was programmed with and reports active:false for
// everything else.
type fakeMASIntrospection struct {
	mu     sync.Mutex
	tokens map[string]map[string]any
}

func (f *fakeMASIntrospection) set(token string, response map[string]any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.tokens[token] = response
}

func (f *fakeMASIntrospection) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	response, ok := f.tokens[r.PostForm.Get("token")]
	if !ok {
		response = map[string]any{"active": false}
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
}

// setupMASIntegrationRig builds the shared MSC3861 stack on the given
// database backend with a fake MAS introspection endpoint wired in.
func setupMASIntegrationRig(t *testing.T, dbType test.DBType) (httputil.Routers, userapiAPI.ClientUserAPI, *fakeMASIntrospection, *config.Zendrite, func()) {
	t.Helper()

	introspection := &fakeMASIntrospection{tokens: map[string]map[string]any{}}
	introspectionServer := httptest.NewServer(introspection)
	t.Cleanup(introspectionServer.Close)

	routers, cfg, userAPI, closeDB := setupMSC3861Stack(t, dbType, func(cfg *config.Zendrite) {
		cfg.MSCs.MSC3861.Issuer = introspectionServer.URL
		cfg.MSCs.MSC3861.IntrospectionEndpoint = introspectionServer.URL + "/introspect"
	})
	return routers, userAPI, introspection, cfg, closeDB
}

// introspectionForDevice builds the introspection response MAS would produce
// for an access token belonging to localpart with the given device scope.
func introspectionForDevice(localpart, deviceID string) map[string]any {
	scope := fmt.Sprintf("openid urn:matrix:client:device:%s", deviceID)
	return map[string]any{
		"active":   true,
		"sub":      "opaque-sub-" + localpart,
		"username": localpart,
		"scope":    scope,
	}
}

// masProvisionDevice provisions the account and a device for it the way MAS
// does before the first client request, failing the test on any error.
func masProvisionDevice(t *testing.T, routers httputil.Routers, localpart, deviceID string) {
	t.Helper()
	if rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/provision_user", "test-admin-token", fmt.Sprintf(`{"localpart":%q}`, localpart)); rec.Code != http.StatusCreated && rec.Code != http.StatusOK {
		t.Fatalf("provision_user: expected 2xx, got %d: %s", rec.Code, rec.Body.String())
	}
	if rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", fmt.Sprintf(`{"localpart":%q,"device_id":%q}`, localpart, deviceID)); rec.Code != http.StatusCreated {
		t.Fatalf("upsert_device %s: expected 201, got %d: %s", deviceID, rec.Code, rec.Body.String())
	}
}

// TestMASIntegration_DeviceTokenSurvivesUpsert verifies the ordering hazard
// between provisioning and token binding: MAS provisions the device row
// before the client presents its access token, and the introspection path
// then binds the real token into that row. A later relogin upsert must never
// recreate the row, which would rotate the session ID and break the client.
func TestMASIntegration_DeviceTokenSurvivesUpsert(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		routers, userAPI, introspection, _, closeDB := setupMASIntegrationRig(t, dbType)
		defer closeDB()

		const localpart = "relogin"
		masProvisionDevice(t, routers, localpart, "REL1")

		// The client's first authenticated request binds the real token.
		const realToken = "syz_real_access_token_1"
		introspection.set(realToken, introspectionForDevice(localpart, "REL1"))

		queryToken := func() (int64, string) {
			t.Helper()
			var res userapiAPI.QueryAccessTokenResponse
			if err := userAPI.QueryAccessToken(t.Context(), &userapiAPI.QueryAccessTokenRequest{AccessToken: realToken}, &res); err != nil {
				t.Fatalf("QueryAccessToken failed: %v", err)
			}
			if res.Device == nil {
				t.Fatal("expected device for bound token")
			}
			if res.Device.ID != "REL1" {
				t.Fatalf("token bound to device %q, want REL1", res.Device.ID)
			}
			return res.Device.SessionID, res.Device.DisplayName
		}

		sessionID, displayName := queryToken()

		// MAS upserts the same device again on relogin: the row must be
		// reused, not recreated.
		if rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"relogin","device_id":"REL1"}`); rec.Code != http.StatusOK {
			t.Fatalf("relogin upsert_device: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		sessionID2, displayName2 := queryToken()
		if sessionID2 != sessionID {
			t.Errorf("session ID rotated across upsert: %d -> %d (device row was recreated)", sessionID, sessionID2)
		}
		if displayName2 != displayName {
			t.Errorf("display name changed across upsert: %q -> %q", displayName, displayName2)
		}
	})
}

// TestMASIntegration_SyncDevicesKeepsDehydratedDevice verifies that
// sync_devices reconciles the device list with MAS's view but never deletes
// the dehydrated device (MSC3814), which has no MAS session and therefore
// never appears in the target set.
func TestMASIntegration_SyncDevicesKeepsDehydratedDevice(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		routers, userAPI, _, cfg, closeDB := setupMASIntegrationRig(t, dbType)
		defer closeDB()

		const localpart = "syncuser"
		userID := fmt.Sprintf("@%s:%s", localpart, cfg.ClientAPI.Matrix.ServerName)

		masProvisionDevice(t, routers, localpart, "SYNC1")
		masProvisionDevice(t, routers, localpart, "SYNC2")

		// Store a dehydrated device outside of MAS.
		var storeRes userapiAPI.PerformStoreDehydratedDeviceResponse
		if err := userAPI.PerformStoreDehydratedDevice(t.Context(), &userapiAPI.PerformStoreDehydratedDeviceRequest{
			UserID:     userID,
			DeviceID:   "DEHYDRATED1",
			DeviceData: json.RawMessage(`{"algorithm":"org.example.dehydration.v1"}`),
		}, &storeRes); err != nil {
			t.Fatalf("failed to store dehydrated device: %v", err)
		}

		// Sync to only SYNC2: SYNC1 must go, the dehydrated device must stay.
		if rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/sync_devices", "test-admin-token", `{"localpart":"syncuser","devices":["SYNC2"]}`); rec.Code != http.StatusOK {
			t.Fatalf("sync_devices: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}

		var devices userapiAPI.QueryDevicesResponse
		if err := userAPI.QueryDevices(t.Context(), &userapiAPI.QueryDevicesRequest{UserID: userID}, &devices); err != nil {
			t.Fatalf("QueryDevices failed: %v", err)
		}
		got := map[string]bool{}
		for _, d := range devices.Devices {
			got[d.ID] = true
		}
		if got["SYNC1"] {
			t.Error("SYNC1 should have been deleted by sync_devices")
		}
		if !got["SYNC2"] {
			t.Error("SYNC2 should have survived sync_devices")
		}
		if !got["DEHYDRATED1"] {
			t.Error("dehydrated device must survive sync_devices")
		}

		// The dehydrated device record itself is also intact.
		var dehyd userapiAPI.QueryDehydratedDeviceResponse
		if err := userAPI.QueryDehydratedDevice(t.Context(), &userapiAPI.QueryDehydratedDeviceRequest{UserID: userID}, &dehyd); err != nil {
			t.Fatalf("QueryDehydratedDevice failed: %v", err)
		}
		if !dehyd.Found || dehyd.DeviceID != "DEHYDRATED1" {
			t.Errorf("dehydrated device record = found:%v id:%q, want found:true id:DEHYDRATED1", dehyd.Found, dehyd.DeviceID)
		}
	})
}

// TestMASIntegration_DeleteAndReactivateUserLifecycle verifies that a token
// bound before delete_user stops resolving afterwards, and that a fresh
// login after reactivate_user binds a new device again.
func TestMASIntegration_DeleteAndReactivateUserLifecycle(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		routers, userAPI, introspection, _, closeDB := setupMASIntegrationRig(t, dbType)
		defer closeDB()

		const localpart = "lifecycle"
		masProvisionDevice(t, routers, localpart, "LIFE1")

		// Bind a real token and verify it resolves.
		const realToken = "syz_lifecycle_token"
		introspection.set(realToken, introspectionForDevice(localpart, "LIFE1"))
		assertTokenResolves := func(wantDevice bool) {
			t.Helper()
			var res userapiAPI.QueryAccessTokenResponse
			if err := userAPI.QueryAccessToken(t.Context(), &userapiAPI.QueryAccessTokenRequest{AccessToken: realToken}, &res); err != nil {
				t.Fatalf("QueryAccessToken failed: %v", err)
			}
			if wantDevice && res.Device == nil {
				t.Fatal("expected device for active token")
			}
			if !wantDevice && res.Device != nil {
				t.Fatalf("expected no device after deletion, got %+v", res.Device)
			}
		}
		assertTokenResolves(true)

		// MAS deletes the user: the bound token must stop resolving.
		if rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/delete_user", "test-admin-token", `{"localpart":"lifecycle","erase":false}`); rec.Code != http.StatusOK {
			t.Fatalf("delete_user: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		assertTokenResolves(false)

		// MAS reactivates the account and a fresh login works again: MAS
		// provisions a new device, the client binds a new token.
		if rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/reactivate_user", "test-admin-token", `{"localpart":"lifecycle"}`); rec.Code != http.StatusOK {
			t.Fatalf("reactivate_user: expected 200, got %d: %s", rec.Code, rec.Body.String())
		}
		if rec := masAdminCall(t, routers, http.MethodPost, "/_synapse/mas/upsert_device", "test-admin-token", `{"localpart":"lifecycle","device_id":"LIFE2"}`); rec.Code != http.StatusCreated {
			t.Fatalf("upsert_device after reactivation: expected 201, got %d: %s", rec.Code, rec.Body.String())
		}
		introspection.set("syz_lifecycle_token_2", introspectionForDevice(localpart, "LIFE2"))
		var res userapiAPI.QueryAccessTokenResponse
		if err := userAPI.QueryAccessToken(t.Context(), &userapiAPI.QueryAccessTokenRequest{AccessToken: "syz_lifecycle_token_2"}, &res); err != nil {
			t.Fatalf("QueryAccessToken failed: %v", err)
		}
		if res.Device == nil || res.Device.ID != "LIFE2" {
			t.Errorf("expected device LIFE2 after fresh login, got %+v", res.Device)
		}
	})
}
