package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/go-chi/chi/v5"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

type mockKeyAPI struct {
	t             *testing.T
	userResponses map[string]api.QueryKeysResponse
}

func (m mockKeyAPI) QueryKeys(ctx context.Context, req *api.QueryKeysRequest, res *api.QueryKeysResponse) {
	res.MasterKeys = m.userResponses[req.UserID].MasterKeys
	res.SelfSigningKeys = m.userResponses[req.UserID].SelfSigningKeys
	res.UserSigningKeys = m.userResponses[req.UserID].UserSigningKeys
	if m.t != nil {
		m.t.Logf("QueryKeys: %+v => %+v", req, res)
	}
}

func (m mockKeyAPI) PerformUploadDeviceKeys(ctx context.Context, req *api.PerformUploadDeviceKeysRequest, res *api.PerformUploadDeviceKeysResponse) {
	// Just a dummy upload which always succeeds
}

func getAccountByPassword(ctx context.Context, req *api.QueryAccountByPasswordRequest, res *api.QueryAccountByPasswordResponse) error {
	res.Exists = true
	res.Account = &api.Account{UserID: fmt.Sprintf("@%s:%s", req.Localpart, req.ServerName)}
	return nil
}

// Tests that if there is no existing master key for the user, the request is allowed.
func Test_UploadCrossSigningDeviceKeys_ValidRequest(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"master_key": {"user_id": "@user:example.com", "usage": ["master"], "keys": {"ed25519:1": "key1"}},
		"self_signing_key": {"user_id": "@user:example.com", "usage": ["self_signing"], "keys": {"ed25519:2": "key2"}},
		"user_signing_key": {"user_id": "@user:example.com", "usage": ["user_signing"], "keys": {"ed25519:3": "key3"}}
	}`))
	req.Header.Set("Content-Type", "application/json")

	keyserverAPI := &mockKeyAPI{
		userResponses: map[string]api.QueryKeysResponse{
			"@user:example.com": {},
		},
	}
	device := &api.Device{UserID: "@user:example.com", ID: "device"}
	cfg := &config.ClientAPI{}

	res := UploadCrossSigningDeviceKeys(req, keyserverAPI, device, getAccountByPassword, cfg, &config.MSCs{})
	if res.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
	}
}

// Require UIA if there is an existing master key and there is no auth provided.
func Test_UploadCrossSigningDeviceKeys_Unauthorized(t *testing.T) {
	userID := "@user:example.com"

	// Note that there is no auth field.
	request := fclient.CrossSigningKeys{
		MasterKey: fclient.CrossSigningKey{
			Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("key1")},
			Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeMaster},
			UserID: userID,
		},
		SelfSigningKey: fclient.CrossSigningKey{
			Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("key2")},
			Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeSelfSigning},
			UserID: userID,
		},
		UserSigningKey: fclient.CrossSigningKey{
			Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("key3")},
			Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeUserSigning},
			UserID: userID,
		},
	}

	b := bytes.Buffer{}
	m := json.NewEncoder(&b)
	err := m.Encode(request)
	if err != nil {
		t.Fatal(err)
	}

	req := httptest.NewRequest(http.MethodPost, "/", &b)
	req.Header.Set("Content-Type", "application/json")

	keyserverAPI := &mockKeyAPI{
		t: t,
		userResponses: map[string]api.QueryKeysResponse{
			"@user:example.com": {
				MasterKeys: map[string]fclient.CrossSigningKey{
					"@user:example.com": {UserID: "@user:example.com", Usage: []fclient.CrossSigningKeyPurpose{"master"}, Keys: map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("key1")}},
				},
				SelfSigningKeys: nil,
				UserSigningKeys: nil,
			},
		},
	}
	device := &api.Device{UserID: "@user:example.com", ID: "device"}
	cfg := &config.ClientAPI{}

	res := UploadCrossSigningDeviceKeys(req, keyserverAPI, device, getAccountByPassword, cfg, &config.MSCs{})
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, res.Code)
	}
}

// Invalid JSON is rejected.
func Test_UploadCrossSigningDeviceKeys_InvalidJSON(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"auth": {"type": "m.login.password", "session": "session", "user": "user", "password": "password"},
		"master_key": {"user_id": "@user:example.com", "usage": ["master"], "keys": {"ed25519:1": "key1"}},
		"self_signing_key": {"user_id": "@user:example.com", "usage": ["self_signing"], "keys": {"ed25519:2": "key2"}},
		"user_signing_key": {"user_id": "@user:example.com", "usage": ["user_signing"], "keys": {"ed25519:3": "key3"}
	}`)) // Missing closing brace
	req.Header.Set("Content-Type", "application/json")

	keyserverAPI := &mockKeyAPI{}
	device := &api.Device{UserID: "@user:example.com", ID: "device"}
	cfg := &config.ClientAPI{}

	res := UploadCrossSigningDeviceKeys(req, keyserverAPI, device, getAccountByPassword, cfg, &config.MSCs{})
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, res.Code)
	}
}

// Require UIA if an existing master key is present and the keys differ.
func Test_UploadCrossSigningDeviceKeys_ExistingKeysMismatch(t *testing.T) {
	// Again, no auth provided
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"master_key": {"user_id": "@user:example.com", "usage": ["master"], "keys": {"ed25519:1": "key1"}},
		"self_signing_key": {"user_id": "@user:example.com", "usage": ["self_signing"], "keys": {"ed25519:2": "key2"}},
		"user_signing_key": {"user_id": "@user:example.com", "usage": ["user_signing"], "keys": {"ed25519:3": "key3"}}
	}`))
	req.Header.Set("Content-Type", "application/json")

	keyserverAPI := &mockKeyAPI{
		userResponses: map[string]api.QueryKeysResponse{
			"@user:example.com": {
				MasterKeys: map[string]fclient.CrossSigningKey{
					"@user:example.com": {UserID: "@user:example.com", Usage: []fclient.CrossSigningKeyPurpose{"master"}, Keys: map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("different_key")}},
				},
			},
		},
	}
	device := &api.Device{UserID: "@user:example.com", ID: "device"}

	cfg, _, _ := testrig.CreateConfig(t, test.DBTypeSQLite)
	cfg.Global.ServerName = "example.com"

	res := UploadCrossSigningDeviceKeys(req, keyserverAPI, device, getAccountByPassword, &cfg.ClientAPI, &config.MSCs{})
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, res.Code)
	}
}

// Under MSC3861 the UIA challenge uses the m.oauth auth type, pointing at the
// account management URL. Without an admin token the provider cannot confirm
// the reset, so the fallback applies and resubmitting the issued session
// completes the stage.
func Test_UploadCrossSigningDeviceKeys_OAuthUIA(t *testing.T) {
	userID := "@user:example.com"
	mscCfg := &config.MSCs{
		MSCs: []string{"msc3861"},
		MSC3861: config.MSC3861Config{
			AccountManagementURL: "https://account.example.com/manage",
		},
	}
	keyserverAPI := &mockKeyAPI{
		userResponses: map[string]api.QueryKeysResponse{
			userID: {
				MasterKeys: map[string]fclient.CrossSigningKey{
					userID: {UserID: userID, Usage: []fclient.CrossSigningKeyPurpose{"master"}, Keys: map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("different_key")}},
				},
			},
		},
	}
	device := &api.Device{UserID: userID, ID: "device"}
	cfg := &config.ClientAPI{}

	// First request without auth: expect an m.oauth UIA challenge.
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"master_key": {"user_id": "@user:example.com", "usage": ["master"], "keys": {"ed25519:1": "key1"}},
		"self_signing_key": {"user_id": "@user:example.com", "usage": ["self_signing"], "keys": {"ed25519:2": "key2"}},
		"user_signing_key": {"user_id": "@user:example.com", "usage": ["user_signing"], "keys": {"ed25519:3": "key3"}}
	}`))
	req.Header.Set("Content-Type", "application/json")

	res := UploadCrossSigningDeviceKeys(req, keyserverAPI, device, getAccountByPassword, cfg, mscCfg)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, res.Code)
	}
	uia, ok := res.JSON.(userInteractiveResponse)
	if !ok {
		t.Fatalf("expected userInteractiveResponse, got %T", res.JSON)
	}
	if len(uia.Flows) != 1 || len(uia.Flows[0].Stages) != 1 || uia.Flows[0].Stages[0] != authtypes.LoginTypeOAuth {
		t.Fatalf("expected a single m.oauth flow, got %+v", uia.Flows)
	}
	oauthParams, ok := uia.Params["m.oauth"].(map[string]any)
	if !ok {
		t.Fatalf("expected m.oauth params, got %+v", uia.Params)
	}
	wantURL := "https://account.example.com/manage?action=org.matrix.cross_signing_reset&device_id=device"
	if oauthParams["url"] != wantURL {
		t.Errorf("expected m.oauth url %q, got %q", wantURL, oauthParams["url"])
	}
	if uia.Session == "" {
		t.Fatal("expected a session ID in the UIA response")
	}
	// The stage must not be listed as completed when the challenge is issued.
	for _, stage := range uia.Completed {
		if stage == authtypes.LoginTypeOAuth {
			t.Fatal("m.oauth must not be listed as completed when the challenge is issued")
		}
	}

	// Resubmitting a session that was never issued must not complete UIA; a
	// fresh challenge with a new server-side session ID is returned instead.
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"auth": {"session": "never-issued-session"},
		"master_key": {"user_id": "@user:example.com", "usage": ["master"], "keys": {"ed25519:1": "key1"}},
		"self_signing_key": {"user_id": "@user:example.com", "usage": ["self_signing"], "keys": {"ed25519:2": "key2"}},
		"user_signing_key": {"user_id": "@user:example.com", "usage": ["user_signing"], "keys": {"ed25519:3": "key3"}}
	}`))
	req.Header.Set("Content-Type", "application/json")

	res = UploadCrossSigningDeviceKeys(req, keyserverAPI, device, getAccountByPassword, cfg, mscCfg)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, res.Code)
	}
	reissued, ok := res.JSON.(userInteractiveResponse)
	if !ok {
		t.Fatalf("expected userInteractiveResponse, got %T", res.JSON)
	}
	if reissued.Session == "never-issued-session" {
		t.Fatal("client-supplied session ID must not be honored; expected a server-generated one")
	}
	for _, stage := range reissued.Completed {
		if stage == authtypes.LoginTypeOAuth {
			t.Fatal("m.oauth must not be listed as completed for a never-issued session")
		}
	}

	// Resubmitting the issued session completes the stage in fallback mode.
	req = httptest.NewRequest(http.MethodPost, "/", strings.NewReader(`{
		"auth": {"session": "`+uia.Session+`"},
		"master_key": {"user_id": "@user:example.com", "usage": ["master"], "keys": {"ed25519:1": "key1"}},
		"self_signing_key": {"user_id": "@user:example.com", "usage": ["self_signing"], "keys": {"ed25519:2": "key2"}},
		"user_signing_key": {"user_id": "@user:example.com", "usage": ["user_signing"], "keys": {"ed25519:3": "key3"}}
	}`))
	req.Header.Set("Content-Type", "application/json")

	res = UploadCrossSigningDeviceKeys(req, keyserverAPI, device, getAccountByPassword, cfg, mscCfg)
	if res.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
	}
}

// oauthUIATestKeyAPI returns a key API where every given user has an existing
// master key that differs from the uploaded keys, so UIA is always required.
func oauthUIATestKeyAPI(userIDs ...string) *mockKeyAPI {
	m := &mockKeyAPI{userResponses: map[string]api.QueryKeysResponse{}}
	for _, userID := range userIDs {
		m.userResponses[userID] = api.QueryKeysResponse{
			MasterKeys: map[string]fclient.CrossSigningKey{
				userID: {UserID: userID, Usage: []fclient.CrossSigningKeyPurpose{"master"}, Keys: map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("different_key")}},
			},
		}
	}
	return m
}

// oauthUIAMSCConfig returns an MSC3861 config. An empty admin token means the
// MAS admin routes are not registered, so the provider cannot confirm a
// cross-signing reset and the resubmission fallback applies.
func oauthUIAMSCConfig(adminToken string) *config.MSCs {
	return &config.MSCs{
		MSCs: []string{"msc3861"},
		MSC3861: config.MSC3861Config{
			AccountManagementURL: "https://account.example.com/manage",
			AdminToken:           adminToken,
		},
	}
}

// recordCrossSigningConfirmation simulates MAS calling the admin endpoint after
// the user confirmed the reset in the account management web UI.
func recordCrossSigningConfirmation(t *testing.T, cfg *config.ClientAPI, userID string) {
	t.Helper()
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("userId", userID)
	masReq := httptest.NewRequest(http.MethodPost, "/", nil)
	masReq = masReq.WithContext(context.WithValue(masReq.Context(), chi.RouteCtxKey, rctx))
	res := MASAllowCrossSigningReplacement(masReq, cfg)
	if res.Code != http.StatusOK {
		t.Fatalf("expected status %d from MAS admin endpoint, got %d", http.StatusOK, res.Code)
	}
}

func crossSigningUploadBody(t *testing.T, userID, session string) *strings.Reader {
	t.Helper()
	auth := ""
	if session != "" {
		auth = fmt.Sprintf(`"auth": {"session": %q},`, session)
	}
	return strings.NewReader(fmt.Sprintf(`{
		%s
		"master_key": {"user_id": %q, "usage": ["master"], "keys": {"ed25519:1": "key1"}},
		"self_signing_key": {"user_id": %q, "usage": ["self_signing"], "keys": {"ed25519:2": "key2"}},
		"user_signing_key": {"user_id": %q, "usage": ["user_signing"], "keys": {"ed25519:3": "key3"}}
	}`, auth, userID, userID, userID))
}

func uploadCrossSigning(t *testing.T, keyserverAPI UploadKeysAPI, device *api.Device, session string, mscCfg *config.MSCs) util.JSONResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", crossSigningUploadBody(t, device.UserID, session))
	req.Header.Set("Content-Type", "application/json")
	return UploadCrossSigningDeviceKeys(req, keyserverAPI, device, getAccountByPassword, &config.ClientAPI{}, mscCfg)
}

// In the fallback mode a session issued for one user must not complete the
// m.oauth stage for another user, and a failed attempt must not consume the
// issued session.
func Test_UploadCrossSigningDeviceKeys_OAuthUIA_SessionUserBound(t *testing.T) {
	userA := "@alice-oauth:example.com"
	userB := "@bob-oauth:example.com"
	mscCfg := oauthUIAMSCConfig("")
	keyserverAPI := oauthUIATestKeyAPI(userA, userB)
	deviceA := &api.Device{UserID: userA, ID: "deviceA"}
	deviceB := &api.Device{UserID: userB, ID: "deviceB"}

	// User A gets an m.oauth challenge.
	res := uploadCrossSigning(t, keyserverAPI, deviceA, "", mscCfg)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, res.Code)
	}
	uia, ok := res.JSON.(userInteractiveResponse)
	if !ok {
		t.Fatalf("expected userInteractiveResponse, got %T", res.JSON)
	}
	if uia.Session == "" {
		t.Fatal("expected a session ID in the UIA response")
	}

	// User B cannot complete the stage with user A's session.
	res = uploadCrossSigning(t, keyserverAPI, deviceB, uia.Session, mscCfg)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, res.Code)
	}
	reissued, ok := res.JSON.(userInteractiveResponse)
	if !ok {
		t.Fatalf("expected userInteractiveResponse, got %T", res.JSON)
	}
	if reissued.Session == uia.Session {
		t.Fatal("a session issued for another user must not be honored")
	}

	// The failed attempt must not have consumed user A's session.
	res = uploadCrossSigning(t, keyserverAPI, deviceA, uia.Session, mscCfg)
	if res.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
	}
}

// With an admin token configured, a confirmation recorded through the MAS admin
// endpoint completes the m.oauth stage and is consumed on use.
func Test_UploadCrossSigningDeviceKeys_OAuthUIA_MASConfirmation(t *testing.T) {
	userID := "@carol-oauth:example.com"
	mscCfg := oauthUIAMSCConfig("admin-token")
	keyserverAPI := oauthUIATestKeyAPI(userID)
	device := &api.Device{UserID: userID, ID: "device"}

	cfg, _, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cfg.Global.ServerName = "example.com"

	recordCrossSigningConfirmation(t, &cfg.ClientAPI, userID)

	// The confirmation completes the stage even without a server-issued
	// session, as the provider confirmed the user did the web flow.
	uploadRes := uploadCrossSigning(t, keyserverAPI, device, "some-client-session", mscCfg)
	if uploadRes.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, uploadRes.Code)
	}

	// The confirmation is single-use: the next attempt gets a fresh challenge.
	uploadRes = uploadCrossSigning(t, keyserverAPI, device, "another-client-session", mscCfg)
	if uploadRes.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, uploadRes.Code)
	}
}

// With an admin token configured the provider can confirm the reset, so
// resubmitting a server-issued session must not complete the m.oauth stage on
// its own; only the provider confirmation does.
func Test_UploadCrossSigningDeviceKeys_OAuthUIA_AdminTokenRequiresConfirmation(t *testing.T) {
	userID := "@dave-oauth:example.com"
	mscCfg := oauthUIAMSCConfig("admin-token")
	keyserverAPI := oauthUIATestKeyAPI(userID)
	device := &api.Device{UserID: userID, ID: "device"}

	cfg, _, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cfg.Global.ServerName = "example.com"

	// The first request gets an m.oauth challenge pointing at the account
	// management UI.
	res := uploadCrossSigning(t, keyserverAPI, device, "", mscCfg)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, res.Code)
	}
	uia, ok := res.JSON.(userInteractiveResponse)
	if !ok {
		t.Fatalf("expected userInteractiveResponse, got %T", res.JSON)
	}
	if uia.Session == "" {
		t.Fatal("expected a session ID in the UIA response")
	}
	oauthParams, ok := uia.Params["m.oauth"].(map[string]any)
	if !ok {
		t.Fatalf("expected m.oauth params, got %+v", uia.Params)
	}
	wantURL := "https://account.example.com/manage?action=org.matrix.cross_signing_reset&device_id=device"
	if oauthParams["url"] != wantURL {
		t.Errorf("expected m.oauth url %q, got %q", wantURL, oauthParams["url"])
	}

	// Resubmitting that session without a provider confirmation must not
	// complete the stage; a fresh challenge is returned instead.
	res = uploadCrossSigning(t, keyserverAPI, device, uia.Session, mscCfg)
	if res.Code != http.StatusUnauthorized {
		t.Fatalf("expected status %d, got %d", http.StatusUnauthorized, res.Code)
	}
	reissued, ok := res.JSON.(userInteractiveResponse)
	if !ok {
		t.Fatalf("expected userInteractiveResponse, got %T", res.JSON)
	}
	if reissued.Session == uia.Session {
		t.Fatal("expected a fresh session ID after an unconfirmed resubmission")
	}
	for _, stage := range reissued.Completed {
		if stage == authtypes.LoginTypeOAuth {
			t.Fatal("m.oauth must not be completed without a provider confirmation")
		}
	}

	// Once the provider confirms the reset, the resubmission succeeds.
	recordCrossSigningConfirmation(t, &cfg.ClientAPI, userID)
	res = uploadCrossSigning(t, keyserverAPI, device, reissued.Session, mscCfg)
	if res.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, res.Code)
	}
}

// The MAS admin endpoint rejects user IDs that are not local.
func Test_MASAllowCrossSigningReplacement_RejectsRemoteUser(t *testing.T) {
	cfg, _, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	cfg.Global.ServerName = "example.com"

	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("userId", "@user:remote.example.org")
	masReq := httptest.NewRequest(http.MethodPost, "/", nil)
	masReq = masReq.WithContext(context.WithValue(masReq.Context(), chi.RouteCtxKey, rctx))
	res := MASAllowCrossSigningReplacement(masReq, &cfg.ClientAPI)
	if res.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d", http.StatusBadRequest, res.Code)
	}
}

func Test_KeysDiffer_MasterKeyMismatch(t *testing.T) {
	existingMasterKey := fclient.CrossSigningKey{
		UserID: "@user:example.com",
		Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeMaster},
		Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("existing_key")},
	}
	keyResp := api.QueryKeysResponse{}
	uploadReq := &crossSigningRequest{
		PerformUploadDeviceKeysRequest: api.PerformUploadDeviceKeysRequest{
			CrossSigningKeys: fclient.CrossSigningKeys{
				MasterKey: fclient.CrossSigningKey{
					UserID: "@user:example.com",
					Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeMaster},
					Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("new_key")},
				},
			},
		},
	}
	userID := "@user:example.com"

	result := keysDiffer(existingMasterKey, keyResp, uploadReq, userID)
	if !result {
		t.Fatalf("expected keys to differ, but they did not")
	}
}

func Test_KeysDiffer_SelfSigningKeyMismatch(t *testing.T) {
	existingMasterKey := fclient.CrossSigningKey{
		UserID: "@user:example.com",
		Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeMaster},
		Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("key")},
	}
	keyResp := api.QueryKeysResponse{
		SelfSigningKeys: map[string]fclient.CrossSigningKey{
			"@user:example.com": {
				UserID: "@user:example.com",
				Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeSelfSigning},
				Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:2": spec.Base64Bytes("existing_key")},
			},
		},
	}
	uploadReq := &crossSigningRequest{
		PerformUploadDeviceKeysRequest: api.PerformUploadDeviceKeysRequest{
			CrossSigningKeys: fclient.CrossSigningKeys{
				SelfSigningKey: fclient.CrossSigningKey{
					UserID: "@user:example.com",
					Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeSelfSigning},
					Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:2": spec.Base64Bytes("new_key")},
				},
			},
		},
	}
	userID := "@user:example.com"

	result := keysDiffer(existingMasterKey, keyResp, uploadReq, userID)
	if !result {
		t.Fatalf("expected keys to differ, but they did not")
	}
}

func Test_KeysDiffer_UserSigningKeyMismatch(t *testing.T) {
	existingMasterKey := fclient.CrossSigningKey{
		UserID: "@user:example.com",
		Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeMaster},
		Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("key")},
	}
	keyResp := api.QueryKeysResponse{
		UserSigningKeys: map[string]fclient.CrossSigningKey{
			"@user:example.com": {
				UserID: "@user:example.com",
				Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeUserSigning},
				Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:3": spec.Base64Bytes("existing_key")},
			},
		},
	}
	uploadReq := &crossSigningRequest{
		PerformUploadDeviceKeysRequest: api.PerformUploadDeviceKeysRequest{
			CrossSigningKeys: fclient.CrossSigningKeys{
				UserSigningKey: fclient.CrossSigningKey{
					UserID: "@user:example.com",
					Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeUserSigning},
					Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:3": spec.Base64Bytes("new_key")},
				},
			},
		},
	}
	userID := "@user:example.com"

	result := keysDiffer(existingMasterKey, keyResp, uploadReq, userID)
	if !result {
		t.Fatalf("expected keys to differ, but they did not")
	}
}

func Test_KeysDiffer_AllKeysMatch(t *testing.T) {
	existingMasterKey := fclient.CrossSigningKey{
		UserID: "@user:example.com",
		Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeMaster},
		Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("key")},
	}
	keyResp := api.QueryKeysResponse{
		SelfSigningKeys: map[string]fclient.CrossSigningKey{
			"@user:example.com": {
				UserID: "@user:example.com",
				Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeSelfSigning},
				Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:2": spec.Base64Bytes("key")},
			},
		},
		UserSigningKeys: map[string]fclient.CrossSigningKey{
			"@user:example.com": {
				UserID: "@user:example.com",
				Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeUserSigning},
				Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:3": spec.Base64Bytes("key")},
			},
		},
	}
	uploadReq := &crossSigningRequest{
		PerformUploadDeviceKeysRequest: api.PerformUploadDeviceKeysRequest{
			CrossSigningKeys: fclient.CrossSigningKeys{
				MasterKey: fclient.CrossSigningKey{
					UserID: "@user:example.com",
					Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeMaster},
					Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:1": spec.Base64Bytes("key")},
				},
				SelfSigningKey: fclient.CrossSigningKey{
					UserID: "@user:example.com",
					Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeSelfSigning},
					Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:2": spec.Base64Bytes("key")},
				},
				UserSigningKey: fclient.CrossSigningKey{
					UserID: "@user:example.com",
					Usage:  []fclient.CrossSigningKeyPurpose{fclient.CrossSigningKeyPurposeUserSigning},
					Keys:   map[gomatrixserverlib.KeyID]spec.Base64Bytes{"ed25519:3": spec.Base64Bytes("key")},
				},
			},
		},
	}
	userID := "@user:example.com"

	result := keysDiffer(existingMasterKey, keyResp, uploadReq, userID)
	if result {
		t.Fatalf("expected keys to match, but they did not")
	}
}
