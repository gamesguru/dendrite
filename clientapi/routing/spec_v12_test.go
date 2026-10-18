// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	clientapi "codefloe.com/pat-s/zendrite/clientapi/api"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	roomserver "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type validityUserAPI struct {
	userapi.ClientUserAPI
	token *clientapi.RegistrationToken
	err   error
}

func (a *validityUserAPI) PerformAdminGetRegistrationToken(context.Context, string) (*clientapi.RegistrationToken, error) {
	return a.token, a.err
}

func TestRegistrationTokenValidity(t *testing.T) {
	future, past := time.Now().Add(time.Hour).UnixMilli(), time.Now().Add(-time.Hour).UnixMilli()
	var one int32 = 1
	for _, tc := range []struct {
		name                    string
		token                   *clientapi.RegistrationToken
		err                     error
		query                   string
		disabled, requiresToken bool
		code                    int
		valid                   bool
	}{
		{name: "unlimited", token: &clientapi.RegistrationToken{}, query: "?token=valid", code: http.StatusOK, valid: true},
		{name: "future", token: &clientapi.RegistrationToken{ExpiryTime: &future}, query: "?token=valid", code: http.StatusOK, valid: true},
		{name: "expired", token: &clientapi.RegistrationToken{ExpiryTime: &past}, query: "?token=valid", code: http.StatusOK},
		{name: "pending", token: &clientapi.RegistrationToken{UsesAllowed: &one, Pending: &one}, query: "?token=valid", code: http.StatusOK},
		{name: "completed", token: &clientapi.RegistrationToken{UsesAllowed: &one, Completed: &one}, query: "?token=valid", code: http.StatusOK},
		{name: "unknown", err: sql.ErrNoRows, query: "?token=unknown", code: http.StatusOK},
		{name: "storage failure", err: errors.New("storage unavailable"), query: "?token=valid", code: http.StatusInternalServerError},
		{name: "missing token", code: http.StatusBadRequest},
		{name: "disabled", disabled: true, query: "?token=valid", code: http.StatusForbidden},
		{name: "token registration while disabled", disabled: true, requiresToken: true, token: &clientapi.RegistrationToken{}, query: "?token=valid", code: http.StatusOK, valid: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v1/register/m.login.registration_token/validity"+tc.query, nil)
			res := RegistrationTokenValidity(req, &validityUserAPI{token: tc.token, err: tc.err}, &config.ClientAPI{RegistrationDisabled: tc.disabled, RegistrationRequiresToken: tc.requiresToken})
			require.Equal(t, tc.code, res.Code)
			if res.Code == http.StatusOK {
				require.Equal(t, map[string]bool{"valid": tc.valid}, res.JSON)
			}
		})
	}
}

type uploadFallbackAPI struct {
	userapi.ClientKeyAPI
	t *testing.T
}

func (a *uploadFallbackAPI) PerformUploadKeys(_ context.Context, req *userapi.PerformUploadKeysRequest, res *userapi.PerformUploadKeysResponse) error {
	require.Equal(a.t, "@alice:local.test", req.UserID)
	require.Equal(a.t, "DEVICE", req.DeviceID)
	require.Len(a.t, req.FallbackKeys, 1)
	require.Equal(a.t, req.UserID, req.FallbackKeys[0].UserID)
	require.Equal(a.t, req.DeviceID, req.FallbackKeys[0].DeviceID)
	require.JSONEq(a.t, `{"key":"fallback","fallback":true}`, string(req.FallbackKeys[0].KeyJSON["signed_curve25519:key"]))
	res.OneTimeKeyCounts = []userapi.OneTimeKeysCount{{KeyCount: map[string]int{}}}
	return nil
}

func TestUploadFallbackKeys(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/keys/upload", strings.NewReader(`{"fallback_keys":{"signed_curve25519:key":{"key":"fallback","fallback":true}}}`))
	res := UploadKeys(req, &uploadFallbackAPI{t: t}, &userapi.Device{UserID: "@alice:local.test", ID: "DEVICE"})
	require.Equal(t, http.StatusOK, res.Code)
}

type aliasRoomserver struct {
	roomserver.ClientRoomserverAPI
	aliases  map[string]string
	previous *types.HeaderedEvent
}

func (a *aliasRoomserver) QueryCurrentState(_ context.Context, _ *roomserver.QueryCurrentStateRequest, res *roomserver.QueryCurrentStateResponse) error {
	res.StateEvents = map[gomatrixserverlib.StateKeyTuple]*types.HeaderedEvent{{EventType: spec.MRoomCanonicalAlias, StateKey: ""}: a.previous}
	return nil
}

func (a *aliasRoomserver) GetRoomIDForAlias(_ context.Context, req *roomserver.GetRoomIDForAliasRequest, res *roomserver.GetRoomIDForAliasResponse) error {
	res.RoomID = a.aliases[req.Alias]
	return nil
}

type aliasFederation struct {
	fclient.FederationClient
	t      *testing.T
	roomID string
	err    error
}

func (f *aliasFederation) LookupRoomAlias(_ context.Context, origin, remote spec.ServerName, alias string) (fclient.RespDirectory, error) {
	require.Equal(f.t, spec.ServerName("local.test"), origin)
	require.Equal(f.t, spec.ServerName("remote.test"), remote)
	require.Equal(f.t, "#remote:remote.test", alias)
	return fclient.RespDirectory{RoomID: f.roomID}, f.err
}

func TestCanonicalAliasValidation(t *testing.T) {
	var cfg config.Zendrite
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	cfg.Global.ServerName = "local.test"
	const roomID = "!room:local.test"
	for _, tc := range []struct {
		name       string
		content    roomserver.AliasEvent
		remoteRoom string
		remoteErr  error
		bad        bool
	}{
		{name: "clear"},
		{name: "local", content: roomserver.AliasEvent{Alias: "#local:local.test"}},
		{name: "duplicate", content: roomserver.AliasEvent{Alias: "#local:local.test", AltAliases: []string{"#local:local.test"}}},
		{name: "remote", content: roomserver.AliasEvent{Alias: "#remote:remote.test"}, remoteRoom: roomID},
		{name: "only alternative", content: roomserver.AliasEvent{AltAliases: []string{"#remote:remote.test"}}, remoteRoom: roomID},
		{name: "missing alternative", content: roomserver.AliasEvent{AltAliases: []string{"#missing:local.test"}}, bad: true},
		{name: "wrong remote room", content: roomserver.AliasEvent{Alias: "#remote:remote.test"}, remoteRoom: "!other:remote.test", bad: true},
		{name: "remote unavailable", content: roomserver.AliasEvent{Alias: "#remote:remote.test"}, remoteErr: errors.New("unavailable"), bad: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rs := &aliasRoomserver{aliases: map[string]string{"#local:local.test": roomID}}
			res := validateCanonicalAliases(t.Context(), tc.content, roomID, &cfg.ClientAPI, rs, &aliasFederation{t: t, roomID: tc.remoteRoom, err: tc.remoteErr})
			if tc.bad {
				require.NotNil(t, res)
				matrixErr, ok := res.JSON.(spec.MatrixError)
				require.True(t, ok)
				require.Equal(t, spec.ErrorBadAlias, matrixErr.ErrCode)
			} else {
				require.Nil(t, res)
			}
		})
	}
	// An alias retained from the old state must not be resolved again.
	event, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10).NewEventFromTrustedJSON([]byte(`{"room_id":"!room:local.test","sender":"@alice:local.test","type":"m.room.canonical_alias","state_key":"","content":{"alias":"#removed:local.test","alt_aliases":["#remote:remote.test"]}}`), false)
	require.NoError(t, err)
	rs := &aliasRoomserver{previous: &types.HeaderedEvent{PDU: event}}
	require.Nil(t, validateCanonicalAliases(t.Context(), roomserver.AliasEvent{AltAliases: []string{"#removed:local.test", "#remote:remote.test"}}, roomID, &cfg.ClientAPI, rs, nil))
}

func TestRemoteProfileForbidden(t *testing.T) {
	var cfg config.Zendrite
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	fed := &profileFederation{t: t, err: spec.HTTPError{Code: http.StatusForbidden}}
	req := httptest.NewRequest(http.MethodGet, "/profile/@bob:remote.test", nil)
	responses := []int{GetProfile(req, nil, &cfg.ClientAPI, "@bob:remote.test", nil, fed).Code, GetProfileField(req, nil, &cfg.ClientAPI, "@bob:remote.test", "displayname", nil, fed).Code, GetProfileField(req, nil, &cfg.ClientAPI, "@bob:remote.test", "avatar_url", nil, fed).Code}
	for _, code := range responses {
		require.Equal(t, http.StatusForbidden, code)
	}
}

func TestRegistrationTokenValidityRoute(t *testing.T) {
	for _, delegated := range []bool{false, true} {
		t.Run(map[bool]string{false: "native", true: "delegated"}[delegated], func(t *testing.T) {
			var cfg config.Zendrite
			cfg.Defaults(config.DefaultOpts{})
			cfg.ClientAPI.RegistrationDisabled = false
			cfg.ClientAPI.RegistrationRequiresToken = true
			cfg.ClientAPI.RateLimiting.Threshold = 1
			cfg.ClientAPI.RateLimiting.CooloffMS = time.Hour.Milliseconds()
			if delegated {
				cfg.MSCs.MSCs = []string{"msc3861"}
			}
			routers := httputil.NewRouters()
			Setup(routers, &cfg, nil, nil, &validityUserAPI{token: &clientapi.RegistrationToken{}}, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics, t.Context())
			path := "/_matrix/client/v1/register/m.login.registration_token/validity?token=valid"
			req := httptest.NewRequest(http.MethodGet, path, nil)
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			if delegated {
				require.Equal(t, http.StatusForbidden, rec.Code)
			} else {
				require.Equal(t, http.StatusOK, rec.Code)
				require.JSONEq(t, `{"valid":true}`, rec.Body.String())
			}
			rec = httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			require.Equal(t, http.StatusTooManyRequests, rec.Code)
		})
	}
}
