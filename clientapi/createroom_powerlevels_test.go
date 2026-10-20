// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package clientapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
)

// Public rooms leave invites to moderators, as in Synapse, while private
// chats let every member invite.
func TestCreateRoomInvitePowerLevel(t *testing.T) {
	alice := test.NewUser(t)
	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	routers := httputil.NewRouters()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
	AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, nil, nil, userAPI, nil, nil, nil, caching.DisableMetrics)
	accessTokens := map[*test.User]userDevice{alice: {}}
	createAccessTokens(t, accessTokens, userAPI, processCtx.Context(), routers)

	tests := map[string]struct {
		body       map[string]any
		wantInvite int64
	}{
		"public_chat":           {body: map[string]any{"preset": "public_chat"}, wantInvite: 50},
		"private_chat":          {body: map[string]any{"preset": "private_chat"}, wantInvite: 0},
		"trusted_private_chat":  {body: map[string]any{"preset": "trusted_private_chat"}, wantInvite: 0},
		"public_chat, override": {body: map[string]any{"preset": "public_chat", "power_level_content_override": map[string]any{"invite": 10}}, wantInvite: 10},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/createRoom",
				test.WithJSONBody(t, tc.body),
				test.WithQueryParams(map[string]string{"access_token": accessTokens[alice].accessToken})))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			roomID, err := spec.NewRoomID(gjson.GetBytes(rec.Body.Bytes(), "room_id").Str)
			require.NoError(t, err)
			ev, err := rsAPI.CurrentStateEvent(context.Background(), *roomID, spec.MRoomPowerLevels, "")
			require.NoError(t, err)
			require.NotNil(t, ev)
			assert.Equal(t, tc.wantInvite, gjson.GetBytes(ev.Content(), "invite").Int())
		})
	}
}

// Room versions with privileged creators (MSC4289) forbid creators in the
// power levels' users map.
func TestCreateRoomOverrideWithCreatorsInUsers(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	charlie := test.NewUser(t)
	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	routers := httputil.NewRouters()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
	AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, nil, nil, userAPI, nil, nil, nil, caching.DisableMetrics)
	accessTokens := map[*test.User]userDevice{alice: {}}
	createAccessTokens(t, accessTokens, userAPI, processCtx.Context(), routers)

	tests := map[string]struct {
		body      map[string]any
		wantUsers map[string]int64
		wantCode  int
	}{
		"v12, creator in users": {
			body: map[string]any{
				"room_version": "12",
				"preset":       "public_chat",
				"visibility":   "private",
				"power_level_content_override": map[string]any{
					"invite": 100,
					"users":  map[string]any{alice.ID: 100, bob.ID: 50},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		"v12, additional creator in users": {
			body: map[string]any{
				"room_version":     "12",
				"creation_content": map[string]any{"additional_creators": []string{charlie.ID}},
				"power_level_content_override": map[string]any{
					"users": map[string]any{alice.ID: 100, charlie.ID: 100, bob.ID: 50},
				},
			},
			wantCode: http.StatusBadRequest,
		},
		"v10, creator in users is kept": {
			body: map[string]any{
				"room_version": "10",
				"power_level_content_override": map[string]any{
					"users": map[string]any{alice.ID: 100, bob.ID: 50},
				},
			},
			wantUsers: map[string]int64{alice.ID: 100, bob.ID: 50},
			wantCode:  http.StatusOK,
		},
	}
	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, test.NewRequest(t, http.MethodPost, "/_matrix/client/v3/createRoom",
				test.WithJSONBody(t, tc.body),
				test.WithQueryParams(map[string]string{"access_token": accessTokens[alice].accessToken})))
			wantCode := tc.wantCode
			if wantCode == 0 {
				wantCode = http.StatusOK
			}
			require.Equal(t, wantCode, rec.Code, rec.Body.String())
			if rec.Code != http.StatusOK {
				return
			}
			roomID, err := spec.NewRoomID(gjson.GetBytes(rec.Body.Bytes(), "room_id").Str)
			require.NoError(t, err)
			ev, err := rsAPI.CurrentStateEvent(context.Background(), *roomID, spec.MRoomPowerLevels, "")
			require.NoError(t, err)
			require.NotNil(t, ev)
			gotUsers := map[string]int64{}
			gjson.GetBytes(ev.Content(), "users").ForEach(func(k, v gjson.Result) bool {
				gotUsers[k.Str] = v.Int()
				return true
			})
			assert.Equal(t, tc.wantUsers, gotUsers)
		})
	}
}
