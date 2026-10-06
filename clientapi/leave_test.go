// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package clientapi

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/federationapi"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
)

// TestLeaveReason covers MSC2367 for POST /rooms/{roomId}/leave: the optional
// reason ends up in the leave event, and the body itself stays optional.
func TestLeaveReason(t *testing.T) {
	alice := test.NewUser(t)
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		cfg.ClientAPI.RateLimiting.Enabled = false
		defer closeDB()
		natsInstance := jetstream.NATSInstance{}
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		routers := httputil.NewRouters()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		fsAPI := federationapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, nil, rsAPI, caches, nil, true)
		rsAPI.SetFederationAPI(fsAPI, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		rsAPI.SetUserAPI(userAPI)
		AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, nil, nil, userAPI, nil, nil, nil, caching.DisableMetrics) //nolint:contextcheck

		accessTokens := map[*test.User]userDevice{alice: {}}
		createAccessTokens(t, accessTokens, userAPI, ctx, routers)
		token := accessTokens[alice].accessToken

		leave := func(roomID, body string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(http.MethodPost, fmt.Sprintf("/_matrix/client/v3/rooms/%s/leave?access_token=%s", roomID, token), strings.NewReader(body))
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			return rec
		}
		// leaveContent returns the content of alice's membership event in the
		// room once she has left it.
		leaveContent := func(room *test.Room) gjson.Result {
			res := api.QueryCurrentStateResponse{}
			require.NoError(t, rsAPI.QueryCurrentState(ctx, &api.QueryCurrentStateRequest{
				RoomID:      room.ID,
				StateTuples: []gomatrixserverlib.StateKeyTuple{{EventType: spec.MRoomMember, StateKey: alice.ID}},
			}, &res))
			for _, ev := range res.StateEvents {
				return gjson.ParseBytes(ev.Content())
			}
			t.Fatalf("no membership event for %s in %s", alice.ID, room.ID)
			return gjson.Result{}
		}

		for name, tc := range map[string]struct {
			body       string
			wantReason string
		}{
			"with reason":   {body: `{"reason": "off to Kaer Morhen"}`, wantReason: "off to Kaer Morhen"},
			"empty object":  {body: `{}`},
			"no body":       {body: ""},
			"empty reason":  {body: `{"reason": ""}`},
			"other members": {body: `{"reason": "bye", "unrelated": 1}`, wantReason: "bye"},
		} {
			t.Run(name, func(t *testing.T) {
				room := test.NewRoom(t, alice)
				require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))

				rec := leave(room.ID, tc.body)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

				content := leaveContent(room)
				assert.Equal(t, spec.Leave, content.Get("membership").Str)
				reason := content.Get("reason")
				if tc.wantReason == "" {
					assert.False(t, reason.Exists(), content.Raw)
				} else {
					assert.Equal(t, tc.wantReason, reason.Str)
				}
			})
		}

		t.Run("invalid body", func(t *testing.T) {
			room := test.NewRoom(t, alice)
			require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))
			rec := leave(room.ID, `{"reason": 1}`)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	})
}
