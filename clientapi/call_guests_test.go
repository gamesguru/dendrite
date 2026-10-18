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
	"net/url"
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
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/internal/transactions"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/syncapi"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
	uapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// TestCallGuests drives a call guest through what Element Call does with one,
// and checks that everything else stays out of reach.
func TestCallGuests(t *testing.T) {
	alice := test.NewUser(t)
	guest := test.NewUser(t, test.WithAccountType(uapi.AccountTypeCallGuest))

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := context.Background()
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cfg.ClientAPI.RateLimiting.Enabled = false
		cfg.ClientAPI.DelayedEvents.MaxDelay = time.Hour
		cfg.ClientAPI.DelayedEvents.ManagementRateLimiting.Enabled = false
		natsInstance := jetstream.NATSInstance{}
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		routers := httputil.NewRouters()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		fsAPI := federationapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, nil, rsAPI, caches, nil, true)
		rsAPI.SetFederationAPI(fsAPI, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		rsAPI.SetUserAPI(userAPI)
		syncapi.AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, userAPI, rsAPI, caches, caching.DisableMetrics, nil, nil)
		AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, transactions.New(processCtx.Context()), nil, userAPI, nil, nil, nil, caching.DisableMetrics)

		accessTokens := map[*test.User]userDevice{alice: {}, guest: {}}
		createAccessTokens(t, accessTokens, userAPI, ctx, routers)

		// A public room like the ones Element Call creates: anyone may set
		// any state, so only the call guest rules keep the guest from
		// changing what the room is.
		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		powerLevels := eventutil.InitialPowerLevelsContent(gomatrixserverlib.MustGetRoomVersion(room.Version), alice.ID)
		powerLevels.StateDefault = 0
		powerLevels.Events = map[string]int64{}
		room.CreateAndInsert(t, alice, spec.MRoomPowerLevels, powerLevels, test.WithStateKey(""))
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))

		do := func(user *test.User, method, path, body string) *httptest.ResponseRecorder {
			t.Helper()
			req := httptest.NewRequest(method, path, strings.NewReader(body))
			req.Header.Set("Authorization", "Bearer "+accessTokens[user].accessToken)
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			return rec
		}
		expect := func(rec *httptest.ResponseRecorder, code int, errcode string) {
			t.Helper()
			require.Equal(t, code, rec.Code, rec.Body.String())
			if errcode != "" {
				require.Equal(t, errcode, gjson.GetBytes(rec.Body.Bytes(), "errcode").Str, rec.Body.String())
			}
		}
		roomPath := "/_matrix/client/v3/rooms/" + url.PathEscape(room.ID)
		statePath := func(eventType, stateKey string) string {
			return roomPath + "/state/" + eventType + "/" + url.PathEscape(stateKey)
		}

		t.Run("startup endpoints", func(t *testing.T) {
			expect(do(guest, http.MethodGet, "/_matrix/client/v3/account/whoami", ""), http.StatusOK, "")
			expect(do(guest, http.MethodGet, "/_matrix/client/v3/capabilities", ""), http.StatusOK, "")
			expect(do(guest, http.MethodGet, "/_matrix/client/v3/pushrules/", ""), http.StatusOK, "")
			expect(do(guest, http.MethodGet, "/_matrix/client/v3/devices", ""), http.StatusOK, "")
			expect(do(guest, http.MethodGet, "/_matrix/client/unstable/org.matrix.msc4143/rtc/transports", ""), http.StatusOK, "")
			expect(do(guest, http.MethodPost, "/_matrix/client/v3/user/"+url.PathEscape(guest.ID)+"/openid/request_token", "{}"), http.StatusOK, "")
			expect(do(guest, http.MethodPost, "/_matrix/client/v3/user/"+url.PathEscape(guest.ID)+"/filter", "{}"), http.StatusOK, "")
			expect(do(guest, http.MethodPut, "/_matrix/client/v3/user/"+url.PathEscape(guest.ID)+"/account_data/com.example", "{}"), http.StatusOK, "")
			expect(do(guest, http.MethodGet, "/_matrix/client/v3/sync?timeout=0", ""), http.StatusOK, "")
			// Push rules can be read, not changed.
			expect(do(guest, http.MethodPut, "/_matrix/client/v3/pushrules/global/override/com.example", `{"actions":[]}`), http.StatusForbidden, "M_FORBIDDEN")
		})

		t.Run("profile", func(t *testing.T) {
			profilePath := "/_matrix/client/v3/profile/" + url.PathEscape(guest.ID)
			expect(do(guest, http.MethodPut, profilePath+"/displayname", `{"displayname":"Guest"}`), http.StatusOK, "")
			expect(do(guest, http.MethodPut, profilePath+"/avatar_url", `{"avatar_url":"mxc://test/abc"}`), http.StatusForbidden, "M_FORBIDDEN")
			// Normal users are unaffected.
			expect(do(alice, http.MethodPut, "/_matrix/client/v3/profile/"+url.PathEscape(alice.ID)+"/avatar_url", `{"avatar_url":"mxc://test/abc"}`), http.StatusOK, "")
		})

		t.Run("joins", func(t *testing.T) {
			expect(do(guest, http.MethodPost, "/_matrix/client/v3/join/"+url.PathEscape(room.ID), "{}"), http.StatusOK, "")
			// Rooms that only another server could let the guest into.
			for _, path := range []string{
				"/_matrix/client/v3/join/" + url.PathEscape("!unknown:remote.example") + "?via=remote.example",
				"/_matrix/client/v3/join/" + url.PathEscape("#call:remote.example"),
				"/_matrix/client/v3/rooms/" + url.PathEscape("!unknown:remote.example") + "/join?via=remote.example",
			} {
				rec := do(guest, http.MethodPost, path, "{}")
				expect(rec, http.StatusForbidden, "M_FORBIDDEN")
				assert.Contains(t, gjson.GetBytes(rec.Body.Bytes(), "error").Str, "only join rooms on this server", path)
			}
		})

		t.Run("events", func(t *testing.T) {
			callMember := `{"application":"m.call","call_id":"","device_id":"` + accessTokens[guest].deviceID + `","focus_active":{"type":"livekit"},"foci_preferred":[]}`
			expect(do(guest, http.MethodPut, statePath("org.matrix.msc3401.call.member", "_"+guest.ID+"_"+accessTokens[guest].deviceID), callMember), http.StatusOK, "")
			expect(do(guest, http.MethodPut, roomPath+"/send/m.room.message/txn1", `{"msgtype":"m.text","body":"hello"}`), http.StatusOK, "")
			expect(do(guest, http.MethodGet, roomPath+"/state", ""), http.StatusOK, "")
			expect(do(guest, http.MethodGet, roomPath+"/messages?dir=b", ""), http.StatusOK, "")
			expect(do(guest, http.MethodGet, roomPath+"/joined_members", ""), http.StatusOK, "")
			// A delayed leave of the call, as Element Call schedules.
			rec := do(guest, http.MethodPut, statePath("org.matrix.msc3401.call.member", "_"+guest.ID+"_"+accessTokens[guest].deviceID)+"?org.matrix.msc4140.delay=60000", "{}")
			expect(rec, http.StatusOK, "")
			delayID := gjson.GetBytes(rec.Body.Bytes(), "delay_id").Str
			expect(do(guest, http.MethodPost, "/_matrix/client/unstable/org.matrix.msc4140/delayed_events/"+delayID+"/restart", "{}"), http.StatusOK, "")

			// Its own membership, such as a new display name, is fine.
			expect(do(guest, http.MethodPut, statePath(spec.MRoomMember, guest.ID), `{"membership":"join","displayname":"Guest"}`), http.StatusOK, "")

			for _, tc := range []struct{ eventType, stateKey, content string }{
				{spec.MRoomMember, alice.ID, `{"membership":"leave"}`},
				{spec.MRoomMember, "@someone:test", `{"membership":"invite"}`},
				{spec.MRoomMember, "@someone:test", `{"membership":"ban"}`},
				{spec.MRoomPowerLevels, "", `{"users":{"` + guest.ID + `":100}}`},
				{spec.MRoomJoinRules, "", `{"join_rule":"invite"}`},
				{spec.MRoomHistoryVisibility, "", `{"history_visibility":"world_readable"}`},
				{spec.MRoomEncryption, "", `{"algorithm":"m.megolm.v1.aes-sha2"}`},
				{spec.MRoomCanonicalAlias, "", `{}`},
				{"m.room.server_acl", "", `{"allow":["*"]}`},
				{"m.room.tombstone", "", `{"body":"gone","replacement_room":"!other:test"}`},
			} {
				for _, query := range []string{"", "?org.matrix.msc4140.delay=1000"} {
					// Refused by the call guest rules, not by the room's
					// authorisation rules; nor may it be scheduled for later.
					rec := do(guest, http.MethodPut, statePath(tc.eventType, tc.stateKey)+query, tc.content)
					expect(rec, http.StatusForbidden, "M_FORBIDDEN")
					assert.Contains(t, gjson.GetBytes(rec.Body.Bytes(), "error").Str, "Call guest", tc.eventType)
				}
			}
			// The same events stay open to normal users.
			expect(do(alice, http.MethodPut, statePath(spec.MRoomHistoryVisibility, ""), `{"history_visibility":"shared"}`), http.StatusOK, "")
		})

		t.Run("denied endpoints", func(t *testing.T) {
			for _, tc := range []struct{ method, path, body string }{
				{http.MethodPost, "/_matrix/client/v3/createRoom", `{"preset":"private_chat","is_direct":true,"invite":["` + alice.ID + `"]}`},
				{http.MethodPost, roomPath + "/invite", `{"user_id":"` + alice.ID + `"}`},
				{http.MethodPost, roomPath + "/kick", `{"user_id":"` + alice.ID + `"}`},
				{http.MethodPost, roomPath + "/ban", `{"user_id":"` + alice.ID + `"}`},
				{http.MethodPost, roomPath + "/upgrade", `{"new_version":"10"}`},
				{http.MethodPost, "/_matrix/client/v3/knock/" + url.PathEscape(room.ID), "{}"},
				{http.MethodPost, "/_matrix/client/v3/user_directory/search", `{"search_term":"a"}`},
				{http.MethodPut, "/_matrix/client/v3/directory/room/" + url.PathEscape("#alias:test"), `{"room_id":"` + room.ID + `"}`},
				{http.MethodPut, "/_matrix/client/v3/directory/list/room/" + url.PathEscape(room.ID), `{"visibility":"public"}`},
				{http.MethodPost, "/_matrix/client/v3/account/password", "{}"},
				{http.MethodPost, "/_matrix/client/v3/room_keys/version", "{}"},
				{http.MethodPost, "/_matrix/client/v3/pushers/set", "{}"},
				{http.MethodGet, "/_matrix/client/v3/notifications", ""},
				{http.MethodGet, "/_matrix/client/v1/rooms/" + url.PathEscape(room.ID) + "/hierarchy", ""},
				{http.MethodPost, roomPath + "/report/$event", "{}"},
			} {
				rec := do(guest, tc.method, tc.path, tc.body)
				assert.Equal(t, http.StatusForbidden, rec.Code, fmt.Sprintf("%s %s: %s", tc.method, tc.path, rec.Body.String()))
				assert.Equal(t, "M_FORBIDDEN", gjson.GetBytes(rec.Body.Bytes(), "errcode").Str, tc.path)
				assert.Contains(t, gjson.GetBytes(rec.Body.Bytes(), "error").Str, "Call guest", tc.path)
			}
		})

		t.Run("leave", func(t *testing.T) {
			expect(do(guest, http.MethodPost, roomPath+"/leave", "{}"), http.StatusOK, "")
		})
	})
}
