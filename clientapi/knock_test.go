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
	"codefloe.com/pat-s/zendrite/syncapi"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
)

// TestKnock covers knocking on a room this server is in: the knock reaches
// the room, /sync reports it under rooms.knock, and it ends either with an
// invite or with the knocker rescinding it.
func TestKnock(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	ctx := context.Background()

	knockRoom := func(version gomatrixserverlib.RoomVersion) *test.Room {
		room := test.NewRoom(t, alice, test.RoomVersion(version), test.RoomPreset(test.PresetPrivateChat))
		room.CreateAndInsert(t, alice, spec.MRoomJoinRules, map[string]any{"join_rule": spec.Knock}, test.WithStateKey(""))
		room.CreateAndInsert(t, alice, spec.MRoomName, map[string]any{"name": "Kaer Morhen"}, test.WithStateKey(""))
		return room
	}
	answeredRoom := knockRoom(gomatrixserverlib.RoomVersionV12)
	rescindedRoom := knockRoom(gomatrixserverlib.RoomVersionV10)
	inviteOnlyRoom := test.NewRoom(t, alice, test.RoomPreset(test.PresetPrivateChat))

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
		syncapi.AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, userAPI, rsAPI, caches, caching.DisableMetrics)
		AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, nil, nil, userAPI, nil, nil, nil, caching.DisableMetrics) //nolint:contextcheck

		accessTokens := map[*test.User]userDevice{alice: {}, bob: {}}
		createAccessTokens(t, accessTokens, userAPI, ctx, routers)

		for _, room := range []*test.Room{answeredRoom, rescindedRoom, inviteOnlyRoom} {
			require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))
		}

		do := func(user *test.User, method, path string, body map[string]any) *httptest.ResponseRecorder {
			opts := []test.HTTPRequestOpt{test.WithQueryParams(map[string]string{"access_token": accessTokens[user].accessToken})}
			if body != nil {
				opts = append(opts, test.WithJSONBody(t, body))
			}
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, test.NewRequest(t, method, path, opts...))
			return rec
		}
		// syncUntil polls /sync for bob from since until check accepts the response.
		syncUntil := func(since string, check func(gjson.Result) bool) (gjson.Result, string) {
			var last gjson.Result
			deadline := time.Now().Add(10 * time.Second)
			for time.Now().Before(deadline) {
				params := map[string]string{"access_token": accessTokens[bob].accessToken, "timeout": "0"}
				if since != "" {
					params["since"] = since
				}
				rec := httptest.NewRecorder()
				routers.Client.ServeHTTP(rec, test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/sync", test.WithQueryParams(params)))
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				last = gjson.ParseBytes(rec.Body.Bytes())
				if check(last) {
					return last, last.Get("next_batch").Str
				}
				time.Sleep(50 * time.Millisecond)
			}
			t.Fatalf("sync never matched, last response: %s", last.Raw)
			return last, ""
		}
		roomKey := func(section, roomID string) string {
			return fmt.Sprintf("rooms.%s.%s", section, gjson.Escape(roomID))
		}

		// Knocking on a room that does not allow it is refused.
		rec := do(bob, http.MethodPost, "/_matrix/client/v3/knock/"+inviteOnlyRoom.ID, map[string]any{})
		assert.Equal(t, http.StatusForbidden, rec.Code, rec.Body.String())

		for _, room := range []*test.Room{answeredRoom, rescindedRoom} {
			rec = do(bob, http.MethodPost, "/_matrix/client/v3/knock/"+room.ID, map[string]any{"reason": "let me in"})
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Equal(t, room.ID, gjson.GetBytes(rec.Body.Bytes(), "room_id").Str)
		}

		sync, since := syncUntil("", func(res gjson.Result) bool {
			return res.Get(roomKey("knock", answeredRoom.ID)).Exists() && res.Get(roomKey("knock", rescindedRoom.ID)).Exists()
		})
		for _, room := range []*test.Room{answeredRoom, rescindedRoom} {
			knockState := sync.Get(roomKey("knock", room.ID) + ".knock_state.events").Array()
			byType := map[string]gjson.Result{}
			for _, ev := range knockState {
				byType[ev.Get("type").Str] = ev
			}
			// MSC4311 makes the create event mandatory in stripped state.
			assert.Contains(t, byType, spec.MRoomCreate, room.Version)
			assert.Equal(t, "Kaer Morhen", byType[spec.MRoomName].Get("content.name").Str, room.Version)
			assert.Equal(t, spec.Knock, byType[spec.MRoomMember].Get("content.membership").Str, room.Version)
			assert.Equal(t, "let me in", byType[spec.MRoomMember].Get("content.reason").Str, room.Version)
			assert.False(t, byType[spec.MRoomCreate].Get("signatures").Exists(), "knock_state must be stripped")
			assert.False(t, sync.Get(roomKey("invite", room.ID)).Exists(), room.Version)
			assert.False(t, sync.Get(roomKey("join", room.ID)).Exists(), room.Version)
		}

		// Sliding sync lists knocked rooms with their stripped state, but not
		// as invites.
		rec = do(bob, http.MethodPost, "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync", map[string]any{
			"lists": map[string]any{
				"all":     map[string]any{"ranges": [][]int{{0, 20}}, "timeline_limit": 0},
				"invites": map[string]any{"ranges": [][]int{{0, 20}}, "timeline_limit": 0, "filters": map[string]any{"is_invite": true}},
			},
		})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		sliding := gjson.ParseBytes(rec.Body.Bytes())
		assert.Zero(t, sliding.Get("lists.invites.count").Int(), sliding.Get("lists").Raw)
		for _, room := range []*test.Room{answeredRoom, rescindedRoom} {
			roomData := sliding.Get("rooms." + gjson.Escape(room.ID))
			require.True(t, roomData.Exists(), "knocked room %s missing from sliding sync: %s", room.ID, sliding.Raw)
			assert.False(t, roomData.Get("invite_state").Exists(), "knocked room must not look like an invite")
			var types []string
			for _, ev := range roomData.Get("stripped_state").Array() {
				types = append(types, ev.Get("type").Str)
			}
			assert.Contains(t, types, spec.MRoomCreate, room.Version)
			assert.Contains(t, types, spec.MRoomMember, room.Version)
		}

		// Alice answers one knock with an invite: the room moves from knock
		// to invite without passing through leave.
		rec = do(alice, http.MethodPost, fmt.Sprintf("/_matrix/client/v3/rooms/%s/invite", answeredRoom.ID), map[string]any{"user_id": bob.ID})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		sync, since = syncUntil(since, func(res gjson.Result) bool {
			return res.Get(roomKey("invite", answeredRoom.ID)).Exists()
		})
		assert.False(t, sync.Get(roomKey("leave", answeredRoom.ID)).Exists(), "an answered knock is not a leave")
		assert.False(t, sync.Get(roomKey("knock", answeredRoom.ID)).Exists())

		// Bob rescinds the other knock.
		rec = do(bob, http.MethodPost, fmt.Sprintf("/_matrix/client/v3/rooms/%s/leave", rescindedRoom.ID), map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		syncUntil(since, func(res gjson.Result) bool {
			return res.Get(roomKey("leave", rescindedRoom.ID)).Exists()
		})
		sync, _ = syncUntil("", func(gjson.Result) bool { return true })
		assert.False(t, sync.Get(roomKey("knock", rescindedRoom.ID)).Exists(), "a rescinded knock must not come back on initial sync")
	})
}
