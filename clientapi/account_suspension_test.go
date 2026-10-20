// Copyright 2026 Patrick Schratz
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

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/internal/transactions"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
	uapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// TestAccountSuspension covers account suspension (MSC3823) through the
// admin suspend endpoints (MSC4323).
func TestAccountSuspension(t *testing.T) {
	admin := test.NewUser(t, test.WithAccountType(uapi.AccountTypeAdmin))
	otherAdmin := test.NewUser(t, test.WithAccountType(uapi.AccountTypeAdmin))
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	charlie := test.NewUser(t)
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		cfg.ClientAPI.RateLimiting.Enabled = false
		defer closeDB()
		natsInstance := jetstream.NATSInstance{}
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		routers := httputil.NewRouters()
		AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, transactions.New(processCtx.Context()), nil, userAPI, nil, nil, nil, caching.DisableMetrics) //nolint:contextcheck
		accessTokens := map[*test.User]userDevice{admin: {}, otherAdmin: {}, alice: {}, bob: {}, charlie: {}}
		createAccessTokens(t, accessTokens, userAPI, ctx, routers)

		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(bob.ID))
		other := test.NewRoom(t, bob, test.RoomPreset(test.PresetPublicChat))
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, other.Events(), "test", "test", "test", nil, false))

		txn := 0
		do := func(method, path string, u *test.User, body any) (int, gjson.Result) {
			t.Helper()
			opts := []test.HTTPRequestOpt{test.WithQueryParams(map[string]string{"access_token": accessTokens[u].accessToken})}
			if body != nil {
				opts = append(opts, test.WithJSONBody(t, body))
			}
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, test.NewRequest(t, method, path, opts...))
			return rec.Code, gjson.ParseBytes(rec.Body.Bytes())
		}
		send := func(u *test.User, eventType string, content map[string]any) (int, gjson.Result) {
			t.Helper()
			txn++
			return do(http.MethodPut, fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/%s/txn%d", room.ID, eventType, txn), u, content)
		}
		suspendPath := func(u *test.User) string { return "/_matrix/client/v1/admin/suspend/" + u.ID }
		requireSuspended := func(code int, res gjson.Result) {
			t.Helper()
			require.Equal(t, http.StatusForbidden, code, res.Raw)
			require.Equal(t, "M_USER_SUSPENDED", res.Get("errcode").Str, res.Raw)
		}
		requireOK := func(code int, res gjson.Result) {
			t.Helper()
			require.Equal(t, http.StatusOK, code, res.Raw)
		}

		_, caps := do(http.MethodGet, "/_matrix/client/v3/capabilities", admin, nil)
		require.True(t, caps.Get(`capabilities.m\.account_moderation.suspend`).Bool(), caps.Raw)
		require.True(t, caps.Get(`capabilities.m\.account_moderation.lock`).Bool(), caps.Raw)

		code, res := do(http.MethodPut, suspendPath(otherAdmin), admin, map[string]any{"suspended": true})
		require.Equal(t, http.StatusForbidden, code, res.Raw)
		require.Equal(t, "M_FORBIDDEN", res.Get("errcode").Str)
		code, res = do(http.MethodPut, suspendPath(alice), admin, map[string]any{"suspended": "yes"})
		require.Equal(t, http.StatusBadRequest, code, res.Raw)
		code, res = do(http.MethodPut, suspendPath(alice), bob, map[string]any{"suspended": true})
		require.Equal(t, http.StatusForbidden, code, res.Raw)

		_, bobMessage := send(bob, "m.room.message", map[string]any{"msgtype": "m.text", "body": "from bob"})
		_, aliceMessage := send(alice, "m.room.message", map[string]any{"msgtype": "m.text", "body": "from alice"})
		bobMessageID := bobMessage.Get("event_id").Str
		aliceMessageID := aliceMessage.Get("event_id").Str
		require.NotEmpty(t, bobMessageID)
		require.NotEmpty(t, aliceMessageID)

		code, res = do(http.MethodPut, suspendPath(alice), admin, map[string]any{"suspended": true})
		requireOK(code, res)
		require.True(t, res.Get("suspended").Bool(), res.Raw)
		code, res = do(http.MethodGet, suspendPath(alice), admin, nil)
		requireOK(code, res)
		require.True(t, res.Get("suspended").Bool(), res.Raw)

		// Reading and logging in still work.
		requireOK(do(http.MethodGet, "/_matrix/client/v3/account/whoami", alice, nil))
		requireOK(do(http.MethodGet, fmt.Sprintf("/_matrix/client/v3/rooms/%s/state", room.ID), alice, nil))

		// Sending, joining, inviting and profile changes are refused.
		requireSuspended(send(alice, "m.room.message", map[string]any{"msgtype": "m.text", "body": "hi"}))
		requireSuspended(do(http.MethodPut, fmt.Sprintf("/_matrix/client/v3/rooms/%s/state/m.room.topic/", room.ID), alice, map[string]any{"topic": "x"}))
		requireSuspended(do(http.MethodPost, "/_matrix/client/v3/join/"+other.ID, alice, map[string]any{}))
		requireSuspended(do(http.MethodPost, fmt.Sprintf("/_matrix/client/v3/rooms/%s/invite", room.ID), alice, map[string]any{"user_id": charlie.ID}))
		requireSuspended(do(http.MethodPost, fmt.Sprintf("/_matrix/client/v3/rooms/%s/kick", room.ID), alice, map[string]any{"user_id": bob.ID}))
		requireSuspended(do(http.MethodPost, "/_matrix/client/v3/createRoom", alice, map[string]any{}))
		requireSuspended(do(http.MethodPut, "/_matrix/client/v3/profile/"+alice.ID+"/displayname", alice, map[string]any{"displayname": "new"}))

		// Alice may redact her own events, but not Bob's, although she
		// created the room.
		requireSuspended(send(alice, "m.room.redaction", map[string]any{"redacts": bobMessageID}))
		requireSuspended(do(http.MethodPost, fmt.Sprintf("/_matrix/client/v3/rooms/%s/redact/%s", room.ID, bobMessageID), alice, map[string]any{}))
		requireOK(send(alice, "m.room.redaction", map[string]any{"redacts": aliceMessageID}))

		// Other users are not affected.
		requireOK(send(bob, "m.room.message", map[string]any{"msgtype": "m.text", "body": "still here"}))

		// Leaving is allowed.
		requireOK(do(http.MethodPost, fmt.Sprintf("/_matrix/client/v3/rooms/%s/leave", room.ID), alice, map[string]any{}))

		code, res = do(http.MethodPut, suspendPath(alice), admin, map[string]any{"suspended": false})
		requireOK(code, res)
		require.False(t, res.Get("suspended").Bool(), res.Raw)
		requireOK(do(http.MethodPost, "/_matrix/client/v3/join/"+other.ID, alice, map[string]any{}))
		requireOK(do(http.MethodPut, "/_matrix/client/v3/profile/"+alice.ID+"/displayname", alice, map[string]any{"displayname": "new"}))
	})
}
