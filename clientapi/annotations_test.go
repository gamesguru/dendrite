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

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
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
)

// A user may not send the same annotation twice on one event, unless the
// first one has been redacted (MSC2677).
func TestDuplicateAnnotation(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat))
	room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(bob.ID))
	target := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"msgtype": "m.text", "body": "hi"})
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
		require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))

		routers := httputil.NewRouters()
		AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, transactions.New(processCtx.Context()), nil, userAPI, nil, nil, nil, caching.DisableMetrics) //nolint:contextcheck
		// Another process, with its own transaction cache, as after an HA takeover.
		other := httputil.NewRouters()
		AddPublicRoutes(processCtx, other, cfg, &natsInstance, nil, rsAPI, nil, transactions.New(processCtx.Context()), nil, userAPI, nil, nil, nil, caching.DisableMetrics) //nolint:contextcheck
		accessTokens := map[*test.User]userDevice{alice: {}, bob: {}}
		createAccessTokens(t, accessTokens, userAPI, ctx, routers)

		put := func(routers httputil.Routers, user *test.User, path string, body map[string]any) *httptest.ResponseRecorder {
			t.Helper()
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, test.NewRequest(t, http.MethodPut,
				fmt.Sprintf("/_matrix/client/v3/rooms/%s/%s", room.ID, path),
				test.WithJSONBody(t, body),
				test.WithQueryParams(map[string]string{"access_token": accessTokens[user].accessToken})))
			return rec
		}
		react := func(routers httputil.Routers, user *test.User, eventType, key, txnID string) *httptest.ResponseRecorder {
			t.Helper()
			return put(routers, user, fmt.Sprintf("send/%s/%s", eventType, txnID), map[string]any{
				"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": target.EventID(), "key": key},
			})
		}
		requireOK := func(rec *httptest.ResponseRecorder) string {
			t.Helper()
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			return gjson.GetBytes(rec.Body.Bytes(), "event_id").Str
		}
		requireDuplicate := func(rec *httptest.ResponseRecorder) {
			t.Helper()
			require.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
			assert.Equal(t, "M_DUPLICATE_ANNOTATION", gjson.GetBytes(rec.Body.Bytes(), "errcode").Str)
		}

		first := requireOK(react(routers, alice, "m.reaction", "👍", "a1"))
		requireDuplicate(react(routers, alice, "m.reaction", "👍", "a2"))
		requireDuplicate(react(other, alice, "m.reaction", "👍", "a2"))
		requireOK(react(routers, alice, "m.reaction", "👎", "a3"))
		requireOK(react(routers, bob, "m.reaction", "👍", "b1"))
		requireOK(react(routers, alice, "m.room.message", "👍", "a4"))

		// A retry of the transaction that sent the annotation is answered
		// with it, also by a process that has not seen the transaction.
		assert.Equal(t, first, requireOK(react(other, alice, "m.reaction", "👍", "a1")))

		// Once redacted, the annotation may be sent again.
		requireOK(put(routers, alice, fmt.Sprintf("redact/%s/r1", first), map[string]any{}))
		requireOK(react(routers, alice, "m.reaction", "👍", "a5"))
		requireDuplicate(react(routers, alice, "m.reaction", "👍", "a6"))
	})
}
