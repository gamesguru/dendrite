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

	"github.com/gamesguru/gomatrixserverlib"
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

// A redaction sent through /send carries the redacted event ID in
// content.redacts. MSC4169 copies it to the top level for room versions
// before v11, and MSC2174 makes v11+ rooms read it from content. Such a
// redaction gets the same permission check as /redact.
func TestSendRedaction(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
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
		accessTokens := map[*test.User]userDevice{alice: {}, bob: {}}
		createAccessTokens(t, accessTokens, userAPI, ctx, routers)

		for _, ver := range []gomatrixserverlib.RoomVersion{gomatrixserverlib.RoomVersionV10, gomatrixserverlib.RoomVersionV11} {
			t.Run("v"+string(ver), func(t *testing.T) {
				room := test.NewRoom(t, alice, test.RoomVersion(ver), test.RoomPreset(test.PresetPublicChat))
				room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(bob.ID))
				require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))

				send := func(user *test.User, eventType, txnID string, content map[string]any, wantCode int) string {
					t.Helper()
					rec := httptest.NewRecorder()
					routers.Client.ServeHTTP(rec, test.NewRequest(t, http.MethodPut,
						fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/%s/%s", room.ID, eventType, txnID),
						test.WithJSONBody(t, content),
						test.WithQueryParams(map[string]string{"access_token": accessTokens[user].accessToken})))
					require.Equal(t, wantCode, rec.Code, rec.Body.String())
					return gjson.GetBytes(rec.Body.Bytes(), "event_id").Str
				}

				messageID := send(alice, "m.room.message", "txn-msg", map[string]any{"msgtype": "m.text", "body": "hello"}, http.StatusOK)

				// Bob is on the same server but lacks the redact power level,
				// so he must not redact Alice's message.
				send(bob, "m.room.redaction", "txn-bob", map[string]any{"redacts": messageID}, http.StatusForbidden)
				send(alice, "m.room.redaction", "txn-empty", map[string]any{"reason": "no target"}, http.StatusBadRequest)
				message := api.GetEvent(ctx, rsAPI, room.ID, messageID)
				require.NotNil(t, message)
				require.Equal(t, "hello", gjson.GetBytes(message.Content(), "body").Str)

				redactionID := send(alice, "m.room.redaction", "txn-redact", map[string]any{"redacts": messageID, "reason": "oops"}, http.StatusOK)

				redaction := api.GetEvent(ctx, rsAPI, room.ID, redactionID)
				require.NotNil(t, redaction)
				assert.Equal(t, messageID, redaction.Redacts())
				assert.Equal(t, messageID, gjson.GetBytes(redaction.JSON(), "redacts").Str)
				assert.Equal(t, messageID, gjson.GetBytes(redaction.Content(), "redacts").Str)

				message = api.GetEvent(ctx, rsAPI, room.ID, messageID)
				require.NotNil(t, message)
				assert.False(t, gjson.GetBytes(message.Content(), "body").Exists(), "message was not redacted: %s", message.JSON())
			})
		}
	})
}
