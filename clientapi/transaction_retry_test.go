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

// A retried transaction that reaches a process which has not seen it, as
// after an HA takeover, must be answered with the event the first attempt
// stored instead of storing a second event (#398).
func TestTransactionRetryAcrossProcesses(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
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

		// Two client APIs stand in for the former and the new active
		// process: each has its own in-memory transaction cache.
		first := httputil.NewRouters()
		AddPublicRoutes(processCtx, first, cfg, &natsInstance, nil, rsAPI, nil, transactions.New(processCtx.Context()), nil, userAPI, nil, nil, nil, caching.DisableMetrics) //nolint:contextcheck
		second := httputil.NewRouters()
		AddPublicRoutes(processCtx, second, cfg, &natsInstance, nil, rsAPI, nil, transactions.New(processCtx.Context()), nil, userAPI, nil, nil, nil, caching.DisableMetrics) //nolint:contextcheck
		accessTokens := map[*test.User]userDevice{alice: {}}
		createAccessTokens(t, accessTokens, userAPI, ctx, first)

		send := func(routers httputil.Routers, txnID, body string) string {
			t.Helper()
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, test.NewRequest(t, http.MethodPut,
				fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/m.room.message/%s", room.ID, txnID),
				test.WithJSONBody(t, map[string]any{"msgtype": "m.text", "body": body}),
				test.WithQueryParams(map[string]string{"access_token": accessTokens[alice].accessToken})))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			return gjson.GetBytes(rec.Body.Bytes(), "event_id").Str
		}
		messagesWithBody := func(body string) int {
			t.Helper()
			res := api.QueryLatestEventsAndStateResponse{}
			require.NoError(t, rsAPI.QueryLatestEventsAndState(ctx, &api.QueryLatestEventsAndStateRequest{RoomID: room.ID}, &res))
			count := 0
			// Walk back from the forward extremities through prev_events.
			seen := map[string]bool{}
			queue := res.LatestEvents
			for len(queue) > 0 {
				id := queue[0]
				queue = queue[1:]
				if seen[id] {
					continue
				}
				seen[id] = true
				ev := api.GetEvent(ctx, rsAPI, room.ID, id)
				if ev == nil {
					continue
				}
				if gjson.GetBytes(ev.Content(), "body").Str == body {
					count++
				}
				queue = append(queue, ev.PrevEventIDs()...)
			}
			return count
		}

		original := send(first, "txn-1", "hello")
		// The retry builds a new event on the other process, which must be
		// discarded in favor of the original.
		retried := send(second, "txn-1", "hello")
		assert.Equal(t, original, retried)
		assert.Equal(t, 1, messagesWithBody("hello"), "the retry stored a second event")

		// A different transaction is a new event.
		other := send(second, "txn-2", "hello")
		assert.NotEqual(t, original, other)
		assert.Equal(t, 2, messagesWithBody("hello"))
	})
}
