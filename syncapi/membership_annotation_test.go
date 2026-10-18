// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package syncapi

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

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

// TestUnsignedMembership checks MSC4115: events carry the requesting user's
// membership at the time of each event, including the change made by the
// user's own membership events, rather than the current membership.
func TestUnsignedMembership(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		alice := test.NewUser(t)
		bob := test.NewUser(t)
		aliceDev := api.Device{ID: "ALICEID", UserID: alice.ID, AccessToken: "ALICE_MEMBERSHIP_TOKEN", AccountType: api.AccountTypeUser}
		bobDev := api.Device{ID: "BOBID", UserID: bob.ID, AccessToken: "BOB_MEMBERSHIP_TOKEN", AccountType: api.AccountTypeUser}

		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cfg.ClientAPI.RateLimiting = config.RateLimiting{Enabled: false}
		routers := httputil.NewRouters()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, &syncUserAPI{accounts: []api.Device{aliceDev, bobDev}}, rsAPI, caches, caching.DisableMetrics, nil, nil)

		room := test.NewRoom(t, alice, test.RoomPreset(test.PresetPublicChat), test.RoomHistoryVisibility(gomatrixserverlib.HistoryVisibilityShared))
		beforeJoin := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "before join"})
		bobJoin := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		whileJoined := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "while joined"})
		require.NoError(t, rsapi.SendEvents(context.Background(), rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false))

		syncUntil(t, routers, bobDev.AccessToken, false, func(syncBody string) bool {
			return gjson.Get(syncBody, fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id=="%s")`, room.ID, whileJoined.EventID())).Exists()
		})

		// A limited initial sync covers events before and after bob joined.
		w := httptest.NewRecorder()
		routers.Client.ServeHTTP(w, test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/sync", test.WithQueryParams(map[string]string{
			"access_token": bobDev.AccessToken,
			"filter":       `{"room":{"timeline":{"limit":10}}}`,
		})))
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		timeline := gjson.Get(w.Body.String(), fmt.Sprintf("rooms.join.%s.timeline.events", room.ID))
		syncMembership := func(eventID string) string {
			return timeline.Get(fmt.Sprintf(`#(event_id=="%s").unsigned.membership`, eventID)).Str
		}
		assert.Equal(t, spec.Leave, syncMembership(beforeJoin.EventID()), "bob had not joined yet")
		assert.Equal(t, spec.Join, syncMembership(bobJoin.EventID()), "the join includes its own change")
		assert.Equal(t, spec.Join, syncMembership(whileJoined.EventID()))

		bobLeave := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Leave}, test.WithStateKey(bob.ID))
		afterLeave := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "after leave"})
		require.NoError(t, rsapi.SendEvents(context.Background(), rsAPI, rsapi.KindNew, []*rstypes.HeaderedEvent{bobLeave, afterLeave}, "test", "test", "test", nil, false))
		syncUntil(t, routers, aliceDev.AccessToken, false, func(syncBody string) bool {
			return gjson.Get(syncBody, fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id=="%s")`, room.ID, afterLeave.EventID())).Exists()
		})

		// /messages reports the membership at each event, not bob's current
		// "leave". Having left, bob no longer sees shared history from before
		// his join, nor anything after his leave.
		w = httptest.NewRecorder()
		routers.Client.ServeHTTP(w, test.NewRequest(t, http.MethodGet, fmt.Sprintf("/_matrix/client/v3/rooms/%s/messages", room.ID), test.WithQueryParams(map[string]string{
			"access_token": bobDev.AccessToken,
			"dir":          "b",
			"limit":        "20",
		})))
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		chunk := gjson.Get(w.Body.String(), "chunk")
		messagesMembership := func(eventID string) gjson.Result {
			return chunk.Get(fmt.Sprintf(`#(event_id=="%s").unsigned.membership`, eventID))
		}
		assert.Equal(t, spec.Join, messagesMembership(bobJoin.EventID()).Str)
		assert.Equal(t, spec.Join, messagesMembership(whileJoined.EventID()).Str)
		assert.Equal(t, spec.Leave, messagesMembership(bobLeave.EventID()).Str, "the leave includes its own change")
		assert.False(t, chunk.Get(fmt.Sprintf(`#(event_id=="%s")`, beforeJoin.EventID())).Exists())
		assert.False(t, chunk.Get(fmt.Sprintf(`#(event_id=="%s")`, afterLeave.EventID())).Exists())

		// The single event endpoint annotates as well.
		w = httptest.NewRecorder()
		routers.Client.ServeHTTP(w, test.NewRequest(t, http.MethodGet, fmt.Sprintf("/_matrix/client/v3/rooms/%s/event/%s", room.ID, whileJoined.EventID()), test.WithQueryParams(map[string]string{
			"access_token": bobDev.AccessToken,
		})))
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		assert.Equal(t, spec.Join, gjson.Get(w.Body.String(), "unsigned.membership").Str)
	})
}
