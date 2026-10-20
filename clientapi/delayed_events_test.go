// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package clientapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
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
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/syncapi"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
	uapi "codefloe.com/pat-s/zendrite/userapi/api"
)

const delayedEventsPath = "/_matrix/client/unstable/org.matrix.msc4140/delayed_events"

// wireFinalised is MSC4140's spelling of the finalized status and field.
const wireFinalised = "finalised" //nolint:misspell // MSC4140's spelling

// delayedEventsHarness is a homeserver with a room in which alice, bob and
// charlie may set com.example.test state, and dave is not a member.
type delayedEventsHarness struct {
	t            *testing.T
	routers      httputil.Routers
	rsAPI        api.ClientRoomserverAPI
	userAPI      uapi.UserInternalAPI
	room         *test.Room
	accessTokens map[*test.User]userDevice
}

const delayedStateType = "com.example.test"

func newDelayedEventsHarness(t *testing.T, dbType test.DBType, users []*test.User, members []*test.User, configure func(*config.ClientAPI)) (*delayedEventsHarness, func()) {
	t.Helper()
	ctx := context.Background()
	cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
	cfg.ClientAPI.RateLimiting.Enabled = false
	cfg.ClientAPI.DelayedEvents.MaxDelay = time.Hour
	cfg.ClientAPI.DelayedEvents.ManagementRateLimiting.Enabled = false
	configure(&cfg.ClientAPI)
	natsInstance := jetstream.NATSInstance{}
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	routers := httputil.NewRouters()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	fsAPI := federationapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, nil, rsAPI, caches, nil, true)
	rsAPI.SetFederationAPI(fsAPI, nil)
	userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
	rsAPI.SetUserAPI(userAPI)
	// The sync API forwards room events to the user API, which cancels
	// delayed state events when someone else changes that state.
	syncapi.AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, userAPI, rsAPI, caches, caching.DisableMetrics, nil, nil)
	AddPublicRoutes(processCtx, routers, cfg, &natsInstance, nil, rsAPI, nil, transactions.New(processCtx.Context()), nil, userAPI, nil, nil, nil, caching.DisableMetrics)

	accessTokens := map[*test.User]userDevice{}
	for _, user := range users {
		accessTokens[user] = userDevice{}
	}
	createAccessTokens(t, accessTokens, userAPI, ctx, routers)

	creator := members[0]
	room := test.NewRoom(t, creator)
	powerLevels := eventutil.InitialPowerLevelsContent(gomatrixserverlib.MustGetRoomVersion(room.Version), creator.ID)
	powerLevels.Events[delayedStateType] = 0
	room.CreateAndInsert(t, creator, spec.MRoomPowerLevels, powerLevels, test.WithStateKey(""))
	for _, member := range members[1:] {
		room.CreateAndInsert(t, member, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(member.ID))
	}
	require.NoError(t, api.SendEvents(ctx, rsAPI, api.KindNew, room.Events(), "test", "test", "test", nil, false))
	return &delayedEventsHarness{
		t: t, routers: routers, rsAPI: rsAPI, userAPI: userAPI, room: room, accessTokens: accessTokens,
	}, closeDB
}

// do sends a request as user, or without an access token if user is nil.
func (h *delayedEventsHarness) do(user *test.User, method, path, body string) *httptest.ResponseRecorder {
	h.t.Helper()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if user != nil {
		req.Header.Set("Authorization", "Bearer "+h.accessTokens[user].accessToken)
	}
	rec := httptest.NewRecorder()
	h.routers.Client.ServeHTTP(rec, req)
	return rec
}

// scheduleState schedules a com.example.test state event and returns the
// response.
func (h *delayedEventsHarness) scheduleState(user *test.User, stateKey, delay, body string) *httptest.ResponseRecorder {
	h.t.Helper()
	path := fmt.Sprintf("/_matrix/client/v3/rooms/%s/state/%s/%s?org.matrix.msc4140.delay=%s",
		url.PathEscape(h.room.ID), delayedStateType, url.PathEscape(stateKey), delay)
	return h.do(user, http.MethodPut, path, body)
}

// mustScheduleState schedules a state event and returns its delay ID.
func (h *delayedEventsHarness) mustScheduleState(user *test.User, stateKey, delay, body string) string {
	h.t.Helper()
	rec := h.scheduleState(user, stateKey, delay, body)
	require.Equal(h.t, http.StatusOK, rec.Code, rec.Body.String())
	delayID := gjson.GetBytes(rec.Body.Bytes(), "delay_id").Str
	require.NotEmpty(h.t, delayID, rec.Body.String())
	assert.False(h.t, gjson.GetBytes(rec.Body.Bytes(), "event_id").Exists(), rec.Body.String())
	return delayID
}

// state returns the event ID, sender and content of the current
// com.example.test state event, if any.
func (h *delayedEventsHarness) state(stateKey string) *gjson.Result {
	h.t.Helper()
	tuple := gomatrixserverlib.StateKeyTuple{EventType: delayedStateType, StateKey: stateKey}
	res := api.QueryCurrentStateResponse{}
	require.NoError(h.t, h.rsAPI.QueryCurrentState(context.Background(), &api.QueryCurrentStateRequest{
		RoomID: h.room.ID, StateTuples: []gomatrixserverlib.StateKeyTuple{tuple},
	}, &res))
	ev, ok := res.StateEvents[tuple]
	if !ok {
		return nil
	}
	parsed := gjson.Parse(fmt.Sprintf(`{"event_id":%q,"sender":%q,"content":%s}`, ev.EventID(), ev.SenderID(), ev.Content()))
	return &parsed
}

func (h *delayedEventsHarness) get(user *test.User, delayID string) gjson.Result {
	h.t.Helper()
	rec := h.do(user, http.MethodGet, delayedEventsPath+"/"+delayID, "")
	require.Equal(h.t, http.StatusOK, rec.Code, rec.Body.String())
	return gjson.ParseBytes(rec.Body.Bytes())
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

func eventually(t *testing.T, what string, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !check() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting until %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func assertErrCode(t *testing.T, rec *httptest.ResponseRecorder, status int, errcode string) {
	t.Helper()
	assert.Equal(t, status, rec.Code, rec.Body.String())
	assert.Equal(t, errcode, gjson.GetBytes(rec.Body.Bytes(), "errcode").Str, rec.Body.String())
}

// TestDelayedEvents covers the MSC4140 client API that Element Call uses
// through matrix-js-sdk: scheduling with the delay parameter, the management
// and lookup endpoints, sending on timeout, and cancellation by other users'
// state changes and by deactivation.
func TestDelayedEvents(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	charlie := test.NewUser(t)
	dave := test.NewUser(t)
	const maxScheduled = 5

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		h, closeDB := newDelayedEventsHarness(t, dbType, []*test.User{alice, bob, charlie, dave}, []*test.User{alice, bob, charlie}, func(cfg *config.ClientAPI) {
			cfg.DelayedEvents.MaxPerUser = maxScheduled
		})
		defer closeDB()

		t.Run("discovery", func(t *testing.T) {
			rec := h.do(nil, http.MethodGet, "/_matrix/client/versions", "")
			require.Equal(t, http.StatusOK, rec.Code)
			assert.True(t, gjson.GetBytes(rec.Body.Bytes(), `unstable_features.org\.matrix\.msc4140`).Bool(), rec.Body.String())
			rec = h.do(alice, http.MethodGet, "/_matrix/client/v3/capabilities", "")
			require.Equal(t, http.StatusOK, rec.Code)
			capability := gjson.GetBytes(rec.Body.Bytes(), `capabilities.org\.matrix\.msc4140\.delayed_events`)
			assert.JSONEq(t, `{"max_delay_ms":3600000,"max_scheduled":5}`, capability.Raw)
		})

		t.Run("scheduling errors", func(t *testing.T) {
			for _, delay := range []string{"", "soon", "0", "-1", "1.5"} {
				assertErrCode(t, h.scheduleState(alice, "invalid", delay, `{}`), http.StatusBadRequest, "M_INVALID_PARAM")
			}
			assertErrCode(t, h.scheduleState(alice, "too-late", "3600001", `{}`), http.StatusBadRequest, "ORG.MATRIX.MSC4140_DELAY_TOO_LARGE")
			// dave is not in the room, and bob may not set the room name.
			assertErrCode(t, h.scheduleState(dave, "outsider", "1000", `{}`), http.StatusForbidden, "M_FORBIDDEN")
			path := fmt.Sprintf("/_matrix/client/v3/rooms/%s/state/m.room.name/?org.matrix.msc4140.delay=1000", url.PathEscape(h.room.ID))
			assertErrCode(t, h.do(bob, http.MethodPut, path, `{"name":"bob's room"}`), http.StatusForbidden, "M_FORBIDDEN")
			assertErrCode(t, h.scheduleState(alice, "not-an-object", "1000", `[]`), http.StatusBadRequest, "M_BAD_JSON")
			rec := h.do(alice, http.MethodGet, delayedEventsPath, "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Empty(t, gjson.GetBytes(rec.Body.Bytes(), "delayed_events").Array(), "nothing was scheduled")
		})

		t.Run("without a delay the event is sent at once", func(t *testing.T) {
			path := fmt.Sprintf("/_matrix/client/v3/rooms/%s/state/%s/now", url.PathEscape(h.room.ID), delayedStateType)
			rec := h.do(alice, http.MethodPut, path, `{"now":true}`)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.True(t, strings.HasPrefix(gjson.GetBytes(rec.Body.Bytes(), "event_id").Str, "$"), rec.Body.String())
			assert.NotNil(t, h.state("now"))
		})

		t.Run("sent when the delay elapses", func(t *testing.T) {
			before := time.Now().UnixMilli()
			delayID := h.mustScheduleState(alice, "timeout", "200", `{"on":"timeout"}`)
			assert.Nil(t, h.state("timeout"))
			event := h.get(alice, delayID)
			assert.Equal(t, delayID, event.Get("delay_id").Str)
			assert.Equal(t, h.room.ID, event.Get("room_id").Str)
			assert.Equal(t, delayedStateType, event.Get("type").Str)
			assert.Equal(t, "timeout", event.Get("state_key").Str)
			assert.Equal(t, int64(200), event.Get("delay_ms").Int())
			assert.GreaterOrEqual(t, event.Get("delayed_since_ts").Int(), before)
			assert.JSONEq(t, `{"on":"timeout"}`, event.Get("content").Raw)
			assert.False(t, event.Get(wireFinalised).Exists())

			eventually(t, "the delayed event is sent", func() bool { return h.state("timeout") != nil })
			state := h.state("timeout")
			assert.Equal(t, alice.ID, state.Get("sender").Str)
			assert.Equal(t, "timeout", state.Get("content.on").Str)
			eventually(t, "the delayed event is finalized", func() bool { return h.get(alice, delayID).Get(wireFinalised).Exists() })
			finalized := h.get(alice, delayID).Get(wireFinalised)
			assert.Equal(t, state.Get("event_id").Str, finalized.Get("event_id").Str)
			assert.False(t, finalized.Get("error").Exists())
			assert.GreaterOrEqual(t, finalized.Get(wireFinalised+"_ts").Int(), before+200)

			rec := h.do(alice, http.MethodGet, delayedEventsPath+"?status="+wireFinalised+"&delay_id="+delayID, "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			listed := gjson.GetBytes(rec.Body.Bytes(), "delayed_events").Array()
			require.Len(t, listed, 1)
			assert.Equal(t, state.Get("event_id").Str, listed[0].Get(wireFinalised+".event_id").Str)
		})

		t.Run("restart postpones and cancel prevents sending", func(t *testing.T) {
			delayID := h.mustScheduleState(alice, "restarted", "1500", `{}`)
			since := h.get(alice, delayID).Get("delayed_since_ts").Int()
			time.Sleep(1000 * time.Millisecond)
			// Without an access token, the delay ID is enough.
			rec := h.do(nil, http.MethodPost, delayedEventsPath+"/"+delayID+"/restart", "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.JSONEq(t, `{}`, rec.Body.String())
			assert.GreaterOrEqual(t, h.get(alice, delayID).Get("delayed_since_ts").Int(), since+1000)
			// Past the original send time, it is still scheduled.
			time.Sleep(800 * time.Millisecond)
			assert.Nil(t, h.state("restarted"))
			assert.False(t, h.get(alice, delayID).Get(wireFinalised).Exists())

			rec = h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayID+"/cancel", "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			// Retrying the cancellation succeeds; other actions find nothing.
			rec = h.do(nil, http.MethodPost, delayedEventsPath+"/"+delayID+"/cancel", "")
			assert.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assertErrCode(t, h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayID+"/restart", ""), http.StatusNotFound, "M_NOT_FOUND")
			assertErrCode(t, h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayID+"/send", ""), http.StatusNotFound, "M_NOT_FOUND")
			time.Sleep(time.Second)
			assert.Nil(t, h.state("restarted"))
			finalized := h.get(alice, delayID).Get(wireFinalised)
			assert.True(t, finalized.Exists())
			assert.False(t, finalized.Get("event_id").Exists())
			assert.False(t, finalized.Get("error").Exists())
		})

		t.Run("send now", func(t *testing.T) {
			delayID := h.mustScheduleState(alice, "sent-now", "3600000", `{"now":1}`)
			// The form with the action in the body, which older clients use.
			rec := h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayID, `{"action":"send"}`)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			state := h.state("sent-now")
			require.NotNil(t, state)
			assert.Equal(t, h.get(alice, delayID).Get(wireFinalised+".event_id").Str, state.Get("event_id").Str)
			rec = h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayID+"/send", "")
			assert.Equal(t, http.StatusOK, rec.Code, "retrying the send succeeds: %s", rec.Body.String())
			assertErrCode(t, h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayID+"/cancel", ""), http.StatusNotFound, "M_NOT_FOUND")
		})

		t.Run("a failed send keeps the event scheduled", func(t *testing.T) {
			delayID := h.mustScheduleState(charlie, "kicked", "3600000", `{}`)
			// Lower charlie's power level below what the event needs.
			powerLevels := eventutil.InitialPowerLevelsContent(gomatrixserverlib.MustGetRoomVersion(h.room.Version), alice.ID)
			powerLevels.Events[delayedStateType] = 0
			powerLevels.Users[charlie.ID] = -1
			path := fmt.Sprintf("/_matrix/client/v3/rooms/%s/state/m.room.power_levels/", url.PathEscape(h.room.ID))
			body := string(mustJSON(t, powerLevels))
			rec := h.do(alice, http.MethodPut, path, body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			assertErrCode(t, h.do(charlie, http.MethodPost, delayedEventsPath+"/"+delayID+"/send", ""), http.StatusForbidden, "M_FORBIDDEN")
			assert.False(t, h.get(charlie, delayID).Get(wireFinalised).Exists(), "the event stays scheduled")
			rec = h.do(charlie, http.MethodPost, delayedEventsPath+"/"+delayID+"/cancel", "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

			powerLevels.Users[charlie.ID] = 0
			rec = h.do(alice, http.MethodPut, path, string(mustJSON(t, powerLevels)))
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		})

		t.Run("management errors", func(t *testing.T) {
			assertErrCode(t, h.do(nil, http.MethodPost, delayedEventsPath+"/abc", ""), http.StatusBadRequest, "M_NOT_JSON")
			assertErrCode(t, h.do(nil, http.MethodPost, delayedEventsPath+"/abc", `{}`), http.StatusBadRequest, "M_MISSING_PARAM")
			assertErrCode(t, h.do(nil, http.MethodPost, delayedEventsPath+"/abc", `{"action":"oops"}`), http.StatusBadRequest, "M_INVALID_PARAM")
			for _, action := range []string{"cancel", "restart", "send"} {
				assertErrCode(t, h.do(nil, http.MethodPost, delayedEventsPath+"/abc/"+action, ""), http.StatusNotFound, "M_NOT_FOUND")
				assertErrCode(t, h.do(alice, http.MethodPost, delayedEventsPath+"/abc", `{"action":"`+action+`"}`), http.StatusNotFound, "M_NOT_FOUND")
			}
			// An invalid access token is refused rather than ignored.
			req := httptest.NewRequest(http.MethodPost, delayedEventsPath+"/abc/cancel", nil)
			req.Header.Set("Authorization", "Bearer invalid")
			rec := httptest.NewRecorder()
			h.routers.Client.ServeHTTP(rec, req)
			assertErrCode(t, rec, http.StatusUnauthorized, "M_UNKNOWN_TOKEN")
		})

		t.Run("lookup", func(t *testing.T) {
			first := h.mustScheduleState(alice, "lookup-1", "3600000", `{"n":1}`)
			second := h.mustScheduleState(alice, "lookup-2", "3500000", `{"n":2}`)
			assertErrCode(t, h.do(nil, http.MethodGet, delayedEventsPath, ""), http.StatusUnauthorized, "M_MISSING_TOKEN")
			assertErrCode(t, h.do(nil, http.MethodGet, delayedEventsPath+"/"+first, ""), http.StatusUnauthorized, "M_MISSING_TOKEN")
			assertErrCode(t, h.do(bob, http.MethodGet, delayedEventsPath+"/"+first, ""), http.StatusNotFound, "M_NOT_FOUND")
			assertErrCode(t, h.do(alice, http.MethodGet, delayedEventsPath+"/unknown", ""), http.StatusNotFound, "M_NOT_FOUND")
			assertErrCode(t, h.do(alice, http.MethodGet, delayedEventsPath+"?status=bogus", ""), http.StatusBadRequest, "M_INVALID_PARAM")

			rec := h.do(alice, http.MethodGet, delayedEventsPath, "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			listed := gjson.GetBytes(rec.Body.Bytes(), "delayed_events").Array()
			require.Len(t, listed, 2, rec.Body.String())
			// Ordered by send time, with the field names of earlier revisions.
			assert.Equal(t, second, listed[0].Get("delay_id").Str)
			assert.Equal(t, first, listed[1].Get("delay_id").Str)
			assert.Equal(t, int64(3500000), listed[0].Get("delay").Int())
			assert.Equal(t, listed[0].Get("delayed_since_ts").Int(), listed[0].Get("running_since").Int())
			rec = h.do(alice, http.MethodGet, delayedEventsPath+"?status=scheduled&delay_id="+first, "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			listed = gjson.GetBytes(rec.Body.Bytes(), "delayed_events").Array()
			require.Len(t, listed, 1)
			assert.Equal(t, first, listed[0].Get("delay_id").Str)
			rec = h.do(bob, http.MethodGet, delayedEventsPath, "")
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Empty(t, gjson.GetBytes(rec.Body.Bytes(), "delayed_events").Array())

			for _, delayID := range []string{first, second} {
				require.Equal(t, http.StatusOK, h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayID+"/cancel", "").Code)
			}
		})

		t.Run("another user's change cancels delayed state", func(t *testing.T) {
			own := h.mustScheduleState(alice, "own", "3600000", `{"by":"delayed"}`)
			shared := h.mustScheduleState(alice, "shared", "3600000", `{"by":"delayed"}`)
			// alice's own change keeps her delayed event; bob's change, which
			// is processed after hers, cancels the other one.
			statePath := fmt.Sprintf("/_matrix/client/v3/rooms/%s/state/%s/", url.PathEscape(h.room.ID), delayedStateType)
			require.Equal(t, http.StatusOK, h.do(alice, http.MethodPut, statePath+"own", `{"by":"alice"}`).Code)
			require.Equal(t, http.StatusOK, h.do(bob, http.MethodPut, statePath+"shared", `{"by":"bob"}`).Code)
			eventually(t, "bob's change cancels alice's delayed event", func() bool {
				return h.get(alice, shared).Get(wireFinalised).Exists()
			})
			assert.False(t, h.get(alice, shared).Get(wireFinalised+".event_id").Exists())
			assert.False(t, h.get(alice, own).Get(wireFinalised).Exists())
			require.Equal(t, http.StatusOK, h.do(alice, http.MethodPost, delayedEventsPath+"/"+own+"/cancel", "").Code)
		})

		t.Run("the sender sees the delay ID", func(t *testing.T) {
			delayID := h.mustScheduleState(alice, "unsigned", "3600000", `{}`)
			require.Equal(t, http.StatusOK, h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayID+"/send", "").Code)
			eventID := h.get(alice, delayID).Get(wireFinalised + ".event_id").Str
			find := func(user *test.User) gjson.Result {
				var found gjson.Result
				eventually(t, "the event reaches /sync", func() bool {
					rec := h.do(user, http.MethodGet, "/_matrix/client/v3/sync?timeout=0", "")
					require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
					timeline := gjson.GetBytes(rec.Body.Bytes(), fmt.Sprintf("rooms.join.%s.timeline.events", gjson.Escape(h.room.ID)))
					for _, ev := range timeline.Array() {
						if ev.Get("event_id").Str == eventID {
							found = ev
							return true
						}
					}
					return false
				})
				return found
			}
			unsigned := find(alice).Get("unsigned")
			assert.Equal(t, delayID, unsigned.Get(`org\.matrix\.msc4140\.delay_id`).Str, unsigned.Raw)
			assert.False(t, unsigned.Get("transaction_id").Exists(), unsigned.Raw)
			assert.False(t, find(bob).Get(`unsigned.org\.matrix\.msc4140\.delay_id`).Exists())
		})

		t.Run("unstable scheduling endpoint", func(t *testing.T) {
			path := fmt.Sprintf("/_matrix/client/unstable/org.matrix.msc4140/rooms/%s/delayed_event/%s/txn1", url.PathEscape(h.room.ID), delayedStateType)
			body := `{"delay_ms":3600000,"state_key":"unstable","content":{"n":1}}`
			rec := h.do(alice, http.MethodPut, path, body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			delayID := gjson.GetBytes(rec.Body.Bytes(), "delay_id").Str
			// The transaction ID makes retries idempotent.
			rec = h.do(alice, http.MethodPut, path, body)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Equal(t, delayID, gjson.GetBytes(rec.Body.Bytes(), "delay_id").Str)
			event := h.get(alice, delayID)
			assert.Equal(t, "unstable", event.Get("state_key").Str)
			assert.JSONEq(t, `{"n":1}`, event.Get("content").Raw)
			assertErrCode(t, h.do(alice, http.MethodPut, strings.Replace(path, "txn1", "txn2", 1), `{"content":{}}`), http.StatusBadRequest, "M_MISSING_PARAM")
			assertErrCode(t, h.do(alice, http.MethodPut, strings.Replace(path, "txn1", "txn3", 1), `{"delay_ms":0,"content":{}}`), http.StatusBadRequest, "M_INVALID_PARAM")
			require.Equal(t, http.StatusOK, h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayID+"/cancel", "").Code)

			// So do they for message events sent with the delay parameter.
			sendPath := fmt.Sprintf("/_matrix/client/v3/rooms/%s/send/m.room.message/txn4?org.matrix.msc4140.delay=3600000", url.PathEscape(h.room.ID))
			rec = h.do(alice, http.MethodPut, sendPath, `{"msgtype":"m.text","body":"later"}`)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			delayID = gjson.GetBytes(rec.Body.Bytes(), "delay_id").Str
			rec = h.do(alice, http.MethodPut, sendPath, `{"msgtype":"m.text","body":"later"}`)
			assert.Equal(t, delayID, gjson.GetBytes(rec.Body.Bytes(), "delay_id").Str)
			assert.False(t, h.get(alice, delayID).Get("state_key").Exists())
			require.Equal(t, http.StatusOK, h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayID+"/cancel", "").Code)
		})

		t.Run("a delayed message event can be sticky", func(t *testing.T) {
			roomPath := "/_matrix/client/v3/rooms/" + url.PathEscape(h.room.ID)
			sendNow := func(t *testing.T, delayID string) gjson.Result {
				t.Helper()
				require.Equal(t, http.StatusOK, h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayID+"/send", "").Code)
				eventID := h.get(alice, delayID).Get(wireFinalised + ".event_id").Str
				require.NotEmpty(t, eventID)
				// /event reads from the sync API, which consumes the event later.
				var event gjson.Result
				eventually(t, "the event reaches the sync API", func() bool {
					rec := h.do(alice, http.MethodGet, roomPath+"/event/"+url.PathEscape(eventID), "")
					event = gjson.ParseBytes(rec.Body.Bytes())
					return rec.Code == http.StatusOK
				})
				return event
			}
			schedule := func(t *testing.T, method, path, body string) string {
				t.Helper()
				rec := h.do(alice, method, path, body)
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
				return gjson.GetBytes(rec.Body.Bytes(), "delay_id").Str
			}

			delayID := schedule(t, http.MethodPut,
				roomPath+"/send/m.rtc.member/sticky1?org.matrix.msc4140.delay=3600000&org.matrix.msc4354.sticky_duration_ms=60000", `{"leave":true}`)
			assert.Equal(t, int64(60000), sendNow(t, delayID).Get("msc4354_sticky.duration_ms").Int())

			// The dedicated scheduling endpoint takes the same parameter.
			delayID = schedule(t, http.MethodPut,
				"/_matrix/client/unstable/org.matrix.msc4140/rooms/"+url.PathEscape(h.room.ID)+"/delayed_event/m.rtc.member/sticky2?org.matrix.msc4354.sticky_duration_ms=30000",
				`{"delay_ms":3600000,"content":{"leave":true}}`)
			assert.Equal(t, int64(30000), sendNow(t, delayID).Get("msc4354_sticky.duration_ms").Int())

			// Without the parameter, and for state events, the event is not sticky.
			delayID = schedule(t, http.MethodPut, roomPath+"/send/m.rtc.member/sticky3?org.matrix.msc4140.delay=3600000", `{"leave":true}`)
			assert.False(t, sendNow(t, delayID).Get("msc4354_sticky").Exists())
			delayID = schedule(t, http.MethodPut,
				roomPath+"/state/"+delayedStateType+"/sticky?org.matrix.msc4140.delay=3600000&org.matrix.msc4354.sticky_duration_ms=60000", `{}`)
			assert.False(t, sendNow(t, delayID).Get("msc4354_sticky").Exists())

			assertErrCode(t, h.do(alice, http.MethodPut,
				roomPath+"/send/m.rtc.member/sticky4?org.matrix.msc4140.delay=3600000&org.matrix.msc4354.sticky_duration_ms=-1", `{}`),
				http.StatusBadRequest, "M_INVALID_PARAM")
		})

		t.Run("per-user limit", func(t *testing.T) {
			for i := range maxScheduled {
				h.mustScheduleState(charlie, fmt.Sprintf("limit-%d", i), fmt.Sprint(3600000-i*1000), `{}`)
			}
			rec := h.scheduleState(charlie, "limit-over", "1000", `{}`)
			assertErrCode(t, rec, http.StatusTooManyRequests, "M_LIMIT_EXCEEDED")
			// The next of charlie's events is due in an hour, less (maxScheduled-1) seconds.
			retryAfter := rec.Header().Get("Retry-After")
			assert.Contains(t, []string{"3596", "3597"}, retryAfter)
			// Other users have their own limit.
			h.mustScheduleState(bob, "limit-bob", "1000", `{}`)
		})

		t.Run("deactivation cancels a user's delayed events", func(t *testing.T) {
			// charlie is at the limit now, so bob is deactivated.
			delayID := h.mustScheduleState(bob, "deactivated", "500", `{}`)
			localpart, serverName, err := gomatrixserverlib.SplitID('@', bob.ID)
			require.NoError(t, err)
			require.NoError(t, h.userAPI.PerformAccountDeactivation(context.Background(), &uapi.PerformAccountDeactivationRequest{
				Localpart: localpart, ServerName: serverName,
			}, &uapi.PerformAccountDeactivationResponse{}))
			event, err := h.userAPI.QueryDelayedEvent(context.Background(), delayID)
			require.NoError(t, err)
			assert.Nil(t, event)
			time.Sleep(time.Second)
			assert.Nil(t, h.state("deactivated"))
		})
	})
}

// TestDelayedEventsLimits covers delayed events when they are disabled, and
// the rate limit on managing them.
func TestDelayedEventsLimits(t *testing.T) {
	alice := test.NewUser(t)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		t.Run("disabled", func(t *testing.T) {
			h, closeDB := newDelayedEventsHarness(t, dbType, []*test.User{alice}, []*test.User{alice}, func(cfg *config.ClientAPI) {
				cfg.DelayedEvents.MaxDelay = 0
			})
			defer closeDB()
			rec := h.do(nil, http.MethodGet, "/_matrix/client/versions", "")
			assert.False(t, gjson.GetBytes(rec.Body.Bytes(), `unstable_features.org\.matrix\.msc4140`).Bool(), rec.Body.String())
			rec = h.do(alice, http.MethodGet, "/_matrix/client/v3/capabilities", "")
			assert.Equal(t, int64(0), gjson.GetBytes(rec.Body.Bytes(), `capabilities.org\.matrix\.msc4140\.delayed_events.max_delay_ms`).Int(), rec.Body.String())
			assertErrCode(t, h.scheduleState(alice, "disabled", "1000", `{}`), http.StatusForbidden, "M_FORBIDDEN")
		})

		t.Run("management rate limit", func(t *testing.T) {
			h, closeDB := newDelayedEventsHarness(t, dbType, []*test.User{alice}, []*test.User{alice}, func(cfg *config.ClientAPI) {
				cfg.DelayedEvents.ManagementRateLimiting = config.RateLimiting{Enabled: true, Threshold: 1, CooloffMS: time.Hour.Milliseconds()}
			})
			defer closeDB()
			delayIDs := []string{
				h.mustScheduleState(alice, "a", "3600000", `{}`),
				h.mustScheduleState(alice, "b", "3600000", `{}`),
				h.mustScheduleState(alice, "c", "3600000", `{}`),
			}
			require.Equal(t, http.StatusOK, h.do(nil, http.MethodPost, delayedEventsPath+"/"+delayIDs[0]+"/cancel", "").Code)
			assertErrCode(t, h.do(nil, http.MethodPost, delayedEventsPath+"/"+delayIDs[1]+"/cancel", ""), http.StatusTooManyRequests, "M_LIMIT_EXCEEDED")
			// An access token is limited separately from the address.
			require.Equal(t, http.StatusOK, h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayIDs[1]+"/cancel", "").Code)
			assertErrCode(t, h.do(alice, http.MethodPost, delayedEventsPath+"/"+delayIDs[2]+"/restart", ""), http.StatusTooManyRequests, "M_LIMIT_EXCEEDED")
			assertErrCode(t, h.do(alice, http.MethodGet, delayedEventsPath, ""), http.StatusTooManyRequests, "M_LIMIT_EXCEEDED")
		})
	})
}
