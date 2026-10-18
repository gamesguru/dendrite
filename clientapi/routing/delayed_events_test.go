package routing

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/internal/transactions"
	"codefloe.com/pat-s/zendrite/roomserver"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
	uapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// TestDelayedEventScheduler drives the MSC4140 scheduler with a fake clock:
// events are sent when due and not before, restarting postpones them,
// canceling and failing finalize them, and a process that starts after a
// crash mid-send resends nothing that was already sent.
func TestDelayedEventScheduler(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	const eventType = "com.example.state"
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		rsAPI.SetUserAPI(userAPI)

		room := test.NewRoom(t, alice)
		powerLevels := eventutil.InitialPowerLevelsContent(gomatrixserverlib.MustGetRoomVersion(room.Version), alice.ID)
		powerLevels.Events[eventType] = 0
		room.CreateAndInsert(t, alice, spec.MRoomPowerLevels, powerLevels, test.WithStateKey(""))
		require.NoError(t, rsapi.SendEvents(ctx, rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false))

		cfg.ClientAPI.DelayedEvents.MaxDelay = time.Hour
		limits := httputil.NewRateLimits(&cfg.ClientAPI.RateLimiting, ctx)                                //nolint:contextcheck
		d := newDelayedEvents(&cfg.ClientAPI, rsAPI, userAPI, nil, transactions.New(ctx), limits, limits) //nolint:contextcheck
		clock := time.Now()
		d.now = func() time.Time { return clock }
		advance := func(by time.Duration) { clock = clock.Add(by) }

		// schedule schedules a state event, or a message event if stateKey is nil.
		schedule := func(delayID string, sender *test.User, stateKey *string, content string) {
			t.Helper()
			localpart, serverName, err := gomatrixserverlib.SplitID('@', sender.ID)
			require.NoError(t, err)
			now := spec.AsTimestamp(clock)
			require.NoError(t, userAPI.PerformDelayedEventAdd(ctx, &uapi.DelayedEvent{
				DelayID: delayID, Localpart: localpart, ServerName: serverName, DeviceID: "DEVICE",
				RoomID: room.ID, Type: eventType, StateKey: stateKey, Content: json.RawMessage(content),
				DelayMS: 1000, DelayedSince: now, SendTS: now + 1000,
			}, 10))
		}
		get := func(delayID string) *uapi.DelayedEvent {
			t.Helper()
			event, err := userAPI.QueryDelayedEvent(ctx, delayID)
			require.NoError(t, err)
			require.NotNil(t, event)
			return event
		}
		stateContent := func(stateKey string) gjson.Result {
			t.Helper()
			tuple := gomatrixserverlib.StateKeyTuple{EventType: eventType, StateKey: stateKey}
			res := rsapi.QueryCurrentStateResponse{}
			require.NoError(t, rsAPI.QueryCurrentState(ctx, &rsapi.QueryCurrentStateRequest{
				RoomID: room.ID, StateTuples: []gomatrixserverlib.StateKeyTuple{tuple},
			}, &res))
			if ev, ok := res.StateEvents[tuple]; ok {
				return gjson.ParseBytes(ev.Content())
			}
			return gjson.Result{}
		}

		t.Run("sent when due, not before, and restart postpones", func(t *testing.T) {
			schedule("due", alice, ptr("due"), `{"n":1}`)
			advance(999 * time.Millisecond)
			require.NoError(t, d.sendDue(ctx, false))
			assert.Equal(t, uapi.DelayedEventScheduled, get("due").Status)

			restarted, err := userAPI.PerformDelayedEventRestart(ctx, "due", spec.AsTimestamp(clock))
			require.NoError(t, err)
			assert.True(t, restarted)
			advance(999 * time.Millisecond)
			require.NoError(t, d.sendDue(ctx, false))
			assert.Equal(t, uapi.DelayedEventScheduled, get("due").Status, "restarting moved the send time")
			assert.False(t, stateContent("due").Exists())

			advance(time.Millisecond)
			require.NoError(t, d.sendDue(ctx, false))
			event := get("due")
			assert.Equal(t, uapi.DelayedEventFinalized, event.Status)
			assert.NotEmpty(t, event.EventID)
			assert.Nil(t, event.Error)
			assert.Equal(t, int64(1), stateContent("due").Get("n").Int())
		})

		t.Run("canceled events are not sent", func(t *testing.T) {
			schedule("canceled", alice, ptr("canceled"), `{"n":1}`)
			canceled, err := userAPI.PerformDelayedEventCancel(ctx, "canceled", spec.AsTimestamp(clock))
			require.NoError(t, err)
			assert.True(t, canceled)
			advance(time.Hour)
			require.NoError(t, d.sendDue(ctx, false))
			assert.False(t, stateContent("canceled").Exists())
			assert.Empty(t, get("canceled").EventID)
		})

		t.Run("a failure is recorded", func(t *testing.T) {
			// bob is not in the room.
			schedule("failing", bob, ptr("failing"), `{"n":1}`)
			advance(time.Hour)
			require.NoError(t, d.sendDue(ctx, false))
			event := get("failing")
			assert.Equal(t, uapi.DelayedEventFinalized, event.Status)
			assert.Empty(t, event.EventID)
			assert.Equal(t, "M_FORBIDDEN", gjson.GetBytes(event.Error, "errcode").Str, string(event.Error))
			assert.False(t, stateContent("failing").Exists())
		})

		t.Run("only the first event for the same state is sent", func(t *testing.T) {
			schedule("first", alice, ptr("shared"), `{"n":1}`)
			advance(time.Millisecond)
			schedule("second", alice, ptr("shared"), `{"n":2}`)
			advance(time.Hour)
			require.NoError(t, d.sendDue(ctx, false))
			assert.NotEmpty(t, get("first").EventID)
			second := get("second")
			assert.Equal(t, uapi.DelayedEventFinalized, second.Status)
			assert.Empty(t, second.EventID)
			assert.Equal(t, int64(1), stateContent("shared").Get("n").Int())
		})

		t.Run("a send interrupted by a crash is not repeated", func(t *testing.T) {
			// The previous process claimed and sent the event, but stopped
			// before it recorded the outcome. A message event, unlike state,
			// is not deduplicated by its content.
			schedule("crashed", alice, nil, `{"body":"hello"}`)
			advance(time.Hour)
			claimed, err := userAPI.PerformDelayedEventClaim(ctx, "crashed")
			require.NoError(t, err)
			require.NotNil(t, claimed)
			sentID, resErr := d.sendEvent(ctx, claimed)
			require.Nil(t, resErr)

			// Without reclaiming, a claimed event is left alone.
			require.NoError(t, d.sendDue(ctx, false))
			assert.Equal(t, uapi.DelayedEventSending, get("crashed").Status)

			// The next process resends it later, which would build an event
			// with another timestamp and ID, but the roomserver recognizes the
			// transaction and names the event that was sent before.
			advance(time.Second)
			require.NoError(t, d.sendDue(ctx, true))
			event := get("crashed")
			assert.Equal(t, uapi.DelayedEventFinalized, event.Status)
			assert.Equal(t, sentID, event.EventID)
			var latest rsapi.QueryLatestEventsAndStateResponse
			require.NoError(t, rsAPI.QueryLatestEventsAndState(ctx, &rsapi.QueryLatestEventsAndStateRequest{RoomID: room.ID}, &latest))
			assert.Equal(t, []string{sentID}, latest.LatestEvents)
		})

		t.Run("finalized events are purged after the retention period", func(t *testing.T) {
			advance(uapi.DelayedEventRetention + time.Hour)
			require.NoError(t, userAPI.PerformDelayedEventsPurge(ctx, spec.AsTimestamp(clock.Add(-uapi.DelayedEventRetention))))
			event, err := userAPI.QueryDelayedEvent(ctx, "due")
			require.NoError(t, err)
			assert.Nil(t, event)
		})
	})
}

func ptr(s string) *string { return &s }
