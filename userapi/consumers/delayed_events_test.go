package consumers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

func TestCancelDelayedStateEvents(t *testing.T) {
	ctx := context.Background()
	const roomID = "!room:localhost"
	const callMember = "org.matrix.msc3401.call.member"

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, close := mustCreateDatabase(t, dbType) //nolint:contextcheck
		defer close()
		cfg := &config.UserAPI{Matrix: &config.Global{SigningIdentity: fclient.SigningIdentity{ServerName: "localhost"}}}
		consumer := OutputRoomEventConsumer{db: db, rsAPI: &FakeUserRoomserverAPI{}, cfg: cfg}

		schedule := func(delayID, localpart, stateKey string) {
			t.Helper()
			require.NoError(t, db.AddDelayedEvent(ctx, &api.DelayedEvent{
				DelayID: delayID, Localpart: localpart, ServerName: "localhost",
				RoomID: roomID, Type: callMember, StateKey: &stateKey, Content: json.RawMessage(`{}`),
				DelayMS: 1000, DelayedSince: 1, SendTS: 1001,
			}, 10))
		}
		status := func(delayID string) api.DelayedEventStatus {
			t.Helper()
			event, err := db.GetDelayedEvent(ctx, delayID)
			require.NoError(t, err)
			return event.Status
		}
		stateEvent := func(id, sender, stateKey string, current bool) *rsapi.OutputNewRoomEvent {
			event := mustCreateEvent(t, `{"event_id":"`+id+`","type":"`+callMember+`","room_id":"`+roomID+
				`","sender":"`+sender+`","state_key":"`+stateKey+`","content":{}}`)
			output := &rsapi.OutputNewRoomEvent{Event: event}
			if current {
				output.AddsStateEventIDs = []string{event.EventID()}
			}
			return output
		}

		schedule("alice-own", "alice", "_@alice:localhost_DEVICE")
		schedule("bob-on-alice", "bob", "_@alice:localhost_DEVICE")
		schedule("bob-own", "bob", "_@bob:localhost_DEVICE")

		// State that does not become current cancels nothing.
		consumer.cancelDelayedStateEvents(ctx, stateEvent("$stale", "@alice:localhost", "_@alice:localhost_DEVICE", false))
		assert.Equal(t, api.DelayedEventScheduled, status("bob-on-alice"))

		// alice's change cancels bob's delayed event for that state, not hers.
		consumer.cancelDelayedStateEvents(ctx, stateEvent("$alice", "@alice:localhost", "_@alice:localhost_DEVICE", true))
		assert.Equal(t, api.DelayedEventScheduled, status("alice-own"))
		assert.Equal(t, api.DelayedEventFinalized, status("bob-on-alice"))
		assert.Equal(t, api.DelayedEventScheduled, status("bob-own"))

		// A remote user's change cancels every local user's.
		consumer.cancelDelayedStateEvents(ctx, stateEvent("$remote", "@mallory:remote", "_@bob:localhost_DEVICE", true))
		assert.Equal(t, api.DelayedEventFinalized, status("bob-own"))
		assert.Equal(t, api.DelayedEventScheduled, status("alice-own"))
	})
}
