package tables_test

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/storage/postgres"
	"codefloe.com/pat-s/zendrite/userapi/storage/sqlite3"
	"codefloe.com/pat-s/zendrite/userapi/storage/tables"
)

func mustCreateDelayedEventsTable(t *testing.T, dbType test.DBType) (tab tables.DelayedEventsTable, close func()) {
	t.Helper()
	connStr, close := test.PrepareDBConnectionString(t, dbType)
	db, err := sqlutil.Open(&config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	}, nil)
	if err != nil {
		t.Fatalf("failed to open database: %s", err)
	}
	switch dbType {
	case test.DBTypePostgres:
		tab, err = postgres.NewPostgresDelayedEventsTable(db)
	case test.DBTypeSQLite:
		tab, err = sqlite3.NewSQLiteDelayedEventsTable(db)
	}
	if err != nil {
		t.Fatalf("failed to create delayed events table: %s", err)
	}
	return tab, close
}

func newDelayedEvent(delayID, localpart, roomID string, stateKey *string, sendTS spec.Timestamp) *api.DelayedEvent {
	const delayMS = 1000
	return &api.DelayedEvent{
		DelayID:      delayID,
		Localpart:    localpart,
		ServerName:   "localhost",
		DeviceID:     "DEVICE",
		RoomID:       roomID,
		Type:         "org.matrix.msc3401.call.member",
		StateKey:     stateKey,
		Content:      json.RawMessage(`{"leave":true}`),
		DelayMS:      delayMS,
		DelayedSince: sendTS - delayMS,
		SendTS:       sendTS,
	}
}

func delayIDs(events []api.DelayedEvent) []string {
	ids := make([]string, 0, len(events))
	for _, event := range events {
		ids = append(ids, event.DelayID)
	}
	return ids
}

func TestDelayedEventsTable(t *testing.T) {
	ctx := context.Background()
	roomID := "!room:localhost"
	member := "@alice:localhost"
	otherMember := "@bob:localhost"

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		tab, closeDB := mustCreateDelayedEventsTable(t, dbType)
		defer closeDB()

		_, ok, err := tab.SelectNextSendTS(ctx, nil)
		require.NoError(t, err)
		assert.False(t, ok, "nothing is scheduled yet")

		// alice schedules a message event and two state events, bob one state event.
		message := newDelayedEvent("message", "alice", roomID, nil, 3000)
		aliceState := newDelayedEvent("alice-state", "alice", roomID, &member, 2000)
		aliceOther := newDelayedEvent("alice-other", "alice", roomID, &otherMember, 5000)
		bobState := newDelayedEvent("bob-state", "bob", roomID, &member, 4000)
		for _, event := range []*api.DelayedEvent{message, aliceState, aliceOther, bobState} {
			require.NoError(t, tab.InsertDelayedEvent(ctx, nil, event))
		}

		got, err := tab.SelectDelayedEvent(ctx, nil, "alice-state")
		require.NoError(t, err)
		want := *aliceState
		want.Status = api.DelayedEventScheduled
		assert.Equal(t, &want, got)
		got, err = tab.SelectDelayedEvent(ctx, nil, "message")
		require.NoError(t, err)
		assert.Nil(t, got.StateKey, "message events have no state key")
		got, err = tab.SelectDelayedEvent(ctx, nil, "unknown")
		require.NoError(t, err)
		assert.Nil(t, got)

		pending, err := tab.SelectDelayedEventsForUser(ctx, nil, "alice", "localhost", false)
		require.NoError(t, err)
		assert.Equal(t, []string{"alice-state", "message", "alice-other"}, delayIDs(pending), "pending events are ordered by send time")
		count, next, err := tab.CountPendingDelayedEvents(ctx, nil, "alice", "localhost")
		require.NoError(t, err)
		assert.Equal(t, 3, count)
		assert.Equal(t, spec.Timestamp(2000), next)

		nextTS, ok, err := tab.SelectNextSendTS(ctx, nil)
		require.NoError(t, err)
		assert.True(t, ok)
		assert.Equal(t, spec.Timestamp(2000), nextTS)

		// Restarting moves the send time to now plus the delay.
		restarted, err := tab.RestartDelayedEvent(ctx, nil, "alice-state", 6000)
		require.NoError(t, err)
		assert.True(t, restarted)
		got, err = tab.SelectDelayedEvent(ctx, nil, "alice-state")
		require.NoError(t, err)
		assert.Equal(t, spec.Timestamp(6000), got.DelayedSince)
		assert.Equal(t, spec.Timestamp(7000), got.SendTS)
		nextTS, _, err = tab.SelectNextSendTS(ctx, nil)
		require.NoError(t, err)
		assert.Equal(t, spec.Timestamp(3000), nextTS)

		// Only the events due at 4000 are claimed, in order of send time.
		claimed, err := tab.ClaimDueDelayedEvents(ctx, nil, 4000, false)
		require.NoError(t, err)
		assert.Equal(t, []string{"message", "bob-state"}, delayIDs(claimed))
		assert.Equal(t, api.DelayedEventSending, claimed[0].Status)
		claimed, err = tab.ClaimDueDelayedEvents(ctx, nil, 4000, false)
		require.NoError(t, err)
		assert.Empty(t, claimed, "claimed events are not claimed twice")
		claimed, err = tab.ClaimDueDelayedEvents(ctx, nil, 0, true)
		require.NoError(t, err)
		assert.Equal(t, []string{"message", "bob-state"}, delayIDs(claimed), "a restarted process reclaims what it left")

		// A claimed event can be neither restarted, canceled nor claimed again.
		restarted, err = tab.RestartDelayedEvent(ctx, nil, "message", 6000)
		require.NoError(t, err)
		assert.False(t, restarted)
		canceled, err := tab.FinalizeDelayedEvent(ctx, nil, "message", api.DelayedEventScheduled, "", nil, 6000)
		require.NoError(t, err)
		assert.False(t, canceled)
		event, err := tab.ClaimDelayedEvent(ctx, nil, "message")
		require.NoError(t, err)
		assert.Nil(t, event)

		// Unclaiming returns an event to the schedule.
		require.NoError(t, tab.UnclaimDelayedEvent(ctx, nil, "bob-state"))
		got, err = tab.SelectDelayedEvent(ctx, nil, "bob-state")
		require.NoError(t, err)
		assert.Equal(t, api.DelayedEventScheduled, got.Status)

		// Sending records the event ID, failing records the error.
		finalized, err := tab.FinalizeDelayedEvent(ctx, nil, "message", api.DelayedEventSending, "$sent", nil, 6100)
		require.NoError(t, err)
		assert.True(t, finalized)
		got, err = tab.SelectDelayedEvent(ctx, nil, "message")
		require.NoError(t, err)
		assert.Equal(t, api.DelayedEventFinalized, got.Status)
		assert.Equal(t, "$sent", got.EventID)
		assert.Nil(t, got.Error)
		assert.Equal(t, spec.Timestamp(6100), got.FinalizedTS)

		event, err = tab.ClaimDelayedEvent(ctx, nil, "alice-other")
		require.NoError(t, err)
		require.NotNil(t, event)
		assert.Equal(t, "alice-other", event.DelayID)
		sendErr := json.RawMessage(`{"errcode":"M_FORBIDDEN","error":"not joined"}`)
		_, err = tab.FinalizeDelayedEvent(ctx, nil, "alice-other", api.DelayedEventSending, "", sendErr, 6200)
		require.NoError(t, err)
		got, err = tab.SelectDelayedEvent(ctx, nil, "alice-other")
		require.NoError(t, err)
		assert.Equal(t, "", got.EventID)
		assert.JSONEq(t, string(sendErr), string(got.Error))

		// A change to alice's member state by alice cancels bob's delayed
		// event for it, but not her own.
		require.NoError(t, tab.CancelDelayedStateEvents(ctx, nil, roomID, "org.matrix.msc3401.call.member", member, "alice", "localhost", 6300))
		got, err = tab.SelectDelayedEvent(ctx, nil, "bob-state")
		require.NoError(t, err)
		assert.Equal(t, api.DelayedEventFinalized, got.Status)
		assert.Equal(t, spec.Timestamp(6300), got.FinalizedTS)
		got, err = tab.SelectDelayedEvent(ctx, nil, "alice-state")
		require.NoError(t, err)
		assert.Equal(t, api.DelayedEventScheduled, got.Status)
		// A remote sender cancels every local user's.
		require.NoError(t, tab.CancelDelayedStateEvents(ctx, nil, roomID, "org.matrix.msc3401.call.member", member, "", "", 6400))
		got, err = tab.SelectDelayedEvent(ctx, nil, "alice-state")
		require.NoError(t, err)
		assert.Equal(t, api.DelayedEventFinalized, got.Status)
		_, ok, err = tab.SelectNextSendTS(ctx, nil)
		require.NoError(t, err)
		assert.False(t, ok, "nothing is scheduled any more")

		finalizedEvents, err := tab.SelectDelayedEventsForUser(ctx, nil, "alice", "localhost", true)
		require.NoError(t, err)
		assert.Equal(t, []string{"alice-state", "alice-other", "message"}, delayIDs(finalizedEvents), "finalized events are newest first")
		pending, err = tab.SelectDelayedEventsForUser(ctx, nil, "alice", "localhost", false)
		require.NoError(t, err)
		assert.Empty(t, pending)

		// Retention keeps the newest finalized events and those finalized recently.
		require.NoError(t, tab.DeleteExcessFinalizedDelayedEvents(ctx, nil, "alice", "localhost", 2))
		finalizedEvents, err = tab.SelectDelayedEventsForUser(ctx, nil, "alice", "localhost", true)
		require.NoError(t, err)
		assert.Equal(t, []string{"alice-state", "alice-other"}, delayIDs(finalizedEvents))
		require.NoError(t, tab.DeleteFinalizedDelayedEvents(ctx, nil, 6250))
		finalizedEvents, err = tab.SelectDelayedEventsForUser(ctx, nil, "alice", "localhost", true)
		require.NoError(t, err)
		assert.Equal(t, []string{"alice-state"}, delayIDs(finalizedEvents))

		// Deleting a user's events leaves other users' alone.
		require.NoError(t, tab.InsertDelayedEvent(ctx, nil, newDelayedEvent("alice-new", "alice", roomID, nil, 9000)))
		require.NoError(t, tab.DeleteDelayedEventsForUser(ctx, nil, "alice", "localhost"))
		for _, delayID := range []string{"alice-state", "alice-new"} {
			got, err = tab.SelectDelayedEvent(ctx, nil, delayID)
			require.NoError(t, err)
			assert.Nil(t, got, fmt.Sprintf("%s was deleted", delayID))
		}
		got, err = tab.SelectDelayedEvent(ctx, nil, "bob-state")
		require.NoError(t, err)
		assert.NotNil(t, got)
	})
}
