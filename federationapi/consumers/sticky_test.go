// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/queue"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	"codefloe.com/pat-s/zendrite/federationapi/types"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/test"
)

// recordingFederationClient records which PDUs were sent to which server.
type recordingFederationClient struct {
	fclient.FederationClient
	mu   sync.Mutex
	sent map[spec.ServerName][]string
}

func (f *recordingFederationClient) SendTransaction(_ context.Context, txn gomatrixserverlib.Transaction) (fclient.RespSend, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, pdu := range txn.PDUs {
		ev, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10).NewEventFromTrustedJSON(pdu, false)
		if err != nil {
			return fclient.RespSend{}, err
		}
		f.sent[txn.Destination] = append(f.sent[txn.Destination], ev.EventID())
	}
	return fclient.RespSend{}, nil
}

func (f *recordingFederationClient) sentTo(server spec.ServerName) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.sent[server]...)
}

// eventsRoomserverAPI serves events by ID.
type eventsRoomserverAPI struct {
	api.FederationRoomserverAPI
	events map[string]*rstypes.HeaderedEvent
}

func (f *eventsRoomserverAPI) QueryEventsByID(_ context.Context, req *api.QueryEventsByIDRequest, res *api.QueryEventsByIDResponse) error {
	for _, id := range req.EventIDs {
		if ev, ok := f.events[id]; ok {
			res.Events = append(res.Events, ev)
		}
	}
	return nil
}

// Servers that join a room receive the unexpired sticky events this server
// sent there before, while servers already in the room, expired and redacted
// events, and other servers' sticky events are left alone.
func TestPushStickyEventsToNewlyJoinedServers(t *testing.T) {
	alice := test.NewUser(t)
	remote := test.NewUser(t, test.WithSigningServer("remote", "ed25519:remote", test.PrivateKeyB))
	room := test.NewRoom(t, alice, test.RoomVersion(gomatrixserverlib.RoomVersionV10))
	sticky := room.CreateAndInsert(t, alice, "m.rtc.member", map[string]any{}, test.WithStickyDuration(time.Hour))
	expired := room.CreateAndInsert(t, alice, "m.rtc.member", map[string]any{}, test.WithStickyDuration(time.Minute), test.WithTimestamp(time.Now().Add(-time.Hour)))
	redacted := room.CreateAndInsert(t, alice, "m.rtc.member", map[string]any{}, test.WithStickyDuration(time.Hour))
	plain := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "plain"})

	processCtx := process.NewProcessContext()
	t.Cleanup(processCtx.ShutdownZendrite)
	db := test.NewInMemoryFederationDatabase()
	stats := statistics.NewStatistics(db, 16, 16, false)
	client := &recordingFederationClient{sent: make(map[spec.ServerName][]string)}
	queues := queue.NewOutgoingQueues(db, processCtx, false, "test", client, &stats, []*fclient.SigningIdentity{{
		ServerName: "test", KeyID: "ed25519:test", PrivateKey: test.PrivateKeyA,
	}})

	redactedCopy := &rstypes.HeaderedEvent{PDU: redacted.PDU}
	redactedPDU, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10).NewEventFromTrustedJSON(redacted.JSON(), false)
	require.NoError(t, err)
	redactedPDU.Redact()
	redactedCopy.PDU = redactedPDU
	rsAPI := &eventsRoomserverAPI{events: map[string]*rstypes.HeaderedEvent{
		sticky.EventID():   sticky,
		expired.EventID():  expired,
		redacted.EventID(): redactedCopy,
		plain.EventID():    plain,
	}}
	s := &OutputRoomEventConsumer{
		ctx:    context.Background(),
		cfg:    &config.FederationAPI{Matrix: &config.Global{ServerName: "test"}},
		rsAPI:  rsAPI,
		queues: queues,
		sticky: newStickyTracker(),
	}

	for _, ev := range []*rstypes.HeaderedEvent{sticky, expired, redacted, plain} {
		s.trackStickyEvent(api.OutputNewRoomEvent{Event: ev, SendAsServer: "test"})
	}
	// Other servers' sticky events are theirs to push.
	room.CreateAndInsert(t, remote, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(remote.ID))
	foreign := room.CreateAndInsert(t, remote, "m.rtc.member", map[string]any{}, test.WithStickyDuration(time.Hour))
	s.trackStickyEvent(api.OutputNewRoomEvent{Event: foreign, SendAsServer: string(api.DoNotSendToOtherServers)})
	require.Len(t, s.sticky.unexpired(room.ID, time.Now()), 2, "only unexpired sticky events of this server are tracked")

	old := []types.JoinedHost{{ServerName: "test"}, {ServerName: "already"}}
	adds := []types.JoinedHost{{ServerName: "already"}, {ServerName: "newcomer"}, {ServerName: "test"}}
	s.pushStickyEvents(room.ID, old, adds)

	require.Eventually(t, func() bool { return len(client.sentTo("newcomer")) > 0 }, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, []string{sticky.EventID()}, client.sentTo("newcomer"))
	require.Empty(t, client.sentTo("already"))

	s.sticky.forget(room.ID)
	require.Empty(t, s.sticky.unexpired(room.ID, time.Now()))
}

// The tracker forgets sticky events once they expire, also in rooms it no
// longer hears about.
func TestStickyTrackerExpiry(t *testing.T) {
	tracker := newStickyTracker()
	now := time.Now()
	tracker.add("!a:test", stickyRef{eventID: "$a", expiresAt: now.Add(time.Minute)}, now)
	tracker.add("!a:test", stickyRef{eventID: "$a", expiresAt: now.Add(time.Minute)}, now)
	tracker.add("!a:test", stickyRef{eventID: "$b", expiresAt: now.Add(time.Hour)}, now)
	require.Len(t, tracker.unexpired("!a:test", now), 2, "an event is tracked once")
	require.Len(t, tracker.unexpired("!a:test", now.Add(2*time.Minute)), 1)

	tracker.add("!b:test", stickyRef{eventID: "$c", expiresAt: now.Add(time.Minute)}, now)
	later := now.Add(2 * time.Hour)
	tracker.add("!c:test", stickyRef{eventID: "$d", expiresAt: later.Add(time.Minute)}, later)
	tracker.mu.Lock()
	defer tracker.mu.Unlock()
	require.NotContains(t, tracker.rooms, "!a:test")
	require.NotContains(t, tracker.rooms, "!b:test")
	require.Contains(t, tracker.rooms, "!c:test")
}
