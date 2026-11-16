package consumers

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/storage"
	fedTypes "codefloe.com/pat-s/zendrite/federationapi/types"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
)

type stubPresenceStore struct {
	storage.Database
	hosts []spec.ServerName
	err   error
}

func (s *stubPresenceStore) GetJoinedHostsForRooms(
	_ context.Context, _ []string, _, _ bool,
) ([]spec.ServerName, error) {
	return s.hosts, s.err
}

type stubPresenceRSAPI struct {
	roomserverAPI.FederationRoomserverAPI
	rooms []spec.RoomID
	err   error
}

func (s *stubPresenceRSAPI) QueryRoomsForUser(
	_ context.Context, _ spec.UserID, _ string,
) ([]spec.RoomID, error) {
	return s.rooms, s.err
}

type sentPresence struct {
	eduOrigin   spec.ServerName
	origin      spec.ServerName
	destination spec.ServerName
	content     fedTypes.Presence
}

type stubPresenceSender struct {
	sent   []sentPresence
	err    map[spec.ServerName]error
	onSend func(origin, destination spec.ServerName)
}

func (s *stubPresenceSender) SendEDU(
	edu *gomatrixserverlib.EDU, origin spec.ServerName, destinations []spec.ServerName,
) error {
	for _, destination := range destinations {
		var content fedTypes.Presence
		if err := json.Unmarshal(edu.Content, &content); err != nil {
			return err
		}
		s.sent = append(s.sent, sentPresence{
			eduOrigin:   spec.ServerName(edu.Origin),
			origin:      origin,
			destination: destination,
			content:     content,
		})
		if s.onSend != nil {
			s.onSend(origin, destination)
		}
		if err := s.err[destination]; err != nil {
			return err
		}
	}
	return nil
}

func presenceTestMessage(userID, presence, status string, ts int64) *nats.Msg {
	message := &nats.Msg{Header: nats.Header{}}
	message.Header.Set("user_id", userID)
	message.Header.Set("presence", presence)
	message.Header.Set("last_active_ts", strconv.FormatInt(ts, 10))
	if status != "" {
		message.Header.Set("status_msg", status)
	}
	return message
}

func newTestPresenceConsumer(t *testing.T, sender *stubPresenceSender, hosts ...spec.ServerName) *OutputPresenceConsumer {
	t.Helper()
	roomID, err := spec.NewRoomID("!room:test")
	require.NoError(t, err)
	return &OutputPresenceConsumer{
		ctx: t.Context(),
		isLocalServerName: func(serverName spec.ServerName) bool {
			return serverName == "test" || serverName == "virtual.test"
		},
		db: &stubPresenceStore{hosts: hosts},
		rsAPI: &stubPresenceRSAPI{
			rooms: []spec.RoomID{*roomID},
		},
		queues:  sender,
		pending: make(map[string]pendingPresence),
	}
}

func TestPresenceCoalescesToLatestUpdate(t *testing.T) {
	sender := &stubPresenceSender{}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")

	require.True(t, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceTestMessage("@alice:test", "online", "first", 1000),
	}))
	require.True(t, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceTestMessage("@alice:test", "unavailable", "second", 2000),
	}))
	require.Len(t, consumer.pending, 1)

	consumer.flushPending()

	require.Empty(t, consumer.pending)
	require.Len(t, sender.sent, 1)
	require.Equal(t, spec.ServerName("test"), sender.sent[0].eduOrigin)
	require.Equal(t, spec.ServerName("test"), sender.sent[0].origin)
	require.Equal(t, spec.ServerName("remote.test"), sender.sent[0].destination)
	require.Len(t, sender.sent[0].content.Push, 1)
	require.Equal(t, "unavailable", sender.sent[0].content.Push[0].Presence)
	require.NotNil(t, sender.sent[0].content.Push[0].StatusMsg)
	require.Equal(t, "second", *sender.sent[0].content.Push[0].StatusMsg)
}

func TestPresenceKeepsVirtualHostOrigin(t *testing.T) {
	sender := &stubPresenceSender{}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")

	require.True(t, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceTestMessage("@alice:test", "online", "", 1000),
	}))
	require.True(t, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceTestMessage("@bob:virtual.test", "online", "", 1000),
	}))

	consumer.flushPending()

	require.Empty(t, consumer.pending)
	require.Len(t, sender.sent, 2)
	origins := map[spec.ServerName]string{}
	for _, sent := range sender.sent {
		require.Equal(t, sent.origin, sent.eduOrigin)
		require.Len(t, sent.content.Push, 1)
		origins[sent.origin] = sent.content.Push[0].UserID
	}
	require.Equal(t, "@alice:test", origins["test"])
	require.Equal(t, "@bob:virtual.test", origins["virtual.test"])
}

func TestPresenceRetriesAfterSendFailure(t *testing.T) {
	sender := &stubPresenceSender{
		err: map[spec.ServerName]error{"remote.test": errors.New("send failed")},
	}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")

	require.True(t, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceTestMessage("@alice:test", "online", "", 1000),
	}))
	consumer.flushPending()

	require.Len(t, sender.sent, 1)
	require.Len(t, consumer.pending, 1)

	sender.err = nil
	consumer.flushPending()

	require.Len(t, sender.sent, 2)
	require.Empty(t, consumer.pending)
}

func TestPresenceGenerationKeepsNewerUpdate(t *testing.T) {
	sender := &stubPresenceSender{}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")
	consumer.queues = sender

	require.True(t, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceTestMessage("@alice:test", "online", "", 1000),
	}))
	injected := false
	sender.onSend = func(_, _ spec.ServerName) {
		if injected {
			return
		}
		injected = true
		require.True(t, consumer.onMessage(t.Context(), []*nats.Msg{
			presenceTestMessage("@alice:test", "unavailable", "", 2000),
		}))
	}

	consumer.flushPending()

	require.Len(t, sender.sent, 1)
	require.Equal(t, "online", sender.sent[0].content.Push[0].Presence)
	require.Len(t, consumer.pending, 1)
	pending, ok := consumer.pending["@alice:test"]
	require.True(t, ok)
	require.Equal(t, "unavailable", pending.presence)
	require.Equal(t, uint64(2), pending.generation)

	sender.onSend = nil
	consumer.flushPending()

	require.Len(t, sender.sent, 2)
	require.Equal(t, "unavailable", sender.sent[1].content.Push[0].Presence)
	require.Empty(t, consumer.pending)
}

func TestPresenceFlushBatchCap(t *testing.T) {
	sender := &stubPresenceSender{}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")

	const users = presenceFlushBatchSize + 1
	for i := 0; i < users; i++ {
		userID := "@u" + strconv.Itoa(i) + ":test"
		require.True(t, consumer.onMessage(t.Context(), []*nats.Msg{
			presenceTestMessage(userID, "online", "", int64(i+1)),
		}))
	}

	consumer.flushPending()

	require.Len(t, sender.sent, 1)
	require.Len(t, sender.sent[0].content.Push, presenceFlushBatchSize)
	require.Len(t, consumer.pending, 1)

	consumer.flushPending()

	require.Len(t, sender.sent, 2)
	require.Len(t, sender.sent[1].content.Push, 1)
	require.Empty(t, consumer.pending)
}
