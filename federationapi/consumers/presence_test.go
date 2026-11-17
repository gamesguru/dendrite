package consumers

import (
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/federationapi/storage"
	fedTypes "codefloe.com/pat-s/zendrite/federationapi/types"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
)

type stubPresenceStore struct {
	storage.Database
	hosts []spec.ServerName
	err   error
}

func (s *stubPresenceStore) GetJoinedHostsForRooms(
	_ context.Context, _ []string, _, _ bool,
) ([]spec.ServerName, error) {
	return append([]spec.ServerName(nil), s.hosts...), s.err
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
	consumer := &OutputPresenceConsumer{
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
	consumer.findDestinations = presenceDestinationFinder(consumer.rsAPI, consumer.db)
	consumer.ackPresence = func(*nats.Msg) error { return nil }
	consumer.extendPresence = func(*nats.Msg) error { return nil }
	consumer.streamSequence = func(msg *nats.Msg) uint64 {
		seq, _ := strconv.ParseUint(msg.Header.Get("test_seq"), 10, 64)
		return seq
	}
	return consumer
}

func TestPresenceCoalescesToLatestUpdate(t *testing.T) {
	sender := &stubPresenceSender{}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")

	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceTestMessage("@alice:test", "online", "first", 1000),
	}))
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
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

func TestPresenceInvalidValueIsAcknowledgedAndDropped(t *testing.T) {
	sender := &stubPresenceSender{}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")
	msg := presenceTestMessage("@alice:test", "invalid", "", 1000)

	action := consumer.onMessage(t.Context(), []*nats.Msg{msg})

	require.Equal(t, jetstream.AckActionAck, action)
	require.Empty(t, consumer.pending)
}

func TestPresenceKeepsVirtualHostOrigin(t *testing.T) {
	sender := &stubPresenceSender{}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")

	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceTestMessage("@alice:test", "online", "", 1000),
	}))
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
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

	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
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

func TestPresenceRetryDoesNotResendToHealthyDestination(t *testing.T) {
	sender := &stubPresenceSender{err: map[spec.ServerName]error{"b.test": errors.New("send failed")}}
	consumer := newTestPresenceConsumer(t, sender, "a.test", "b.test")
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceTestMessage("@alice:test", "online", "", 1000),
	}))
	failures := 0
	sender.onSend = func(_, destination spec.ServerName) {
		if destination == "b.test" {
			failures++
			if failures == 2 {
				delete(sender.err, "b.test")
			}
		}
	}

	consumer.flushPending()
	require.Len(t, consumer.pending, 1)
	consumer.flushPending()

	require.Empty(t, consumer.pending)
	require.Len(t, sender.sent, 3)
	require.Equal(t, spec.ServerName("a.test"), sender.sent[0].destination)
	require.Equal(t, spec.ServerName("b.test"), sender.sent[1].destination)
	require.Equal(t, spec.ServerName("b.test"), sender.sent[2].destination)
}

func TestPresenceRetryReResolvesDestinations(t *testing.T) {
	sender := &stubPresenceSender{err: map[spec.ServerName]error{"b.test": errors.New("send failed")}}
	consumer := newTestPresenceConsumer(t, sender, "a.test", "b.test")
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceTestMessage("@alice:test", "online", "", 1000),
	}))
	consumer.flushPending()
	require.Len(t, sender.sent, 2)
	require.Equal(t, map[spec.ServerName]struct{}{"a.test": {}}, consumer.pending["@alice:test"].sentDestinations)

	// The user left the room hosted on b.test while the retry was pending.
	consumer.findDestinations = func(context.Context, spec.UserID) ([]spec.ServerName, error) {
		return []spec.ServerName{"a.test"}, nil
	}

	consumer.flushPending()

	require.Len(t, sender.sent, 2, "must not send to a destination the user no longer shares")
	require.Empty(t, consumer.pending)
}

func TestPresenceGenerationKeepsNewerUpdate(t *testing.T) {
	sender := &stubPresenceSender{}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")
	consumer.queues = sender

	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceTestMessage("@alice:test", "online", "", 1000),
	}))
	injected := false
	sender.onSend = func(_, _ spec.ServerName) {
		if injected {
			return
		}
		injected = true
		require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
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
	for i := 1; i <= users; i++ {
		userID := "@user" + strconv.Itoa(i) + ":test"
		require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
			presenceTestMessage(userID, "online", "", int64(i)),
		}))
	}

	consumer.flushPending()

	// The 500 oldest users are sent. The batch contents are built by ranging
	// over a map, so compare the set rather than the order within the batch.
	require.Len(t, sender.sent, 1)
	require.Len(t, sender.sent[0].content.Push, presenceFlushBatchSize)
	visited := make(map[string]struct{}, len(sender.sent[0].content.Push))
	for _, push := range sender.sent[0].content.Push {
		visited[push.UserID] = struct{}{}
	}
	for i := 1; i <= presenceFlushBatchSize; i++ {
		require.Contains(t, visited, "@user"+strconv.Itoa(i)+":test")
	}
	require.Len(t, consumer.pending, 1)
	require.Contains(t, consumer.pending, "@user"+strconv.Itoa(users)+":test")

	consumer.flushPending()

	require.Len(t, sender.sent, 2)
	require.Len(t, sender.sent[1].content.Push, 1)
	require.Equal(t, "@user"+strconv.Itoa(users)+":test", sender.sent[1].content.Push[0].UserID)
	require.Empty(t, consumer.pending)
}

func presenceSeqMessage(userID, presence string, ts int64, seq uint64) *nats.Msg {
	msg := presenceTestMessage(userID, presence, "", ts)
	msg.Header.Set("test_seq", strconv.FormatUint(seq, 10))
	return msg
}

// recordAcks makes the consumer record acknowledgements instead of sending them.
func recordAcks(consumer *OutputPresenceConsumer) *[]*nats.Msg {
	var acked []*nats.Msg
	consumer.ackPresence = func(msg *nats.Msg) error {
		acked = append(acked, msg)
		return nil
	}
	return &acked
}

func TestPresenceAcksSupersededMessagesAndBoundsOutstanding(t *testing.T) {
	consumer := newTestPresenceConsumer(t, &stubPresenceSender{}, "remote.test")
	acked := recordAcks(consumer)

	const updates = 50
	msgs := make([]*nats.Msg, updates)
	for i := range msgs {
		msgs[i] = presenceSeqMessage("@alice:test", "online", int64(1000+i), uint64(i+1))
		require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{msgs[i]}))
	}

	// Only the newest message stays outstanding; every older one was acked.
	require.Len(t, consumer.pending["@alice:test"].acks, 1)
	require.Same(t, msgs[updates-1], consumer.pending["@alice:test"].acks[0].msg)
	require.Len(t, *acked, updates-1)
}

func TestPresenceDropsSupersededAckThatFailedButKeepsWatermark(t *testing.T) {
	consumer := newTestPresenceConsumer(t, &stubPresenceSender{}, "remote.test")
	consumer.ackPresence = func(*nats.Msg) error { return errors.New("ack failed") }

	for i := 1; i <= 2; i++ {
		presence := "online"
		if i == 2 {
			presence = "unavailable"
		}
		require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
			presenceSeqMessage("@alice:test", presence, int64(1000+i), uint64(i)),
		}))
	}
	// The failed superseded acknowledgement is left for JetStream redelivery,
	// while the watermark protects the latest state after its pending entry ends.
	update := consumer.pending["@alice:test"]
	require.Len(t, update.acks, 1)
	require.Equal(t, uint64(2), consumer.recentSequences["@alice:test"].sequence)

	// A failed JetStream ack has no finite redelivery deadline. Even after the
	// ordinary watermark TTL, pruning must not allow the old update back in.
	watermark := consumer.recentSequences["@alice:test"]
	watermark.expiresAt = time.Now().Add(-time.Second)
	consumer.recentSequences["@alice:test"] = watermark
	consumer.lastSequencePrune = time.Time{}
	consumer.pendingMu.Lock()
	consumer.pruneRecentPresenceSequences(time.Now())
	consumer.pendingMu.Unlock()
	require.Contains(t, consumer.recentSequences, "@alice:test")
	consumer.ackPresence = func(*nats.Msg) error { return nil }
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceSeqMessage("@alice:test", "online", 1001, 1),
	}))
	require.Equal(t, "unavailable", consumer.pending["@alice:test"].presence)
	require.NotContains(t, consumer.failedSuperseded, "@alice:test")
}

func TestPresenceClearsFailedSupersededRecordWhenAnotherAckSucceeds(t *testing.T) {
	consumer := newTestPresenceConsumer(t, &stubPresenceSender{}, "remote.test")
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceSeqMessage("@alice:test", "online", 1001, 1),
	}))
	// A flush holds this ack handle while a newer message supersedes it.
	old := consumer.pending["@alice:test"].acks[0]

	consumer.ackPresence = func(*nats.Msg) error { return errors.New("ack failed") }
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceSeqMessage("@alice:test", "unavailable", 1002, 2),
	}))
	require.Contains(t, consumer.failedSuperseded["@alice:test"], uint64(1))
	require.True(t, consumer.recentSequences["@alice:test"].retain)

	// The flush retries the same ack and it succeeds: JetStream will not
	// redeliver the message, so nothing may be left to clear it later.
	consumer.ackPresence = func(*nats.Msg) error { return nil }
	require.NoError(t, consumer.ackPendingPresence(old))
	require.NotContains(t, consumer.failedSuperseded, "@alice:test")
	require.False(t, consumer.recentSequences["@alice:test"].retain)
}

func TestPresenceIgnoresStaleRedelivery(t *testing.T) {
	consumer := newTestPresenceConsumer(t, &stubPresenceSender{}, "remote.test")

	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceSeqMessage("@alice:test", "unavailable", 2000, 7),
	}))
	// An older message redelivered after a newer one is acknowledged and dropped.
	require.Equal(t, jetstream.AckActionAck, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceSeqMessage("@alice:test", "online", 1000, 3),
	}))

	pending := consumer.pending["@alice:test"]
	require.Equal(t, "unavailable", pending.presence)
	require.Equal(t, uint64(7), consumer.recentSequences["@alice:test"].sequence)
	require.Len(t, pending.acks, 1)
}

func TestPresenceIgnoresStaleRedeliveryAfterFlush(t *testing.T) {
	old := presenceSeqMessage("@alice:test", "online", 1000, 3)
	newer := presenceSeqMessage("@alice:test", "unavailable", 2000, 7)
	consumer := newTestPresenceConsumer(t, &stubPresenceSender{}, "remote.test")
	consumer.ackPresence = func(msg *nats.Msg) error {
		if msg == old {
			return errors.New("temporary ack failure")
		}
		return nil
	}

	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{old}))
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{newer}))
	consumer.flushPending()
	require.Empty(t, consumer.pending)

	staleRedelivery := presenceSeqMessage("@alice:test", "online", 1000, 3)
	// The failed ack is retried directly and deferred from the worker's ack path.
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{staleRedelivery}))
	require.Empty(t, consumer.pending, "a stale redelivery after flush must not restore old presence")
	require.NotContains(t, consumer.failedSuperseded, "@alice:test")
}

func TestPresenceDefersRedeliveryOfCurrentPendingMessage(t *testing.T) {
	sender := &stubPresenceSender{}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")
	acked := recordAcks(consumer)
	firstDelivery := presenceSeqMessage("@alice:test", "online", 1000, 7)
	redelivery := presenceSeqMessage("@alice:test", "online", 1000, 7)

	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{firstDelivery}))
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{redelivery}))

	pending := consumer.pending["@alice:test"]
	require.Equal(t, "online", pending.presence)
	require.Len(t, pending.acks, 1)
	require.Same(t, redelivery, pending.acks[0].msg, "use the new delivery's acknowledgement handle")
	require.Empty(t, *acked, "a redelivery still waiting for its EDU must remain unacknowledged")

	consumer.flushPending()
	require.Empty(t, consumer.pending)
	require.Len(t, sender.sent, 1)
	require.Equal(t, []*nats.Msg{redelivery}, *acked)
}

func TestPresenceSequenceWatermarkExpires(t *testing.T) {
	consumer := newTestPresenceConsumer(t, &stubPresenceSender{}, "remote.test")
	newer := presenceSeqMessage("@alice:test", "unavailable", 2000, 7)
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{newer}))
	consumer.flushPending()
	require.Empty(t, consumer.pending)

	watermark := consumer.recentSequences["@alice:test"]
	watermark.expiresAt = time.Now().Add(-time.Second)
	consumer.recentSequences["@alice:test"] = watermark

	older := presenceSeqMessage("@alice:test", "online", 1000, 3)
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{older}))
	require.Equal(t, "online", consumer.pending["@alice:test"].presence)
	require.Equal(t, uint64(3), consumer.recentSequences["@alice:test"].sequence)
}

func TestPresenceSequenceWatermarkOutlivesLongFlushInterval(t *testing.T) {
	consumer := newTestPresenceConsumer(t, &stubPresenceSender{}, "remote.test")
	consumer.flushInterval = 3 * time.Minute

	retention := consumer.sequenceWatermarkRetention()
	require.Greater(t, retention, consumer.flushInterval)
	require.GreaterOrEqual(t, retention, defaultPresenceAckWait)
}

func TestPresenceAcksOnlyAfterSuccessfulQueueing(t *testing.T) {
	sender := &stubPresenceSender{err: map[spec.ServerName]error{"remote.test": errors.New("send failed")}}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")
	acked := recordAcks(consumer)
	msg := presenceSeqMessage("@alice:test", "online", 1000, 1)
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{msg}))

	consumer.flushPending()
	require.Empty(t, *acked, "a failed send must not acknowledge the message")
	require.Len(t, consumer.pending, 1)

	sender.err = nil
	consumer.flushPending()
	require.Equal(t, []*nats.Msg{msg}, *acked)
	require.Empty(t, consumer.pending)
}

func TestPresenceRetriesAckFailureWithoutResending(t *testing.T) {
	sender := &stubPresenceSender{}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")
	failAck := true
	var acked []*nats.Msg
	consumer.ackPresence = func(msg *nats.Msg) error {
		if failAck {
			return errors.New("ack failed")
		}
		acked = append(acked, msg)
		return nil
	}
	msg := presenceSeqMessage("@alice:test", "online", 1000, 1)
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{msg}))

	consumer.flushPending()
	require.Len(t, sender.sent, 1)
	require.Len(t, consumer.pending, 1, "an unacknowledged message keeps the entry")

	failAck = false
	consumer.flushPending()
	require.Len(t, sender.sent, 1, "the EDU was already queued and must not be sent again")
	require.Equal(t, []*nats.Msg{msg}, acked)
	require.Empty(t, consumer.pending)
}

func TestPresenceLookupFailureMovesUserToTail(t *testing.T) {
	sender := &stubPresenceSender{}
	consumer := newTestPresenceConsumer(t, sender, "remote.test")
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceSeqMessage("@alice:test", "online", 1000, 1),
	}))
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{
		presenceSeqMessage("@bob:test", "online", 1000, 2),
	}))
	before := consumer.pending["@alice:test"].order
	require.Less(t, before, consumer.pending["@bob:test"].order)

	real := consumer.findDestinations
	consumer.findDestinations = func(ctx context.Context, userID spec.UserID) ([]spec.ServerName, error) {
		if userID.String() == "@alice:test" {
			return nil, errors.New("lookup failed")
		}
		return real(ctx, userID)
	}
	consumer.flushPending()

	require.Contains(t, consumer.pending, "@alice:test")
	require.NotContains(t, consumer.pending, "@bob:test")
	require.Greater(t, consumer.pending["@alice:test"].order, before)
	require.Len(t, consumer.pending["@alice:test"].acks, 1, "a failed lookup must not acknowledge")
}

func TestPresenceKeepAliveExtendsEveryDeferredMessage(t *testing.T) {
	consumer := newTestPresenceConsumer(t, &stubPresenceSender{}, "remote.test")
	var extended []*nats.Msg
	consumer.extendPresence = func(msg *nats.Msg) error {
		extended = append(extended, msg)
		return errors.New("ownership lost") // an error must not stop the others
	}
	a := presenceSeqMessage("@alice:test", "online", 1000, 1)
	b := presenceSeqMessage("@bob:test", "online", 1000, 2)
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{a}))
	require.Equal(t, jetstream.AckActionDefer, consumer.onMessage(t.Context(), []*nats.Msg{b}))

	consumer.extendPending()

	require.ElementsMatch(t, []*nats.Msg{a, b}, extended)
	require.Less(t, pendingPresenceKeepAliveInterval, jetstream.HAConsumerAckWait)
}
