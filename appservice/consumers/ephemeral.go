// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	syncTypes "codefloe.com/pat-s/zendrite/syncapi/types"
)

// ephemeralBatch is the most typing, receipt or presence updates that one
// transaction carries.
const ephemeralBatch = 50

// EphemeralConsumer sends typing notifications, receipts and presence to
// application services that set receive_ephemeral (MSC2409).
type EphemeralConsumer struct {
	ctx       context.Context
	cfg       *config.AppServiceAPI
	jetstream nats.JetStreamContext
	rsAPI     api.AppserviceRoomserverAPI
}

// NewEphemeralConsumer creates an EphemeralConsumer. Call Start() to begin
// consuming.
func NewEphemeralConsumer(
	process *process.ProcessContext,
	cfg *config.AppServiceAPI,
	js nats.JetStreamContext,
	rsAPI api.AppserviceRoomserverAPI,
) *EphemeralConsumer {
	return &EphemeralConsumer{
		ctx:       process.Context(),
		cfg:       cfg,
		jetstream: js,
		rsAPI:     rsAPI,
	}
}

// ephemeralStream is one of the streams an application service receives
// ephemeral data from.
type ephemeralStream struct {
	name  string
	topic string
	// handle turns a batch of messages into the ephemeral data for the
	// application service.
	handle func(d *ephemeralDelivery, ctx context.Context, msgs []*nats.Msg) []json.RawMessage
}

// ephemeralDelivery is the state of one stream's delivery to one application
// service.
type ephemeralDelivery struct {
	*EphemeralConsumer
	state  *appserviceState
	stream string
	// typing is the typing state per room, which m.typing reports in full.
	typing *caching.EDUCache
}

func (s *EphemeralConsumer) streams() []ephemeralStream {
	return []ephemeralStream{
		{name: "Typing", topic: jetstream.OutputTypingEvent, handle: (*ephemeralDelivery).typingEvents},
		{name: "Receipt", topic: jetstream.OutputReceiptEvent, handle: (*ephemeralDelivery).receiptEvents},
		{name: "Presence", topic: jetstream.OutputPresenceEvent, handle: (*ephemeralDelivery).presenceEvents},
	}
}

func (s *EphemeralConsumer) durable(stream ephemeralStream, as *config.ApplicationService) string {
	return s.cfg.Matrix.JetStream.Durable("Appservice" + stream.name + "_" + jetstream.Tokenise(as.ID))
}

// Start consuming for every application service that wants ephemeral data.
// The durables of application services that no longer want it are deleted,
// because the receipt stream keeps messages until every durable acknowledged
// them.
func (s *EphemeralConsumer) Start() error {
	for _, as := range s.cfg.Derived.ApplicationServices {
		appsvc := as
		for _, stream := range s.streams() {
			topic := s.cfg.Matrix.JetStream.Prefixed(stream.topic)
			durable := s.durable(stream, &appsvc)
			if !appsvc.WantsEphemeral() || appsvc.URL == "" {
				err := s.jetstream.DeleteConsumer(topic, durable+"Pull")
				if err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
					return fmt.Errorf("failed to delete %q consumer: %w", durable, err)
				}
				continue
			}
			d := &ephemeralDelivery{
				EphemeralConsumer: s,
				state:             &appserviceState{ApplicationService: &appsvc},
				stream:            stream.name,
			}
			if stream.topic == jetstream.OutputTypingEvent {
				d.typing = caching.NewTypingCache(s.ctx)
				d.typing.SetTimeoutCallback(d.onTypingTimeout)
			}
			handle := stream.handle
			if err := jetstream.JetStreamConsumer(
				s.ctx, s.jetstream, topic, durable, ephemeralBatch,
				func(ctx context.Context, msgs []*nats.Msg) bool {
					return d.send(ctx, handle(d, ctx, msgs), msgs[0]) == nil
				},
				nats.DeliverNew(), nats.ManualAck(), nats.HeadersOnly(),
			); err != nil {
				return fmt.Errorf("failed to create %q consumer: %w", durable, err)
			}
		}
	}
	return nil
}

// send delivers ephemeral data in a transaction of its own. The transaction ID
// derives from the first message, so a redelivered batch reuses it.
func (d *ephemeralDelivery) send(ctx context.Context, events []json.RawMessage, first *nats.Msg) error {
	if len(events) == 0 {
		return nil
	}
	txnID := ""
	if first != nil {
		if metadata, err := first.Metadata(); err == nil {
			txnID = fmt.Sprintf("ephemeral_%s_%d", d.stream, metadata.Sequence.Stream)
		}
	}
	if txnID == "" {
		txnID = fmt.Sprintf("ephemeral_%s_t%d", d.stream, time.Now().UnixNano())
	}
	txn := ApplicationServiceTransaction{Events: []synctypes.ClientEvent{}}
	if d.state.ReceiveEphemeral {
		txn.Ephemeral = events
	} else {
		txn.UnstableEphemeral = events
	}
	body, err := json.Marshal(txn)
	if err != nil {
		return err
	}
	log.WithField("appservice", d.state.ID).Debugf("Appservice worker sending %d ephemeral event(s)", len(events))
	return sendTransaction(ctx, d.cfg, d.jetstream, d.state, body, txnID)
}

// roomInterest caches whether the application service is interested in a
// room for one batch.
func (d *ephemeralDelivery) roomInterest(ctx context.Context, cache map[string]bool, roomID string) bool {
	interested, ok := cache[roomID]
	if !ok {
		interested = isInterestedInRoom(ctx, d.cfg, d.rsAPI, d.state.ApplicationService, roomID)
		cache[roomID] = interested
	}
	return interested
}

// typingEphemeral is an m.typing for application services.
type typingEphemeral struct {
	Type    string `json:"type"`
	RoomID  string `json:"room_id"`
	Content struct {
		UserIDs []string `json:"user_ids"`
	} `json:"content"`
}

func typingEvent(roomID string, userIDs []string) (json.RawMessage, error) {
	ev := typingEphemeral{Type: spec.MTyping, RoomID: roomID}
	ev.Content.UserIDs = append([]string{}, userIDs...)
	slices.Sort(ev.Content.UserIDs)
	return json.Marshal(ev)
}

// typingEvents applies typing updates to the typing state and returns an
// m.typing with the users now typing in each changed room the application
// service is interested in.
func (d *ephemeralDelivery) typingEvents(ctx context.Context, msgs []*nats.Msg) []json.RawMessage {
	var rooms []string
	for _, msg := range msgs {
		roomID := msg.Header.Get(jetstream.RoomID)
		userID := msg.Header.Get(jetstream.UserID)
		typing, err := strconv.ParseBool(msg.Header.Get("typing"))
		if err != nil {
			continue
		}
		if typing {
			timeout, err := strconv.Atoi(msg.Header.Get("timeout_ms"))
			if err != nil {
				continue
			}
			expiry := time.Now().Add(time.Duration(timeout) * time.Millisecond)
			d.typing.AddTypingUser(userID, roomID, &expiry)
		} else {
			d.typing.RemoveUser(userID, roomID)
		}
		if !slices.Contains(rooms, roomID) {
			rooms = append(rooms, roomID)
		}
	}
	interest := map[string]bool{}
	var events []json.RawMessage
	for _, roomID := range rooms {
		if !d.roomInterest(ctx, interest, roomID) {
			continue
		}
		if ev, err := typingEvent(roomID, d.typing.GetTypingUsers(roomID)); err == nil {
			events = append(events, ev)
		}
	}
	return events
}

// onTypingTimeout tells the application service that a user stopped typing
// because their typing notification timed out.
func (d *ephemeralDelivery) onTypingTimeout(_, roomID string, _ int64) {
	if !isInterestedInRoom(d.ctx, d.cfg, d.rsAPI, d.state.ApplicationService, roomID) {
		return
	}
	ev, err := typingEvent(roomID, d.typing.GetTypingUsers(roomID))
	if err != nil {
		return
	}
	if err = d.send(d.ctx, []json.RawMessage{ev}, nil); err != nil {
		log.WithField("appservice", d.state.ID).WithError(err).Debug("Failed to send typing timeout to appservice")
	}
}

// receiptTS is a user's receipt in m.receipt content.
type receiptTS struct {
	TS       spec.Timestamp `json:"ts"`
	ThreadID string         `json:"thread_id,omitempty"`
}

// receiptEvents returns an m.receipt per room for the read receipts of rooms
// the application service is interested in. Private read receipts are only
// sent for users in its namespaces.
func (d *ephemeralDelivery) receiptEvents(ctx context.Context, msgs []*nats.Msg) []json.RawMessage {
	var rooms []string
	// room ID -> event ID -> receipt type -> user ID -> receipt
	content := map[string]map[string]map[string]map[string]receiptTS{}
	interest := map[string]bool{}
	for _, msg := range msgs {
		roomID := msg.Header.Get(jetstream.RoomID)
		userID := msg.Header.Get(jetstream.UserID)
		eventID := msg.Header.Get(jetstream.EventID)
		receiptType := msg.Header.Get("type")
		switch receiptType {
		case "m.read":
		case "m.read.private":
			if !isInterestedInUser(d.cfg, d.state.ApplicationService, userID) {
				continue
			}
		default:
			continue
		}
		timestamp, err := strconv.ParseUint(msg.Header.Get("timestamp"), 10, 64)
		if err != nil || !d.roomInterest(ctx, interest, roomID) {
			continue
		}
		if content[roomID] == nil {
			content[roomID] = map[string]map[string]map[string]receiptTS{}
			rooms = append(rooms, roomID)
		}
		if content[roomID][eventID] == nil {
			content[roomID][eventID] = map[string]map[string]receiptTS{}
		}
		if content[roomID][eventID][receiptType] == nil {
			content[roomID][eventID][receiptType] = map[string]receiptTS{}
		}
		content[roomID][eventID][receiptType][userID] = receiptTS{
			TS:       spec.Timestamp(timestamp),
			ThreadID: msg.Header.Get("thread_id"),
		}
	}
	events := make([]json.RawMessage, 0, len(rooms))
	for _, roomID := range rooms {
		ev, err := json.Marshal(map[string]any{
			"type":    spec.MReceipt,
			"room_id": roomID,
			"content": content[roomID],
		})
		if err == nil {
			events = append(events, ev)
		}
	}
	return events
}

// presenceEvents returns the latest m.presence of each user that is in the
// application service's namespaces or shares a room with a user who is.
func (d *ephemeralDelivery) presenceEvents(ctx context.Context, msgs []*nats.Msg) []json.RawMessage {
	var users []string
	latest := map[string]*nats.Msg{}
	for _, msg := range msgs {
		userID := msg.Header.Get(jetstream.UserID)
		if _, ok := latest[userID]; !ok {
			users = append(users, userID)
		}
		latest[userID] = msg
	}
	var events []json.RawMessage
	for _, userID := range users {
		msg := latest[userID]
		presence, ok := syncTypes.PresenceFromString(msg.Header.Get("presence"))
		if !ok {
			continue
		}
		ts, err := strconv.ParseUint(msg.Header.Get("last_active_ts"), 10, 64)
		if err != nil || !d.presenceInterest(ctx, userID) {
			continue
		}
		p := syncTypes.PresenceInternal{
			ClientFields: syncTypes.PresenceClientResponse{Presence: presence.String()},
			LastActiveTS: spec.Timestamp(ts),
		}
		if values, ok := msg.Header["status_msg"]; ok && len(values) > 0 {
			statusMsg := msg.Header.Get("status_msg")
			p.ClientFields.StatusMsg = &statusMsg
		}
		p.ClientFields.LastActiveAgo = p.LastActiveAgo()
		if presence == syncTypes.PresenceOnline {
			currentlyActive := p.CurrentlyActive()
			p.ClientFields.CurrentlyActive = &currentlyActive
		}
		ev, err := json.Marshal(map[string]any{
			"type":    spec.MPresence,
			"sender":  userID,
			"content": p.ClientFields,
		})
		if err == nil {
			events = append(events, ev)
		}
	}
	return events
}

// presenceInterest reports whether a user's presence applies to the
// application service: the user is in its namespaces, or shares a room with
// a local user who is.
func (d *ephemeralDelivery) presenceInterest(ctx context.Context, userID string) bool {
	if isInterestedInUser(d.cfg, d.state.ApplicationService, userID) {
		return true
	}
	var res api.QuerySharedUsersResponse
	if err := d.rsAPI.QuerySharedUsers(ctx, &api.QuerySharedUsersRequest{UserID: userID, LocalOnly: true}, &res); err != nil {
		log.WithField("appservice", d.state.ID).WithError(err).Error("Unable to query users sharing a room")
		return false
	}
	for other := range res.UserIDsToCount {
		if isInterestedInUser(d.cfg, d.state.ApplicationService, other) {
			return true
		}
	}
	return false
}
