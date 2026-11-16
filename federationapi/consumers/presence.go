// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/federationapi/queue"
	"codefloe.com/pat-s/zendrite/federationapi/storage"
	fedTypes "codefloe.com/pat-s/zendrite/federationapi/types"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// OutputReceiptConsumer consumes events that originate in the clientapi.
type OutputPresenceConsumer struct {
	ctx                     context.Context
	jetstream               nats.JetStreamContext
	durable                 string
	db                      storage.Database
	queues                  *queue.OutgoingQueues
	isLocalServerName       func(spec.ServerName) bool
	rsAPI                   roomserverAPI.FederationRoomserverAPI
	topic                   string
	outboundPresenceEnabled bool
	flushInterval           time.Duration
	origin                  spec.ServerName
	pendingMu               sync.Mutex
	pending                 map[string]pendingPresence
	nextGeneration          uint64
}

type pendingPresence struct {
	presence   string
	status     *string
	lastSeen   spec.Timestamp
	generation uint64
}

const presenceFlushBatchSize = 500

// NewOutputPresenceConsumer creates a new OutputPresenceConsumer. Call Start() to begin consuming events.
func NewOutputPresenceConsumer(
	process *process.ProcessContext,
	cfg *config.FederationAPI,
	js nats.JetStreamContext,
	queues *queue.OutgoingQueues,
	store storage.Database,
	rsAPI roomserverAPI.FederationRoomserverAPI,
) *OutputPresenceConsumer {
	flushInterval := cfg.Matrix.Presence.FederationInterval
	if flushInterval <= 0 {
		flushInterval = 5 * time.Second
	}
	return &OutputPresenceConsumer{
		ctx:                     process.Context(),
		jetstream:               js,
		queues:                  queues,
		db:                      store,
		isLocalServerName:       cfg.Matrix.IsLocalServerName,
		durable:                 cfg.Matrix.JetStream.Durable("FederationAPIPresenceConsumer"),
		topic:                   cfg.Matrix.JetStream.Prefixed(jetstream.OutputPresenceEvent),
		outboundPresenceEnabled: cfg.Matrix.Presence.EnableOutbound,
		flushInterval:           flushInterval,
		origin:                  cfg.Matrix.ServerName,
		pending:                 make(map[string]pendingPresence),
		rsAPI:                   rsAPI,
	}
}

// Start consuming from the clientapi.
func (t *OutputPresenceConsumer) Start() error {
	if !t.outboundPresenceEnabled {
		return nil
	}
	process.Go(t.ctx, t.flushLoop)
	return jetstream.JetStreamConsumer(
		t.ctx, t.jetstream, t.topic, t.durable, 1, t.onMessage,
		nats.DeliverAll(), nats.ManualAck(), nats.HeadersOnly(),
	)
}

func (t *OutputPresenceConsumer) flushLoop() {
	ticker := time.NewTicker(t.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.flushPending()
		}
	}
}

func (t *OutputPresenceConsumer) flushPending() {
	t.pendingMu.Lock()
	users := make([]string, 0, len(t.pending))
	for userID := range t.pending {
		users = append(users, userID)
	}
	sort.Strings(users)
	if len(users) > presenceFlushBatchSize {
		users = users[:presenceFlushBatchSize]
	}
	snapshot := make(map[string]pendingPresence, len(users))
	for _, userID := range users {
		snapshot[userID] = t.pending[userID]
	}
	t.pendingMu.Unlock()
	if len(snapshot) == 0 {
		return
	}

	byDestination := make(map[spec.ServerName][]fedTypes.PresenceContent)
	destinationUsers := make(map[spec.ServerName][]string)
	failed := make(map[string]bool)
	for userID, update := range snapshot {
		parsedUserID, err := spec.NewUserID(userID, true)
		if err != nil {
			failed[userID] = true
			continue
		}
		roomIDs, err := t.rsAPI.QueryRoomsForUser(t.ctx, *parsedUserID, "join") //nolint:contextcheck
		if err != nil {
			log.WithError(err).WithField("user_id", userID).Warn("failed to find rooms for pending presence")
			failed[userID] = true
			continue
		}
		roomIDStrs := make([]string, len(roomIDs))
		for i, roomID := range roomIDs {
			roomIDStrs[i] = roomID.String()
		}
		joined, err := t.db.GetJoinedHostsForRooms(t.ctx, roomIDStrs, true, true) //nolint:contextcheck
		if err != nil {
			log.WithError(err).WithField("user_id", userID).Warn("failed to find destinations for pending presence")
			failed[userID] = true
			continue
		}
		if _, ok := types.PresenceFromString(update.presence); !ok {
			continue
		}
		age := time.Since(update.lastSeen.Time()).Milliseconds()
		if age < 0 {
			age = 0
		}
		presenceData := types.PresenceInternal{LastActiveTS: update.lastSeen}
		content := fedTypes.PresenceContent{
			CurrentlyActive: presenceData.CurrentlyActive(),
			LastActiveAgo:   age,
			Presence:        update.presence,
			StatusMsg:       update.status,
			UserID:          userID,
		}
		for _, host := range joined {
			byDestination[host] = append(byDestination[host], content)
			destinationUsers[host] = append(destinationUsers[host], userID)
		}
	}

	for destination, updates := range byDestination {
		edu := &gomatrixserverlib.EDU{Type: spec.MPresence, Origin: string(t.origin)}
		content, err := json.Marshal(fedTypes.Presence{Push: updates})
		if err != nil {
			log.WithError(err).Error("failed to marshal presence EDU batch")
			for _, userID := range destinationUsers[destination] {
				failed[userID] = true
			}
			continue
		}
		edu.Content = content
		if err = t.queues.SendEDU(edu, t.origin, []spec.ServerName{destination}); err != nil {
			log.WithError(err).WithField("destination", destination).Warn("failed to queue presence EDU batch")
			for _, userID := range destinationUsers[destination] {
				failed[userID] = true
			}
		}
	}

	t.pendingMu.Lock()
	for userID, sent := range snapshot {
		if !failed[userID] && t.pending[userID].generation == sent.generation {
			delete(t.pending, userID)
		}
	}
	t.pendingMu.Unlock()
}

// onMessage is called in response to a message received on the presence
// events topic from the client api.
func (t *OutputPresenceConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	// only send presence events which originated from us
	userID := msg.Header.Get(jetstream.UserID)
	_, serverName, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		log.WithError(err).WithField("user_id", userID).Error("failed to extract domain from receipt sender")
		return true
	}
	if !t.isLocalServerName(serverName) {
		return true
	}
	if _, err = spec.NewUserID(userID, true); err != nil {
		log.WithError(err).WithField("user_id", userID).Warn("ignoring invalid user ID in presence event")
		return true
	}

	presence := msg.Header.Get("presence")
	ts, err := strconv.Atoi(msg.Header.Get("last_active_ts"))
	if err != nil || ts < 0 {
		return true
	}

	var statusMsg *string = nil
	if data, ok := msg.Header["status_msg"]; ok && len(data) > 0 {
		status := msg.Header.Get("status_msg")
		statusMsg = &status
	}

	t.pendingMu.Lock()
	t.nextGeneration++
	t.pending[userID] = pendingPresence{
		presence:   presence,
		status:     statusMsg,
		lastSeen:   spec.Timestamp(ts),
		generation: t.nextGeneration,
	}
	t.pendingMu.Unlock()

	return true
}
