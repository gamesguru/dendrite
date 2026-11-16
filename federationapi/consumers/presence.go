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

// presenceEDUSender is the subset of queue.OutgoingQueues this consumer uses,
// so tests can observe and fail sends without a live federation queue.
type presenceEDUSender interface {
	SendEDU(e *gomatrixserverlib.EDU, origin spec.ServerName, destinations []spec.ServerName) error
}

// OutputReceiptConsumer consumes events that originate in the clientapi.
type OutputPresenceConsumer struct {
	ctx                     context.Context
	jetstream               nats.JetStreamContext
	durable                 string
	db                      storage.Database
	queues                  presenceEDUSender
	isLocalServerName       func(spec.ServerName) bool
	rsAPI                   roomserverAPI.FederationRoomserverAPI
	topic                   string
	outboundPresenceEnabled bool
	flushInterval           time.Duration
	pendingMu               sync.Mutex
	pending                 map[string]pendingPresence
	nextGeneration          uint64
}

type pendingPresence struct {
	presence   string
	status     *string
	lastSeen   spec.Timestamp
	generation uint64
	// origin is the server name the user belongs to. It differs from the
	// primary server name for virtual-host users and determines the EDU
	// origin and signing identity.
	origin spec.ServerName
}

// presenceBatchKey groups presence updates that can share one EDU.
type presenceBatchKey struct {
	origin      spec.ServerName
	destination spec.ServerName
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
	// Flush the oldest updates first. Sorting by user ID would let churn from
	// early-alphabet users starve the rest of the map under the batch cap.
	sort.Slice(users, func(i, j int) bool {
		return t.pending[users[i]].generation < t.pending[users[j]].generation
	})
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

	byDestination := make(map[presenceBatchKey][]fedTypes.PresenceContent)
	destinationUsers := make(map[presenceBatchKey][]string)
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
			// Users from different virtual hosts must keep their own origin,
			// and the origin signs the EDU, so they cannot share a batch.
			key := presenceBatchKey{origin: update.origin, destination: host}
			byDestination[key] = append(byDestination[key], content)
			destinationUsers[key] = append(destinationUsers[key], userID)
		}
	}

	// Sort the keys so the send order, and tests, are deterministic.
	keys := make([]presenceBatchKey, 0, len(byDestination))
	for key := range byDestination {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].origin != keys[j].origin {
			return keys[i].origin < keys[j].origin
		}
		return keys[i].destination < keys[j].destination
	})
	for _, key := range keys {
		updates := byDestination[key]
		edu := &gomatrixserverlib.EDU{Type: spec.MPresence, Origin: string(key.origin)}
		content, err := json.Marshal(fedTypes.Presence{Push: updates})
		if err != nil {
			log.WithError(err).Error("failed to marshal presence EDU batch")
			for _, userID := range destinationUsers[key] {
				failed[userID] = true
			}
			continue
		}
		edu.Content = content
		if err = t.queues.SendEDU(edu, key.origin, []spec.ServerName{key.destination}); err != nil {
			log.WithError(err).WithFields(log.Fields{
				"origin":      string(key.origin),
				"destination": string(key.destination),
			}).Warn("failed to queue presence EDU batch")
			for _, userID := range destinationUsers[key] {
				failed[userID] = true
			}
		}
	}

	// A failed send keeps the user pending, so the next flush sends it to every
	// destination again, including ones that already accepted this update.
	// Presence is latest-state and idempotent, so a duplicate EDU is harmless;
	// tracking success per destination would complicate the pending map for no
	// semantic gain.
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
		origin:     serverName,
	}
	t.pendingMu.Unlock()

	// Acknowledge once the update is coalesced in memory. That is deliberate:
	// presence is ephemeral and the design accepts losing at most one flush
	// interval on a crash or shutdown. Deferring the acknowledgement would
	// redeliver the batch and duplicate sends for a state that a newer update
	// supersedes anyway.
	return true
}
