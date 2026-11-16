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
	findDestinations        func(context.Context, spec.UserID) ([]spec.ServerName, error)
	ackPresence             func(*nats.Msg) error
	extendPresence          func(*nats.Msg) error
	streamSequence          func(*nats.Msg) uint64
	isLocalServerName       func(spec.ServerName) bool
	rsAPI                   roomserverAPI.FederationRoomserverAPI
	topic                   string
	outboundPresenceEnabled bool
	flushInterval           time.Duration
	pendingMu               sync.Mutex
	pending                 map[string]pendingPresence
	recentSequences         map[string]recentPresenceSequence
	lastSequencePrune       time.Time
	nextGeneration          uint64
	nextOrder               uint64
}

type pendingPresence struct {
	presence   string
	status     *string
	lastSeen   spec.Timestamp
	generation uint64
	order      uint64
	// origin is the server name the user belongs to. It differs from the
	// primary server name for virtual-host users and determines the EDU
	// origin and signing identity.
	origin spec.ServerName
	// Hosts that accepted this update, so retries avoid resending to them while
	// still picking up newly joined destinations.
	sentDestinations map[spec.ServerName]struct{}
	acks             []*pendingPresenceAck
}

type recentPresenceSequence struct {
	sequence  uint64
	expiresAt time.Time
}

type pendingPresenceAck struct {
	msg        *nats.Msg
	generation uint64
	mu         sync.Mutex
	acked      bool
}

// presenceBatchKey groups presence updates that can share one EDU.
type presenceBatchKey struct {
	origin      spec.ServerName
	destination spec.ServerName
}

const (
	presenceFlushBatchSize = 500
	// Retain sequence watermarks long enough to recognize delayed redeliveries
	// after the pending presence entry has been flushed. HA sets the ack wait to
	// HAConsumerAckWait, but outside HA JetStream's default is 30s, so five HA
	// ack waits would expire before a non-HA redelivery arrives.
	presenceSequenceRetention     = 2 * time.Minute
	presenceSequencePruneInterval = presenceSequenceRetention / 4
	defaultPresenceAckWait        = 30 * time.Second
	// The default flush interval applies when the configuration leaves the
	// federation presence interval unset.
	defaultPresenceFlushInterval = 5 * time.Second
	// The keep-alive interval extends deferred JetStream deliveries until their
	// presence EDU is queued. It must stay well inside the ack wait.
	pendingPresenceKeepAliveInterval = jetstream.DeferredProgressInterval
)

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
		flushInterval = defaultPresenceFlushInterval
	}
	consumer := &OutputPresenceConsumer{
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
	consumer.findDestinations = presenceDestinationFinder(rsAPI, store)
	owner := jetstream.OwnershipOf(js)
	consumer.ackPresence = func(msg *nats.Msg) error {
		if err := jetstream.AckOwned(consumer.ctx, owner, msg); err != nil {
			return err
		}
		jetstream.RecordConsumerAck(consumer.durable)
		return nil
	}
	consumer.streamSequence = func(msg *nats.Msg) uint64 {
		if meta, err := msg.Metadata(); err == nil {
			return meta.Sequence.Stream
		}
		return 0
	}
	consumer.extendPresence = func(msg *nats.Msg) error { return jetstream.ProgressOwned(owner, msg) }
	return consumer
}

// presenceDestinationFinder resolves the hosts that share a joined room with a
// user. NewOutputPresenceConsumer and the tests both build it here.
func presenceDestinationFinder(
	rsAPI roomserverAPI.FederationRoomserverAPI, store storage.Database,
) func(context.Context, spec.UserID) ([]spec.ServerName, error) {
	return func(ctx context.Context, userID spec.UserID) ([]spec.ServerName, error) {
		roomIDs, err := rsAPI.QueryRoomsForUser(ctx, userID, "join")
		if err != nil {
			return nil, err
		}
		roomIDStrs := make([]string, len(roomIDs))
		for i, roomID := range roomIDs {
			roomIDStrs[i] = roomID.String()
		}
		return store.GetJoinedHostsForRooms(ctx, roomIDStrs, true, true)
	}
}

// Start consuming from the clientapi.
func (t *OutputPresenceConsumer) Start() error {
	if !t.outboundPresenceEnabled {
		return nil
	}
	process.Go(t.ctx, t.flushLoop)
	process.Go(t.ctx, t.keepPendingAliveLoop)
	return jetstream.JetStreamConsumerWithAckAction(
		t.ctx, t.jetstream, t.topic, t.durable, 1, t.onMessage,
		nats.DeliverAll(), nats.ManualAck(), nats.HeadersOnly(),
	)
}

// Keep deferred deliveries alive until the corresponding latest presence state
// has been durably placed in the federation queue.
func (t *OutputPresenceConsumer) keepPendingAliveLoop() {
	ticker := time.NewTicker(pendingPresenceKeepAliveInterval)
	defer ticker.Stop()
	for {
		select {
		case <-t.ctx.Done():
			return
		case <-ticker.C:
			t.extendPending()
		}
	}
}

// extendPending extends every deferred delivery. Ownership is checked per
// message, so a former owner stops extending and JetStream redelivers.
func (t *OutputPresenceConsumer) extendPending() {
	t.pendingMu.Lock()
	t.pruneRecentPresenceSequences(time.Now())
	var messages []*nats.Msg
	for _, update := range t.pending {
		for _, pendingAck := range update.acks {
			messages = append(messages, pendingAck.msg)
		}
	}
	t.pendingMu.Unlock()
	for _, msg := range messages {
		if err := t.extendPresence(msg); err != nil {
			log.WithError(err).Warn("failed to extend pending presence message acknowledgement")
		}
	}
}

// pruneRecentPresenceSequences expires high-watermarks after a redelivery has
// had several ack-wait intervals to arrive. Callers hold pendingMu.
func (t *OutputPresenceConsumer) pruneRecentPresenceSequences(now time.Time) {
	if now.Sub(t.lastSequencePrune) < presenceSequencePruneInterval {
		return
	}
	for userID, sequence := range t.recentSequences {
		if !sequence.expiresAt.After(now) {
			delete(t.recentSequences, userID)
		}
	}
	t.lastSequencePrune = now
}

func (t *OutputPresenceConsumer) sequenceWatermarkRetention() time.Duration {
	retention := presenceSequenceRetention
	minimum := t.flushInterval + defaultPresenceAckWait
	if minimum > retention {
		retention = minimum
	}
	return retention
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
	// Flush the oldest updates first. Failed updates are moved to the tail below
	// so a persistent destination failure cannot starve other users.
	sort.Slice(users, func(i, j int) bool {
		left, right := t.pending[users[i]], t.pending[users[j]]
		if left.order == right.order {
			return users[i] < users[j]
		}
		return left.order < right.order
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
	lookupFailed := make(map[string]bool)
	failedDestinations := make(map[string]map[spec.ServerName]struct{})
	for userID, update := range snapshot {
		parsedUserID, err := spec.NewUserID(userID, true)
		if err != nil {
			lookupFailed[userID] = true
			continue
		}
		if _, ok := types.PresenceFromString(update.presence); !ok {
			lookupFailed[userID] = true
			continue
		}
		joined, err := t.findDestinations(t.ctx, *parsedUserID)
		if err != nil {
			log.WithError(err).WithField("user_id", userID).Warn("failed to find destinations for pending presence")
			lookupFailed[userID] = true
			continue
		}
		// Re-resolve membership on every attempt. Retry failures and include
		// newly joined destinations, while skipping destinations that already
		// durably accepted this update.
		unsent := joined[:0]
		for _, destination := range joined {
			if _, sent := update.sentDestinations[destination]; !sent {
				unsent = append(unsent, destination)
			}
		}
		joined = unsent
		sort.Slice(joined, func(i, j int) bool { return joined[i] < joined[j] })
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
				markPresenceDestinationFailed(failedDestinations, userID, key.destination)
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
				markPresenceDestinationFailed(failedDestinations, userID, key.destination)
			}
		}
	}

	acked := make(map[*nats.Msg]struct{})
	ackFailed := make(map[string]bool)
	sentDestinations := make(map[string]map[spec.ServerName]struct{})
	for userID, sent := range snapshot {
		if lookupFailed[userID] || len(failedDestinations[userID]) > 0 {
			continue
		}
		for _, pendingAck := range sent.acks {
			if pendingAck.generation > sent.generation {
				continue
			}
			if err := t.ackPendingPresence(pendingAck); err != nil {
				log.WithError(err).WithField("user_id", userID).Warn("failed to acknowledge queued presence update")
				ackFailed[userID] = true
				continue
			}
			acked[pendingAck.msg] = struct{}{}
		}
	}
	for key, userIDs := range destinationUsers {
		if len(failedDestinationsForKey(failedDestinations, userIDs, key.destination)) > 0 {
			continue
		}
		for _, userID := range userIDs {
			markPresenceDestinationFailed(sentDestinations, userID, key.destination)
		}
	}

	t.pendingMu.Lock()
	for userID, sent := range snapshot {
		current, exists := t.pending[userID]
		if !exists {
			continue
		}
		current.acks = removeAckedPresenceMessages(current.acks, acked)
		if current.generation == sent.generation {
			if lookupFailed[userID] {
				t.nextOrder++
				current.order = t.nextOrder
			} else {
				remaining := failedDestinations[userID]
				if remaining == nil {
					remaining = make(map[spec.ServerName]struct{})
				}
				for destination := range sentDestinations[userID] {
					if current.sentDestinations == nil {
						current.sentDestinations = make(map[spec.ServerName]struct{})
					}
					current.sentDestinations[destination] = struct{}{}
				}
				// Move users whose ack failed to the tail too, so persistent ack
				// failures cannot occupy the batch ahead of unsent users. Their
				// sent destinations are kept, so the retry only re-acks.
				if len(remaining) > 0 || ackFailed[userID] {
					t.nextOrder++
					current.order = t.nextOrder
				}
			}
			if len(current.acks) == 0 && !lookupFailed[userID] && len(failedDestinations[userID]) == 0 {
				delete(t.pending, userID)
				continue
			}
		}
		t.pending[userID] = current
	}
	t.pendingMu.Unlock()
}

func (t *OutputPresenceConsumer) ackPendingPresence(pendingAck *pendingPresenceAck) error {
	pendingAck.mu.Lock()
	defer pendingAck.mu.Unlock()
	if pendingAck.acked {
		return nil
	}
	if err := t.ackPresence(pendingAck.msg); err != nil {
		return err
	}
	pendingAck.acked = true
	return nil
}

func failedDestinationsForKey(failed map[string]map[spec.ServerName]struct{}, userIDs []string, destination spec.ServerName) []string {
	var failedUsers []string
	for _, userID := range userIDs {
		if _, ok := failed[userID][destination]; ok {
			failedUsers = append(failedUsers, userID)
		}
	}
	return failedUsers
}

func markPresenceDestinationFailed(failed map[string]map[spec.ServerName]struct{}, userID string, destination spec.ServerName) {
	if failed[userID] == nil {
		failed[userID] = make(map[spec.ServerName]struct{})
	}
	failed[userID][destination] = struct{}{}
}

func removeAckedPresenceMessages(acks []*pendingPresenceAck, acked map[*nats.Msg]struct{}) []*pendingPresenceAck {
	remaining := make([]*pendingPresenceAck, 0, len(acks))
	for _, pendingAck := range acks {
		if _, ok := acked[pendingAck.msg]; !ok {
			remaining = append(remaining, pendingAck)
		}
	}
	return remaining
}

// onMessage is called in response to a message received on the presence
// events topic from the client api.
func (t *OutputPresenceConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) jetstream.AckAction {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	// only send presence events which originated from us
	userID := msg.Header.Get(jetstream.UserID)
	_, serverName, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		log.WithError(err).WithField("user_id", userID).Error("failed to extract domain from receipt sender")
		return jetstream.AckActionAck
	}
	if !t.isLocalServerName(serverName) {
		return jetstream.AckActionAck
	}
	if _, err = spec.NewUserID(userID, true); err != nil {
		log.WithError(err).WithField("user_id", userID).Warn("ignoring invalid user ID in presence event")
		return jetstream.AckActionAck
	}

	presence := msg.Header.Get("presence")
	if _, ok := types.PresenceFromString(presence); !ok {
		log.WithField("user_id", userID).Warn("ignoring invalid presence value in presence event")
		return jetstream.AckActionAck
	}
	ts, err := strconv.Atoi(msg.Header.Get("last_active_ts"))
	if err != nil || ts < 0 {
		return jetstream.AckActionAck
	}

	var statusMsg *string = nil
	if data, ok := msg.Header["status_msg"]; ok && len(data) > 0 {
		status := msg.Header.Get("status_msg")
		statusMsg = &status
	}

	streamSeq := t.streamSequence(msg)

	t.pendingMu.Lock()
	now := time.Now()
	t.pruneRecentPresenceSequences(now)
	if recent, ok := t.recentSequences[userID]; ok && !recent.expiresAt.After(now) {
		delete(t.recentSequences, userID)
	}
	recent, hasRecent := t.recentSequences[userID]
	previous := t.pending[userID]
	if streamSeq != 0 && hasRecent && streamSeq <= recent.sequence {
		// Refresh the watermark while a duplicate is being redelivered so
		// repeated ack failures cannot expire the guard prematurely.
		recent.expiresAt = now.Add(t.sequenceWatermarkRetention())
		t.recentSequences[userID] = recent
		if streamSeq == recent.sequence {
			if _, stillPending := t.pending[userID]; stillPending {
				// This is the current message being redelivered before its EDU
				// was queued. Keep it deferred and use this delivery's ack handle.
				previous.acks = []*pendingPresenceAck{{msg: msg, generation: previous.generation}}
				t.pending[userID] = previous
				t.pendingMu.Unlock()
				return jetstream.AckActionDefer
			}
		}
		t.pendingMu.Unlock()
		return jetstream.AckActionAck
	}
	if streamSeq != 0 {
		if t.recentSequences == nil {
			t.recentSequences = make(map[string]recentPresenceSequence)
		}
		t.recentSequences[userID] = recentPresenceSequence{
			sequence: streamSeq, expiresAt: now.Add(t.sequenceWatermarkRetention()),
		}
	}
	t.nextGeneration++
	t.nextOrder++
	// Presence is latest state, so the newest message supersedes prior ones.
	superseded := previous.acks
	acks := []*pendingPresenceAck{{msg: msg, generation: t.nextGeneration}}
	t.pending[userID] = pendingPresence{
		presence:   presence,
		status:     statusMsg,
		lastSeen:   spec.Timestamp(ts),
		generation: t.nextGeneration,
		order:      t.nextOrder,
		origin:     serverName,
		acks:       acks,
	}
	t.pendingMu.Unlock()

	for _, old := range superseded {
		if err := t.ackPendingPresence(old); err != nil {
			log.WithError(err).WithField("user_id", userID).Warn("failed to acknowledge superseded presence update; leaving it for redelivery")
		}
	}

	// Keep the durable input message outstanding until the resulting EDU is
	// safely in the federation queue. A process crash before then lets JetStream
	// redeliver it instead of silently losing the coalesced presence update.
	return jetstream.AckActionDefer
}
