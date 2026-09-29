// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

// Package input contains the code processes new room events.
package input

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/getsentry/sentry-go"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	fedapi "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/roomserver/acls"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/internal/query"
	"codefloe.com/pat-s/zendrite/roomserver/producers"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/process"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// Inputer is responsible for consuming from the roomserver input
// streams and processing the events. All input events are queued
// into a single NATS stream and the order is preserved strictly.
// The `room_id` message header will contain the room ID which will
// be used to assign the pending event to a per-room worker.
//
// The input API maintains an ephemeral headers-only consumer. It
// will speed through the stream working out which room IDs are
// pending and create durable consumers for them. The durable
// consumer will then be used for each room worker goroutine to
// fetch events one by one and process them. Each room having a
// durable consumer of its own means there is no head-of-line
// blocking between rooms. Filtering ensures that each durable
// consumer only receives events for the room it is interested in.
//
// The ephemeral consumer closely tracks the newest events. The
// per-room durable consumers will only progress through the stream
// as events are processed.
//
//	      A BC *  -> positions of each consumer (* = ephemeral)
//	      ⌄ ⌄⌄ ⌄
//	ABAABCAABCAA  -> newest (letter = subject for each message)
//
// In this example, A is still processing an event but has two
// pending events to process afterwards. Both B and C are caught
// up, so they will do nothing until a new event comes in for B
// or C.
type Inputer struct {
	Cfg                 *config.RoomServer
	ProcessContext      *process.ProcessContext
	DB                  storage.RoomDatabase
	NATSClient          *nats.Conn
	JetStream           nats.JetStreamContext
	Durable             nats.SubOpt
	ServerName          spec.ServerName
	SigningIdentity     func(ctx context.Context, roomID spec.RoomID, senderID spec.UserID) (fclient.SigningIdentity, error)
	FSAPI               fedapi.RoomserverFederationAPI
	RSAPI               api.RoomserverInternalAPI
	KeyRing             gomatrixserverlib.JSONVerifier
	ACLs                *acls.ServerACLs
	InputRoomEventTopic string
	OutputProducer      *producers.RoomEventProducer
	workers             sync.Map // room ID -> *worker

	Queryer       *query.Queryer
	UserAPI       userapi.RoomserverUserAPI
	EnableMetrics bool

	// missingStateCooldown tracks rooms where state resolution recently failed.
	// When state resolution via federation fails for a room, we record the failure
	// time and skip further expensive federation lookups until the cooldown expires.
	// This prevents a broken room from burning CPU and memory on every incoming
	// transaction by repeatedly loading and discarding thousands of state events.
	missingStateCooldown sync.Map // room ID (string) -> time.Time
}

// missingStateCooldownDuration is how long we skip expensive federation state
// lookups after a failed attempt for a room. This prevents broken rooms (where
// state resolution always fails) from burning CPU and memory on every incoming
// transaction. After this duration, the next event will retry the full state
// resolution.
const missingStateCooldownDuration = 5 * time.Minute

// errMissingStateCooldown fails an input deferred by the missing-state
// cooldown. It is expected for as long as a room is broken, so it is logged
// where the input is deferred rather than reported as a processing failure.
var errMissingStateCooldown = errors.New("state resolution cooldown active")

// If a room consumer is inactive for a while then we will allow NATS
// to clean it up. This stops us from holding onto durable consumers
// indefinitely for rooms that might no longer be active, since they do
// have an interest overhead in the NATS Server. If the room becomes
// active again then we'll recreate the consumer anyway.
const inactiveThreshold = time.Hour * 24

const roomAckSlack = 10 * time.Second

type worker struct {
	sync.Mutex
	r            *Inputer
	roomID       string
	subscription *nats.Subscription
	sentryHub    *sentry.Hub
	ch           chan struct{}
	ctx          context.Context
	cancel       context.CancelCauseFunc
	ephemeralSeq uint64
	// last seq we fully processed
	durableSeq   uint64
	lastActivity time.Time
	// announcements are headers-only deliveries for this room that wait for
	// its durable consumer. The input stream keeps a message only while some
	// consumer still needs it, so acknowledging an announcement before the
	// room's consumer exists would let the stream discard the event. They
	// are keyed by stream sequence, as an announcement left unacknowledged
	// is delivered again.
	announcements map[uint64]*nats.Msg
}

// notify queues a call to _next in the worker's goroutine.
func (w *worker) notify() {
	select {
	case w.ch <- struct{}{}:
	default:
	}
}

// start launches the worker goroutine that processes events serially.
// The caller should take a lock before calling start.
func (w *worker) start() {
	// Worker is already running. A worker whose context is canceled is on its
	// way out: it will not process anything else, so it is replaced rather
	// than left to exit and strand the work it was just asked to do.
	if w.ctx != nil && w.ctx.Err() == nil {
		return
	}

	w.lastActivity = time.Now()
	ctx, cancel := context.WithCancelCause(w.r.ProcessContext.Context())
	w.ctx, w.cancel = ctx, cancel

	if !w.r.ProcessContext.Go(func() {
		logrus.Debugf("worker for room %q started", w.roomID)
		for {
			select {
			case <-ctx.Done():
			case <-w.ch:
				if ctx.Err() == nil {
					w._next() //nolint:contextcheck // ctx only bounds this worker's lifetime; _next works on the process context.
					continue
				}
				// Hand the wake-up to the worker that replaces this one.
				w.notify()
			}
			// Cleanup after the context is canceled, so we still can log the reason for cancellation.
			logrus.WithError(context.Cause(ctx)).Debugf("worker for room %q stopped", w.roomID)
			w.Lock()
			if w.ctx == ctx {
				w.ctx = nil
			}
			w.Unlock()
			return
		}
	}) {
		cancel(context.Canceled)
		w.ctx = nil
		w.cancel = nil
	}
}

// startWorkerForRoom records that the input stream holds an event for the
// room up to seq and wakes the room's worker, starting it if needed. The
// announcement is acknowledged once the room's durable consumer exists.
//
// It runs on the stream-wide headers-only subscription, so it must not wait
// on the broker: a slow call here would hold up the input of every room. The
// worker sets up the room's durable consumer itself, retrying until it
// succeeds, because no further notification may ever come for a room whose
// queued events are all announced already, as when createRoom queues every
// initial event at once and waits for them.
func (r *Inputer) startWorkerForRoom(roomID string, seq uint64, announcement *nats.Msg) {
	v, _ := r.workers.LoadOrStore(roomID, &worker{
		r:         r,
		roomID:    roomID,
		sentryHub: sentry.CurrentHub().Clone(),
		ch:        make(chan struct{}, 1),
	})
	w, ok := v.(*worker)
	if !ok {
		return
	}
	w.Lock()
	// An announcement left unacknowledged is delivered again, possibly after
	// newer ones.
	w.ephemeralSeq = max(w.ephemeralSeq, seq)
	logrus.Debugf("loaded worker for room %q at seq %d (subscription=%v)", roomID, seq, w.subscription)
	subscribed := w.subscription != nil
	if !subscribed && announcement != nil {
		if w.announcements == nil {
			w.announcements = map[uint64]*nats.Msg{}
		}
		w.announcements[seq] = announcement
	}
	w.start()
	w.Unlock()
	w.notify()
	if subscribed && announcement != nil {
		_ = announcement.Ack()
	}
}

// consumerSetupTimeout bounds each JetStream request that sets up a room's
// durable consumer. A replicated consumer is not answered for until its own
// Raft group has a leader, which on a loaded cluster can take longer than the
// client's five-second default. Abandoning the request then only means
// asking again for a consumer that has no leader yet.
const consumerSetupTimeout = 30 * time.Second

// consumerSetupRetryDelay spaces attempts to set up a room's durable consumer.
var consumerSetupRetryDelay = time.Second

// subscribe creates or updates the room's durable consumer and binds a pull
// subscription to it. It must only be called by the worker's goroutine.
func (w *worker) subscribe(ctx context.Context) error {
	r := w.r
	ctx, cancel := context.WithTimeout(ctx, consumerSetupTimeout)
	defer cancel()
	streamName := r.Cfg.Matrix.JetStream.Prefixed(jetstream.InputRoomEvent)
	consumer := r.Cfg.Matrix.JetStream.Prefixed("RoomInput" + jetstream.Tokenise(w.roomID))
	subject := r.Cfg.Matrix.JetStream.Prefixed(jetstream.InputRoomEventSubj(w.roomID))

	logger := logrus.WithFields(logrus.Fields{
		"stream_name": streamName,
		"consumer":    consumer,
	})
	// Create the consumer. We do this as a specific step rather than
	// letting PullSubscribe create it for us because we need the consumer
	// to outlive the subscription. If we do it this way, we can Bind in the
	// next step, and when we Unsubscribe, the consumer continues to live. If
	// we leave PullSubscribe to create the durable consumer, Unsubscribe will
	// delete it because it thinks it "owns" it, which in turn breaks the
	// interest-based retention storage policy.
	// If the durable consumer already exists, this is effectively a no-op.
	// Another interesting tid-bit here: the ACK policy is set to "all" so that
	// if we acknowledge a message, we also acknowledge everything that comes
	// before it. This is necessary because otherwise our consumer will never
	// acknowledge things we filtered out for other subjects and therefore they
	// will linger around forever.

	info, err := r.JetStream.ConsumerInfo(streamName, consumer, nats.Context(ctx))
	if err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
		return fmt.Errorf("get consumer info: %w", err)
	}

	ackWait := MaximumMissingProcessingTime + roomAckSlack
	if jetstream.OwnershipOf(r.JetStream) != nil {
		ackWait = jetstream.HAConsumerAckWait
	}
	consumerConfig := &nats.ConsumerConfig{
		Durable:           consumer,
		AckPolicy:         nats.AckExplicitPolicy,
		DeliverPolicy:     nats.DeliverAllPolicy,
		FilterSubject:     subject,
		AckWait:           ackWait,
		InactiveThreshold: inactiveThreshold,
	}

	// The consumer already exists, try to update if necessary.
	if info != nil {
		// Not using reflect.DeepEqual here, since consumerConfig does not explicitly set
		// e.g. the consumerName, which is added by NATS later. So this would result
		// in constantly updating/recreating the consumer.
		switch {
		case info.Config.AckWait.Nanoseconds() != consumerConfig.AckWait.Nanoseconds():
			// Initially we had a AckWait of 2m 10s, now we have 5m 10s, so we need to update
			// existing consumers.
			fallthrough
		case info.Config.AckPolicy != consumerConfig.AckPolicy:
			// We've changed the AckPolicy from AckAll to AckExplicit, this needs a
			// recreation of the consumer. (Note: Only a few changes actually need a recreat)
			logger.Warn("Consumer already exists, trying to update it.")
			// Try updating the consumer first
			if _, err = r.JetStream.UpdateConsumer(streamName, consumerConfig, nats.Context(ctx)); err != nil {
				// We failed to update the consumer, recreate it
				logger.WithError(err).Warn("Unable to update consumer, recreating...")
				if err = r.JetStream.DeleteConsumer(streamName, consumer, nats.Context(ctx)); err != nil {
					logger.WithError(err).Fatal("Unable to delete consumer")
					return err
				}
				// Set info to nil, so it can be recreated with the correct config.
				info = nil
			}
		}
	}

	if info == nil {
		// Create the consumer with the correct config
		if _, err = r.JetStream.AddConsumer(streamName, consumerConfig, nats.Context(ctx)); err != nil {
			return fmt.Errorf("create consumer: %w", err)
		}
	}

	// Bind to our durable consumer. We want to receive all messages waiting
	// for this subject and we want to manually acknowledge them, so that we
	// can ensure they are only cleaned up when we are done processing them.
	sub, err := r.JetStream.PullSubscribe(
		subject, consumer,
		nats.ManualAck(),
		nats.DeliverAll(),
		nats.AckWait(ackWait),
		nats.Bind(r.InputRoomEventTopic, consumer),
		nats.InactiveThreshold(inactiveThreshold),
		// No nats.Context here: the subscription would end with it.
	)
	if err != nil {
		return fmt.Errorf("subscribe: %w", err)
	}
	w.Lock()
	w.subscription = sub
	announcements := w.announcements
	w.announcements = nil
	w.Unlock()
	// The room's consumer now holds its events, so the stream-wide
	// announcements are no longer needed to keep them.
	for _, announcement := range announcements {
		_ = announcement.Ack()
	}
	return nil
}

// Start creates an ephemeral non-durable consumer on the roomserver
// input topic. It is configured to deliver us headers only because we
// don't actually care about the contents of the message at this point,
// we only care about the `room_id` field. Once a message arrives, we
// will look to see if we have a worker for that room which has its
// own consumer. If we don't, we'll start one.
func (r *Inputer) Start() error {
	if r.EnableMetrics {
		prometheus.MustRegister(roomserverInputBackpressure, processRoomEventDuration)
	}
	_, err := r.JetStream.Subscribe(
		"", // This is blank because we specified it in BindStream.
		func(m *nats.Msg) {
			r.ProcessContext.Do(func() {
				roomID := m.Header.Get(jetstream.RoomID)
				meta, _ := m.Metadata()
				r.startWorkerForRoom(roomID, meta.Sequence.Stream, m)
			})
		},
		nats.HeadersOnly(),
		nats.DeliverAll(),
		nats.AckExplicit(),
		// The room's worker acknowledges each announcement once the room's
		// durable consumer exists, rather than as soon as this callback
		// returns, so the input stream keeps the event until then.
		nats.ManualAck(),
		nats.ReplayInstant(),
		nats.BindStream(r.InputRoomEventTopic),
	)

	r.ProcessContext.Go(r.expireClientTransactions)

	// Make sure that the room consumers have the right config.
	stream := r.Cfg.Matrix.JetStream.Prefixed(jetstream.InputRoomEvent)
	for consumer := range r.JetStream.Consumers(stream) {
		switch {
		case consumer.Config.Durable == "":
			continue // Ignore ephemeral consumers
		case consumer.Config.InactiveThreshold != inactiveThreshold:
			consumer.Config.InactiveThreshold = inactiveThreshold
			if _, cerr := r.JetStream.UpdateConsumer(stream, &consumer.Config); cerr != nil {
				logrus.WithError(cerr).Warnf("Failed to update inactive threshold on consumer %q", consumer.Name)
			}
		}
	}

	return err
}

// clientTransactionRetention is how long a client transaction ID keeps
// deduplicating retries, as in Synapse. Clients retry within minutes.
const clientTransactionRetention = 24 * time.Hour

// expireClientTransactions periodically forgets client transactions older
// than clientTransactionRetention.
func (r *Inputer) expireClientTransactions() {
	ticker := time.NewTicker(time.Hour)
	defer ticker.Stop()
	for {
		removed, err := r.DB.ExpireClientTransactions(r.ProcessContext.Context(), time.Now().Add(-clientTransactionRetention))
		if err != nil && r.ProcessContext.Context().Err() == nil {
			logrus.WithError(err).Warn("Failed to expire client transactions")
		} else if removed > 0 {
			logrus.WithField("removed", removed).Debug("Expired client transactions")
		}
		select {
		case <-r.ProcessContext.Context().Done():
			return
		case <-ticker.C:
		}
	}
}

// _next is called by the worker for the room. It must only be called
// by the worker's goroutine.
func (w *worker) _next() {
	// Look up what the next event is that's waiting to be processed.
	ctx, cancel := context.WithTimeout(w.r.ProcessContext.Context(), time.Minute)
	defer cancel()
	// This consumer manages its own subscription, so the barrier in
	// jetstream.JetStreamConsumer does not cover it. Without this check a
	// retired owner would keep fetching, fail because the database is fenced,
	// and acknowledge the messages anyway, discarding room events before the
	// promoted activation could redeliver them.
	if err := jetstream.AwaitOwnership(ctx, jetstream.OwnershipOf(w.r.JetStream)); err != nil {
		logrus.WithError(err).WithField("room_id", w.roomID).
			Warn("Not processing room events: ownership is no longer held")
		// Ownership that is merely unconfirmed for longer than this fetch
		// deadline is transient, so queue another attempt. Returning without
		// one would leave the worker waiting for new activity in this room,
		// which may never come, with events already queued unprocessed.
		if errors.Is(err, context.DeadlineExceeded) {
			w.notify()
		}
		return
	}
	w.sentryHub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTag("room_id", w.roomID)
	})
	if w.subscription == nil {
		if err := w.subscribe(w.r.ProcessContext.Context()); err != nil {
			if w.r.ProcessContext.Context().Err() != nil {
				return
			}
			logrus.WithError(err).WithField("room_id", w.roomID).Error("Failed to subscribe to the room's input consumer, retrying")
			// The room has queued events, so try again now rather than when
			// the room next sees activity, which may never happen.
			if process.Sleep(w.r.ProcessContext.Context(), consumerSetupRetryDelay) {
				w.notify()
			}
			return
		}
	}
	msgs, err := jetstream.FetchOwned(ctx, w.subscription, 1, jetstream.OwnershipOf(w.r.JetStream))
	switch {
	case err == nil:
		// Is the server shutting down? If so, stop processing.
		if w.r.ProcessContext.Context().Err() != nil {
			return
		}
		// Make sure that once we're done here, we queue up another call
		// to _next in the worker goroutine.
		defer w.notify()
	case jetstream.IsTransientFetchError(err):
		// Keep the durable subscription and retry even if no new room
		// activity arrives. Broker failover must not strand queued events.
		if process.Sleep(w.r.ProcessContext.Context(), jetstream.FetchRetryDelay) {
			w.notify()
		}
		return
	case errors.Is(err, nats.ErrTimeout) || errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled):
		if jetstream.OwnershipOf(w.r.JetStream) != nil && ctx.Err() == nil && time.Since(w.lastActivity) < time.Minute {
			w.notify()
			return
		}
		// Is the server shutting down? If so, stop processing.
		if w.r.ProcessContext.Context().Err() != nil {
			return
		}
		// The context exceeded, so we've been waiting for more than a
		// minute for activity in this room. At this point we will shut
		// down the subscriber to free up resources. It'll get started
		// again if new activity happens.
		w.Lock()
		// inside the lock, let's check if the ephemeral consumer saw something new!
		// If so, we do have new messages after all, they just came at a bad time.
		if w.ephemeralSeq > w.durableSeq {
			w.Unlock()
			w.notify()
			return
		}

		if err = w.subscription.Unsubscribe(); err != nil {
			logrus.WithError(err).Errorf("Failed to unsubscribe to stream for room %q", w.roomID)
		}
		w.cancel(errors.New("inactivity timeout"))
		w.subscription = nil
		w.Unlock()
		return
	case errors.Is(err, nats.ErrConsumerDeleted) || errors.Is(err, nats.ErrConsumerNotFound):
		w.Lock()
		defer w.Unlock()
		// The consumer is gone, therefore it's reached the inactivity
		// threshold. Clean up and stop processing at this point, if a
		// new event comes in for this room then the ordered consumer
		// over the entire stream will recreate this anyway.
		if err = w.subscription.Unsubscribe(); err != nil {
			logrus.WithError(err).Errorf("Failed to unsubscribe to stream for room %q", w.roomID)
		}
		w.cancel(errors.New("consumer deleted by NATS due to inactivity"))
		w.subscription = nil
		return
	default:
		// Something went wrong while trying to fetch the next event
		// from the queue. In which case, we'll shut down the subscriber.
		// atomically clear the subscription and unsubscribe
		w.Lock()

		logrus.WithError(err).Errorf("Failed to get next stream message for room %q", w.roomID)
		if uerr := w.subscription.Unsubscribe(); uerr != nil {
			logrus.WithError(uerr).Errorf("Failed to unsubscribe to stream for room %q", w.roomID)
		}
		w.subscription = nil
		// Events announced for this room are still queued: subscribe again
		// shortly rather than wait for room activity that may never come.
		if w.ephemeralSeq > w.durableSeq {
			w.Unlock()
			if process.Sleep(w.r.ProcessContext.Context(), consumerSetupRetryDelay) {
				w.notify()
			}
			return
		}
		// Otherwise wait to be notified about new room activity again.
		// Maybe the problem will be corrected by then.
		w.cancel(fmt.Errorf("fetch failed: %w, restarting subscriber on new activity", err))
		w.Unlock()
		return
	}

	// Since we either Ack() or Term() the message at this point, we can defer decrementing the room backpressure
	defer roomserverInputBackpressure.With(prometheus.Labels{"room_id": w.roomID}).Dec()

	// If no error was reported, but we didn't get exactly one message,
	// then skip over this and try again on the next iteration.
	if len(msgs) != 1 {
		return
	}

	// A fetch issued before suspension may finish after this owner retires.
	if err = jetstream.AwaitOwnership(ctx, jetstream.OwnershipOf(w.r.JetStream)); err != nil {
		return
	}
	w.lastActivity = time.Now()

	// Try to unmarshal the input room event. If the JSON unmarshalling
	// fails then we'll terminate the message — this notifies NATS that
	// we are done with the message and never want to see it again.
	msg := msgs[0]
	meta, _ := msg.Metadata()
	w.durableSeq = meta.Sequence.Stream

	var inputRoomEvent api.InputRoomEvent
	if err = json.Unmarshal(msg.Data, &inputRoomEvent); err != nil {
		// using AckWait here makes the call synchronous; 5 seconds is the default value used by NATS
		_ = msg.Term(nats.AckWait(time.Second * 5)) //nolint:mnd
		return
	}

	w.sentryHub.ConfigureScope(func(scope *sentry.Scope) {
		scope.SetTag("event_id", inputRoomEvent.Event.EventID())
	})

	// Process the room event. An asynchronous input that fails on this
	// server's own dependencies is retried first; if it still fails, the
	// message is acknowledged below all the same. We'll store the error
	// result as a string, because we might want to return that to the
	// caller if it was a synchronous request.
	var errString string
	wasRejected := false
	err = w.processInput(msg, &inputRoomEvent)
	// A retried client transaction is done: the caller is told which event
	// the transaction produced instead.
	var duplicateOf string
	if dupErr := (types.DuplicateTransactionError{}); errors.As(err, &dupErr) {
		duplicateOf = dupErr.EventID
		err = nil
	}
	if err != nil {
		var rejErr types.RejectedError
		if errors.As(err, &rejErr) {
			// Don't send events that were rejected to Sentry
			logrus.WithError(err).WithFields(logrus.Fields{
				"room_id":  w.roomID,
				"event_id": inputRoomEvent.Event.EventID(),
				"type":     inputRoomEvent.Event.Type(),
			}).Warn("Roomserver rejected event")
			wasRejected = true
		} else if !errors.Is(err, errMissingStateCooldown) {
			if !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled) {
				w.sentryHub.CaptureException(err)
			}
			logrus.WithError(err).WithFields(logrus.Fields{
				"room_id":  w.roomID,
				"event_id": inputRoomEvent.Event.EventID(),
				"type":     inputRoomEvent.Event.Type(),
			}).Warn("Roomserver failed to process event")
		}
		// Even though we failed to process this message (e.g. due to Zendrite restarting and receiving a context canceled),
		// the message may already have been queued for redelivery or will be, so this makes sure that we still reprocess the msg
		// after restarting. We only Ack if the context was not yet canceled.
		//
		// A retired owner must not acknowledge here either: its failure is the
		// fence refusing the write, and acknowledging would discard an event
		// the successor still has to process.
		if w.r.ProcessContext.Context().Err() == nil && w.ownedForAck() {
			_ = msg.AckSync()
		}
		errString = err.Error()
	} else if w.ownedForAck() {
		_ = msg.AckSync()
	}

	if wasRejected {
		// Keep the auth reason alongside the marker: without it the client is
		// told only that something was rejected, and the "why" stays buried in
		// the server logs.
		errString = api.InputWasRejected + ": " + err.Error()
	}

	// A retired worker must not answer a successor's pending synchronous input.
	if !w.ownedForAck() {
		return
	}

	// If it was a synchronous input request then the "sync" field
	// will be present in the message. That means that someone is
	// waiting for a response. The temporary inbox name is present in
	// that field, so send back the error string (if any). If there
	// was no error then we'll return a blank message, which means
	// that everything was OK.
	if replyTo := msg.Header.Get("sync"); replyTo != "" {
		reply := &nats.Msg{Subject: replyTo, Data: []byte(errString), Header: nats.Header{}}
		if duplicateOf != "" {
			reply.Header.Set("event_id", inputRoomEvent.Event.EventID())
			reply.Header.Set("duplicate_of", duplicateOf)
		}
		if err = w.r.NATSClient.PublishMsg(reply); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"room_id":  w.roomID,
				"event_id": inputRoomEvent.Event.EventID(),
				"type":     inputRoomEvent.Event.Type(),
			}).Warn("Roomserver failed to respond for sync event")
		}
	}
}

// queueInputRoomEvents queues events into the roomserver input
// stream in NATS.
func (r *Inputer) queueInputRoomEvents(
	ctx context.Context,
	request *api.InputRoomEventsRequest,
) (replySub *nats.Subscription, err error) {
	// If the request is synchronous then we need to create a
	// temporary inbox to wait for responses on, and then create
	// a subscription to it. If it's asynchronous then we won't
	// bother, so these values will remain empty.
	var replyTo string
	if !request.Asynchronous {
		replyTo = nats.NewInbox()
		replySub, err = r.NATSClient.SubscribeSync(replyTo)
		if err != nil {
			return nil, fmt.Errorf("r.NATSClient.SubscribeSync: %w", err)
		}
		if replySub == nil {
			// This shouldn't ever happen, but it doesn't hurt to check
			// because we can potentially avoid a nil pointer panic later
			// if it did for some reason.
			return nil, fmt.Errorf("expected a subscription to the temporary inbox")
		}
	}

	// For each event, marshal the input room event and then
	// send it into the input queue.
	for _, e := range request.InputRoomEvents {
		roomID := e.Event.RoomID().String()
		subj := r.Cfg.Matrix.JetStream.Prefixed(jetstream.InputRoomEventSubj(roomID))
		msg := &nats.Msg{
			Subject: subj,
			Header:  nats.Header{},
		}
		msg.Header.Set("room_id", roomID)
		if replyTo != "" {
			msg.Header.Set("sync", replyTo)
		}
		msg.Header.Set("virtual_host", string(request.VirtualHost))
		msg.Data, err = json.Marshal(e)
		if err != nil {
			return nil, fmt.Errorf("json.Marshal: %w", err)
		}
		if _, err = jetstream.Publish(ctx, r.JetStream, msg); err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"room_id":  roomID,
				"event_id": e.Event.EventID(),
				"subj":     subj,
			}).Error("Roomserver failed to queue async event")
			return nil, fmt.Errorf("r.JetStream.PublishMsg: %w", err)
		}

		// Now that the event is queued, increment the room backpressure
		roomserverInputBackpressure.With(prometheus.Labels{"room_id": roomID}).Inc()
	}
	return
}

// InputRoomEvents implements api.RoomserverInternalAPI.
func (r *Inputer) InputRoomEvents(
	ctx context.Context,
	request *api.InputRoomEventsRequest,
	response *api.InputRoomEventsResponse,
) {
	// Queue up the event into the roomserver.
	replySub, err := r.queueInputRoomEvents(ctx, request)
	if err != nil {
		response.ErrMsg = err.Error()
		return
	}

	// If we aren't waiting for synchronous responses then we can
	// give up here, there is nothing further to do.
	if replySub == nil {
		return
	}

	// Otherwise, we'll want to sit and wait for the responses
	// from the roomserver. There will be one response for every
	// input we submitted. The last error value we receive will
	// be the one returned as the error string.
	defer replySub.Drain() //nolint:errcheck
	for i := 0; i < len(request.InputRoomEvents); i++ {
		msg, err := replySub.NextMsgWithContext(ctx)
		if err != nil {
			response.ErrMsg = err.Error()
			return
		}
		if len(msg.Data) > 0 {
			response.ErrMsg = string(msg.Data)
		}
		if original := msg.Header.Get("duplicate_of"); original != "" {
			if response.DuplicateOf == nil {
				response.DuplicateOf = map[string]string{}
			}
			response.DuplicateOf[msg.Header.Get("event_id")] = original
		}
	}
}

var roomserverInputBackpressure = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "zendrite",
		Subsystem: "roomserver",
		Name:      "input_backpressure",
		Help:      "How many events are queued for input for a given room",
	},
	[]string{"room_id"},
)

// ownedForAck reports whether this activation may still acknowledge work on the
// input stream. It never waits: acknowledgement happens after the work is done,
// so a stale owner leaves the message pending for redelivery instead.
func (w *worker) ownedForAck() bool {
	owner := jetstream.OwnershipOf(w.r.JetStream)
	if owner == nil {
		return true
	}
	if err := owner.Valid(); err != nil {
		logrus.WithError(err).WithField("room_id", w.roomID).
			Warn("Not acknowledging room event: ownership is no longer held")
		return false
	}
	return true
}
