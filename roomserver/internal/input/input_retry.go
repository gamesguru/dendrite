// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/process"
)

// Retrying an input that failed on this server's own dependencies. Tests
// shorten them.
var (
	inputRetryBaseDelay = 250 * time.Millisecond
	inputRetryMaxDelay  = 5 * time.Second
	// The budget bounds how long one input is retried. It outlasts a
	// PostgreSQL failover, a broker leader election and the HA ownership
	// freshness window, but a dependency that stays down cannot hold the
	// room's queue for longer than this.
	inputRetryBudget = 2 * time.Minute
)

// processInput processes an input from the room's queue, retrying it while it
// fails because this server's own database, broker or HA ownership is
// unavailable, as during a PostgreSQL failover.
//
// Acknowledging such a failure discards the input. For an event received over
// federation that loses it: the remote's /send transaction has succeeded, so it
// never resends the event, and only a later event in the room that refers to
// it may fetch it again.
//
// The message is held and processed again here rather than negatively
// acknowledged, because the room's inputs must be processed in order: the
// room's consumer would deliver the next input while a Nak'd one waits out its
// delay. Holding it keeps the rest of the room's queue behind it, as it is
// behind any input that takes long to process.
//
// Only asynchronous inputs, such as events received over federation, are
// retried. A synchronous caller, such as a client sending a message, is
// waiting for the reply and has a deadline of its own. It is answered with the
// failure straight away, as before, and can retry; retrying here instead would
// keep it waiting beyond its deadline, and could store an event its caller was
// told had failed.
func (w *worker) processInput(msg *nats.Msg, input *api.InputRoomEvent) error {
	ctx := w.r.ProcessContext.Context()
	owner := jetstream.OwnershipOf(w.r.JetStream)
	virtualHost := spec.ServerName(msg.Header.Get("virtual_host"))
	run := func() error {
		stopProgress := jetstream.KeepAliveDelivery(ctx, owner, []*nats.Msg{msg})
		defer stopProgress()
		return w.r.processRoomEvent(ctx, virtualHost, input)
	}
	if msg.Header.Get("sync") != "" {
		return run()
	}
	logger := logrus.WithFields(logrus.Fields{
		"room_id":  w.roomID,
		"event_id": input.Event.EventID(),
		"type":     input.Event.Type(),
	})
	return retryWhileUnavailable(ctx, owner, logger, func(retry int) error {
		if retry > 0 {
			// Processing may have changed the input, e.g. to carry the state
			// it resolved, so start again from the message as queued.
			var fresh api.InputRoomEvent
			if err := json.Unmarshal(msg.Data, &fresh); err != nil {
				return fmt.Errorf("json.Unmarshal: %w", err)
			}
			*input = fresh
			// Outside HA the acknowledgement deadline covers one attempt,
			// not a series of them. This is sent without waiting for a
			// reply: with a context, InProgress becomes a request, which
			// waits for as long as the context allows if the broker drops
			// it, as it may while the stream elects a new leader.
			if err := msg.InProgress(); err != nil {
				logger.WithError(err).Warn("Failed to extend the delivery of a retried room event")
			}
		}
		return run()
	})
}

// retryWhileUnavailable calls attempt until it succeeds, fails for a reason
// other than this server's own dependencies being unavailable, or
// inputRetryBudget is spent, backing off between attempts, and returns the
// last error.
//
// It gives up without retrying when the server is stopping or this activation
// no longer owns the stream: the caller then leaves the input unacknowledged,
// and it is redelivered after a restart or to the promoted activation. While
// HA ownership is merely unconfirmed, as when the database is failing over, it
// waits for ownership before the next attempt, since processing would only be
// refused.
func retryWhileUnavailable(ctx context.Context, owner jetstream.Ownership, logger *logrus.Entry, attempt func(retry int) error) error {
	giveUp := time.Now().Add(inputRetryBudget)
	delay := inputRetryBaseDelay
	for retry := 0; ; retry++ {
		err := attempt(retry)
		if err == nil || ctx.Err() != nil || !retryableInputError(ctx, err) {
			return err
		}
		if time.Now().Add(delay).After(giveUp) {
			logger.WithError(err).WithField("retries", retry).
				Errorf("Roomserver gives up retrying event after %s", inputRetryBudget)
			return err
		}
		logger.WithError(err).WithFields(logrus.Fields{"retry": retry + 1, "delay": delay}).
			Warn("Roomserver failed to process event on this server's own dependencies, retrying")
		if !process.Sleep(ctx, delay) {
			return err
		}
		ownedCtx, cancel := context.WithDeadline(ctx, giveUp)
		ownedErr := jetstream.AwaitOwnership(ownedCtx, owner)
		cancel()
		if ownedErr != nil {
			return err
		}
		delay = min(2*delay, inputRetryMaxDelay) //nolint:mnd // Exponential backoff.
	}
}

// retryableInputError reports whether processing an input failed on this
// server's own dependencies, so that the same input may succeed once they
// recover.
//
// Unlike the missing-state lookup, which counts only errors the database
// driver reports itself, this also counts dropped connections: a query cut
// off by a PostgreSQL failover fails with a plain network error, as in #426.
// Failures of remote servers do not reach here as such. Processing turns them
// into a rejection or into an error of its own, and a rejection is never
// retried.
func retryableInputError(ctx context.Context, err error) bool {
	var rejected types.RejectedError
	if errors.As(err, &rejected) {
		return false
	}
	return unavailableHere(ctx, err) || sqlutil.IsTransient(err)
}
