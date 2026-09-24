package jetstream

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/setup/process"
)

// JetStreamConsumer starts a durable consumer on the given subject with the
// given durable name. The function will be called when one or more messages
// is available, up to the maximum batch size specified. If the batch is set to
// 1 then messages will be delivered one at a time. If the function is called,
// the messages array is guaranteed to be at least 1 in size. Any provided NATS
// options will be passed through to the pull subscriber creation. The consumer
// will continue to run until the context expires, at which point it will stop.
func JetStreamConsumer(
	ctx context.Context, js nats.JetStreamContext, subj, durable string, batch int,
	f func(ctx context.Context, msgs []*nats.Msg) bool,
	opts ...nats.SubOpt,
) error {
	var ownership Ownership
	if protected, isProtected := js.(protectedJetStream); isProtected {
		ownership = protected.ownership
	}
	defer func(durable string) {
		if _, protected := js.(protectedJetStream); protected {
			return
		}
		// If there are existing consumers from before they were pull
		// consumers, we need to clean up the old push consumers. However,
		// in order to not affect the interest-based policies, we need to
		// do this *after* creating the new pull consumers, which have
		// "Pull" suffixed to their name.
		if _, err := js.ConsumerInfo(subj, durable); err == nil {
			if err := js.DeleteConsumer(subj, durable); err != nil {
				logrus.WithContext(ctx).Warnf("Failed to clean up old consumer %q", durable)
			}
		}
	}(durable)

	durable += "Pull"
	if ownership != nil {
		if err := ConfigureHADelivery(ctx, js, subj, durable); err != nil {
			return err
		}
		opts = append(opts, nats.AckWait(HAConsumerAckWait))
	}
	sub, err := js.PullSubscribe(subj, durable, opts...)
	if err != nil {
		sentry.CaptureException(err)
		logrus.WithContext(ctx).WithError(err).Warnf("Failed to configure durable %q", durable)
		return err
	}
	process.Go(ctx, func() { jetStreamConsumerWorker(ctx, sub, subj, batch, f, ownership) })
	return nil
}

const (
	// FetchRetryDelay bounds retries while brokers reconnect or elect a leader.
	FetchRetryDelay = 250 * time.Millisecond
	// RedeliveryBaseDelay is how long a consumer waits before the first
	// redelivery of a message it refused.
	redeliveryBaseDelay = 250 * time.Millisecond
	// RedeliveryMaxDelay caps the backoff. A consumer that has been failing
	// for a while should keep checking, but not faster than this.
	redeliveryMaxDelay = 30 * time.Second
)

// IsTransientFetchError reports transport and leader changes that invalidate a
// pull request without invalidating its durable consumer. Retrying preserves
// queued work; deleting the subscription or stopping the worker strands it.
func IsTransientFetchError(err error) bool {
	return errors.Is(err, nats.ErrFetchDisconnected) ||
		errors.Is(err, nats.ErrDisconnected) ||
		errors.Is(err, nats.ErrConnectionReconnecting) ||
		errors.Is(err, nats.ErrNoResponders) ||
		errors.Is(err, nats.ErrConsumerLeadershipChanged) ||
		(err != nil && err.Error() == "nats: Server Shutdown")
}

// redeliveryDelay reports how long to wait before redelivering a message the
// consumer refused, backing off exponentially with the number of attempts.
//
// Refusing without a delay spins. The failures that make a consumer refuse are
// usually the persistent kind, a dependency that is down rather than a single
// unlucky call, so an immediate redelivery hammers that dependency for as long
// as the outage lasts, which is precisely when it can least afford it.
//
// Consumers deliberately set no MaxDeliver. For a homeserver, silently
// discarding work is worse than retrying it forever: the backoff bounds the
// cost of retrying, and consumerRedeliveries makes a message that will never
// succeed visible to an operator.
func redeliveryDelay(msg *nats.Msg) time.Duration {
	// A message with no readable ack metadata is treated as a first attempt,
	// so it still waits rather than coming straight back.
	attempts := uint64(1)
	if meta, err := msg.Metadata(); err == nil && meta.NumDelivered > attempts {
		attempts = meta.NumDelivered
	}
	return redeliveryBackoff(attempts)
}

// redeliveryBackoff doubles the base delay once per attempt, up to the cap.
func redeliveryBackoff(attempts uint64) time.Duration {
	delay := redeliveryBaseDelay
	for i := uint64(1); i < attempts; i++ {
		if delay >= redeliveryMaxDelay {
			break
		}
		delay *= 2
	}
	return min(delay, redeliveryMaxDelay)
}

func jetStreamConsumerWorker(
	ctx context.Context, sub *nats.Subscription, subj string, batch int,
	f func(ctx context.Context, msgs []*nats.Msg) bool,
	ownership Ownership,
) {
	// ownedOrStop reports whether this activation may still act on the stream.
	// A retired owner must not fetch or acknowledge: acknowledging work it did
	// not complete would discard it, so the consumer stops and leaves the
	// message pending for the promoted activation to redeliver.
	//
	// Unconfirmed ownership is not retirement. AwaitOwnership waits it out
	// rather than returning, because a worker that returns is never restarted.
	ownedOrStop := func(stage string) bool {
		if err := AwaitOwnership(ctx, ownership); err != nil {
			logrus.WithContext(ctx).WithField("subject", subj).WithError(err).
				Warnf("Stopping consumer before %s: ownership is no longer held", stage)
			return false
		}
		return true
	}
	for {
		// If the parent context has given up then there's no point in
		// carrying on doing anything, so stop the listener.
		select {
		case <-ctx.Done():
			return
		default:
		}
		if !ownedOrStop("fetching") {
			return
		}
		// The context behavior here is surprising — we supply a context
		// so that we can interrupt the fetch if we want, but NATS will still
		// enforce its own deadline (roughly 5 seconds by default). Therefore
		// it is our responsibility to check whether our context expired or
		// not when a context error is returned. Footguns. Footguns everywhere.
		msgs, err := FetchOwned(ctx, sub, batch, ownership)
		if err != nil {
			switch {
			case errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded):
				// Work out whether it was the JetStream context that expired
				// or whether it was our supplied context.
				select {
				case <-ctx.Done():
					// The supplied context expired, so we want to stop the
					// consumer altogether.
					return
				default:
					// The JetStream context expired, so the fetch probably
					// just timed out and we should try again.
					continue
				}
			case errors.Is(err, nats.ErrTimeout):
				// Pull request was invalidated, try again.
				continue
			case IsTransientFetchError(err):
				if !process.Sleep(ctx, FetchRetryDelay) {
					return
				}
				continue
			default:
				// Something else went wrong.
				logrus.WithContext(ctx).WithField("subject", subj).WithError(err).Warn("Error on pull subscriber fetch")
				return
			}
		}
		if len(msgs) < 1 {
			continue
		}
		if !ownedOrStop("processing") {
			return
		}
		msgs = ownedGeneration(ctx, ownership, subj, msgs)
		if len(msgs) == 0 {
			continue
		}
		for _, msg := range msgs {
			if err = msg.InProgress(nats.Context(ctx)); err != nil {
				logrus.WithContext(ctx).WithField("subject", subj).Warn(fmt.Errorf("msg.InProgress: %w", err))
				sentry.CaptureException(err)
				continue
			}
		}
		stopProgress := KeepAliveDelivery(ctx, ownership, msgs)
		acknowledge := f(ctx, msgs)
		stopProgress()
		// Ownership can be lost while the callback is running, so it is
		// rechecked here rather than only before the fetch.
		if !ownedOrStop("acknowledging") {
			return
		}
		if acknowledge {
			for _, msg := range msgs {
				if err = msg.AckSync(nats.Context(ctx)); err != nil {
					logrus.WithContext(ctx).WithField("subject", subj).Warn(fmt.Errorf("msg.AckSync: %w", err))
					sentry.CaptureException(err)
				}
			}
		} else {
			for _, msg := range msgs {
				delay := redeliveryDelay(msg)
				consumerRedeliveries.WithLabelValues(subj).Inc()
				if err = msg.NakWithDelay(delay, nats.Context(ctx)); err != nil {
					logrus.WithContext(ctx).WithField("subject", subj).Warn(fmt.Errorf("msg.NakWithDelay: %w", err))
					sentry.CaptureException(err)
				}
			}
		}
	}
}

// ownedGeneration records messages published by a retired activation.
//
// Publication is gated on the ownership freshness window, so a retired owner
// can still publish for a short period after its successor activates. The
// messages are processed rather than discarded, because a suspended owner
// publishes only for writes it already committed, and dropping them would lose
// events that exist in the database. Counting them makes the overlap visible.
func ownedGeneration(ctx context.Context, ownership Ownership, subj string, msgs []*nats.Msg) []*nats.Msg {
	if ownership == nil {
		return msgs
	}
	for _, msg := range msgs {
		stamped := msg.Header.Get(GenerationHeader)
		if stamped == "" {
			continue
		}
		generation, err := strconv.ParseInt(stamped, 10, 64)
		if err != nil || generation >= ownership.Generation() {
			continue
		}
		retiredGenerationMessages.WithLabelValues(subj).Inc()
		logrus.WithContext(ctx).WithFields(logrus.Fields{
			"subject":    subj,
			"published":  generation,
			"generation": ownership.Generation(),
		}).Warn("Processing a message published by a retired activation")
	}
	return msgs
}
