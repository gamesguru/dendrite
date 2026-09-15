package jetstream

import (
	"context"
	"errors"
	"fmt"
	"strconv"

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
	sub, err := js.PullSubscribe(subj, durable, opts...)
	if err != nil {
		sentry.CaptureException(err)
		logrus.WithContext(ctx).WithError(err).Warnf("Failed to configure durable %q", durable)
		return err
	}
	process.Go(ctx, func() { jetStreamConsumerWorker(ctx, sub, subj, batch, f, ownership) })
	return nil
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
		msgs, err := sub.Fetch(batch, nats.Context(ctx))
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
			case errors.Is(err, nats.ErrConsumerLeadershipChanged):
				// Leadership changed so pending pull requests became invalidated,
				// just try again.
				continue
			case err.Error() == "nats: Server Shutdown":
				// The server is shutting down, but we'll rely on reconnect
				// behavior to try and either connect us to another node (if
				// clustered) or to reconnect when the server comes back up.
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
		acknowledge := f(ctx, msgs)
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
				if err = msg.Nak(nats.Context(ctx)); err != nil {
					logrus.WithContext(ctx).WithField("subject", subj).Warn(fmt.Errorf("msg.Nak: %w", err))
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
