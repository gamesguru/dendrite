package jetstream

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"

	"codefloe.com/pat-s/zendrite/setup/process"
)

// publishAttemptTimeout bounds one publication attempt. Tests shorten it.
//
// A publication sent to a broker that fails before acknowledging it is never
// answered, and the client does not resend it after reconnecting to another
// broker. Without a bound, the caller waits for as long as its context allows,
// which for an inbound federation transaction is indefinitely, and every
// request queued behind it waits too.
var publishAttemptTimeout = 5 * time.Second

// Publish publishes msg and retries attempts lost to a broker failure until
// ctx ends.
//
// Every attempt carries the same Nats-Msg-Id, so the stream stores the message
// once even when an attempt that went unacknowledged was in fact persisted.
// The streams keep JetStream's default two-minute deduplication window, which
// comfortably covers retries during a leader election.
func Publish(ctx context.Context, js nats.JetStreamContext, msg *nats.Msg) (*nats.PubAck, error) {
	if msg.Header == nil {
		msg.Header = nats.Header{}
	}
	if msg.Header.Get(nats.MsgIdHdr) == "" {
		msg.Header.Set(nats.MsgIdHdr, uuid.NewString())
	}
	for {
		attempt, cancel := context.WithTimeout(ctx, publishAttemptTimeout)
		ack, err := js.PublishMsg(msg, nats.Context(attempt))
		cancel()
		if err == nil {
			return ack, nil
		}
		if ctx.Err() != nil || !retryablePublishError(err) {
			return nil, err
		}
		if !process.Sleep(ctx, FetchRetryDelay) {
			return nil, fmt.Errorf("%w: %w", ctx.Err(), err)
		}
	}
}

// retryablePublishError reports failures that say nothing about whether the
// stream is willing to take the message: a lost acknowledgement, a broker that
// is reconnecting, or a stream that is electing a leader. Refusals, such as an
// ownership barrier or a rejected subject, are returned to the caller.
func retryablePublishError(err error) bool {
	return errors.Is(err, context.DeadlineExceeded) ||
		errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoStreamResponse) ||
		IsTransientFetchError(err)
}
