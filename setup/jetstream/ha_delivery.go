package jetstream

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
)

// HAConsumerAckWait bounds redelivery when a former owner stops making progress.
// Healthy long-running handlers extend it with KeepAliveDelivery.
const HAConsumerAckWait = 5 * time.Second

// ConfigureHADelivery preserves a durable consumer while bounding orphaned work.
// Never delete a consumer: interest retention can discard its pending messages.
func ConfigureHADelivery(ctx context.Context, js nats.JetStreamContext, stream, durable string) error {
	owner := OwnershipOf(js)
	if owner == nil {
		return nil
	}
	if err := owner.Valid(); err != nil {
		return err
	}
	info, err := js.ConsumerInfo(stream, durable, nats.Context(ctx))
	if errors.Is(err, nats.ErrConsumerNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	cfg := info.Config
	// BackOff overrides AckWait; do not silently change a custom retry policy.
	if len(cfg.BackOff) != 0 {
		return fmt.Errorf("HA consumer %s has an incompatible backoff policy", durable)
	}
	if cfg.AckWait == HAConsumerAckWait {
		return nil
	}
	cfg.AckWait = HAConsumerAckWait
	_, err = js.UpdateConsumer(stream, &cfg, nats.Context(ctx))
	return err
}

// KeepAliveDelivery extends an active handler's delivery without holding work
// hostage after its owner freezes or retires. The caller must stop it before ack.
func KeepAliveDelivery(ctx context.Context, owner Ownership, msgs []*nats.Msg) func() {
	if owner == nil {
		return func() {}
	}
	ctx, cancel := context.WithCancelCause(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(HAConsumerAckWait / 5)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if owner.Valid() != nil {
					return
				}
				for _, msg := range msgs {
					// Sent without waiting for a reply. The broker can leave a
					// progress update unanswered, as while the stream elects a
					// new leader, and waiting for it would stop the keepalive
					// until the handler returns, so the message would be
					// redelivered while it is still being processed.
					if err := msg.InProgress(); err != nil {
						return
					}
				}
			}
		}
	}()
	return func() { cancel(context.Canceled); <-done }
}

// ownedPullExpiry bounds how long the broker keeps an owned pull request.
const ownedPullExpiry = time.Second

// ownedPullsExpire is when the last owned pull request this process sent
// expires at the broker, in Unix nanoseconds.
var ownedPullsExpire atomic.Int64

// FetchOwned keeps a frozen owner's outstanding broker pulls short-lived.
// The parent context retains cancellation while the broker request expires.
func FetchOwned(ctx context.Context, sub *nats.Subscription, batch int, owner Ownership) ([]*nats.Msg, error) {
	if owner == nil {
		return sub.Fetch(batch, nats.Context(ctx))
	}
	expires := time.Now().Add(ownedPullExpiry)
	for {
		last := ownedPullsExpire.Load()
		if last >= expires.UnixNano() || ownedPullsExpire.CompareAndSwap(last, expires.UnixNano()) {
			break
		}
	}
	ctx, cancel := context.WithDeadline(ctx, expires)
	defer cancel()
	return sub.Fetch(batch, nats.Context(ctx))
}

// WaitForOwnedPulls returns once every owned pull request this process sent
// has expired at the broker, or when ctx ends.
//
// Stopping a worker does not withdraw its pull request from the broker, which
// can still deliver a message to it until it expires. Such a message is lost
// to the stopped process and waits the whole acknowledgement timeout before
// it is redelivered, so an owner that hands over waits for this before it
// releases the Lease and lets a successor publish.
func WaitForOwnedPulls(ctx context.Context) {
	wait := time.Until(time.Unix(0, ownedPullsExpire.Load()))
	if wait <= 0 {
		return
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}
