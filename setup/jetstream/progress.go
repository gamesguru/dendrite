package jetstream

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"

	"codefloe.com/pat-s/zendrite/setup/process"
)

// defaultConsumerStatsInterval is how often a consumer's backlog is read from
// the broker.
const defaultConsumerStatsInterval = 30 * time.Second

// consumerStatsInterval is how often a consumer's backlog is read from the
// broker, in nanoseconds. Tests shorten it while monitors from earlier tests
// are still running, so it is atomic.
var consumerStatsInterval atomic.Int64

func init() { //nolint:gochecknoinits // A default for an atomic, which has no literal form.
	consumerStatsInterval.Store(int64(defaultConsumerStatsInterval))
}

// monitorBacklog refreshes consumer_pending_messages from the broker until
// ctx ends.
//
// The worker updates the gauge from each delivery's metadata, so a worker that
// stopped fetching, or one whose pulls keep failing, would leave it at its last
// value, often zero, while messages pile up. Reading the backlog from the
// broker keeps it current, so a growing backlog with an old
// consumer_last_ack_timestamp_seconds reveals the stall. A failed read, such
// as during a leader election, leaves the last value standing.
func monitorBacklog(ctx context.Context, sub *nats.Subscription, durable string) {
	pending := consumerPending.WithLabelValues(durable)
	for process.Sleep(ctx, time.Duration(consumerStatsInterval.Load())) {
		if info, err := sub.ConsumerInfo(); err == nil {
			pending.Set(float64(info.NumPending))
		}
	}
}

// countFetchError records a failed pull request of a durable consumer.
func countFetchError(durable string) {
	consumerFetchErrors.WithLabelValues(durable).Inc()
}
