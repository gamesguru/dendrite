package jetstream

import (
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
)

// retiredGenerationMessages counts messages that reached a consumer carrying a
// generation older than the current activation's.
//
// Publication is gated on the ownership freshness window rather than on a
// broker-side check, so a short overlap is possible after a takeover. The
// subject label is bounded by the fixed set of stream subjects.
var retiredGenerationMessages = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "zendrite", Subsystem: "ha", Name: "retired_generation_messages_total",
	Help: "Messages consumed that were published by a retired activation.",
}, []string{"subject"})

// consumerRedeliveries counts messages a consumer refused and asked to have
// redelivered.
//
// Refusal is the correct response to a transient failure, but a subject whose
// count climbs steadily is a message that cannot be processed and is being
// retried forever. Consumers deliberately set no MaxDeliver, so this counter is
// the only way to see that from outside.
var consumerRedeliveries = prometheus.NewCounterVec(prometheus.CounterOpts{
	Namespace: "zendrite", Subsystem: "jetstream", Name: "consumer_redeliveries_total",
	Help: "Messages refused by a consumer and scheduled for redelivery.",
}, []string{"subject"})

// The consumer progress metrics below are labeled by the durable consumer
// name, such as ZendriteSyncAPIRoomServerConsumerPull, which is a fixed set.
//
// A consumer that spends minutes on one message still answers health probes
// and serves reads, so liveness alone hides it. The two timestamp gauges show
// the stall while it is happening, which the histograms cannot: they only
// observe a batch once it has finished.

// consumerProcessingSeconds observes how long the callback took for each
// fetched batch.
var consumerProcessingSeconds = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "zendrite", Subsystem: "jetstream", Name: "consumer_processing_seconds",
	Help:    "Time a consumer spent processing one fetched batch of messages.",
	Buckets: []float64{.005, .025, .1, .5, 1, 5, 15, 60, 300, 900},
}, []string{"consumer"})

// consumerMessageAge observes how long a message waited between publication
// and the consumer starting to process it, which is the consumer's backlog.
var consumerMessageAge = prometheus.NewHistogramVec(prometheus.HistogramOpts{
	Namespace: "zendrite", Subsystem: "jetstream", Name: "consumer_message_age_seconds",
	Help:    "Time between a message's publication and the consumer starting to process it.",
	Buckets: []float64{.005, .025, .1, .5, 1, 5, 15, 60, 300, 900},
}, []string{"consumer"})

// consumerProcessingStarted is the Unix time the batch being processed was
// started, or zero while the consumer is idle.
var consumerProcessingStarted = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "zendrite", Subsystem: "jetstream", Name: "consumer_processing_started_timestamp_seconds",
	Help: "Unix time the consumer started processing its current batch, or 0 while idle.",
}, []string{"consumer"})

// consumerProcessingMessage is the Unix publication time of the oldest
// message in the batch being processed, or zero while the consumer is idle.
// Messages are delivered in order, so while the consumer is busy this is the
// age of the oldest work it has not finished.
var consumerProcessingMessage = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "zendrite", Subsystem: "jetstream", Name: "consumer_processing_message_timestamp_seconds",
	Help: "Unix publication time of the oldest message in the batch being processed, or 0 while idle.",
}, []string{"consumer"})

// consumerPending is the number of stream messages after the last one the
// consumer was delivered, as reported in that delivery's metadata.
var consumerPending = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "zendrite", Subsystem: "jetstream", Name: "consumer_pending_messages",
	Help: "Messages waiting behind the last one delivered to the consumer.",
}, []string{"consumer"})

// consumerLastAck is the Unix time the consumer last acknowledged a message.
var consumerLastAck = prometheus.NewGaugeVec(prometheus.GaugeOpts{
	Namespace: "zendrite", Subsystem: "jetstream", Name: "consumer_last_ack_timestamp_seconds",
	Help: "Unix time the consumer last acknowledged a message.",
}, []string{"consumer"})

func init() { //nolint:gochecknoinits // Matches the existing component metric registration.
	prometheus.MustRegister(
		retiredGenerationMessages, consumerRedeliveries,
		consumerProcessingSeconds, consumerMessageAge, consumerProcessingStarted,
		consumerProcessingMessage, consumerPending, consumerLastAck,
	)
}

// consumerProgress records the progress metrics of one durable consumer.
type consumerProgress struct {
	processingSeconds prometheus.Observer
	messageAge        prometheus.Observer
	processingStarted prometheus.Gauge
	processingMessage prometheus.Gauge
	pending           prometheus.Gauge
	lastAck           prometheus.Gauge
	currentBatchStart time.Time
}

func newConsumerProgress(consumer string) *consumerProgress {
	return &consumerProgress{
		processingSeconds: consumerProcessingSeconds.WithLabelValues(consumer),
		messageAge:        consumerMessageAge.WithLabelValues(consumer),
		processingStarted: consumerProcessingStarted.WithLabelValues(consumer),
		processingMessage: consumerProcessingMessage.WithLabelValues(consumer),
		pending:           consumerPending.WithLabelValues(consumer),
		lastAck:           consumerLastAck.WithLabelValues(consumer),
	}
}

// start records that the consumer is about to process msgs.
func (p *consumerProgress) start(msgs []*nats.Msg) {
	p.currentBatchStart = time.Now()
	p.processingStarted.Set(unixSeconds(p.currentBatchStart))
	var oldest time.Time
	for _, msg := range msgs {
		meta, err := msg.Metadata()
		if err != nil {
			continue
		}
		p.messageAge.Observe(p.currentBatchStart.Sub(meta.Timestamp).Seconds())
		p.pending.Set(float64(meta.NumPending))
		if oldest.IsZero() || meta.Timestamp.Before(oldest) {
			oldest = meta.Timestamp
		}
	}
	if !oldest.IsZero() {
		p.processingMessage.Set(unixSeconds(oldest))
	}
}

// finish records that the callback has returned for the batch passed to start.
func (p *consumerProgress) finish() {
	p.processingSeconds.Observe(time.Since(p.currentBatchStart).Seconds())
	p.processingStarted.Set(0)
	p.processingMessage.Set(0)
}

// acked records that a message was acknowledged.
func (p *consumerProgress) acked() {
	p.lastAck.SetToCurrentTime()
}

func unixSeconds(t time.Time) float64 {
	return float64(t.UnixNano()) / float64(time.Second)
}
