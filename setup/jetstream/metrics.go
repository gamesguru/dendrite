package jetstream

import "github.com/prometheus/client_golang/prometheus"

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

func init() { //nolint:gochecknoinits // Matches the existing component metric registration.
	prometheus.MustRegister(retiredGenerationMessages, consumerRedeliveries)
}
