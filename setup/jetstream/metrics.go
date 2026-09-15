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

func init() { //nolint:gochecknoinits // Matches the existing component metric registration.
	prometheus.MustRegister(retiredGenerationMessages)
}
