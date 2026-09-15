package ha

import "github.com/prometheus/client_golang/prometheus"

// HA metrics use fixed label sets only. Candidate identities and generations
// are deliberately excluded: they grow without bound across failovers.
var (
	currentPhase = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "zendrite", Subsystem: "ha", Name: "phase",
		Help: "Current candidate phase, 1 for the active phase and 0 for the others.",
	}, []string{"phase"})

	leadershipTransitions = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zendrite", Subsystem: "ha", Name: "leadership_transitions_total",
		Help: "Leadership acquisitions and losses observed by this candidate.",
	}, []string{"transition"})

	renewalFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "zendrite", Subsystem: "ha", Name: "renewal_failures_total",
		Help: "Lease renewal failures observed by this candidate.",
	})

	fencingFailures = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "zendrite", Subsystem: "ha", Name: "fencing_failures_total",
		Help: "Ownership establishment failures, labeled by the blocking boundary.",
	}, []string{"boundary"})

	recoveryDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zendrite", Subsystem: "ha", Name: "recovery_duration_seconds",
		Help: "Seconds from leadership acquisition to readiness for the current activation.",
	})

	shutdownDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zendrite", Subsystem: "ha", Name: "shutdown_duration_seconds",
		Help: "Seconds from ownership loss to the end of draining for the current activation.",
	})
)

func init() { //nolint:gochecknoinits // Matches the existing component metric registration.
	prometheus.MustRegister(currentPhase, leadershipTransitions, renewalFailures, fencingFailures, recoveryDuration, shutdownDuration)
	for _, phase := range allPhases {
		currentPhase.WithLabelValues(string(phase)).Set(0)
	}
	for _, transition := range []string{"acquired", "lost"} {
		leadershipTransitions.WithLabelValues(transition).Add(0)
	}
	for _, boundary := range requiredBoundaries {
		fencingFailures.WithLabelValues(string(boundary)).Add(0)
	}
}

var allPhases = []Phase{PhaseStarting, PhaseStandby, PhaseFencing, PhaseRecovering, PhaseActive, PhaseDraining, PhaseStopped}

// observePhase keeps exactly one phase series set, so alerts can select on a
// phase without needing to know which candidate reported it.
func observePhase(phase Phase) {
	for _, known := range allPhases {
		value := 0.0
		if known == phase {
			value = 1
		}
		currentPhase.WithLabelValues(string(known)).Set(value)
	}
}
