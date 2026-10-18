package ha

import (
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

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

	handoverDuration = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zendrite", Subsystem: "ha", Name: "handover_duration_seconds",
		Help: "Seconds a planned handover spent announcing itself and waiting for requests in flight.",
	})

	standbyPrepared = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zendrite", Subsystem: "ha", Name: "standby_prepared",
		Help: "1 while this standby reaches the database and may take over the deployment, 0 otherwise.",
	})

	buildCompatible = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "zendrite", Subsystem: "ha", Name: "build_compatible",
		Help: "0 when the deployment's upgrade contract does not admit this build, 1 otherwise.",
	})

	buildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "zendrite", Subsystem: "ha", Name: "build_info",
		Help: "This build's HA upgrade contract; always 1.",
	}, []string{"protocol", "level", "upgrade_from", "rollback_to"})
)

func init() { //nolint:gochecknoinits // Matches the existing component metric registration.
	prometheus.MustRegister(currentPhase, leadershipTransitions, renewalFailures, fencingFailures, recoveryDuration, shutdownDuration, handoverDuration, standbyPrepared, buildCompatible, buildInfo)
	buildCompatible.Set(1)
	c := Release()
	buildInfo.WithLabelValues(strconv.Itoa(c.Protocol), strconv.Itoa(c.Level), strconv.Itoa(c.UpgradeFrom), strconv.Itoa(c.RollbackTo)).Set(1)
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

var allPhases = []Phase{PhaseStarting, PhaseStandby, PhaseFencing, PhaseRecovering, PhaseActive, PhaseHandingOver, PhaseDraining, PhaseStopped}

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
