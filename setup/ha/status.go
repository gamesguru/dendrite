package ha

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/ownership"
	"codefloe.com/pat-s/zendrite/setup/lifecycle"
)

// Phase describes what a candidate is doing. It is reported separately from
// readiness so that an operator can tell a healthy standby apart from a
// candidate that is stuck establishing ownership or recovering.
type Phase string

const (
	PhaseStarting   Phase = "starting"
	PhaseStandby    Phase = "standby"
	PhaseFencing    Phase = "fencing"
	PhaseRecovering Phase = "recovering"
	PhaseActive     Phase = "active"
	// PhaseHandingOver still serves requests in flight during a planned
	// handover, but routers must not send it new ones.
	PhaseHandingOver Phase = "handing-over"
	PhaseDraining    Phase = "draining"
	PhaseStopped     Phase = "stopped"
)

// Status is the candidate's probe and observability surface.
//
// It exists because a standby never binds the Matrix listeners: without a
// separate surface a standby would look dead to Kubernetes and be restarted,
// and its metrics would be unscrapable.
type Status struct {
	mu         sync.Mutex
	phase      Phase
	since      time.Time
	reason     string
	boundary   Boundary
	owner      ownership.Owner
	server     StateReporter
	acquiredAt time.Time
	handoverAt time.Time
	lostAt     time.Time
	leader     bool
	// preparation is set once a standby checks whether it may take over.
	// Until then, and without HA preparation at all, a standby is healthy.
	preparation *preparation
}

// preparation is a standby's latest check of whether it may take over.
type preparation struct {
	version  string
	contract Contract
	recorded Recorded
	err      error
}

// StateReporter is the activation's own view of itself, which is more
// authoritative than anything the election loop can infer.
type StateReporter interface{ State() lifecycle.State }

// NewStatus returns a status surface for a candidate that has not yet begun
// electing. It reports live but never ready.
func NewStatus() *Status {
	return &Status{phase: PhaseStarting, since: time.Now()}
}

// Track follows an activation's own lifecycle, so recovery, activity, and
// draining are reported from the homeserver rather than guessed by the caller.
func (s *Status) Track(server StateReporter) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.server = server
}

// TrackOwnership makes traffic and active-candidate probes use the same fencing
// validity as application admission, including after a frozen process resumes.
func (s *Status) TrackOwnership(owner ownership.Owner) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.owner = owner
}

// Observe records a phase transition and clears any previous blocking reason.
func (s *Status) Observe(phase Phase, reason string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enter(phase)
	s.reason = reason
	s.boundary = ""
}

// enter records a transition once, wherever it is first observed. Callers hold
// the lock. Transitions arrive both from the election loop and from the
// activation's own state, so accounting lives here rather than at each caller.
func (s *Status) enter(phase Phase) {
	if s.phase == phase {
		return
	}
	previous := s.phase
	s.phase = phase
	s.since = time.Now()
	observePhase(phase)
	switch phase {
	case PhaseFencing:
		s.acquiredAt = s.since
		// Leadership is recorded, not inferred from the phase name: a standby
		// that stops would otherwise report itself as the leader.
		s.leader = true
		leadershipTransitions.WithLabelValues("acquired").Inc()
	case PhaseActive:
		if !s.acquiredAt.IsZero() {
			recoveryDuration.Set(s.since.Sub(s.acquiredAt).Seconds())
		}
	case PhaseHandingOver:
		s.handoverAt = s.since
	case PhaseDraining:
		s.lostAt = s.since
		if previous == PhaseActive || previous == PhaseRecovering || previous == PhaseFencing || previous == PhaseHandingOver {
			leadershipTransitions.WithLabelValues("lost").Inc()
		}
		if previous == PhaseHandingOver {
			handoverDuration.Set(s.since.Sub(s.handoverAt).Seconds())
		}
		s.leader = false
	case PhaseStopped:
		if !s.lostAt.IsZero() {
			shutdownDuration.Set(s.since.Sub(s.lostAt).Seconds())
		}
		s.leader = false
	case PhaseStarting, PhaseStandby, PhaseRecovering:
	}
}

// Fail records why a candidate cannot proceed, naming the blocking boundary
// when the failure came from ownership establishment.
func (s *Status) Fail(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	// The status endpoint is served unauthenticated on all interfaces, and
	// error text from the database layer carries the user, database name and
	// host. The full cause goes to the log; the endpoint reports the boundary
	// and a fixed summary.
	logrus.WithError(err).Warn("HA candidate cannot proceed")
	var blocked *BlockedError
	if errors.As(err, &blocked) {
		s.boundary = blocked.Boundary
		s.reason = fmt.Sprintf("ownership could not be established at the %s boundary; see the candidate logs", blocked.Boundary)
		fencingFailures.WithLabelValues(string(blocked.Boundary)).Inc()
		return
	}
	s.reason = "the candidate cannot proceed; see the candidate logs"
}

// Phase reports the current phase, preferring the activation's own state once
// one exists so that a stopped or draining homeserver is never reported active.
func (s *Status) Phase() Phase {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enter(s.phase.merge(s.server))
	return s.phase
}

func (p Phase) merge(server StateReporter) Phase {
	if server == nil {
		return p
	}
	switch server.State() {
	case lifecycle.Recovering:
		return PhaseRecovering
	case lifecycle.Active:
		return PhaseActive
	case lifecycle.HandingOver:
		return PhaseHandingOver
	case lifecycle.Draining:
		return PhaseDraining
	case lifecycle.Stopped:
		return PhaseStopped
	case lifecycle.Standby:
		return p
	}
	return p
}

// Refresh observes the activation's current state and updates the metrics.
//
// Phase, transition and duration metrics are derived from transitions, and a
// transition is only noticed when something looks. Without a refresh on scrape
// the phase gauge stays pinned at whatever the last probe saw, and transitions
// between probes are missed entirely.
func (s *Status) Refresh() { s.Phase() }

// Ready reports whether this candidate should receive Matrix traffic.
func (s *Status) Ready() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enter(s.phase.merge(s.server))
	return s.ready()
}

// ready is called with mu held. Phase alone can remain active until the
// election loop notices a loss that the fencing gate has already observed.
func (s *Status) ready() bool {
	return s.phase == PhaseActive && s.reason == "" && (s.owner == nil || s.owner.Valid() == nil)
}

// Healthy reports candidate availability, independently of Matrix eligibility.
// A prepared standby is healthy; one that may not take over, incomplete
// promotion, failures and shutdown are not.
// Only use this for pod readiness when a separate router checks Ready.
func (s *Status) Healthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enter(s.phase.merge(s.server))
	return (s.phase == PhaseStandby && s.reason == "" && s.prepared()) || s.ready()
}

// prepared is called with mu held.
func (s *Status) prepared() bool { return s.preparation == nil || s.preparation.err == nil }

// errPreparationPending reports that a standby has not yet checked whether it
// may take over. Until it has, it must not look prepared: a rollout would
// otherwise replace the next candidate before this one was checked at all.
var errPreparationPending = errors.New("this standby has not yet checked whether it may take over")

// ExpectPreparation makes a standby unhealthy until its first preparation
// check completes.
func (s *Status) ExpectPreparation(version string, contract Contract) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.preparation = &preparation{version: version, contract: contract, err: errPreparationPending}
	standbyPrepared.Set(0)
}

// Prepared records a standby's check of whether it may take over: whether it
// reaches the database, and whether the deployment's upgrade contract admits
// its build. A standby that may not take over is not healthy, so a rollout
// that replaces the running build with an incompatible one stalls with the
// old candidates still serving, instead of discovering the problem at
// takeover.
func (s *Status) Prepared(version string, contract Contract, recorded Recorded, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	previous := s.preparation
	s.preparation = &preparation{version: version, contract: contract, recorded: recorded, err: err}
	compatible := 1.0
	if errors.Is(err, ErrIncompatibleBuild) {
		compatible = 0
	}
	buildCompatible.Set(compatible)
	if err == nil {
		standbyPrepared.Set(1)
	} else {
		standbyPrepared.Set(0)
	}
	switch {
	case errors.Is(err, errPreparationPending):
	case err != nil && (previous == nil || previous.err == nil || previous.err.Error() != err.Error()):
		logrus.WithError(err).WithFields(logrus.Fields{
			"build_level":      contract.Level,
			"deployment_level": recorded.Level,
			"deployment_floor": recorded.Floor,
		}).Warn("HA standby is not prepared to take over")
	case err == nil && (previous == nil || previous.err != nil):
		logrus.WithFields(logrus.Fields{
			"version":            version,
			"build_level":        contract.Level,
			"deployment_level":   recorded.Level,
			"deployment_version": recorded.Version,
		}).Info("HA standby is prepared to take over")
	}
}

// Handler serves the probe and status endpoints.
//
// Liveness, candidate health and traffic readiness are separate. A standby
// passes candidate health but cannot receive Matrix traffic. A routing layer
// must still check /health, which fails until fencing and recovery finish.
func (s *Status) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /_zendrite/monitor/up", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /_zendrite/monitor/health", func(w http.ResponseWriter, r *http.Request) {
		// A router that treats 404 as "stop sending new requests" keeps the
		// requests already in flight, which a planned handover is waiting
		// for. Any other failure cuts them.
		if s.Phase() == PhaseHandingOver {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			s.write(w)
			return
		}
		if !s.Ready() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			s.write(w)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /_zendrite/monitor/candidate", func(w http.ResponseWriter, _ *http.Request) {
		if !s.Healthy() {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusServiceUnavailable)
			s.write(w)
			return
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("GET /_zendrite/monitor/status", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		s.write(w)
	})
	return mux
}

type statusBody struct {
	Phase    Phase        `json:"phase"`
	Leader   bool         `json:"leader"`
	Ready    bool         `json:"ready"`
	Since    string       `json:"since"`
	Reason   string       `json:"reason,omitempty"`
	Boundary Boundary     `json:"blocked_boundary,omitempty"`
	Upgrade  *upgradeBody `json:"upgrade,omitempty"`
}

// upgradeBody reports this build's upgrade contract against the deployment's
// record. The reason is a fixed summary, like the blocking reason, because
// the endpoint is unauthenticated; the log carries the full cause.
type upgradeBody struct {
	Version           string `json:"version"`
	Protocol          int    `json:"protocol"`
	Level             int    `json:"level"`
	UpgradeFrom       int    `json:"upgrade_from"`
	RollbackTo        int    `json:"rollback_to"`
	DeploymentLevel   int    `json:"deployment_level,omitempty"`
	DeploymentFloor   int    `json:"deployment_floor,omitempty"`
	DeploymentVersion string `json:"deployment_version,omitempty"`
	Prepared          bool   `json:"prepared"`
	Compatible        bool   `json:"compatible"`
	Reason            string `json:"reason,omitempty"`
}

func (p *preparation) body() *upgradeBody {
	if p == nil {
		return nil
	}
	body := &upgradeBody{
		Version: p.version, Protocol: p.contract.Protocol, Level: p.contract.Level, UpgradeFrom: p.contract.UpgradeFrom, RollbackTo: p.contract.RollbackTo,
		DeploymentLevel: p.recorded.Level, DeploymentFloor: p.recorded.Floor, DeploymentVersion: p.recorded.Version,
		Prepared: p.err == nil, Compatible: !errors.Is(p.err, ErrIncompatibleBuild),
	}
	switch {
	case !body.Compatible:
		body.Reason = "the deployment's upgrade contract does not admit this build; an offline upgrade is required, see the candidate logs"
	case errors.Is(p.err, errPreparationPending):
		body.Reason = "checking whether this build may take over"
	case p.err != nil:
		body.Reason = "cannot read the deployment's compatibility record; see the candidate logs"
	}
	return body
}

func (s *Status) write(w http.ResponseWriter) {
	s.mu.Lock()
	s.enter(s.phase.merge(s.server))
	body := statusBody{
		Phase:    s.phase,
		Leader:   s.leader,
		Since:    s.since.UTC().Format(time.RFC3339),
		Reason:   s.reason,
		Boundary: s.boundary,
		Ready:    s.ready(),
		Upgrade:  s.preparation.body(),
	}
	s.mu.Unlock()
	if err := json.NewEncoder(w).Encode(body); err != nil {
		logrus.WithError(err).Debug("Failed to write HA status response")
	}
}
