package ha

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

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
	PhaseDraining   Phase = "draining"
	PhaseStopped    Phase = "stopped"
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
	server     StateReporter
	acquiredAt time.Time
	lostAt     time.Time
	leader     bool
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
	case PhaseDraining:
		s.lostAt = s.since
		if previous == PhaseActive || previous == PhaseRecovering || previous == PhaseFencing {
			leadershipTransitions.WithLabelValues("lost").Inc()
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
func (s *Status) Ready() bool { return s.Phase() == PhaseActive }

// Healthy reports candidate availability, independently of Matrix eligibility.
// Standbys are healthy; incomplete promotion, failures and shutdown are not.
// Only use this for pod readiness when a separate router checks Ready.
func (s *Status) Healthy() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.enter(s.phase.merge(s.server))
	return (s.phase == PhaseStandby || s.phase == PhaseActive) && s.reason == ""
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
	Phase    Phase    `json:"phase"`
	Leader   bool     `json:"leader"`
	Ready    bool     `json:"ready"`
	Since    string   `json:"since"`
	Reason   string   `json:"reason,omitempty"`
	Boundary Boundary `json:"blocked_boundary,omitempty"`
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
	}
	s.mu.Unlock()
	body.Ready = body.Phase == PhaseActive
	if err := json.NewEncoder(w).Encode(body); err != nil {
		logrus.WithError(err).Debug("Failed to write HA status response")
	}
}
