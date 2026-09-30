//go:build linux

package hachaos

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"
)

// rollingUpgradeScenario replaces the running build with the next
// compatible one while clients keep working. It is the zero-downtime claim:
// no client or inbound federation request may fail, and the only documented
// exception, search while the successor rebuilds its index, is measured
// separately.
func rollingUpgradeScenario() scenario {
	return scenario{
		name:          "rolling-upgrade",
		description:   "Every candidate is replaced by the next compatible build one at a time with SIGTERM, standbys first and the owner last, under continuous client, federation and MSC traffic. The owner hands over; no client or inbound federation request may fail.",
		tiers:         allTiers,
		uninterrupted: true,
		run: func(ctx context.Context, s *run) error {
			if s.e.settings.next == "" {
				return errors.New("HACHAOS_ZENDRITE_NEXT is not set")
			}
			s.startSearchProbe(ctx)
			s.faultf("deployment", "rolling upgrade to %s", filepath.Base(s.e.settings.next))
			return s.roll(ctx, s.e.settings.next, nil)
		},
	}
}

// successorFailsScenario kills the successor of a planned handover while it
// activates. A failed upgrade must fall back to an ordinary takeover by
// another candidate, with every invariant intact.
func successorFailsScenario() scenario {
	return scenario{
		name:        "rolling-upgrade-successor-fails",
		description: "During a rolling upgrade to the next build, the candidate that takes over from the handing-over owner is killed while it activates. Another candidate must take over and every acknowledged write must survive.",
		tiers:       allTiers,
		run: func(ctx context.Context, s *run) error {
			if s.e.settings.next == "" {
				return errors.New("HACHAOS_ZENDRITE_NEXT is not set")
			}
			s.faultf("deployment", "rolling upgrade to %s", filepath.Base(s.e.settings.next))
			return s.roll(ctx, s.e.settings.next, func(owner *slot) error {
				// Kill whichever candidate takes the Lease first, as soon as
				// it has begun to activate.
				var successor *slot
				err := waitFor(ctx, 3*s.e.timings.Lease+time.Minute, func() error {
					for _, sl := range s.e.slots {
						if sl == owner {
							continue
						}
						if st, err := sl.status(ctx); err == nil && st.Leader {
							successor = sl
							return nil
						}
					}
					return errors.New("no candidate has taken over yet")
				})
				if err != nil {
					return err
				}
				s.e.timeline.add(eventFault, successor.name, "SIGKILL the successor while it activates")
				successor.halt(false)
				return successor.start()
			})
		},
	}
}

// brokerLossScenario loses the broker leading the room input stream while the
// owner hands over, so the successor activates against a stream that is
// electing a new leader.
func brokerLossScenario() scenario {
	return scenario{
		name:        "rolling-upgrade-broker-leader-lost",
		description: "During a rolling upgrade to the next build, the broker leading the room input stream is killed while the owner hands over, and restarted 20 seconds later. The upgrade must complete and every acknowledged write must survive.",
		tiers:       allTiers,
		run: func(ctx context.Context, s *run) error {
			if s.e.settings.next == "" {
				return errors.New("HACHAOS_ZENDRITE_NEXT is not set")
			}
			s.faultf("deployment", "rolling upgrade to %s", filepath.Base(s.e.settings.next))
			return s.roll(ctx, s.e.settings.next, func(owner *slot) error {
				err := waitFor(ctx, time.Minute, func() error {
					st, err := owner.status(ctx)
					if err != nil || st.Phase != "handing-over" {
						return errors.New("the owner is not handing over yet")
					}
					return nil
				})
				if err != nil {
					return err
				}
				node, err := s.e.nats.streamLeader("InputRoomEvent")
				if err != nil {
					return err
				}
				s.e.timeline.add(eventFault, node.name, "SIGKILL the input stream leader during the handover")
				node.proc.kill()
				time.Sleep(20 * time.Second)
				return s.e.nats.restart(ctx, node)
			})
		},
	}
}

// roll replaces every candidate's build one at a time, standbys first, as a
// rollout with maxSurge 0 and maxUnavailable 1 does. The owner is stopped
// last with SIGTERM and hands over. When set, duringHandover runs while the
// owner hands over and shuts down.
func (s *run) roll(ctx context.Context, bin string, duringHandover func(owner *slot) error) error {
	leader, err := s.active()
	if err != nil {
		return err
	}
	order := make([]*slot, 0, len(s.e.slots))
	for _, sl := range s.e.slots {
		if sl != leader {
			order = append(order, sl)
		}
	}
	order = append(order, leader)
	for _, sl := range order {
		var during chan error
		if sl == leader {
			s.e.timeline.add(eventFault, sl.name, "SIGTERM the owner for a planned handover")
			if duringHandover != nil {
				during = make(chan error, 1)
				go func() { during <- duringHandover(sl) }()
			}
		} else {
			s.e.timeline.add(eventFault, sl.name, "SIGTERM standby for the upgrade")
		}
		sl.halt(true)
		if during != nil {
			if err = <-during; err != nil {
				return err
			}
		}
		sl.setBinary(bin)
		if err = sl.start(); err != nil {
			return err
		}
		if sl == leader {
			if err = s.successor(leader, 3*s.e.timings.Lease+time.Minute); err != nil {
				return err
			}
		}
		// A rollout replaces the next pod only once this one is ready, which
		// for a standby means prepared to take over.
		if err = s.e.waitStandbys(ctx, 2*time.Minute); err != nil {
			return fmt.Errorf("after upgrading %s: %w", sl.name, err)
		}
	}
	s.plannedUntil = time.Now()
	return nil
}

// upgradeReport is what a planned upgrade measured, reported next to the
// recovery times of unplanned faults.
type upgradeReport struct {
	Window time.Duration `json:"window"`
	// Requests and Failed count client and inbound federation requests that
	// overlapped the upgrade, by kind.
	Requests map[string]int `json:"requests"`
	Failed   map[string]int `json:"failed"`
	// MaxLatency and P99Latency cover successful writes, which a handover
	// delays while the router holds them for the successor. Syncs are left
	// out: a long poll's duration is its timeout, not service latency.
	MaxLatency map[string]time.Duration `json:"max_latency"`
	P99Latency map[string]time.Duration `json:"p99_latency"`
	// OutboundFailed counts federation requests to the remote server that
	// failed during the window. They are this server's own retries, reported
	// as retry dependence rather than unavailability.
	OutboundFailed int `json:"outbound_federation_failed"`
	// Search is the documented exception: the successor rebuilds its index
	// and refuses searches with 503 until it has.
	Search searchReport `json:"search"`
}

type searchReport struct {
	Requests    int           `json:"requests"`
	Unavailable int           `json:"unavailable"`
	Failed      int           `json:"failed"`
	Longest     time.Duration `json:"longest_unavailable"`
}

// verifyUninterrupted checks the zero-downtime claim for a planned upgrade:
// every client request and every inbound federation request that overlapped
// the upgrade succeeded. Retrying does not count as uninterrupted service.
func verifyUninterrupted(r *result, w *workload, e *environment, s *run) {
	from, until := r.FaultAt, s.plannedUntil
	if until.IsZero() {
		r.failf("the planned upgrade did not finish")
		return
	}
	report := &upgradeReport{Window: until.Sub(from), Requests: map[string]int{}, Failed: map[string]int{}, MaxLatency: map[string]time.Duration{}, P99Latency: map[string]time.Duration{}}
	overlaps := func(o outcome) bool { return o.Start.Before(until) && o.End.After(from) }
	for _, kind := range []string{kindWrite, kindSync, kindMSCReply} {
		var latencies []time.Duration
		for _, o := range w.outcomes(kind) {
			if !overlaps(o) {
				continue
			}
			report.Requests[kind]++
			if !o.OK {
				report.Failed[kind]++
				continue
			}
			latencies = append(latencies, o.End.Sub(o.Start))
		}
		if kind == kindSync || len(latencies) == 0 {
			continue
		}
		slices.Sort(latencies)
		report.MaxLatency[kind] = latencies[len(latencies)-1]
		report.P99Latency[kind] = latencies[(len(latencies)*99)/100]
	}
	for _, o := range e.federationOutcomes(true) {
		if !overlaps(outcome{Start: o.Start, End: o.End.Add(time.Nanosecond)}) {
			continue
		}
		report.Requests["federation-inbound"]++
		if !o.OK {
			report.Failed["federation-inbound"]++
		}
	}
	for _, o := range e.federationOutcomes(false) {
		if !o.OK && !o.Start.Before(from) && o.Start.Before(until) {
			report.OutboundFailed++
		}
	}
	report.Search = s.stopSearchProbe(from)
	r.Upgrade = report

	kinds := make([]string, 0, len(report.Failed))
	for kind := range report.Failed {
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	for _, kind := range kinds {
		r.failf("%d of %d %s requests failed during the planned upgrade", report.Failed[kind], report.Requests[kind], kind)
	}
	var latency []string
	for _, kind := range []string{kindWrite, kindMSCReply} {
		if max, ok := report.MaxLatency[kind]; ok {
			latency = append(latency, fmt.Sprintf("%s max %s, p99 %s", kind, max.Round(time.Millisecond), report.P99Latency[kind].Round(time.Millisecond)))
		}
	}
	r.notef("planned upgrade took %s; %d client and federation requests overlapped it; write latency: %s",
		report.Window.Round(time.Millisecond), sum(report.Requests), strings.Join(latency, "; "))
	if report.OutboundFailed > 0 {
		r.notef("%d outbound federation requests failed during the upgrade and were retried by the successor", report.OutboundFailed)
	}
	if report.Search.Unavailable > 0 {
		r.notef("search, the documented exception: %d of %d probes answered 503 while the successor rebuilt its index, the longest run for %s",
			report.Search.Unavailable, report.Search.Requests, report.Search.Longest.Round(time.Millisecond))
	}
	if report.Search.Failed > 0 {
		r.failf("%d search probes failed with something other than the documented 503", report.Search.Failed)
	}
}

func sum(counts map[string]int) int {
	total := 0
	for _, n := range counts {
		total += n
	}
	return total
}

// searchProbe searches continuously through the router, so a planned upgrade
// reports how long search was unavailable.
type searchProbe struct {
	stop    context.CancelCauseFunc
	done    chan struct{}
	mu      sync.Mutex
	samples []searchSample
}

type searchSample struct {
	At     time.Time
	Status int
	Err    string
}

const searchProbeInterval = 500 * time.Millisecond

func (s *run) startSearchProbe(ctx context.Context) {
	ctx, stop := context.WithCancelCause(ctx)
	p := &searchProbe{stop: stop, done: make(chan struct{})}
	s.search = p
	// Its own client, so its 503s are not counted as the workload's.
	client := newClient(s.e.router.url()).withToken(s.w.alice)
	go func() {
		defer close(p.done)
		for ctx.Err() == nil {
			sample := searchSample{At: time.Now(), Status: http.StatusOK}
			attempt, cancel := context.WithTimeout(ctx, 30*time.Second)
			_, err := client.search(attempt, "hachaos")
			cancel()
			if ctx.Err() != nil {
				return
			}
			var mx *mxError
			switch {
			case errors.As(err, &mx):
				sample.Status, sample.Err = mx.Status, mx.Error()
			case err != nil:
				sample.Status, sample.Err = 0, err.Error()
			}
			p.mu.Lock()
			p.samples = append(p.samples, sample)
			p.mu.Unlock()
			select {
			case <-ctx.Done():
			case <-time.After(searchProbeInterval):
			}
		}
	}()
}

// stopSearchProbe ends the probe and summarizes it from the fault onwards.
func (s *run) stopSearchProbe(from time.Time) searchReport {
	var report searchReport
	p := s.search
	if p == nil {
		return report
	}
	p.stop(errors.New("probe finished"))
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	var unavailableSince time.Time
	for _, sample := range p.samples {
		if sample.At.Before(from) {
			continue
		}
		report.Requests++
		switch sample.Status {
		case http.StatusOK:
			if !unavailableSince.IsZero() {
				report.Longest = max(report.Longest, sample.At.Sub(unavailableSince))
				unavailableSince = time.Time{}
			}
		case http.StatusServiceUnavailable:
			report.Unavailable++
			if unavailableSince.IsZero() {
				unavailableSince = sample.At
			}
		default:
			report.Failed++
		}
	}
	return report
}
