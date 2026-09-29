//go:build linux

package hachaos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"
)

// scenario is one fault. Run injects it, waits for the system to respond,
// and heals it; the harness then measures recovery and verifies every
// invariant against what the workload observed.
type scenario struct {
	name        string
	description string
	tiers       []string
	// suspends marks faults during which the design deliberately stops
	// serving rather than risk an unsafe takeover, so the recovery objective
	// does not apply while the fault lasts.
	suspends bool
	run      func(ctx context.Context, s *run) error
}

var allTiers = []string{tierCI, tierFull, tierReference}

var fullTiers = []string{tierFull, tierReference}

func scenarios() []scenario {
	return []scenario{
		{
			name:        "leader-sigkill",
			description: "The active candidate is killed with SIGKILL, as a crashed process or node looks to its peers.",
			tiers:       allTiers,
			run: func(ctx context.Context, s *run) error {
				leader, err := s.active()
				if err != nil {
					return err
				}
				s.faultf(leader.name, "SIGKILL active candidate")
				if err = leader.current().signal(syscall.SIGKILL); err != nil {
					return err
				}
				err = s.successor(leader, 3*s.e.timings.Lease+time.Minute)
				return err
			},
		},
		{
			name:        "leader-graceful-shutdown",
			description: "The active candidate receives SIGTERM, as a pod deletion or node drain delivers.",
			tiers:       allTiers,
			run: func(ctx context.Context, s *run) error {
				leader, err := s.active()
				if err != nil {
					return err
				}
				s.faultf(leader.name, "SIGTERM active candidate")
				if err = leader.current().signalMain(syscall.SIGTERM); err != nil {
					return err
				}
				err = s.successor(leader, 3*s.e.timings.Lease+time.Minute)
				return err
			},
		},
		{
			name:        "leader-paused-beyond-lease",
			description: "The active candidate is frozen with SIGSTOP until a successor is serving, then resumed. The resumed process must not act.",
			tiers:       allTiers,
			run: func(ctx context.Context, s *run) error {
				leader, err := s.active()
				if err != nil {
					return err
				}
				s.faultf(leader.name, "SIGSTOP active candidate")
				if err = s.pause(leader); err != nil {
					return err
				}
				if err = s.successor(leader, 3*s.e.timings.Lease+time.Minute); err != nil {
					_ = s.resume(leader)
					return err
				}
				// Let the successor serve before the old owner returns.
				time.Sleep(5 * time.Second)
				if err = s.resume(leader); err != nil {
					return err
				}
				s.expectRefused(ctx, leader, 5*time.Second)
				return nil
			},
		},
		{
			name:        "leader-partitioned-from-kubernetes",
			description: "The active candidate loses the Kubernetes API but keeps its database, broker and media: the asymmetric partition in which only the Lease is lost.",
			tiers:       allTiers,
			run: func(ctx context.Context, s *run) error {
				leader, err := s.active()
				if err != nil {
					return err
				}
				s.faultf(leader.name, "partition active candidate from the Kubernetes API")
				s.partition(leader.name, blackhole, "kube")
				err = s.successor(leader, 3*s.e.timings.Lease+time.Minute)
				s.expectRefused(ctx, leader, 3*time.Second)
				s.healAll()
				return err
			},
		},
		{
			name:        "leader-kubernetes-responses-lost",
			description: "Renewals from the active candidate reach the Kubernetes API but the responses are lost, so the Lease stays fresh while its holder believes renewal failed.",
			tiers:       fullTiers,
			run: func(ctx context.Context, s *run) error {
				leader, err := s.active()
				if err != nil {
					return err
				}
				s.faultf(leader.name, "drop Kubernetes API responses to the active candidate")
				s.partition(leader.name, dropDownstream, "kube")
				err = s.successor(leader, 3*s.e.timings.Lease+time.Minute)
				s.healAll()
				return err
			},
		},
		{
			name:        "leader-partitioned-from-database",
			description: "The active candidate loses PostgreSQL but keeps the Kubernetes API. Its ownership goes stale; it must give up the Lease so a candidate that can reach the database takes over while the partition lasts.",
			tiers:       allTiers,
			run: func(ctx context.Context, s *run) error {
				leader, err := s.active()
				if err != nil {
					return err
				}
				s.faultf(leader.name, "partition active candidate from PostgreSQL")
				s.partition(leader.name, blackhole, "postgres")
				// Stale after renew_deadline, abandoned a Lease duration later,
				// then the Lease must expire before a successor acquires it.
				err = s.successor(leader, s.e.timings.Renew+2*s.e.timings.Lease+time.Minute)
				s.healAll()
				return err
			},
		},
		{
			name:        "leader-partitioned-from-everything",
			description: "The active candidate loses every dependency at once, as a node isolated from the network would.",
			tiers:       fullTiers,
			run: func(ctx context.Context, s *run) error {
				leader, err := s.active()
				if err != nil {
					return err
				}
				s.faultf(leader.name, "isolate active candidate")
				s.partition(leader.name, blackhole, "postgres", "nats", "kube", "s3", "federation")
				err = s.successor(leader, 3*s.e.timings.Lease+time.Minute)
				time.Sleep(5 * time.Second)
				s.healAll()
				if err == nil {
					s.expectRefused(ctx, leader, 3*time.Second)
				}
				return err
			},
		},
		{
			name:        "kubernetes-api-outage",
			description: "The Kubernetes API server is frozen for longer than the Lease duration, then resumes. No candidate can prove leadership meanwhile.",
			tiers:       allTiers,
			suspends:    true,
			run: func(ctx context.Context, s *run) error {
				s.faultf("kube-apiserver", "SIGSTOP kube-apiserver")
				if err := s.e.kube.apiserver.signal(syscall.SIGSTOP); err != nil {
					return err
				}
				time.Sleep(s.e.timings.Lease + 10*time.Second)
				s.e.timeline.add(eventFault, "kube-apiserver", "SIGCONT kube-apiserver")
				if err := s.e.kube.apiserver.signal(syscall.SIGCONT); err != nil {
					return err
				}
				_, err := s.active()
				return err
			},
		},
		{
			name:        "postgres-primary-failover",
			description: "The PostgreSQL primary is killed and its synchronous standby promoted behind the same service address, as an operator such as CloudNativePG does.",
			tiers:       allTiers,
			run: func(ctx context.Context, s *run) error {
				primary := s.e.pg.primaryNode()
				s.faultf(primary.name, "SIGKILL PostgreSQL primary")
				primary.proc.kill()
				// Operators detect a lost primary after a few seconds.
				time.Sleep(3 * time.Second)
				if err := s.e.pg.failover(ctx); err != nil {
					return err
				}
				if _, err := s.active(); err != nil {
					return err
				}
				return s.e.pg.rebuildStandby(ctx)
			},
		},
		{
			name:        "nats-stream-leader-loss",
			description: "The broker leading the room input stream is killed, then restarted with its storage after the cluster has elected a new leader.",
			tiers:       allTiers,
			run: func(ctx context.Context, s *run) error {
				node, err := s.e.nats.streamLeader("InputRoomEvent")
				if err != nil {
					return err
				}
				s.faultf(node.name, "SIGKILL stream leader")
				node.proc.kill()
				time.Sleep(20 * time.Second)
				return s.e.nats.restart(ctx, node)
			},
		},
		{
			name:        "media-store-outage",
			description: "The S3 server is frozen for 20 seconds. Media fails while it is down; nothing else may, and no takeover is expected.",
			tiers:       fullTiers,
			run: func(ctx context.Context, s *run) error {
				leader, err := s.active()
				if err != nil {
					return err
				}
				s.faultf("s3", "SIGSTOP S3 server")
				if err = s.e.s3.proc.signal(syscall.SIGSTOP); err != nil {
					return err
				}
				time.Sleep(20 * time.Second)
				s.e.timeline.add(eventFault, "s3", "SIGCONT S3 server")
				if err = s.e.s3.proc.signal(syscall.SIGCONT); err != nil {
					return err
				}
				if active := s.e.router.activeSlot(); active != leader {
					s.r.failf("a media outage moved ownership from %s", leader.name)
				}
				return nil
			},
		},
		{
			name:        "fencing-cannot-be-proven",
			description: "A database backend of the old owner cannot be terminated, so no successor can prove the old owner is gone. Promotion must stay blocked until it can.",
			tiers:       allTiers,
			suspends:    true,
			run: func(ctx context.Context, s *run) error {
				leader, err := s.active()
				if err != nil {
					return err
				}
				generation, err := s.currentGeneration(ctx)
				if err != nil {
					return err
				}
				backends, err := s.applicationBackends(ctx, generation)
				if err != nil {
					return err
				}
				if len(backends) == 0 {
					return fmt.Errorf("generation %d has no database backends to hold", generation)
				}
				stuck := backends[0]
				// A stopped backend cannot act on pg_terminate_backend, which
				// is what a backend blocked in uninterruptible I/O looks like.
				if err = syscall.Kill(stuck, syscall.SIGSTOP); err != nil {
					return fmt.Errorf("stop backend %d: %w", stuck, err)
				}
				s.faultf(leader.name, "SIGKILL active candidate while its backend %d cannot be terminated", stuck)
				if err = leader.current().signal(syscall.SIGKILL); err != nil {
					_ = syscall.Kill(stuck, syscall.SIGCONT)
					return err
				}
				// The router needs one failed check to drop the killed
				// candidate; only then does "no active candidate" mean
				// something.
				if err = waitFor(ctx, 10*time.Second, func() error {
					if s.e.router.activeSlot() != nil {
						return errors.New("the killed candidate is still routed")
					}
					return nil
				}); err != nil {
					_ = syscall.Kill(stuck, syscall.SIGCONT)
					return err
				}
				s.holdNoActive(s.e.timings.Lease+2*s.e.timings.Ownership, "a backend of the retired generation was still connected")
				blocked := false
				for _, sl := range s.e.slots {
					if st, err := sl.status(ctx); err == nil && st.Boundary == "postgresql" {
						blocked = true
					}
				}
				if !blocked {
					s.r.notef("no candidate was observed reporting the postgresql boundary at the end of the hold; blocked candidates exit and restart, so the report is transient")
				}
				s.e.timeline.add(eventFault, "postgres", "SIGCONT backend %d", stuck)
				if err = syscall.Kill(stuck, syscall.SIGCONT); err != nil {
					return err
				}
				_, err = s.active()
				return err
			},
		},
		{
			name:        "leadership-lost-during-startup",
			description: "The active candidate is killed, and its successor loses the Kubernetes API while it is still fencing or recovering. It must abandon startup without serving.",
			tiers:       allTiers,
			run: func(ctx context.Context, s *run) error {
				leader, err := s.active()
				if err != nil {
					return err
				}
				// Startup normally takes well under a second, too short to
				// interrupt reliably. Standbys do not use the broker, so
				// cutting it off for all of them only affects the one that
				// wins the Lease: its startup stalls connecting to JetStream.
				// The killed leader restarts as a standby and may win too.
				for _, sl := range s.e.slots {
					s.partition(sl.name, blackhole, "nats")
				}
				s.faultf(leader.name, "SIGKILL active candidate")
				if err = leader.current().signal(syscall.SIGKILL); err != nil {
					return err
				}
				var starting *slot
				err = waitFor(ctx, 3*s.e.timings.Lease+time.Minute, func() error {
					for _, sl := range s.e.slots {
						if st, err := sl.status(ctx); err == nil && (st.Phase == "fencing" || st.Phase == "recovering") {
							starting = sl
							return nil
						}
					}
					return errors.New("no candidate is starting")
				})
				if err != nil {
					return err
				}
				s.e.timeline.add(eventFault, starting.name, "partition starting successor from the Kubernetes API")
				s.partition(starting.name, blackhole, "kube")
				// Let the other candidates start normally.
				for _, sl := range s.e.slots {
					if sl != starting {
						s.partition(sl.name, pass, "nats")
					}
				}
				var next *slot
				err = waitFor(ctx, 3*s.e.timings.Lease+time.Minute, func() error {
					if st, err := starting.status(ctx); err == nil && st.Ready {
						return errPermanent(fmt.Errorf("%s became ready after losing the Kubernetes API during startup", starting.name))
					}
					next = s.e.router.activeSlot()
					if next == nil || next == starting {
						return errors.New("no other candidate is active yet")
					}
					return nil
				})
				if err == nil && next == starting {
					s.r.failf("%s became active after losing the Kubernetes API during startup", starting.name)
				}
				s.healAll()
				return err
			},
		},
		{
			name:        "queued-federation-delivery",
			description: "Every candidate is cut off from the remote server so outbound events queue, the active candidate is killed, and the partition heals after takeover. The successor must deliver the queue.",
			tiers:       allTiers,
			run: func(ctx context.Context, s *run) error {
				leader, err := s.active()
				if err != nil {
					return err
				}
				for _, sl := range s.e.slots {
					s.e.fed.setFault(sl.name, blackhole)
				}
				// Let events queue for the unreachable destination.
				time.Sleep(10 * time.Second)
				s.faultf(leader.name, "SIGKILL active candidate with a queued federation backlog")
				if err = leader.current().signal(syscall.SIGKILL); err != nil {
					return err
				}
				err = s.successor(leader, 3*s.e.timings.Lease+time.Minute)
				time.Sleep(5 * time.Second)
				s.healAll()
				return err
			},
		},
		{
			name:        "rolling-restart",
			description: "Every candidate is restarted one at a time with SIGTERM, standbys first, as a rollout with maxSurge 0 and maxUnavailable 1 does.",
			tiers:       allTiers,
			run: func(ctx context.Context, s *run) error {
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
					if sl == leader {
						s.faultf(sl.name, "SIGTERM active candidate during rollout")
					} else {
						s.e.timeline.add(eventFault, sl.name, "SIGTERM standby during rollout")
					}
					old := sl.current()
					sl.halt(true)
					if err = sl.start(); err != nil {
						return err
					}
					if sl == leader {
						if err = s.successor(leader, 3*s.e.timings.Lease+time.Minute); err != nil {
							return err
						}
					}
					if err = s.e.waitStandbys(ctx, 2*time.Minute); err != nil {
						return fmt.Errorf("after restarting %s (pid %d): %w", sl.name, old.pid(), err)
					}
				}
				return nil
			},
		},
		{
			name:        "coordinated-upgrade",
			description: "A candidate running a different build must not join the running deployment; the documented maintenance-window upgrade must then preserve every acknowledged write.",
			tiers:       allTiers,
			run: func(ctx context.Context, s *run) error {
				if s.e.settings.next == "" {
					return errors.New("HACHAOS_ZENDRITE_NEXT is not set")
				}
				leader, err := s.active()
				if err != nil {
					return err
				}
				var standby *slot
				for _, sl := range s.e.slots {
					if sl != leader {
						standby = sl
						break
					}
				}
				// Mixed versions: the new build must fail closed.
				standby.halt(true)
				standby.setBinary(s.e.settings.next)
				if err = standby.start(); err != nil {
					return err
				}
				time.Sleep(s.e.timings.Lease)
				if active := s.e.router.activeSlot(); active != leader {
					s.r.failf("starting a different build moved traffic from %s", leader.name)
				}
				if st, err := standby.status(ctx); err == nil && st.Ready {
					s.r.failf("%s reported ready while running a different build", standby.name)
				}
				// Maintenance window: stop everything, reset compatibility,
				// start one new candidate, then scale up.
				s.faultf("deployment", "stop every candidate for the maintenance window")
				for _, sl := range s.e.slots {
					sl.halt(true)
				}
				if err = s.resetCompatibility(ctx); err != nil {
					return err
				}
				for _, sl := range s.e.slots {
					sl.setBinary(s.e.settings.next)
				}
				if err = s.e.slots[0].start(); err != nil {
					return err
				}
				if _, err = s.active(); err != nil {
					return err
				}
				if _, err = s.e.pg.installAudit(ctx); err != nil {
					return err
				}
				for _, sl := range s.e.slots[1:] {
					if err = sl.start(); err != nil {
						return err
					}
				}
				return s.e.waitStandbys(ctx, 2*time.Minute)
			},
		},
		mscIndexFailureScenario(),
	}
}

// scenarioGroups splits the scenarios into the CI matrix groups.
var scenarioGroups = map[string]string{
	"leader-sigkill":                     "crash",
	"leader-graceful-shutdown":           "crash",
	"leader-paused-beyond-lease":         "crash",
	"rolling-restart":                    "crash",
	"leader-partitioned-from-kubernetes": "partition",
	"leader-kubernetes-responses-lost":   "partition",
	"leader-partitioned-from-database":   "partition",
	"leader-partitioned-from-everything": "partition",
	"kubernetes-api-outage":              "partition",
	"postgres-primary-failover":          "dependency",
	"nats-stream-leader-loss":            "dependency",
	"media-store-outage":                 "dependency",
	"fencing-cannot-be-proven":           "dependency",
	"leadership-lost-during-startup":     "lifecycle",
	"queued-federation-delivery":         "lifecycle",
	"coordinated-upgrade":                "lifecycle",
	"msc-relationship-index-failure":     "dependency",
}

func selectScenarios(t *testing.T, s settings) []scenario {
	t.Helper()
	var names []string
	if list := os.Getenv("HACHAOS_SCENARIOS"); list != "" {
		names = strings.Split(list, ",")
	}
	var out []scenario
	group := os.Getenv("HACHAOS_GROUP")
	// Names and groups narrow the tier's set rather than replacing it, so CI
	// can split one tier into parallel groups.
	for _, sc := range scenarios() {
		if !slices.Contains(sc.tiers, s.tier) {
			continue
		}
		if len(names) > 0 && !slices.Contains(names, sc.name) {
			continue
		}
		if group != "" && scenarioGroups[sc.name] != group {
			continue
		}
		out = append(out, sc)
	}
	if len(out) == 0 {
		t.Fatalf("no scenario selected for tier %s and HACHAOS_SCENARIOS=%q", s.tier, os.Getenv("HACHAOS_SCENARIOS"))
	}
	return out
}

// TestQualification runs the selected fault scenarios, each in a fresh
// environment, and writes the release evidence report.
func TestQualification(t *testing.T) {
	s := loadSettings(t)
	var results []result
	for _, sc := range selectScenarios(t, s) {
		t.Run(sc.name, func(t *testing.T) {
			r := runScenario(t, s, sc)
			results = append(results, r)
			for _, f := range r.Failures {
				t.Error(f)
			}
			for _, n := range r.Notes {
				t.Log(n)
			}
		})
	}
	if err := writeReport(s, results); err != nil {
		t.Errorf("write report: %v", err)
	}
}

func runScenario(t *testing.T, s settings, sc scenario) (r result) {
	started := time.Now()
	ctx := t.Context()
	r = result{Name: sc.name, Description: sc.description, Tier: s.tier, Timings: s.timings}
	r.Recovery.Target = s.target
	r.Recovery.SuspendsObjective = sc.suspends
	defer func() {
		r.Duration = time.Since(started)
		r.Passed = len(r.Failures) == 0
	}()
	e := newEnvironment(t, ctx, s, sc.name)
	if s.seedRooms > 0 {
		if err := seedReference(ctx, e, s.seedUsers, s.seedRooms); err != nil {
			r.failf("seed reference workload: %v", err)
			return r
		}
	}
	w, err := startWorkload(ctx, e)
	if err != nil {
		r.failf("start workload: %v", err)
		return r
	}
	defer w.halt()
	// Steady state before the fault, so sync tokens, queues and caches are
	// in the state a real deployment would be in.
	time.Sleep(10 * time.Second)
	before, err := w.uploadProbe(ctx)
	if err != nil {
		r.failf("upload before the fault: %v", err)
		return r
	}
	sr := &run{e: e, w: w, r: &r, pauses: map[string][]time.Time{}}
	if err = sc.run(ctx, sr); err != nil {
		r.failf("scenario: %v", err)
	}
	sr.healAll()
	if r.FaultAt.IsZero() {
		r.FaultAt = time.Now()
	}
	if _, err = e.waitActive(ctx, 5*time.Minute); err != nil {
		r.failf("no candidate became active after the scenario: %v", err)
		return r
	}
	// Keep traffic flowing after recovery, so every kind of request has a
	// chance to succeed against the successor.
	// Remote servers back off after a failed send, so inbound federation
	// can lag client traffic; wait for both, up to the objective.
	recoverBy := r.FaultAt.Add(s.target)
	for time.Now().Before(recoverBy) {
		_, wrote := recoveredAt(w.outcomes(kindWrite), r.FaultAt)
		_, federated := recoveredAt(e.federationOutcomes(true), r.FaultAt)
		if wrote && federated {
			break
		}
		time.Sleep(time.Second)
	}
	time.Sleep(15 * time.Second)
	w.halt()
	measureRecovery(&r, w, e)
	settle := 2 * time.Minute
	verifyClientState(ctx, &r, w, e, settle)
	verifyMSCs(ctx, &r, w.msc, settle)
	verifyMedia(ctx, &r, w, before)
	verifySearch(ctx, &r, w, settle)
	verifyOwnership(ctx, &r, e, sr.pauses)
	saveScenario(e, &r, w, started)
	return r
}

// saveScenario writes the scenario's result and client ledger next to its
// logs, so a failure can be investigated without rerunning it.
func saveScenario(e *environment, r *result, w *workload, started time.Time) {
	r.Duration = time.Since(started)
	r.Passed = len(r.Failures) == 0
	sends, remoteSends, toDevice, receipts := w.snapshot()
	body, err := json.MarshalIndent(map[string]any{
		"result": r, "sends": sends, "remote_sends": remoteSends, "to_device": toDevice, "receipts": receipts, "syncs": w.syncTraces(),
		"federation": e.fed.requests(), "media": e.s3.mutations(),
	}, "", "  ")
	if err != nil {
		return
	}
	_ = os.WriteFile(filepath.Join(e.logs, "result.json"), body, 0o644)
}
