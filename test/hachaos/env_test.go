//go:build linux

package hachaos

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"codefloe.com/pat-s/zendrite/test"
)

// settings select what a run covers.
type settings struct {
	tier       string
	artifacts  string
	zendrite   string
	next       string
	self       string
	timings    timings
	candidates int
	// rate is the steady client write rate in messages per second.
	rate float64
	// target is the recovery-time objective from fault to successful
	// client and federation requests.
	target time.Duration
	// seedRooms and seedUsers size the reference workload.
	seedRooms int
	seedUsers int
	// mscs are the experimental MSCs enabled on every server, and exercised
	// by the MSC probe. HACHAOS_MSCS=none runs without them.
	mscs []string
}

const (
	tierCI        = "ci"
	tierFull      = "full"
	tierReference = "reference"
)

func loadSettings(t *testing.T) settings {
	t.Helper()
	tier := os.Getenv("HACHAOS_TIER")
	if tier == "" {
		t.Skip("HACHAOS_TIER is not set; run build/scripts/ha-qualification.sh")
	}
	s := settings{
		tier:       tier,
		artifacts:  os.Getenv("HACHAOS_ARTIFACTS"),
		zendrite:   os.Getenv("HACHAOS_ZENDRITE"),
		next:       os.Getenv("HACHAOS_ZENDRITE_NEXT"),
		candidates: 3,
		target:     120 * time.Second,
		mscs:       haMSCs,
	}
	if os.Getenv("HACHAOS_MSCS") == "none" {
		s.mscs = nil
	}
	self, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	s.self = self
	switch tier {
	case tierCI:
		// Shorter election timings keep the gate bounded. They respect the
		// same ordering constraints as the reference timings.
		s.timings = timings{Lease: 10 * time.Second, Renew: 6 * time.Second, Retry: 2 * time.Second, Ownership: 15 * time.Second}
		s.rate = 2
	case tierFull:
		s.timings = referenceTimings
		s.rate = 5
	case tierReference:
		s.timings = referenceTimings
		s.rate = 5
		s.seedRooms, s.seedUsers = 1000, 100
	default:
		t.Fatalf("unknown HACHAOS_TIER %q", tier)
	}
	if s.zendrite == "" {
		t.Fatal("HACHAOS_ZENDRITE must name the zendrite binary under test")
	}
	if s.artifacts == "" {
		s.artifacts = t.TempDir()
	}
	return s
}

// environment is one isolated deployment: fresh storage, fresh control
// plane, fresh candidates. Scenarios never share one, so a fault left behind
// by one scenario cannot explain a result in another.
type environment struct {
	t        *testing.T
	ctx      context.Context
	settings settings
	timings  timings
	dir      string
	logs     string
	owners   *owners
	timeline *timeline

	pg     *pgCluster
	nats   *natsCluster
	s3     *s3Service
	kube   *kubeAPI
	fed    *gateway
	router *router
	slots  []*slot
	remote *slot

	haKey     string
	remoteKey string

	observer *observer
	stopOnce sync.Once
}

func newEnvironment(t *testing.T, ctx context.Context, s settings, name string) *environment {
	t.Helper()
	// Not t.TempDir: its parent is private to root, and PostgreSQL runs as
	// another user. HACHAOS_KEEP also needs to outlive the test.
	dir, err := os.MkdirTemp("", "hachaos-"+name+"-") //nolint:usetesting // See above.
	if err != nil {
		t.Fatal(err)
	}
	// PostgreSQL runs as another user and must be able to reach its data.
	if err = os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	logs := filepath.Join(s.artifacts, name)
	if err = os.MkdirAll(logs, 0o755); err != nil {
		t.Fatal(err)
	}
	tl, err := newTimeline(filepath.Join(logs, "timeline.jsonl"))
	if err != nil {
		t.Fatal(err)
	}
	e := &environment{t: t, ctx: ctx, settings: s, timings: s.timings, dir: dir, logs: logs, owners: newOwners(), timeline: tl}
	t.Cleanup(e.stop)
	must := func(err error, what string) {
		t.Helper()
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
	}
	for _, sub := range []string{"pg", "nats", "s3", "kube", "shared", "remote"} {
		must(os.MkdirAll(filepath.Join(dir, sub), 0o755), "create "+sub)
	}
	e.haKey = filepath.Join(dir, "shared", "matrix_key.pem")
	e.remoteKey = filepath.Join(dir, "remote", "matrix_key.pem")
	must(test.NewMatrixKey(e.haKey), "generate signing key")
	must(test.NewMatrixKey(e.remoteKey), "generate remote signing key")

	e.pg, err = startPostgres(ctx, filepath.Join(dir, "pg"), logs, e.owners, tl)
	must(err, "start PostgreSQL")
	e.nats, err = startNATS(ctx, filepath.Join(dir, "nats"), logs, s.self, e.owners, tl)
	must(err, "start NATS")
	e.s3, err = startS3(ctx, filepath.Join(dir, "s3"), logs, s.self, e.owners, tl)
	must(err, "start S3")
	e.kube, err = startKube(ctx, filepath.Join(dir, "kube"), logs, e.owners, tl)
	must(err, "start Kubernetes API")
	e.fed, err = startGateway(e.owners, tl)
	must(err, "start federation gateway")

	for i := range s.candidates {
		sl := &slot{index: i, name: fmt.Sprintf("cand-%d", i), dir: filepath.Join(dir, fmt.Sprintf("cand-%d", i)), logs: logs, owners: e.owners, timeline: tl, bin: s.zendrite, restartIn: time.Second}
		must(os.MkdirAll(sl.dir, 0o755), "create candidate directory")
		sl.httpPort, err = freePort()
		must(err, "allocate port")
		sl.statusPort, err = freePort()
		must(err, "allocate port")
		sl.config = filepath.Join(sl.dir, "zendrite.yaml")
		body, err := haConfig(e, sl)
		must(err, "render candidate configuration")
		must(writeConfig(sl.config, body), "write candidate configuration")
		sl.env = append(e.kube.candidateEnv(), "HTTPS_PROXY="+e.fed.proxyURL(), "GOTRACEBACK=all")
		e.slots = append(e.slots, sl)
	}
	e.router, err = startRouter(ctx, e.slots, tl)
	must(err, "start router")
	e.fed.route(haServerName, e.router.url())

	remote := &slot{name: "remote", dir: filepath.Join(dir, "remote"), logs: logs, owners: e.owners, timeline: tl, bin: s.zendrite, restartIn: time.Second}
	remote.httpPort, err = freePort()
	must(err, "allocate port")
	remote.config = filepath.Join(remote.dir, "zendrite.yaml")
	body, err := remoteConfig(e, remote.dir)
	must(err, "render remote configuration")
	must(writeConfig(remote.config, body), "write remote configuration")
	remote.env = []string{"HTTPS_PROXY=" + e.fed.proxyURL()}
	e.remote = remote
	e.fed.route(remoteServerName, remote.httpURL())

	e.observer = startObserver(ctx, e.slots, tl)

	// Bootstrap as the documentation prescribes: one candidate first, then
	// scale up once it is active.
	must(e.slots[0].start(), "start first candidate")
	_, err = e.waitActive(ctx, 3*time.Minute)
	must(err, "first candidate did not become active")
	installed, err := e.pg.installAudit(ctx)
	must(err, "install database audit")
	tl.add(eventNote, "postgres", "audit trigger installed on %d tables", installed)
	for _, sl := range e.slots[1:] {
		must(sl.start(), "start candidate")
	}
	must(remote.start(), "start remote homeserver")
	must(waitFor(ctx, 2*time.Minute, func() error { return newClient(remote.httpURL()).versions(ctx) }), "remote homeserver did not start")
	must(e.waitStandbys(ctx, 2*time.Minute), "standbys did not become healthy")
	return e
}

// waitActive waits until the router has an active backend that answers a
// client request, and reports which candidate it is.
func (e *environment) waitActive(ctx context.Context, timeout time.Duration) (*slot, error) {
	var active *slot
	err := waitFor(ctx, timeout, func() error {
		active = e.router.activeSlot()
		if active == nil {
			return fmt.Errorf("no candidate passes the active-only check")
		}
		return newClient(e.router.url()).versions(ctx)
	})
	return active, err
}

// waitStandbys waits until every candidate other than the active one reports
// a healthy standby phase.
func (e *environment) waitStandbys(ctx context.Context, timeout time.Duration) error {
	return waitFor(ctx, timeout, func() error {
		standbys := 0
		for _, sl := range e.slots {
			st, err := sl.status(ctx)
			if err != nil {
				return fmt.Errorf("%s: %w", sl.name, err)
			}
			if st.Phase == "standby" {
				standbys++
			}
		}
		if standbys != len(e.slots)-1 {
			return fmt.Errorf("%d of %d standbys ready", standbys, len(e.slots)-1)
		}
		return nil
	})
}

// federationOutcomes lists federation requests in one direction: inbound from
// the remote server to the HA server, or outbound from any candidate.
func (e *environment) federationOutcomes(inbound bool) []outcome {
	var out []outcome
	for _, req := range e.fed.requests() {
		if inbound && (req.Owner != "remote" || req.Host != haServerName) {
			continue
		}
		if !inbound && (!strings.HasPrefix(req.Owner, "cand-") || req.Host != remoteServerName) {
			continue
		}
		out = append(out, outcome{Start: req.At, End: req.At, OK: req.Status/100 == 2})
	}
	return out
}

func (e *environment) stop() {
	e.stopOnce.Do(func() {
		if e.observer != nil {
			e.observer.stop()
		}
		for _, sl := range e.slots {
			sl.halt(false)
		}
		if e.remote != nil {
			e.remote.halt(true)
		}
		if e.router != nil {
			e.router.stop()
		}
		if e.fed != nil {
			e.fed.stop()
		}
		if e.kube != nil {
			e.kube.stop()
		}
		if e.s3 != nil {
			e.s3.stop()
		}
		if e.nats != nil {
			e.nats.stop()
		}
		if e.pg != nil {
			e.pg.stop()
		}
		e.timeline.close()
		if os.Getenv("HACHAOS_KEEP") == "" {
			_ = os.RemoveAll(e.dir)
		}
	})
}
