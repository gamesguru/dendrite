//go:build linux

package hachaos

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

// run is one scenario in progress.
type run struct {
	e      *environment
	w      *workload
	r      *result
	pauses map[string][]time.Time

	leaderSlot *slot
	leaderProc *process
	// plannedUntil is when a planned upgrade or restart finished.
	plannedUntil time.Time
	search       *searchProbe
	// rejoined is when a broker that was lost rejoined the cluster.
	rejoined time.Time
}

// faultf records the moment of injection, which every recovery time is
// measured from. A scenario with several steps marks the first disruptive one.
func (s *run) faultf(subject, format string, args ...any) {
	ev := s.e.timeline.add(eventFault, subject, format, args...)
	if s.r.FaultAt.IsZero() {
		s.r.FaultAt = ev.At
	}
}

// active waits for the serving candidate and remembers its process, so a
// successor can be recognized even when it is a restart in the same slot.
func (s *run) active() (*slot, error) {
	sl, err := s.e.waitActive(s.e.ctx, 3*time.Minute)
	if err == nil {
		s.leaderSlot, s.leaderProc = sl, sl.current()
	}
	return sl, err
}

// successor waits until a process other than old's is serving traffic.
func (s *run) successor(old *slot, timeout time.Duration) error {
	oldProc := old.current()
	if old == s.leaderSlot {
		oldProc = s.leaderProc
	}
	return waitFor(s.e.ctx, timeout, func() error {
		next := s.e.router.activeSlot()
		if next == nil || next.current() == oldProc {
			return errors.New("no successor is active yet")
		}
		return newClient(s.e.router.url()).versions(s.e.ctx)
	})
}

func (s *run) pause(sl *slot) error {
	proc := sl.current()
	if proc == nil {
		return fmt.Errorf("%s is not running", sl.name)
	}
	s.e.timeline.add(eventFault, sl.name, "SIGSTOP pid %d", proc.pid())
	return proc.signal(syscall.SIGSTOP)
}

func (s *run) resume(sl *slot) error {
	proc := sl.current()
	if proc == nil {
		return fmt.Errorf("%s is not running", sl.name)
	}
	s.pauses[sl.name] = append(s.pauses[sl.name], time.Now())
	s.e.timeline.add(eventFault, sl.name, "SIGCONT pid %d", proc.pid())
	return proc.signal(syscall.SIGCONT)
}

// partition applies f to one candidate's traffic towards every dependency
// named in deps: postgres, nats, kube, s3, federation.
func (s *run) partition(owner string, f fault, deps ...string) {
	for _, dep := range deps {
		switch dep {
		case "postgres":
			s.e.pg.vip.setFault(owner, f)
		case "nats":
			for _, node := range s.e.nats.nodes {
				node.proxy.setFault(owner, f)
			}
		case "kube":
			s.e.kube.proxy.setFault(owner, f)
		case "s3":
			s.e.s3.setFault(owner, f)
		case "federation":
			s.e.fed.setFault(owner, f)
		}
	}
}

func (s *run) healAll() {
	s.e.pg.vip.heal()
	for _, node := range s.e.nats.nodes {
		node.proxy.heal()
	}
	s.e.kube.proxy.heal()
	s.e.s3.heal()
	s.e.fed.heal()
}

// staleWrite sends a write straight to a candidate, bypassing the router, as
// a request parked on an old keep-alive connection would arrive. A retired
// owner must refuse it.
func (s *run) staleWrite(ctx context.Context, sl *slot) error {
	c := newClient(sl.httpURL()).withToken(s.w.alice)
	c.http.Timeout = 10 * time.Second
	_, err := c.send(ctx, s.w.room, "stale-"+nonce(), "hachaos-stale "+nonce())
	return err
}

// expectRefused asserts that a retired owner refuses writes for a while
// after it resumes.
func (s *run) expectRefused(ctx context.Context, sl *slot, window time.Duration) {
	deadline := time.Now().Add(window)
	attempts, refused := 0, 0
	for time.Now().Before(deadline) {
		err := s.staleWrite(ctx, sl)
		attempts++
		if err == nil {
			s.r.failf("%s accepted a write after its successor was promoted", sl.name)
			return
		}
		refused++
		time.Sleep(200 * time.Millisecond)
	}
	s.r.notef("%s refused %d of %d direct writes after resuming", sl.name, refused, attempts)
}

// holdNoActive asserts that no candidate becomes routable for the duration.
func (s *run) holdNoActive(d time.Duration, why string) {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if active := s.e.router.activeSlot(); active != nil {
			s.r.failf("%s became active while %s", active.name, why)
			return
		}
		time.Sleep(routerCheckInterval)
	}
}

// applicationBackends lists the PostgreSQL backends that belong to a
// generation's activation.
func (s *run) applicationBackends(ctx context.Context, generation int64) ([]int, error) {
	var pids []int
	err := s.e.pg.query(ctx, func(conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, `SELECT pid FROM pg_stat_activity WHERE application_name ~ ('^zendrite:[0-9a-f]+:' || $1::bigint || ':')`, generation)
		if err != nil {
			return err
		}
		pids, err = pgx.CollectRows(rows, pgx.RowTo[int])
		return err
	})
	return pids, err
}

func (s *run) currentGeneration(ctx context.Context) (int64, error) {
	history, err := s.e.pg.ownershipHistory(ctx)
	if err != nil {
		return 0, err
	}
	if len(history) == 0 {
		return 0, errors.New("no ownership recorded")
	}
	return history[len(history)-1].Generation, nil
}

// resetCompatibility is the deliberate operator step of a coordinated
// upgrade: with every candidate stopped, clear both compatibility records.
func (s *run) resetCompatibility(ctx context.Context) error {
	if err := s.e.kube.client.CoordinationV1().Leases(leaseNamespace).Delete(ctx, leaseName, deleteOptions()); err != nil {
		return fmt.Errorf("delete Lease: %w", err)
	}
	return s.e.pg.query(ctx, func(conn *pgx.Conn) error {
		_, err := conn.Exec(ctx, "DELETE FROM zendrite_ha_compatibility")
		return err
	})
}
