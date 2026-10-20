package ha

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/mediaapi/storage/filestore"
	"codefloe.com/pat-s/zendrite/mediaapi/types"
	basepkg "codefloe.com/pat-s/zendrite/setup/base"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/lifecycle"
	"codefloe.com/pat-s/zendrite/test"
)

// takeover promotes a first activation, then a successor, and returns both
// gates. The first gate is left as a resumed owner would be: still running,
// still connected, and not yet aware that it was replaced.
// Promote a first activation, then a successor, returning a pool that the first activation opened and
// used before the successor was promoted, so the sweep has a real backend to
// terminate rather than only a lazily dialed one.
func takeoverWithPool(t *testing.T, retiredCfg, promotedCfg config.HA) (retired, promoted *PostgresGate, dsn string, pooled *sql.DB) {
	t.Helper()
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	scope := Candidate{LeaseName: "matrix", LeaseNamespace: "example"}

	retired = NewPostgresGate(dsn, retiredCfg)
	first := scope
	first.Identity = "candidate-a"
	require.NoError(t, retired.Acquire(t.Context(), first))

	pooled = activationConn(t, dsn, *retired.Activation())
	require.NoError(t, pooled.PingContext(t.Context()), "the retired owner must hold a live backend before takeover")

	promoted = NewPostgresGate(dsn, promotedCfg)
	second := scope
	second.Identity = "candidate-b"
	require.NoError(t, promoted.Acquire(t.Context(), second))
	return retired, promoted, dsn, pooled
}

// pausedCandidate never refreshes its confirmation, which is what a process
// suspended past its lease looks like from inside.
func pausedCandidate() config.HA { return testHAConfig(100*time.Millisecond, time.Hour) }

// healthyCandidate keeps its confirmation fresh.
//
// The interval doubles as the budget for each confirmation attempt, so it has
// to comfortably exceed one PostgreSQL handshake. A fresh connection costs
// 17-27ms on a loaded runner, so a 20ms interval spends most attempts failing
// SASL authentication on the deadline instead of confirming anything.
func healthyCandidate() config.HA { return testHAConfig(time.Hour, 500*time.Millisecond) }

// TestPausedOwnerIsRefusedAtEveryBoundary is the acceptance case for #349: a
// leader paused beyond its lease expiry, resumed after a successor took over,
// with full connectivity to every dependency and no knowledge that it lost the
// Lease. It must be unable to commit a stale authoritative effect anywhere.
func TestPausedOwnerIsRefusedAtEveryBoundary(t *testing.T) {
	// A long freshness window would let a paused owner act on a stale
	// confirmation, so the window is what the test must expire.
	retired, promoted, dsn, pooled := takeoverWithPool(t, pausedCandidate(), healthyCandidate())

	// The pause is modeled by time passing without a heartbeat.
	require.Eventually(t, func() bool { return retired.Valid() != nil }, 30*time.Second, 10*time.Millisecond)

	t.Run("database", func(t *testing.T) {
		// The pool is opened and used before takeover, so a real backend
		// exists and has already passed the guard. A pool opened afterwards
		// would only exercise guardConnection refusing a fresh dial and would
		// stay green with backend fencing disabled entirely.
		_, err := pooled.ExecContext(t.Context(), "CREATE TABLE IF NOT EXISTS stale_effect (value TEXT)")
		require.Error(t, err, "a resumed owner must not commit")
	})

	t.Run("broker", func(t *testing.T) {
		protected := jetstream.Protected(nil, retired)
		_, err := protected.PublishMsg(&nats.Msg{Subject: "owned", Data: []byte("stale")})
		require.Error(t, err, "a resumed owner must not publish")
		_, err = protected.Publish("owned", []byte("stale"))
		require.Error(t, err, "the convenience publisher must be refused too")
		require.Error(t, protected.DeleteStream("owned"), "a resumed owner must not mutate streams")
		require.Error(t, protected.DeleteConsumer("owned", "worker"), "a resumed owner must not mutate consumers")
		require.Error(t, jetstream.Owned(protected), "delivery inside a callback must be refused")
	})

	t.Run("admission", func(t *testing.T) {
		require.False(t, Admit(func() bool { return true }, retired)(),
			"a resumed owner must not admit application requests")
	})

	t.Run("dispatch", func(t *testing.T) {
		guarded := basepkg.GuardFederationClient(nil, retired)
		_, err := guarded.SendTransaction(t.Context(), gomatrixserverlib.Transaction{})
		require.Error(t, err, "a resumed owner must not start a new outbound transaction")
	})

	t.Run("media", func(t *testing.T) {
		guarded := filestore.Guarded(unusableFileStore{}, retired)
		require.Error(t, guarded.Delete(t.Context(), "hash"), "a resumed owner must not delete media")
		require.Error(t, guarded.DeleteThumbnail(t.Context(), "hash", types.ThumbnailSize{Width: 32, Height: 32}),
			"a resumed owner must not replace or delete thumbnails")
	})

	t.Run("successor is unaffected", func(t *testing.T) {
		require.NoError(t, promoted.Valid())
		writes := activationConn(t, dsn, *promoted.Activation())
		_, err := writes.ExecContext(t.Context(), "CREATE TABLE IF NOT EXISTS promoted_effect (value TEXT)")
		require.NoError(t, err)
	})
}

// TestOwnerPartitionedFromKubernetesCannotOverlap covers the asymmetric
// partition: the leader cannot reach the Kubernetes API, so its election loop
// never learns anything and its leader context stays live, while PostgreSQL and
// NATS remain reachable.
//
// The elector below deliberately never reports loss, so nothing except the
// database can tell this candidate that it was replaced.
func TestOwnerPartitionedFromKubernetesCannotOverlap(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	scope := Candidate{LeaseName: "matrix", LeaseNamespace: "example"}

	partitioned := NewPostgresGate(dsn, healthyCandidate())
	first := scope
	first.Identity = "candidate-a"
	require.NoError(t, partitioned.Acquire(t.Context(), first))
	pooled := activationConn(t, dsn, *partitioned.Activation())
	require.NoError(t, pooled.PingContext(t.Context()))

	// The Lease is still held as far as this candidate can tell.
	leader, keepLeadership := context.WithCancelCause(context.WithoutCancel(t.Context()))
	defer keepLeadership(errors.New("test finished"))
	require.NoError(t, leader.Err(), "the partitioned leader must still believe it holds the Lease")

	promoted := NewPostgresGate(dsn, healthyCandidate())
	second := scope
	second.Identity = "candidate-b"
	require.NoError(t, promoted.Acquire(t.Context(), second))

	// Report why confirmation is not landing rather than only that it did not.
	// Ownership stays valid for the whole freshness window while the heartbeat
	// fails, so a bare timeout here says nothing about the cause.
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.ErrorIsf(c, partitioned.Valid(), ErrOwnershipRetired,
			"last confirmation failure: %q", partitioned.LastConfirmationError())
	}, 30*time.Second, 20*time.Millisecond,
		"a leader that keeps data-plane access must still discover it was replaced")
	require.NoError(t, leader.Err(), "discovery must not depend on the Lease context being canceled")

	_, err := pooled.ExecContext(t.Context(), "SELECT 1")
	require.Error(t, err, "the partitioned leader must not overlap with the successor")
	require.NoError(t, promoted.Valid())
}

// TestUncertainFencingBlocksTakeover covers the fail-closed rule: when a
// candidate cannot prove that every retired backend is gone, it must stay
// unready and name the boundary rather than activate anyway.
func TestUncertainFencingBlocksTakeover(t *testing.T) {
	cfg := testHAConfig(time.Hour, time.Hour)
	cfg.LeaseName, cfg.LeaseNamespace = "matrix", "example"
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)

	first := NewPostgresGate(dsn, cfg)
	require.NoError(t, first.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	held := activationConn(t, dsn, *first.Activation())
	require.NoError(t, held.PingContext(t.Context()))

	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })
	successor := Activation{Generation: first.Activation().Generation + 1, Identity: "candidate-b", Scope: "example/matrix"}

	horizon, err := fenceHorizon(t.Context(), conn)
	require.NoError(t, err)

	// Termination is made a no-op so a pass completes without converging,
	// which is uncertain fencing rather than a bare timeout.
	//
	// The budget only has to outlast one fence-and-count pass, which the
	// sweep can never satisfy here. It is generous because a pass that does
	// not finish in time reports "cannot prove" instead, and that would make
	// this test flaky on a loaded runner rather than catch a regression.
	require.NoError(t, held.PingContext(t.Context()))
	budget, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	err = NewPostgresGate(dsn, cfg).sweep(budget, unterminable{conn}, successor, horizon)
	require.Error(t, err)
	require.ErrorContains(t, err, "backend(s) of a retired activation are still connected",
		"the failure must report how many backends blocked promotion")
}

// unterminable counts honestly but terminates nothing, modeling a backend the
// promoter cannot stop.
type unterminable struct{ *pgx.Conn }

func (u unterminable) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	if strings.Contains(sql, "pg_terminate_backend") {
		return u.Conn.Query(ctx, "SELECT 1 WHERE false")
	}
	return u.Conn.Query(ctx, sql, args...)
}

// TestDispatchedTransactionsAreNotRecalled documents the external delivery
// guarantee: a transaction already in flight completes, and only newly
// admitted dispatch is refused. Delivery is at least once, never exactly once.
func TestDispatchedTransactionsAreNotRecalled(t *testing.T) {
	cfg := testHAConfig(time.Hour, time.Hour)
	cfg.LeaseName, cfg.LeaseNamespace = "matrix", "example"
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	gate := NewPostgresGate(dsn, cfg)
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))

	dispatched := make(chan struct{})
	release := make(chan struct{})
	inner := &blockingFederationClient{dispatched: dispatched, release: release}
	guarded := basepkg.GuardFederationClient(inner, gate)

	inFlight := make(chan error, 1)
	go func() {
		_, err := guarded.SendTransaction(t.Context(), gomatrixserverlib.Transaction{})
		inFlight <- err
	}()
	<-dispatched

	// Ownership is lost while the request is on the wire.
	gate.Retire()
	_, err := guarded.SendTransaction(t.Context(), gomatrixserverlib.Transaction{})
	require.ErrorIs(t, err, ErrOwnershipRetired, "new dispatch must be refused")

	close(release)
	require.NoError(t, <-inFlight, "a transaction already dispatched is not recalled")
}

type blockingFederationClient struct {
	fclient.FederationClient
	dispatched chan struct{}
	release    chan struct{}
	once       sync.Once
}

func (c *blockingFederationClient) SendTransaction(context.Context, gomatrixserverlib.Transaction) (fclient.RespSend, error) {
	// Signal once: a dispatch-guard regression calls this twice, and closing a
	// closed channel would panic and take down the whole test binary instead
	// of failing this one assertion.
	c.once.Do(func() { close(c.dispatched) })
	<-c.release
	return fclient.RespSend{}, nil
}

// stuckApplication never returns from Run, modeling a constructor or worker
// that does not observe cancellation.
type stuckApplication struct{ stopped chan struct{} }

func (a *stuckApplication) Run(context.Context) error { <-a.stopped; return nil }
func (a *stuckApplication) Stop()                     {}

// TestStuckApplicationForcesTermination exercises the shutdown budget. An
// application that will not stop must surface ErrShutdownTimeout, which the
// binary turns into a nonzero exit rather than returning to standby.
func TestStuckApplicationForcesTermination(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		leader, lose := context.WithCancelCause(ctx)
		defer lose(nil)
		acquired := make(chan context.Context, 1)
		elector := electionFunc(func(ctx context.Context) {
			acquired <- leader
			select {
			case <-ctx.Done():
			case <-leader.Done():
			}
		})
		app := &stuckApplication{stopped: make(chan struct{})}
		defer close(app.stopped)

		done := make(chan error, 1)
		go func() {
			done <- control(ctx, elector, acquired, nil, Candidate{}, gateFunc(func(context.Context, Candidate) error { return nil }), app, time.Second, NewStatus())
		}()
		synctest.Wait()
		lose(ErrLeadershipLost)

		err := <-done
		require.ErrorIs(t, err, ErrLeadershipLost)
		require.ErrorIs(t, err, lifecycle.ErrShutdownTimeout, "a stuck application must force process termination")
	})
}

// unusableFileStore fails loudly if the guard ever lets a call through.
type unusableFileStore struct{ filestore.FileStore }

func (unusableFileStore) Delete(context.Context, types.Base64Hash) error {
	panic("a retired activation reached the media backend")
}

func (unusableFileStore) DeleteThumbnail(context.Context, types.Base64Hash, types.ThumbnailSize) error {
	panic("a retired activation reached the media backend")
}
