package ha

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

func TestPostgresGateFencesThePreviousActivation(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	candidate := Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}

	first := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.NoError(t, first.Acquire(t.Context(), candidate))
	require.Equal(t, int64(1), first.Activation().Generation)
	retired := activationConn(t, dsn, *first.Activation())
	require.NoError(t, retired.PingContext(t.Context()))

	successor := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.NoError(t, successor.Acquire(t.Context(), Candidate{Identity: "candidate-b", LeaseName: "matrix", LeaseNamespace: "example"}))
	require.Equal(t, int64(2), successor.Activation().Generation)

	require.Error(t, retired.PingContext(t.Context()), "the previous activation must lose database access")
	promoted := activationConn(t, dsn, *successor.Activation())
	require.NoError(t, promoted.PingContext(t.Context()))
}

func TestPostgresGateReportsTheBlockingBoundary(t *testing.T) {
	gate := NewPostgresGate("postgresql://zendrite@127.0.0.1:1/nonexistent", testHAConfig(time.Hour, time.Hour))
	err := gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"})
	require.Error(t, err)
	var blocked *BlockedError
	require.ErrorAs(t, err, &blocked)
	require.Equal(t, BoundaryPostgres, blocked.Boundary)
	require.Nil(t, gate.Activation(), "a blocked candidate must not report an activation")
}

func TestPostgresGateDoesNotActivateOnCancellation(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(errors.New("promotion canceled"))
	gate := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.Error(t, gate.Acquire(ctx, Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	require.Nil(t, gate.Activation())

	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })
	current, err := readOwnership(t.Context(), conn, "example/matrix")
	require.NoError(t, err)
	require.Equal(t, int64(0), current.Generation, "a canceled promotion must not retire a generation")
}

// TestResumedOwnerCannotCommitAfterTakeover exercises the failure that the
// Lease alone cannot prevent: a leader suspended beyond its lease that resumes
// after a successor is promoted, still holding database connectivity.
func TestResumedOwnerCannotCommitAfterTakeover(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	scoped := Candidate{LeaseName: "matrix", LeaseNamespace: "example"}

	old := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	oldCandidate := scoped
	oldCandidate.Identity = "candidate-a"
	require.NoError(t, old.Acquire(t.Context(), oldCandidate))

	writes := activationConn(t, dsn, *old.Activation())
	_, err := writes.ExecContext(t.Context(), "CREATE TABLE authoritative_effect (value TEXT)")
	require.NoError(t, err)

	// The old owner is suspended mid-transaction, before its commit.
	suspended, err := writes.BeginTx(t.Context(), nil)
	require.NoError(t, err)
	_, err = suspended.ExecContext(t.Context(), "INSERT INTO authoritative_effect VALUES ('stale')")
	require.NoError(t, err)

	successor := scoped
	successor.Identity = "candidate-b"
	promoted := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.NoError(t, promoted.Acquire(t.Context(), successor))

	// It resumes after takeover and tries to finish its work.
	require.Error(t, suspended.Commit(), "a retired activation must not commit an in-flight transaction")
	_, err = writes.ExecContext(t.Context(), "INSERT INTO authoritative_effect VALUES ('stale')")
	require.Error(t, err, "a retired activation must not open a fresh connection and write")

	survivor := activationConn(t, dsn, *promoted.Activation())
	var rows int
	require.NoError(t, survivor.QueryRowContext(t.Context(), "SELECT count(*) FROM authoritative_effect").Scan(&rows))
	require.Equal(t, 0, rows, "no stale authoritative effect may survive takeover")
}

func TestPostgresGateRefusesConnectionsBeforePromotion(t *testing.T) {
	gate := NewPostgresGate("postgresql://zendrite@127.0.0.1/zendrite", testHAConfig(time.Hour, time.Hour))
	require.Empty(t, gate.ApplicationName(), "an unpromoted candidate must not mark connections")
	require.Error(t, gate.VerifyConnection(t.Context(), nil), "an unpromoted candidate must not open application connections")
}

func TestPostgresGateGuardsConnectionsAfterPromotion(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	gate := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	require.Equal(t, applicationName(*gate.Activation()), gate.ApplicationName())

	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })
	require.NoError(t, gate.VerifyConnection(t.Context(), conn))

	successor := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.NoError(t, successor.Acquire(t.Context(), Candidate{Identity: "candidate-b", LeaseName: "matrix", LeaseNamespace: "example"}))
	require.ErrorContains(t, gate.VerifyConnection(t.Context(), conn), "retired")
}

type boundedGate struct{ boundaries []Boundary }

func (g boundedGate) Acquire(context.Context, Candidate) error { return nil }
func (g boundedGate) Boundaries() []Boundary                   { return g.boundaries }

func TestPromotionRequiresEveryProtectedBoundary(t *testing.T) {
	err := verifyCoverage(boundedGate{boundaries: []Boundary{BoundaryPostgres}})
	require.ErrorIs(t, err, ErrFencingIncomplete)
	for _, missing := range []Boundary{BoundaryJetStream, BoundaryMedia, BoundaryDispatch} {
		require.ErrorContains(t, err, string(missing), "the blocking boundaries must be named")
	}
	require.NotContains(t, err.Error(), string(BoundaryPostgres), "a covered boundary must not be reported as missing")

	require.NoError(t, verifyCoverage(boundedGate{boundaries: requiredBoundaries}))
}

func TestPostgresGateCoversEveryProtectedBoundary(t *testing.T) {
	gate := NewPostgresGate("postgresql://zendrite@127.0.0.1/zendrite", testHAConfig(time.Hour, time.Hour))
	require.ElementsMatch(t, requiredBoundaries, gate.Boundaries())
	require.NoError(t, verifyCoverage(gate), "promotion must no longer be blocked by missing coverage")
}

func TestPostgresGateReportsOwnershipToOtherBoundaries(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	gate := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.Error(t, gate.Valid(), "an unpromoted candidate must not act at any boundary")

	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	require.NoError(t, gate.Valid())
	require.Equal(t, int64(1), gate.Generation())
	require.Equal(t, "candidate-a", gate.Identity())

	gate.Retire()
	require.Error(t, gate.Valid(), "a retired activation must stop acting at every boundary")
}

// TestPostgresGateRetiresOnRefusedConnection makes the database the trigger:
// the first connection PostgreSQL refuses tells the rest of the process that
// its ownership is gone, without waiting for the election loop to catch up.
func TestPostgresGateRetiresOnRefusedConnection(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	gate := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))

	successor := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.NoError(t, successor.Acquire(t.Context(), Candidate{Identity: "candidate-b", LeaseName: "matrix", LeaseNamespace: "example"}))
	require.NoError(t, gate.Valid(), "ownership loss is only observable once the database refuses")

	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })
	require.Error(t, gate.VerifyConnection(t.Context(), conn))
	require.Error(t, gate.Valid())
}

// TestPostgresGateKeepsOwnershipThroughTransientErrors separates losing
// ownership from failing to check it. A network error must not retire a leader
// that still holds the current generation.
func TestPostgresGateKeepsOwnershipThroughTransientErrors(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	gate := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))

	broken, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	require.NoError(t, broken.Close(t.Context()))

	require.Error(t, gate.VerifyConnection(t.Context(), broken), "a closed connection cannot be verified")
	require.NoError(t, gate.Valid(), "an unverifiable connection is not proof of lost ownership")
}

func testHAConfig(freshness, interval time.Duration) config.HA {
	return config.HA{
		LeaseName: "matrix", LeaseNamespace: "example",
		OwnershipTimeout: 45 * time.Second,
		RenewDeadline:    freshness,
		RetryPeriod:      interval,
	}
}

// TestOwnershipGoesStaleWithoutAHeartbeat is the suspended-owner case at
// boundaries the database cannot reach. A process that was paused cannot know
// it still owns the homeserver, so it must refuse to act until it re-verifies.
func TestOwnershipGoesStaleWithoutAHeartbeat(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	gate := NewPostgresGate(dsn, testHAConfig(150*time.Millisecond, time.Hour))
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	require.NoError(t, gate.Valid())

	require.Eventually(t, func() bool { return gate.Valid() != nil }, 30*time.Second, 10*time.Millisecond,
		"ownership must go stale once it has not been re-verified")
	require.ErrorIs(t, gate.Valid(), ErrOwnershipStale)
}

func TestHeartbeatKeepsOwnershipFresh(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	gate := NewPostgresGate(dsn, testHAConfig(3*time.Second, 100*time.Millisecond))
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))

	// The window is comfortably larger than a round trip so a slow database on
	// a loaded runner cannot null the connection and fail the test.
	require.Never(t, func() bool { return gate.Valid() != nil }, time.Second, 50*time.Millisecond,
		"a heartbeating owner must stay valid")
}

func TestHeartbeatRetiresTheOwnerAfterTakeover(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	gate := NewPostgresGate(dsn, testHAConfig(time.Hour, 100*time.Millisecond))
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	require.NoError(t, gate.Valid())

	successor := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.NoError(t, successor.Acquire(t.Context(), Candidate{Identity: "candidate-b", LeaseName: "matrix", LeaseNamespace: "example"}))

	require.Eventually(t, func() bool { return errors.Is(gate.Valid(), ErrOwnershipRetired) }, 30*time.Second, 20*time.Millisecond,
		"the heartbeat must observe that a successor took over")
}

// TestUnconfirmedOwnershipIsAbandoned is a Lease holder that loses the
// database. Its ownership goes stale and it refuses to act, but it can still
// renew the Lease, so it must eventually give ownership up rather than keep
// every standby out for as long as the partition lasts.
func TestUnconfirmedOwnershipIsAbandoned(t *testing.T) {
	cfg := testHAConfig(100*time.Millisecond, 20*time.Millisecond)
	cfg.LeaseDuration = 200 * time.Millisecond
	// Nothing listens on port 1, so every confirmation fails.
	gate := NewPostgresGate("postgres://zendrite@127.0.0.1:1/zendrite?sslmode=disable&connect_timeout=1", cfg)
	gate.activation.Store(&Activation{Generation: 1, Identity: "candidate-a", Scope: "example/matrix"})
	confirmed := time.Now()
	gate.verified.Store(confirmed.UnixNano())
	go gate.heartbeat(t.Context())

	select {
	case <-gate.Abandoned():
	case <-time.After(10 * time.Second):
		t.Fatal("ownership that cannot be confirmed must be abandoned")
	}
	require.GreaterOrEqual(t, time.Since(confirmed), cfg.RenewDeadline+cfg.LeaseDuration,
		"ownership must not be abandoned before the freshness window and the Lease duration have passed")
}

func TestConfirmedOwnershipIsNotAbandoned(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	cfg := testHAConfig(time.Second, 50*time.Millisecond)
	cfg.LeaseDuration = 100 * time.Millisecond
	gate := NewPostgresGate(dsn, cfg)
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	select {
	case <-gate.Abandoned():
		t.Fatal("a heartbeating owner must not abandon ownership")
	case <-time.After(time.Second):
	}
}
