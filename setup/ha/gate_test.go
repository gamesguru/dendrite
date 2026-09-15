package ha

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/test"
)

func TestPostgresGateFencesThePreviousActivation(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	candidate := Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}

	first := NewPostgresGate(dsn, 45*time.Second)
	require.NoError(t, first.Acquire(t.Context(), candidate))
	require.Equal(t, int64(1), first.Activation().Generation)
	retired := activationConn(t, dsn, *first.Activation())
	require.NoError(t, retired.PingContext(t.Context()))

	successor := NewPostgresGate(dsn, 45*time.Second)
	require.NoError(t, successor.Acquire(t.Context(), Candidate{Identity: "candidate-b", LeaseName: "matrix", LeaseNamespace: "example"}))
	require.Equal(t, int64(2), successor.Activation().Generation)

	require.Error(t, retired.PingContext(t.Context()), "the previous activation must lose database access")
	promoted := activationConn(t, dsn, *successor.Activation())
	require.NoError(t, promoted.PingContext(t.Context()))
}

func TestPostgresGateReportsTheBlockingBoundary(t *testing.T) {
	gate := NewPostgresGate("postgresql://zendrite@127.0.0.1:1/nonexistent", time.Second)
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
	gate := NewPostgresGate(dsn, 45*time.Second)
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

	old := NewPostgresGate(dsn, 45*time.Second)
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
	promoted := NewPostgresGate(dsn, 45*time.Second)
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
	gate := NewPostgresGate("postgresql://zendrite@127.0.0.1/zendrite", 45*time.Second)
	require.Empty(t, gate.ApplicationName(), "an unpromoted candidate must not mark connections")
	require.Error(t, gate.VerifyConnection(t.Context(), nil), "an unpromoted candidate must not open application connections")
}

func TestPostgresGateGuardsConnectionsAfterPromotion(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	gate := NewPostgresGate(dsn, 45*time.Second)
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	require.Equal(t, applicationName(*gate.Activation()), gate.ApplicationName())

	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })
	require.NoError(t, gate.VerifyConnection(t.Context(), conn))

	successor := NewPostgresGate(dsn, 45*time.Second)
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

func TestPostgresGateCoversOnlyTheDatabaseBoundary(t *testing.T) {
	gate := NewPostgresGate("postgresql://zendrite@127.0.0.1/zendrite", 45*time.Second)
	require.Equal(t, []Boundary{BoundaryPostgres}, gate.Boundaries())
	require.ErrorIs(t, verifyCoverage(gate), ErrFencingIncomplete)
}
