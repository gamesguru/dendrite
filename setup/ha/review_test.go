package ha

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

// unguardedConn opens a connection that claims an activation without passing
// the ownership guard, modeling a retired owner whose client is still
// dialing while a successor sweeps.
func unguardedConn(t *testing.T, dsn string, activation Activation) *pgx.Conn {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.RuntimeParams["application_name"] = applicationName(activation)
	conn, err := pgx.ConnectConfig(t.Context(), cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })
	return conn
}

// TestSweepNeverFencesANewerGeneration covers a promoter frozen between its
// advance and its sweep. When it resumes, the generation it holds has already
// been retired, and terminating the live successor's backends would turn a
// stale candidate into a denial of service against the working homeserver.
func TestSweepNeverFencesANewerGeneration(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })

	const scope = "example/matrix"
	stale, err := advanceOwnership(t.Context(), conn, scope, "candidate-a", 0)
	require.NoError(t, err)
	live, err := advanceOwnership(t.Context(), conn, scope, "candidate-b", stale.Generation)
	require.NoError(t, err)

	serving := activationConn(t, dsn, live)
	require.NoError(t, serving.PingContext(t.Context()))

	horizon, err := fenceHorizon(t.Context(), conn)
	require.NoError(t, err)
	terminated, err := fenceBackends(t.Context(), conn, stale, horizon)
	require.NoError(t, err)
	require.Zero(t, terminated, "a stale promoter must fence only earlier generations")
	require.NoError(t, serving.PingContext(t.Context()), "the live successor must keep serving")
}

// TestBackendsAfterTheHorizonAreNotFencingEvidence covers a retired owner that
// is still taking traffic and so keeps dialing the database.
//
// Those connections are refused by guardConnection, because they necessarily
// read the generation committed before they started. Counting them as evidence
// of incomplete fencing would let a retired owner hold the sweep above zero for
// the whole ownership budget, turning a failover into a crash loop for the
// standby.
func TestBackendsAfterTheHorizonAreNotFencingEvidence(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })

	const scope = "example/matrix"
	retired, err := advanceOwnership(t.Context(), conn, scope, "candidate-a", 0)
	require.NoError(t, err)
	promoted, err := advanceOwnership(t.Context(), conn, scope, "candidate-b", retired.Generation)
	require.NoError(t, err)
	horizon, err := fenceHorizon(t.Context(), conn)
	require.NoError(t, err)

	// The retired owner dials again after the advance committed.
	unguardedConn(t, dsn, retired)
	remaining, err := retiredBackends(t.Context(), conn, promoted, horizon)
	require.NoError(t, err)
	require.Zero(t, remaining, "a backend started after the horizon cannot hold the retired generation")

	require.NoError(t, NewPostgresGate(dsn, 45*time.Second).sweep(t.Context(), conn, promoted, horizon),
		"promotion must converge instead of waiting out the ownership budget")
}

// TestRefusedConnectionsDoNotLeakBackends covers pgx returning an AfterConnect
// failure without closing the connection it opened. A retired owner under load
// would otherwise exhaust max_connections exactly when the successor needs its
// pool.
func TestRefusedConnectionsDoNotLeakBackends(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	retired := NewPostgresGate(dsn, 45*time.Second)
	require.NoError(t, retired.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	promoted := NewPostgresGate(dsn, 45*time.Second)
	require.NoError(t, promoted.Acquire(t.Context(), Candidate{Identity: "candidate-b", LeaseName: "matrix", LeaseNamespace: "example"}))

	connConfig, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	connConfig.RuntimeParams["application_name"] = retired.ApplicationName()
	db := stdlib.OpenDB(*connConfig, stdlib.OptionAfterConnect(retired.GuardConnection))
	t.Cleanup(func() { _ = db.Close() })
	for range 5 {
		require.Error(t, db.PingContext(t.Context()))
	}

	observer, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = observer.Close(context.WithoutCancel(t.Context())) })
	var leaked int
	require.NoError(t, observer.QueryRow(t.Context(),
		"SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = $1",
		retired.ApplicationName()).Scan(&leaked))
	require.Zero(t, leaked, "a refused connection must not leave a live backend")
}

// TestPrepareStorageReportsWhyItWasRefused keeps the one error that explains a
// fencing event from being replaced by a generic connection failure.
func TestPrepareStorageReportsWhyItWasRefused(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	cfg := &config.Zendrite{}
	cfg.Global.HA = config.HA{Enabled: true, Compatibility: "same-binary-and-config"}
	cfg.Global.DatabaseOptions.ConnectionString = config.DataSource(dsn)

	retired := NewPostgresGate(dsn, 45*time.Second)
	require.NoError(t, retired.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	promoted := NewPostgresGate(dsn, 45*time.Second)
	require.NoError(t, promoted.Acquire(t.Context(), Candidate{Identity: "candidate-b", LeaseName: "matrix", LeaseNamespace: "example"}))

	_, err := PrepareStorage(t.Context(), cfg, retired)
	require.ErrorIs(t, err, ErrGenerationRetired)
}
