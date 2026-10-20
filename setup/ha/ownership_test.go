package ha

import (
	"context"
	"database/sql"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/test"
)

func ownershipConn(t *testing.T) *pgx.Conn {
	t.Helper()
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(t.Context()) })
	return conn
}

func TestAdvanceOwnershipBootstrapsFirstGeneration(t *testing.T) {
	conn := ownershipConn(t)
	current, err := readOwnership(t.Context(), conn, "matrix/example")
	require.NoError(t, err)
	require.Equal(t, int64(0), current.Generation)

	activation, err := advanceOwnership(t.Context(), conn, "matrix/example", "candidate-a", current.Generation)
	require.NoError(t, err)
	require.Equal(t, int64(1), activation.Generation)
	require.Equal(t, "candidate-a", activation.Identity)
}

func TestAdvanceOwnershipRejectsRetiredGeneration(t *testing.T) {
	conn := ownershipConn(t)
	first, err := advanceOwnership(t.Context(), conn, "matrix/example", "candidate-a", 0)
	require.NoError(t, err)

	_, err = advanceOwnership(t.Context(), conn, "matrix/example", "candidate-b", first.Generation-1)
	require.ErrorIs(t, err, ErrOwnershipRaced)

	current, err := readOwnership(t.Context(), conn, "matrix/example")
	require.NoError(t, err)
	require.Equal(t, first.Generation, current.Generation)
	require.Equal(t, "candidate-a", current.Identity)
}

func TestAdvanceOwnershipRejectsForeignScope(t *testing.T) {
	conn := ownershipConn(t)
	_, err := advanceOwnership(t.Context(), conn, "matrix/example", "candidate-a", 0)
	require.NoError(t, err)

	_, err = advanceOwnership(t.Context(), conn, "other/homeserver", "candidate-b", 1)
	require.ErrorIs(t, err, ErrOwnershipScope)

	_, err = readOwnership(t.Context(), conn, "other/homeserver")
	require.ErrorIs(t, err, ErrOwnershipScope)
}

func TestAdvanceOwnershipSerializesConcurrentPromoters(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	const promoters = 4
	results := make(chan error, promoters)
	for i := range promoters {
		go func() {
			conn, err := pgx.Connect(t.Context(), dsn)
			if err != nil {
				results <- err
				return
			}
			defer func() { _ = conn.Close(context.WithoutCancel(t.Context())) }()
			_, err = advanceOwnership(t.Context(), conn, "matrix/example", fmt.Sprintf("candidate-%d", i), 0)
			results <- err
		}()
	}
	winners := 0
	for range promoters {
		if err := <-results; err == nil {
			winners++
		} else {
			require.ErrorIs(t, err, ErrOwnershipRaced)
		}
	}
	require.Equal(t, 1, winners, "exactly one promoter may advance from a given generation")
}

// activationConn opens a pooled connection that identifies itself as belonging
// to the supplied activation, the way an application component would.
func activationConn(t *testing.T, dsn string, activation Activation) *sql.DB {
	t.Helper()
	cfg, err := pgx.ParseConfig(dsn)
	require.NoError(t, err)
	cfg.RuntimeParams["application_name"] = applicationName(activation)
	db := stdlib.OpenDB(*cfg, stdlib.OptionAfterConnect(guardConnection(activation)))
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestApplicationNameSurvivesPostgresTruncation(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })

	const scope = "matrix/a-long-homeserver-lease-name-in-a-long-namespace"
	const identity = "a-very-long-pod-uid_0f8fad5b-d9cb-469f-a165-70867728950e"
	activation, err := advanceOwnership(t.Context(), conn, scope, identity, 0)
	require.NoError(t, err)
	require.Less(t, len(applicationName(activation)), 64, "PostgreSQL truncates application_name at 63 bytes")

	db := activationConn(t, dsn, activation)
	var stored string
	require.NoError(t, db.QueryRowContext(t.Context(), "SELECT application_name FROM pg_stat_activity WHERE pid = pg_backend_pid()").Scan(&stored))
	require.Equal(t, applicationName(activation), stored)
}

func TestFenceBackendsTerminatesRetiredActivations(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })

	const scope = "matrix/example"
	retired, err := advanceOwnership(t.Context(), conn, scope, "candidate-a", 0)
	require.NoError(t, err)
	victim := activationConn(t, dsn, retired)
	require.NoError(t, victim.PingContext(t.Context()))
	// Hold the connection open the way an idle pooled connection would be.
	held, err := victim.Conn(t.Context())
	require.NoError(t, err)

	successor, err := advanceOwnership(t.Context(), conn, scope, "candidate-b", retired.Generation)
	require.NoError(t, err)
	survivor := activationConn(t, dsn, successor)
	require.NoError(t, survivor.PingContext(t.Context()))

	horizon, err := fenceHorizon(t.Context(), conn)
	require.NoError(t, err)
	terminated, err := fenceBackends(t.Context(), conn, successor, horizon)
	require.NoError(t, err)
	require.Positive(t, terminated)
	require.Error(t, held.PingContext(t.Context()), "a retired activation's backend must be terminated")
	require.NoError(t, survivor.PingContext(t.Context()), "the promoted activation's backends must survive")
	require.NoError(t, conn.Ping(t.Context()), "the ownership connection must survive")
}

func TestGuardConnectionRefusesRetiredActivation(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close(context.WithoutCancel(t.Context())) })

	const scope = "matrix/example"
	retired, err := advanceOwnership(t.Context(), conn, scope, "candidate-a", 0)
	require.NoError(t, err)
	_, err = advanceOwnership(t.Context(), conn, scope, "candidate-b", retired.Generation)
	require.NoError(t, err)

	resumed := activationConn(t, dsn, retired)
	require.ErrorContains(t, resumed.PingContext(t.Context()), "retired", "a resumed owner must not re-establish database access")
}

// TestOwnershipHistoryFailsClosedWhenTampered covers ADR 0003's requirement
// that missing or ambiguous history blocks promotion. Deleting the current
// record must not silently reset authority back to a first bootstrap.
func TestOwnershipHistoryFailsClosedWhenTampered(t *testing.T) {
	conn := ownershipConn(t)
	const scope = "matrix/example"
	_, err := advanceOwnership(t.Context(), conn, scope, "candidate-a", 0)
	require.NoError(t, err)

	_, err = conn.Exec(t.Context(), "DELETE FROM zendrite_ha_ownership")
	require.NoError(t, err)

	_, err = readOwnership(t.Context(), conn, scope)
	require.ErrorIs(t, err, ErrOwnershipAmbiguous)
	_, err = advanceOwnership(t.Context(), conn, scope, "candidate-b", 0)
	require.ErrorIs(t, err, ErrOwnershipAmbiguous)
}
