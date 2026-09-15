package ha

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"
)

// Boundary names a protected resource boundary. Promotion reports the boundary
// that blocked it so an operator can see why a candidate stayed unready.
type Boundary string

const (
	BoundaryPostgres  Boundary = "postgresql"
	BoundaryJetStream Boundary = "jetstream"
	BoundaryMedia     Boundary = "media"
	BoundaryDispatch  Boundary = "dispatch"
)

// BlockedError reports that a boundary could not establish enforceable
// ownership. Promotion never falls back to activating anyway.
type BlockedError struct {
	Boundary Boundary
	Err      error
}

func (e *BlockedError) Error() string {
	return fmt.Sprintf("HA ownership blocked at the %s boundary: %s", e.Boundary, e.Err)
}

func (e *BlockedError) Unwrap() error { return e.Err }

// PostgresGate establishes resource-enforced ownership at the database
// boundary. It advances a durable generation, then terminates every backend
// belonging to an earlier activation of the same homeserver.
//
// Enforcement is the database's, not this process's: a backend that connected
// before the advance is terminated here, and one that connects afterwards is
// refused by guardConnection. A retired owner therefore cannot commit, whether
// or not it has noticed that it lost the Lease.
type PostgresGate struct {
	dataSource string
	timeout    time.Duration
	activation atomic.Pointer[Activation]
}

// NewPostgresGate returns a gate bound to the homeserver's global database.
func NewPostgresGate(dataSource string, timeout time.Duration) *PostgresGate {
	return &PostgresGate{dataSource: dataSource, timeout: timeout}
}

// Activation reports the authorized activation, or nil while promotion is
// blocked. Callers must not open application connections without it.
func (g *PostgresGate) Activation() *Activation { return g.activation.Load() }

// Acquire retires every earlier generation and proves that no backend of one
// remains connected. It is bounded by the ownership-establishment budget and
// leaves the candidate unready rather than activating on timeout.
func (g *PostgresGate) Acquire(ctx context.Context, candidate Candidate) error {
	ctx, cancel := context.WithTimeout(ctx, g.timeout)
	defer cancel()
	scope := candidate.LeaseNamespace + "/" + candidate.LeaseName
	conn, err := g.connect(ctx)
	if err != nil {
		return &BlockedError{Boundary: BoundaryPostgres, Err: err}
	}
	defer func() {
		closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), ownershipCloseTimeout)
		defer closeCancel()
		_ = conn.Close(closeCtx)
	}()
	current, err := readOwnership(ctx, conn, scope)
	if err != nil {
		return &BlockedError{Boundary: BoundaryPostgres, Err: err}
	}
	activation, err := advanceOwnership(ctx, conn, scope, candidate.Identity, current.Generation)
	if err != nil {
		return &BlockedError{Boundary: BoundaryPostgres, Err: err}
	}
	horizon, err := fenceHorizon(ctx, conn)
	if err != nil {
		return &BlockedError{Boundary: BoundaryPostgres, Err: err}
	}
	if err = g.sweep(ctx, conn, activation, horizon); err != nil {
		return &BlockedError{Boundary: BoundaryPostgres, Err: err}
	}
	// Re-read before publishing the activation: a promoter that was frozen
	// during its sweep may already have been retired, and must not announce
	// ownership of a generation that no longer holds it.
	confirmed, err := readOwnership(ctx, conn, scope)
	if err != nil {
		return &BlockedError{Boundary: BoundaryPostgres, Err: err}
	}
	if confirmed.Generation != activation.Generation {
		return &BlockedError{Boundary: BoundaryPostgres, Err: fmt.Errorf("%w: held generation %d, current generation %d", ErrGenerationRetired, activation.Generation, confirmed.Generation)}
	}
	g.activation.Store(&activation)
	logrus.WithFields(logrus.Fields{
		"generation": activation.Generation,
		"scope":      activation.Scope,
		"boundary":   BoundaryPostgres,
	}).Info("HA ownership established")
	return nil
}

const (
	ownershipCloseTimeout = 5 * time.Second
	sweepInterval         = 100 * time.Millisecond
)

// connect uses a dedicated connection that is deliberately not marked as an
// application backend, so the sweep below never terminates the promoter.
func (g *PostgresGate) connect(ctx context.Context) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(g.dataSource)
	if err != nil {
		return nil, fmt.Errorf("parse HA database target: %w", err)
	}
	cfg.RuntimeParams["application_name"] = ownershipConnName
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("connect to the HA database: %w", err)
	}
	return conn, nil
}

// sweep terminates retired backends until a pass observes none remaining.
// Repeating closes the window in which a backend was mid-authentication when
// an earlier pass enumerated pg_stat_activity.
func (g *PostgresGate) sweep(ctx context.Context, conn *pgx.Conn, activation Activation, horizon time.Time) error {
	for {
		if _, err := fenceBackends(ctx, conn, activation, horizon); err != nil {
			return err
		}
		remaining, err := retiredBackends(ctx, conn, activation, horizon)
		if err != nil {
			return err
		}
		if remaining == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("%d backend(s) of a retired activation are still connected: %w", remaining, ctx.Err())
		case <-time.After(sweepInterval):
		}
	}
}

// Boundaries reports that this gate enforces ownership at the database only.
// Promotion stays blocked until the remaining boundaries are implemented.
func (g *PostgresGate) Boundaries() []Boundary { return []Boundary{BoundaryPostgres} }

// ApplicationName marks this activation's database connections so a successor
// can recognize and terminate them. It is empty until promotion succeeds.
func (g *PostgresGate) ApplicationName() string {
	activation := g.activation.Load()
	if activation == nil {
		return ""
	}
	return applicationName(*activation)
}

// GuardConnection is the hook run on every new pooled connection, including
// those re-established after a network interruption or a terminated backend.
//
// It closes a refused connection itself, because pgx returns an AfterConnect
// failure without closing the connection it just opened. A retired owner under
// load would otherwise leak a backend per refused reconnection and exhaust
// max_connections exactly when its successor needs a pool.
func (g *PostgresGate) GuardConnection(ctx context.Context, conn *pgx.Conn) error {
	err := g.VerifyConnection(ctx, conn)
	if err != nil {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ownershipCloseTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}
	return err
}

// VerifyConnection refuses a connection that belongs to a retired activation.
// It leaves the connection open; callers that own it decide what to do.
func (g *PostgresGate) VerifyConnection(ctx context.Context, conn *pgx.Conn) error {
	activation := g.activation.Load()
	if activation == nil {
		return fmt.Errorf("HA ownership has not been established; refusing database access")
	}
	return guardConnection(*activation)(ctx, conn)
}
