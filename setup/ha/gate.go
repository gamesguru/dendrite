package ha

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/setup/config"
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
	cfg        config.HA
	activation atomic.Pointer[Activation]
	retired    atomic.Bool
	// verified is the last time the database confirmed this generation.
	// Ownership is only valid while that confirmation is recent, so a process
	// that was suspended refuses to act until it has checked again.
	verified atomic.Int64
	// lastCheck is why the most recent confirmation attempt failed. Ownership
	// stays valid for a whole freshness window while confirmations are
	// failing, so without this the only symptom of a broken heartbeat is a
	// candidate that goes stale much later for no stated reason.
	lastCheck atomic.Pointer[string]
	// abandoned is closed once ownership has gone unconfirmed for so long
	// that holding on to the Lease only keeps a healthier candidate out.
	abandoned   chan struct{}
	abandonOnce sync.Once
}

// NewPostgresGate returns a gate bound to the homeserver's global database.
// Election timings double as the ownership freshness window and heartbeat
// interval, so ownership cannot outlive a Lease that is no longer being
// renewed.
func NewPostgresGate(dataSource string, cfg config.HA) *PostgresGate {
	return &PostgresGate{dataSource: dataSource, cfg: cfg, abandoned: make(chan struct{})}
}

// ErrOwnershipUnconfirmed reports that ownership could not be confirmed for
// longer than the Lease duration after it went stale. The candidate gives up
// leadership so that one which can reach the database can take over.
var ErrOwnershipUnconfirmed = errors.New("HA ownership could not be confirmed for longer than the Lease duration; giving up leadership")

// Abandoned is closed when ownership has been unconfirmed for longer than the
// freshness window plus the Lease duration.
//
// A stale owner refuses every effect, so it is safe, but it still renews the
// Lease if it can reach the Kubernetes API. Without giving up, a candidate
// partitioned from the database would keep every standby out for as long as
// the partition lasts. The Lease duration is the grace period because it is
// the time the design already allows a successor to wait.
func (g *PostgresGate) Abandoned() <-chan struct{} { return g.abandoned }

// abandonIfUnconfirmed gives up ownership once it has been unconfirmed for
// too long, and reports whether it did.
func (g *PostgresGate) abandonIfUnconfirmed() bool {
	if time.Since(time.Unix(0, g.verified.Load())) <= g.freshness()+g.cfg.LeaseDuration {
		return false
	}
	g.abandonOnce.Do(func() {
		logrus.WithField("generation", g.Generation()).Error("HA ownership has not been confirmed for longer than the Lease duration; giving up leadership")
		close(g.abandoned)
	})
	return true
}

// freshness is how long a confirmed generation may be trusted without another
// check, and interval is how often the heartbeat confirms it.
func (g *PostgresGate) freshness() time.Duration { return g.cfg.RenewDeadline }
func (g *PostgresGate) interval() time.Duration  { return g.cfg.RetryPeriod }

// Activation reports the authorized activation, or nil while promotion is
// blocked. Callers must not open application connections without it.
func (g *PostgresGate) Activation() *Activation { return g.activation.Load() }

// Acquire retires every earlier generation and proves that no backend of one
// remains connected. It is bounded by the ownership-establishment budget and
// leaves the candidate unready rather than activating on timeout.
func (g *PostgresGate) Acquire(ctx context.Context, candidate Candidate) error {
	parent := ctx
	ctx, cancel := context.WithTimeout(ctx, g.cfg.OwnershipTimeout)
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
	// Record the confirmation before publishing the activation, so Valid never
	// observes a fresh activation with no confirmation behind it.
	g.verified.Store(time.Now().UnixNano())
	g.activation.Store(&activation)
	go g.heartbeat(parent)
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
	connConfig, err := pgx.ParseConfig(g.dataSource)
	if err != nil {
		return nil, fmt.Errorf("parse HA database target: %w", err)
	}
	connConfig.RuntimeParams["application_name"] = ownershipConnName
	conn, err := pgx.ConnectConfig(ctx, connConfig)
	if err != nil {
		return nil, fmt.Errorf("connect to the HA database: %w", err)
	}
	return conn, nil
}

// sweep terminates retired backends until a pass observes none remaining.
// Repeating closes the window in which a backend was mid-authentication when
// an earlier pass enumerated pg_stat_activity.
//
// Every failure names the retired activation, so a candidate that stays
// unready reports why rather than a bare timeout.
func (g *PostgresGate) sweep(ctx context.Context, conn querier, activation Activation, horizon time.Time) error {
	remaining := -1
	blockedBy := func(cause error) error {
		if remaining < 0 {
			return fmt.Errorf("cannot prove that no backend of a retired activation remains connected: %w", cause)
		}
		return fmt.Errorf("%d backend(s) of a retired activation are still connected: %w", remaining, cause)
	}
	for {
		if _, err := fenceBackends(ctx, conn, activation, horizon); err != nil {
			return blockedBy(err)
		}
		observed, err := retiredBackends(ctx, conn, activation, horizon)
		if err != nil {
			return blockedBy(err)
		}
		remaining = observed
		if remaining == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return blockedBy(ctx.Err())
		case <-time.After(sweepInterval):
		}
	}
}

// ErrOwnershipStale reports that ownership has not been confirmed recently
// enough to be trusted. It is the barrier for effects the database cannot
// reject on its own, such as broker publication and outbound dispatch.
var ErrOwnershipStale = errors.New("HA ownership has not been confirmed within the freshness window")

// ErrOwnershipRetired reports that this activation may no longer act. Every
// boundary must fail closed once it is observed.
var ErrOwnershipRetired = errors.New("HA ownership is retired; this activation may no longer act")

// Generation reports the authorized ownership generation, or zero before
// promotion. It stamps effects at boundaries the database cannot reach.
func (g *PostgresGate) Generation() int64 {
	activation := g.activation.Load()
	if activation == nil {
		return 0
	}
	return activation.Generation
}

// Identity reports this activation's candidate identity.
func (g *PostgresGate) Identity() string {
	activation := g.activation.Load()
	if activation == nil {
		return ""
	}
	return activation.Identity
}

// Valid reports whether this activation may still act. It is deliberately
// cheap so that every protected effect can consult it, and it fails closed
// before promotion, after retirement, and whenever the last confirmation from
// the database is older than the freshness window.
//
// The freshness window is what protects boundaries the database cannot reach.
// A process suspended past its lease resumes with a stale confirmation, so it
// refuses to publish or dispatch until it has checked again, by which time the
// database refuses it outright.
func (g *PostgresGate) Valid() error {
	if g.activation.Load() == nil {
		return fmt.Errorf("%w: ownership has not been established", ErrOwnershipRetired)
	}
	if g.retired.Load() {
		return ErrOwnershipRetired
	}
	if age := time.Since(time.Unix(0, g.verified.Load())); age > g.freshness() {
		return fmt.Errorf("%w: last confirmed %s ago, freshness window is %s", ErrOwnershipStale, age.Round(time.Millisecond), g.freshness())
	}
	return nil
}

// LastConfirmationError reports why the most recent ownership confirmation
// failed, or an empty string when the last attempt succeeded. Ownership can be
// valid while confirmations are failing, for as long as the freshness window
// allows, so this is the only signal that something is wrong before ownership
// goes stale.
func (g *PostgresGate) LastConfirmationError() string {
	if last := g.lastCheck.Load(); last != nil {
		return *last
	}
	return ""
}

// noteCheck records the outcome of a confirmation attempt for reporting.
func (g *PostgresGate) noteCheck(err error) {
	if err == nil {
		g.lastCheck.Store(nil)
		return
	}
	reason := err.Error()
	g.lastCheck.Store(&reason)
}

// heartbeat re-confirms the generation on its own guarded connection until
// ownership ends. A proven newer generation retires the activation; any other
// failure simply stops refreshing, so ownership goes stale and fails closed.
func (g *PostgresGate) heartbeat(ctx context.Context) {
	ticker := time.NewTicker(g.interval())
	defer ticker.Stop()
	var conn *pgx.Conn
	defer func() {
		if conn != nil {
			closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), ownershipCloseTimeout)
			defer cancel()
			_ = conn.Close(closeCtx)
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if g.retired.Load() {
			return
		}
		if conn == nil || conn.IsClosed() {
			checkCtx, cancel := context.WithTimeout(ctx, g.interval())
			fresh, err := g.connect(checkCtx)
			cancel()
			if err != nil {
				g.noteCheck(err)
				if g.abandonIfUnconfirmed() {
					return
				}
				continue
			}
			conn = fresh
		}
		checkCtx, cancel := context.WithTimeout(ctx, g.interval())
		err := g.VerifyConnection(checkCtx, conn)
		cancel()
		g.noteCheck(err)
		switch {
		case err == nil:
			g.verified.Store(time.Now().UnixNano())
		case errors.Is(err, ErrGenerationRetired):
			logrus.WithField("generation", g.Generation()).Warn("HA ownership was taken over by another candidate")
			return
		default:
			// Bound this close like the deferred one: a hung close would
			// stall the heartbeat, and a stalled heartbeat stalls every
			// consumer that waits on ownership.
			closeCtx, closeCancel := context.WithTimeout(context.WithoutCancel(ctx), ownershipCloseTimeout)
			_ = conn.Close(closeCtx)
			closeCancel()
			conn = nil
			if g.abandonIfUnconfirmed() {
				return
			}
		}
	}
}

// Retired reports whether ownership is permanently gone, as opposed to merely
// unconfirmed. Consumers must outlast staleness but stop on retirement.
func (g *PostgresGate) Retired() bool { return g.retired.Load() }

// Retire stops this activation acting at every boundary. The database observes
// loss first, so a refused connection retires the process without waiting for
// the election loop to notice.
func (g *PostgresGate) Retire() { g.retired.Store(true) }

// Boundaries reports which protected boundaries this gate enforces.
// Promotion stays blocked until the remaining boundaries are implemented.
func (g *PostgresGate) Boundaries() []Boundary {
	return []Boundary{BoundaryPostgres, BoundaryJetStream, BoundaryMedia, BoundaryDispatch}
}

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
	err := guardConnection(*activation)(ctx, conn)
	// Only a newer recorded generation proves ownership is gone. A failed
	// check is an unverified connection, not a demotion, and must not retire
	// an activation that still holds authority.
	if errors.Is(err, ErrGenerationRetired) {
		g.Retire()
	}
	return err
}
