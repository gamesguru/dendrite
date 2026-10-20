package ha

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// SetCompatibility records the deployment identity and this build's version.
// The executable itself is no longer pinned: the build's upgrade contract
// decides which other builds may take over from it.
func SetCompatibility(cfg *config.Zendrite, version string) error {
	if !cfg.Global.HA.Enabled {
		return nil
	}
	if err := Release().Validate(); err != nil {
		return fmt.Errorf("invalid HA upgrade contract: %w", err)
	}
	var err error
	cfg.Global.HA.Compatibility, err = cfg.HACompatibility()
	cfg.Global.HA.Version = version
	return err
}

// PrepareStorage runs only after fenced ownership. Its dedicated connection holds
// a migration lock until startup completes. This lock is not an ownership fence.
// The compatibility record is committed before any component migration can run.
//
// The connection carries the activation marker and ownership guard, because it
// can commit schema changes: a successor's sweep must be able to terminate it
// like any other application backend.
func PrepareStorage(ctx context.Context, cfg *config.Zendrite, gate *PostgresGate) (func(), error) {
	if !cfg.Global.HA.Enabled {
		return func() {}, nil
	}
	if cfg.Global.HA.Compatibility == "" {
		return nil, fmt.Errorf("missing HA compatibility identity")
	}
	if gate == nil || gate.Activation() == nil {
		return nil, fmt.Errorf("HA storage: ownership has not been established")
	}
	conn, err := connectAs(ctx, string(cfg.Global.DatabaseOptions.ConnectionString), gate)
	if err != nil {
		// A retired generation is the one cause an operator must see, and it
		// used to be replaced by a generic connection failure. pgx redacts
		// passwords in its own connection errors, so wrapping is safe.
		return nil, fmt.Errorf("HA PostgreSQL connection failed: %w", err)
	}
	closeConn := func() {
		const closeTimeout = 5 * time.Second
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}
	fail := func(message string) (func(), error) { closeConn(); return nil, fmt.Errorf("HA storage: %s", message) }
	var synchronousCommit, fsync string
	if err = conn.QueryRow(ctx, "SHOW synchronous_commit").Scan(&synchronousCommit); err != nil {
		return fail("cannot verify synchronous commit")
	}
	if err = conn.QueryRow(ctx, "SHOW fsync").Scan(&fsync); err != nil {
		return fail("cannot verify durable database writes")
	}
	if (synchronousCommit != "on" && synchronousCommit != "remote_apply") || fsync != "on" {
		return fail("PostgreSQL requires synchronous_commit on or remote_apply and fsync on")
	}
	if _, err = conn.Exec(ctx, "SELECT pg_advisory_lock(350, 1)"); err != nil {
		return fail("cannot acquire migration lock")
	}
	if _, err = conn.Exec(ctx, compatibilitySchema); err != nil {
		return fail("cannot initialize compatibility record")
	}
	recorded, err := readCompatibility(ctx, conn)
	if err != nil {
		return fail("cannot read compatibility record")
	}
	// The candidate checked this before taking the Lease, so a refusal here
	// means another build activated in between. Refusing still happens before
	// any migration, which is the boundary that matters for rollback.
	contract := Release()
	if err = contract.Admit(cfg.Global.HA.Compatibility, recorded); err != nil {
		closeConn()
		return nil, fmt.Errorf("HA storage: %w", err)
	}
	next := contract.Next(cfg.Global.HA.Compatibility, cfg.Global.HA.Version, recorded)
	if _, err = conn.Exec(ctx, `INSERT INTO zendrite_ha_compatibility (id, fingerprint, protocol, level, floor, version, updated_at)
		VALUES (1, $1, $2, $3, $4, $5, now())
		ON CONFLICT (id) DO UPDATE SET fingerprint = $1, protocol = $2, level = $3, floor = $4, version = $5, updated_at = now()`,
		next.Fingerprint, next.Protocol, next.Level, next.Floor, next.Version); err != nil {
		return fail("cannot persist compatibility record")
	}
	fields := logrus.Fields{"deployment_level": next.Level, "deployment_floor": next.Floor, "version": next.Version}
	switch {
	case recorded.Empty():
		logrus.WithFields(fields).Info("HA deployment compatibility recorded")
	case recorded.Level != next.Level:
		fields["previous_level"], fields["previous_version"] = recorded.Level, recorded.Version
		logrus.WithFields(fields).Info("HA deployment level changed")
	}
	if !recorded.Empty() && next.Floor > recorded.Floor {
		logrus.WithFields(fields).Warn("HA deployment floor raised; builds below it can no longer take over, and rolling back to one needs the offline procedure")
	}
	return closeConn, nil
}

// compatibilitySchema creates the compatibility record, and upgrades one
// written by a build without an upgrade contract. The added columns are
// nullable, so such a record reads as having no level and is refused until
// the offline upgrade procedure resets it.
const compatibilitySchema = `
CREATE TABLE IF NOT EXISTS zendrite_ha_compatibility (id INTEGER PRIMARY KEY CHECK (id = 1), fingerprint TEXT NOT NULL);
ALTER TABLE zendrite_ha_compatibility
	ADD COLUMN IF NOT EXISTS protocol INTEGER,
	ADD COLUMN IF NOT EXISTS level INTEGER,
	ADD COLUMN IF NOT EXISTS floor INTEGER,
	ADD COLUMN IF NOT EXISTS version TEXT,
	ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;`

// readCompatibility reads the deployment's compatibility record without
// assuming its columns, so that a standby can read it before any activation
// of its own build has created or upgraded the table.
func readCompatibility(ctx context.Context, conn querier) (Recorded, error) {
	var raw []byte
	err := conn.QueryRow(ctx, "SELECT to_jsonb(c) FROM zendrite_ha_compatibility c WHERE id = 1").Scan(&raw)
	var pgErr *pgconn.PgError
	switch {
	case errors.Is(err, pgx.ErrNoRows), errors.As(err, &pgErr) && pgErr.Code == undefinedTable:
		return Recorded{}, nil
	case err != nil:
		return Recorded{}, fmt.Errorf("read HA compatibility record: %w", err)
	}
	var row struct {
		Fingerprint string  `json:"fingerprint"`
		Protocol    *int    `json:"protocol"`
		Level       *int    `json:"level"`
		Floor       *int    `json:"floor"`
		Version     *string `json:"version"`
	}
	if err = json.Unmarshal(raw, &row); err != nil {
		return Recorded{}, fmt.Errorf("decode HA compatibility record: %w", err)
	}
	recorded := Recorded{Fingerprint: row.Fingerprint}
	if row.Protocol != nil {
		recorded.Protocol = *row.Protocol
	}
	if row.Level != nil {
		recorded.Level = *row.Level
	}
	if row.Floor != nil {
		recorded.Floor = *row.Floor
	}
	if row.Version != nil {
		recorded.Version = *row.Version
	}
	return recorded, nil
}

// undefinedTable is PostgreSQL's SQLSTATE for a missing relation.
const undefinedTable = "42P01"

// connectAs opens a connection bound to the authorized activation, marked so a
// successor can terminate it and guarded so it cannot be re-established once
// the generation is retired.
func connectAs(ctx context.Context, dataSource string, gate *PostgresGate) (*pgx.Conn, error) {
	cfg, err := pgx.ParseConfig(dataSource)
	if err != nil {
		return nil, err
	}
	cfg.RuntimeParams["application_name"] = gate.ApplicationName()
	conn, err := pgx.ConnectConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err = gate.VerifyConnection(ctx, conn); err != nil {
		_ = conn.Close(ctx)
		return nil, err
	}
	return conn, nil
}
