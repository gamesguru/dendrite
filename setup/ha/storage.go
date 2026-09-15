package ha

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// SetCompatibility pins candidates to the exact executable, including local builds.
func SetCompatibility(cfg *config.Zendrite) error {
	if !cfg.Global.HA.Enabled {
		return nil
	}
	path, err := os.Executable()
	if err != nil {
		return fmt.Errorf("identify HA executable: %w", err)
	}
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open HA executable: %w", err)
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return fmt.Errorf("hash HA executable: %w", err)
	}
	cfg.Global.HA.Compatibility, err = cfg.HACompatibility(hex.EncodeToString(h.Sum(nil)))
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
	if _, err = conn.Exec(ctx, "CREATE TABLE IF NOT EXISTS zendrite_ha_compatibility (id INTEGER PRIMARY KEY CHECK (id = 1), fingerprint TEXT NOT NULL)"); err != nil {
		return fail("cannot initialize compatibility record")
	}
	if _, err = conn.Exec(ctx, "INSERT INTO zendrite_ha_compatibility (id, fingerprint) VALUES (1, $1) ON CONFLICT (id) DO NOTHING", cfg.Global.HA.Compatibility); err != nil {
		return fail("cannot persist compatibility record")
	}
	var fingerprint string
	if err = conn.QueryRow(ctx, "SELECT fingerprint FROM zendrite_ha_compatibility WHERE id = 1").Scan(&fingerprint); err != nil {
		return fail("cannot read compatibility record")
	}
	if fingerprint != cfg.Global.HA.Compatibility {
		return fail("configuration or executable differs; an offline coordinated upgrade is required")
	}
	return closeConn, nil
}

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
