//go:build linux

package hachaos

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5"
)

// pgCluster is a PostgreSQL primary with one synchronous standby behind a
// movable address, which is how an operator such as CloudNativePG or Patroni
// presents a read-write service.
type pgCluster struct {
	bin        string
	dir        string
	logs       string
	credential *syscall.Credential
	timeline   *timeline

	mu       sync.Mutex
	primary  *pgNode
	standby  *pgNode
	serial   int
	vip      *proxy
	failures int
}

type pgNode struct {
	name string
	dir  string
	port int
	proc *process
}

func (n *pgNode) addr() string { return fmt.Sprintf("127.0.0.1:%d", n.port) }

const (
	pgAppUser     = "zendrite"
	pgAppPassword = "zendrite"
	pgDatabase    = "zendrite"
	harnessApp    = "hachaos"
)

func findPostgres() (string, error) {
	if dir := os.Getenv("HACHAOS_PG_BIN"); dir != "" {
		return dir, nil
	}
	if path, err := exec.LookPath("pg_ctl"); err == nil {
		return filepath.Dir(path), nil
	}
	matches, _ := filepath.Glob("/usr/lib/postgresql/*/bin/pg_ctl")
	if len(matches) == 0 {
		return "", errors.New("PostgreSQL server binaries not found; set HACHAOS_PG_BIN")
	}
	sort.Strings(matches)
	return filepath.Dir(matches[len(matches)-1]), nil
}

// postgresCredential runs PostgreSQL as the postgres user when the harness is
// root, because the server refuses to run as root.
func postgresCredential() (*syscall.Credential, error) {
	if os.Geteuid() != 0 {
		return nil, nil
	}
	u, err := user.Lookup("postgres")
	if err != nil {
		return nil, fmt.Errorf("running as root requires a postgres user: %w", err)
	}
	uid, _ := strconv.ParseUint(u.Uid, 10, 32)
	gid, _ := strconv.ParseUint(u.Gid, 10, 32)
	return &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}, nil
}

func startPostgres(ctx context.Context, dir, logs string, o *owners, tl *timeline) (*pgCluster, error) {
	bin, err := findPostgres()
	if err != nil {
		return nil, err
	}
	credential, err := postgresCredential()
	if err != nil {
		return nil, err
	}
	c := &pgCluster{bin: bin, dir: dir, logs: logs, credential: credential, timeline: tl}
	c.vip, err = newProxy("postgres", c.primaryAddr, o, tl)
	if err != nil {
		return nil, err
	}
	primary, err := c.initPrimary(ctx)
	if err != nil {
		return nil, err
	}
	c.primary = primary
	if err = c.bootstrap(ctx); err != nil {
		return nil, err
	}
	if c.standby, err = c.newStandby(ctx, primary); err != nil {
		return nil, err
	}
	if err = c.setSynchronous(ctx, true); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *pgCluster) primaryAddr() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.primary.addr()
}

// dsn is the address every candidate uses. It never changes across failover.
func (c *pgCluster) dsn() string {
	return fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", pgAppUser, pgAppPassword, c.vip.addr(), pgDatabase)
}

func (c *pgCluster) run(name string, args ...string) error {
	cmd := exec.Command(filepath.Join(c.bin, name), args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: c.credential}
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s: %w: %s", name, err, out)
	}
	return nil
}

func (c *pgCluster) makeDir(name string) (string, error) {
	c.serial++
	dir := filepath.Join(c.dir, fmt.Sprintf("%s-%d", name, c.serial))
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	if c.credential != nil {
		if err := os.Chown(dir, int(c.credential.Uid), int(c.credential.Gid)); err != nil {
			return "", err
		}
	}
	return dir, nil
}

const pgConfig = `
listen_addresses = '127.0.0.1'
port = %d
unix_socket_directories = '%s'
max_connections = 500
wal_level = replica
max_wal_senders = 10
max_replication_slots = 10
hot_standby = on
wal_log_hints = on
track_commit_timestamp = on
synchronous_commit = on
fsync = on
shared_buffers = 64MB
log_line_prefix = '%%m [%%p] %%q%%a '
`

const pgHBA = `
local all all trust
host all all 127.0.0.1/32 trust
host replication all 127.0.0.1/32 trust
`

func (c *pgCluster) initPrimary(ctx context.Context) (*pgNode, error) {
	dir, err := c.makeDir("pg")
	if err != nil {
		return nil, err
	}
	if err = c.run("initdb", "-D", dir, "-U", "postgres", "--auth=trust", "-E", "UTF8", "--locale=C"); err != nil {
		return nil, err
	}
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	if err = c.configure(dir, port); err != nil {
		return nil, err
	}
	return c.startNode(ctx, fmt.Sprintf("postgres-%d", c.serial), dir, port)
}

func (c *pgCluster) configure(dir string, port int) error {
	f, err := os.OpenFile(filepath.Join(dir, "postgresql.conf"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err = fmt.Fprintf(f, pgConfig, port, dir); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "pg_hba.conf"), []byte(pgHBA), 0o600)
}

func (c *pgCluster) startNode(ctx context.Context, name, dir string, port int) (*pgNode, error) {
	proc, err := startProcess(c.logs, processSpec{name: name, bin: filepath.Join(c.bin, "postgres"), args: []string{"-D", dir}, credential: c.credential})
	if err != nil {
		return nil, err
	}
	node := &pgNode{name: name, dir: dir, port: port, proc: proc}
	if err = waitFor(ctx, time.Minute, func() error {
		if proc.exited() {
			return errPermanent(fmt.Errorf("%s exited during startup", name))
		}
		conn, err := superuser(ctx, node.addr())
		if err != nil {
			return err
		}
		return conn.Close(ctx)
	}); err != nil {
		return nil, err
	}
	c.timeline.add(eventProc, name, "started on %d", port)
	return node, nil
}

func superuser(ctx context.Context, addr string) (*pgx.Conn, error) {
	return superuserTo(ctx, addr, "postgres")
}

func superuserTo(ctx context.Context, addr, database string) (*pgx.Conn, error) {
	host, port, _ := strings.Cut(addr, ":")
	cfg, err := pgx.ParseConfig(fmt.Sprintf("host=%s port=%s user=postgres dbname=%s sslmode=disable connect_timeout=5 application_name=%s", host, port, database, harnessApp))
	if err != nil {
		return nil, err
	}
	return pgx.ConnectConfig(ctx, cfg)
}

func (c *pgCluster) bootstrap(ctx context.Context) error {
	conn, err := superuser(ctx, c.primary.addr())
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	for _, stmt := range []string{
		fmt.Sprintf("CREATE ROLE %s LOGIN PASSWORD '%s'", pgAppUser, pgAppPassword),
		fmt.Sprintf("CREATE DATABASE %s OWNER %s", pgDatabase, pgAppUser),
	} {
		if _, err = conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("%s: %w", stmt, err)
		}
	}
	return nil
}

// newStandby clones the primary and starts streaming from it.
func (c *pgCluster) newStandby(ctx context.Context, primary *pgNode) (*pgNode, error) {
	dir, err := c.makeDir("pg")
	if err != nil {
		return nil, err
	}
	name := fmt.Sprintf("postgres-%d", c.serial)
	conninfo := fmt.Sprintf("host=127.0.0.1 port=%d user=postgres application_name=%s", primary.port, name)
	if err = c.run("pg_basebackup", "-d", conninfo, "-D", dir, "-X", "stream", "-R", "--checkpoint=fast"); err != nil {
		return nil, err
	}
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	if err = c.configure(dir, port); err != nil {
		return nil, err
	}
	return c.startNode(ctx, name, dir, port)
}

// setSynchronous makes commits on the primary wait for the standby, which is
// what the HA storage contract requires for acknowledged writes.
func (c *pgCluster) setSynchronous(ctx context.Context, on bool) error {
	value := ""
	if on {
		value = "*"
	}
	conn, err := superuser(ctx, c.primaryAddr())
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	if _, err = conn.Exec(ctx, fmt.Sprintf("ALTER SYSTEM SET synchronous_standby_names = '%s'", value)); err != nil {
		return err
	}
	_, err = conn.Exec(ctx, "SELECT pg_reload_conf()")
	return err
}

// failover promotes the standby and moves the service address to it, the way
// an operator responds to a lost primary. The old primary is left to the
// caller, which has usually killed or partitioned it.
//
// The promoted server has no standby of its own yet. Keeping synchronous
// commit mandatory would block every write, so, like Patroni without strict
// synchronous mode, it is relaxed until rebuildStandby restores a replica.
func (c *pgCluster) failover(ctx context.Context) error {
	c.mu.Lock()
	standby := c.standby
	c.mu.Unlock()
	if standby == nil {
		return errors.New("no standby to promote")
	}
	conn, err := superuser(ctx, standby.addr())
	if err != nil {
		return err
	}
	defer conn.Close(ctx)
	if _, err = conn.Exec(ctx, "SELECT pg_promote(true, 60)"); err != nil {
		return fmt.Errorf("promote standby: %w", err)
	}
	if _, err = conn.Exec(ctx, "ALTER SYSTEM SET synchronous_standby_names = ''"); err != nil {
		return err
	}
	if _, err = conn.Exec(ctx, "SELECT pg_reload_conf()"); err != nil {
		return err
	}
	c.mu.Lock()
	old := c.primary
	c.primary, c.standby = standby, nil
	c.failures++
	c.mu.Unlock()
	c.vip.resetAll()
	c.timeline.add(eventFault, "postgres", "promoted %s; service address moved from %s", standby.name, old.name)
	c.retire(old)
	return nil
}

// retire stops a former primary for good. Its data directory is kept for
// inspection but it is never started again, since it may hold transactions
// that the promoted server does not.
func (c *pgCluster) retire(node *pgNode) {
	node.proc.kill()
}

// rebuildStandby restores synchronous replication after a failover.
func (c *pgCluster) rebuildStandby(ctx context.Context) error {
	c.mu.Lock()
	primary := c.primary
	c.mu.Unlock()
	standby, err := c.newStandby(ctx, primary)
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.standby = standby
	c.mu.Unlock()
	return c.setSynchronous(ctx, true)
}

func (c *pgCluster) primaryNode() *pgNode {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.primary
}

func (c *pgCluster) stop() {
	c.vip.close()
	c.mu.Lock()
	nodes := []*pgNode{c.primary, c.standby}
	c.mu.Unlock()
	for _, n := range nodes {
		if n != nil {
			n.proc.stop(20 * time.Second)
		}
	}
}

// query runs fn on a superuser connection to the application database on the
// current primary.
func (c *pgCluster) query(ctx context.Context, fn func(conn *pgx.Conn) error) error {
	conn, err := superuserTo(ctx, c.primaryAddr(), pgDatabase)
	if err != nil {
		return err
	}
	defer conn.Close(context.WithoutCancel(ctx))
	return fn(conn)
}

// Audit records which activation committed every write to an application
// table. A statement trigger stores the writer's application_name once per
// transaction and table, and track_commit_timestamp supplies the time the
// transaction became visible. Together they show whether two generations'
// commits ever interleaved, which is the database half of split brain.
const auditSchema = `
CREATE TABLE IF NOT EXISTS hachaos_audit (
	xid xid8 NOT NULL,
	tbl TEXT NOT NULL,
	app TEXT NOT NULL,
	pid INTEGER NOT NULL,
	PRIMARY KEY (xid, tbl)
);
CREATE OR REPLACE FUNCTION hachaos_audit() RETURNS trigger LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
	INSERT INTO hachaos_audit (xid, tbl, app, pid)
	VALUES (pg_current_xact_id(), TG_TABLE_NAME, current_setting('application_name'), pg_backend_pid())
	ON CONFLICT DO NOTHING;
	RETURN NULL;
END $$;
`

// installAudit attaches the audit trigger to every application table that
// does not have it yet. It is idempotent, so it can follow migrations.
func (c *pgCluster) installAudit(ctx context.Context) (int, error) {
	installed := 0
	err := c.query(ctx, func(conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, auditSchema); err != nil {
			return fmt.Errorf("install audit schema: %w", err)
		}
		rows, err := conn.Query(ctx, `SELECT t.tablename FROM pg_tables t
			WHERE t.schemaname = 'public'
			AND t.tablename NOT LIKE 'zendrite_ha_%'
			AND t.tablename NOT LIKE 'hachaos_%'
			AND NOT EXISTS (SELECT 1 FROM pg_trigger g JOIN pg_class r ON g.tgrelid = r.oid
				WHERE r.relname = t.tablename AND g.tgname = 'hachaos_audit')`)
		if err != nil {
			return err
		}
		tables, err := pgx.CollectRows(rows, pgx.RowTo[string])
		if err != nil {
			return err
		}
		for _, table := range tables {
			if _, err = conn.Exec(ctx, fmt.Sprintf(`CREATE TRIGGER hachaos_audit AFTER INSERT OR UPDATE OR DELETE ON %s
				FOR EACH STATEMENT EXECUTE FUNCTION hachaos_audit()`, pgx.Identifier{table}.Sanitize())); err != nil {
				return fmt.Errorf("audit %s: %w", table, err)
			}
			installed++
		}
		return nil
	})
	return installed, err
}

// auditedCommit is one transaction's write to one application table.
type auditedCommit struct {
	Table      string
	App        string
	Generation int64
	Committed  time.Time
}

var appGeneration = regexp.MustCompile(`^zendrite:[0-9a-f]+:([0-9]+):`)

func (c *pgCluster) auditedCommits(ctx context.Context) ([]auditedCommit, error) {
	var out []auditedCommit
	err := c.query(ctx, func(conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, "SELECT tbl, app, pg_xact_commit_timestamp(xmin) FROM hachaos_audit")
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var a auditedCommit
			var committed *time.Time
			if err = rows.Scan(&a.Table, &a.App, &committed); err != nil {
				return err
			}
			if committed != nil {
				a.Committed = *committed
			}
			if m := appGeneration.FindStringSubmatch(a.App); m != nil {
				a.Generation, _ = strconv.ParseInt(m[1], 10, 64)
			}
			out = append(out, a)
		}
		return rows.Err()
	})
	return out, err
}

// whereIs reports which stores hold an event, which separates a write that
// was lost from one that was stored but is not served by a read path.
func (c *pgCluster) whereIs(ctx context.Context, eventID string) string {
	var found []string
	err := c.query(ctx, func(conn *pgx.Conn) error {
		for _, table := range []string{"roomserver_events", "syncapi_output_room_events", "syncapi_output_room_events_topology"} {
			var n int
			if err := conn.QueryRow(ctx, "SELECT count(*) FROM "+table+" WHERE event_id = $1", eventID).Scan(&n); err != nil {
				return err
			}
			if n > 0 {
				found = append(found, table)
			}
		}
		return nil
	})
	switch {
	case err != nil:
		return "unknown: " + err.Error()
	case len(found) == 0:
		return "in no table"
	default:
		return "present in " + strings.Join(found, ", ")
	}
}

// diagnoseEvent describes how the sync API stores an event and its topological
// neighbors, for an event that a read path failed to return.
func (c *pgCluster) diagnoseEvent(ctx context.Context, eventID string) string {
	var b strings.Builder
	err := c.query(ctx, func(conn *pgx.Conn) error {
		fmt.Fprintf(&b, "event %s\n\nsyncapi_output_room_events:\n", eventID)
		var id int64
		var room, kind, sender string
		var exclude bool
		var visibility int16
		if err := conn.QueryRow(ctx, "SELECT id, room_id, type, sender, exclude_from_sync, history_visibility FROM syncapi_output_room_events WHERE event_id = $1", eventID).
			Scan(&id, &room, &kind, &sender, &exclude, &visibility); err != nil {
			return err
		}
		fmt.Fprintf(&b, "  id=%d room=%s type=%s sender=%s exclude_from_sync=%t history_visibility=%d\n\n", id, room, kind, sender, exclude, visibility)
		fmt.Fprintf(&b, "syncapi_output_room_events_topology around it (topological_position, stream_position, event_id, exclude_from_sync, history_visibility):\n")
		rows, err := conn.Query(ctx, `WITH ordered AS (
				SELECT t.event_id, t.topological_position, t.stream_position, e.exclude_from_sync, e.history_visibility,
					row_number() OVER (ORDER BY t.topological_position, t.stream_position) AS n
				FROM syncapi_output_room_events_topology t LEFT JOIN syncapi_output_room_events e ON e.event_id = t.event_id
				WHERE t.room_id = $1)
			SELECT o.topological_position, o.stream_position, o.event_id, o.exclude_from_sync, o.history_visibility FROM ordered o, ordered target
			WHERE target.event_id = $2 AND o.n BETWEEN target.n - 5 AND target.n + 5 ORDER BY o.n`, room, eventID)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var topo, stream int64
			var id string
			var excluded *bool
			var vis *int16
			if err = rows.Scan(&topo, &stream, &id, &excluded, &vis); err != nil {
				return err
			}
			marker := " "
			if id == eventID {
				marker = ">"
			}
			fmt.Fprintf(&b, "%s %d %d %s %v %v\n", marker, topo, stream, id, deref(excluded), deref(vis))
		}
		return rows.Err()
	})
	if err != nil {
		fmt.Fprintf(&b, "diagnosis failed: %v\n", err)
	}
	return b.String()
}

func deref[T any](v *T) any {
	if v == nil {
		return nil
	}
	return *v
}

// ownershipRecord is one row of zendrite_ha_ownership_history.
type ownershipRecord struct {
	Generation int64     `json:"generation"`
	Owner      string    `json:"owner"`
	AcquiredAt time.Time `json:"acquired_at"`
}

func (c *pgCluster) ownershipHistory(ctx context.Context) ([]ownershipRecord, error) {
	var out []ownershipRecord
	err := c.query(ctx, func(conn *pgx.Conn) error {
		rows, err := conn.Query(ctx, "SELECT generation, owner, acquired_at FROM zendrite_ha_ownership_history ORDER BY generation")
		if err != nil {
			return err
		}
		out, err = pgx.CollectRows(rows, func(row pgx.CollectableRow) (ownershipRecord, error) {
			var r ownershipRecord
			err := row.Scan(&r.Generation, &r.Owner, &r.AcquiredAt)
			return r, err
		})
		return err
	})
	return out, err
}
