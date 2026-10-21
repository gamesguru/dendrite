package sqlutil

import (
	"context"
	"database/sql"
	"flag"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// ConnectionOption customizes how PostgreSQL connections are established.
// Standalone deployments use none of them and keep their existing behavior.
type ConnectionOption func(*connectionOptions)

type connectionOptions struct {
	applicationName string
	afterConnect    func(context.Context, *pgx.Conn) error
}

// WithConnectionGuard marks every connection with the supplied application
// name and validates it as it is established, including after a reconnection.
// HA uses this to let PostgreSQL itself reject a retired activation; the guard
// runs before any statement, so no pooled connection escapes it.
func WithConnectionGuard(applicationName string, afterConnect func(context.Context, *pgx.Conn) error) ConnectionOption {
	return func(o *connectionOptions) {
		o.applicationName = applicationName
		o.afterConnect = afterConnect
	}
}

var skipSanityChecks = flag.Bool("skip-db-sanity", false, "Ignore sanity checks on the database connections (NOT RECOMMENDED!)")

// Open opens a database specified by its database driver name and a driver-specific data source name,
// usually consisting of at least a database name and connection information.
func Open(dbProperties *config.DatabaseOptions, writer Writer, options ...ConnectionOption) (*sql.DB, error) {
	var opts connectionOptions
	for _, option := range options {
		option(&opts)
	}
	var err error
	var driverName, dsn string
	switch {
	case dbProperties.ConnectionString.IsSQLite():
		driverName = SQLITE_DRIVER_NAME
		dsn, err = ParseFileURI(dbProperties.ConnectionString)
		if err != nil {
			return nil, fmt.Errorf("ParseFileURI: %w", err)
		}
		dsn = sqliteDSNExtension(dsn, dbProperties.SQLiteWAL)
	case dbProperties.ConnectionString.IsPostgres():
		driverName = "pgx"
		dsn = string(dbProperties.ConnectionString)
	default:
		return nil, fmt.Errorf("invalid database connection string %q", dbProperties.ConnectionString)
	}
	db, err := openDriver(driverName, dsn, opts)
	if err != nil {
		return nil, err
	}
	if driverName != SQLITE_DRIVER_NAME {
		logger := logrus.WithFields(logrus.Fields{
			"max_open_conns":    dbProperties.MaxOpenConns(),
			"max_idle_conns":    dbProperties.MaxIdleConns(),
			"conn_max_lifetime": dbProperties.ConnMaxLifetime(),
		})
		logger.Debug("Setting DB connection limits")
		db.SetMaxOpenConns(dbProperties.MaxOpenConns())
		db.SetMaxIdleConns(dbProperties.MaxIdleConns())
		db.SetConnMaxLifetime(dbProperties.ConnMaxLifetime())

		if !*skipSanityChecks {
			if dbProperties.MaxOpenConns() == 0 {
				logrus.Warnf("WARNING: Configuring 'max_open_conns' to be unlimited is not recommended. This can result in bad performance or deadlocks.")
			}

			if driverName == "pgx" {
				// Perform a quick sanity check if possible that we aren't trying to use more database
				// connections than PostgreSQL is willing to give us.
				var max, reserved int
				if err := db.QueryRow("SELECT setting::integer FROM pg_settings WHERE name='max_connections';").Scan(&max); err != nil {
					return nil, fmt.Errorf("failed to find maximum connections: %w", err)
				}
				if err := db.QueryRow("SELECT setting::integer FROM pg_settings WHERE name='superuser_reserved_connections';").Scan(&reserved); err != nil {
					return nil, fmt.Errorf("failed to find reserved connections: %w", err)
				}
				if configured, allowed := dbProperties.MaxOpenConns(), max-reserved; configured > allowed {
					logrus.Errorf("ERROR: The configured 'max_open_conns' is greater than the %d non-superuser connections that PostgreSQL is configured to allow. This can result in bad performance or deadlocks. Please pay close attention to your configured database connection counts. If you REALLY know what you are doing and want to override this error, pass the --skip-db-sanity option to Zendrite.", allowed)
					return nil, fmt.Errorf("database sanity checks failed")
				}
			}
		}
	}
	return db, nil
}

// openDriver opens the pooled handle, applying the ownership guard when one is
// configured; pgx only exposes a per-connection hook through stdlib.OpenDB, so
// guarded PostgreSQL handles take that path instead of sql.Open.
func openDriver(driverName, dsn string, opts connectionOptions) (*sql.DB, error) {
	if driverName == SQLITE_DRIVER_NAME || (opts.applicationName == "" && opts.afterConnect == nil) {
		return sql.Open(driverName, dsn)
	}
	connConfig, err := pgx.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse PostgreSQL connection string: %w", err)
	}
	if opts.applicationName != "" {
		connConfig.RuntimeParams["application_name"] = opts.applicationName
	}
	afterConnect := opts.afterConnect
	if afterConnect == nil {
		afterConnect = func(context.Context, *pgx.Conn) error { return nil }
	}
	// Probe every HA checkout before application SQL runs. The driver's default
	// skips probes for recently used connections, allowing a former primary's
	// termination error to escape to a client during rapid reuse. Ping failures
	// become ErrBadConn and database/sql can safely reconnect before the query.
	return stdlib.OpenDB(*connConfig, stdlib.OptionAfterConnect(afterConnect),
		stdlib.OptionShouldPing(func(context.Context, stdlib.ShouldPingParams) bool { return true })), nil
}
