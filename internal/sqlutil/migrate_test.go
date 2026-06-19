package sqlutil_test

import (
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"testing"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/test"
)

var dummyMigrations = []sqlutil.Migration{
	{
		Version: "init",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			_, err := txn.ExecContext(ctx, "CREATE TABLE IF NOT EXISTS dummy ( test TEXT );")
			return err
		},
	},
	{
		Version: "v2",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			_, err := txn.ExecContext(ctx, "ALTER TABLE dummy ADD COLUMN test2 TEXT;")
			return err
		},
	},
	{
		Version: "v2", // duplicate, this migration will be skipped
		Up: func(ctx context.Context, txn *sql.Tx) error {
			_, err := txn.ExecContext(ctx, "ALTER TABLE dummy ADD COLUMN test2 TEXT;")
			return err
		},
	},
	{
		Version: "multiple execs",
		Up: func(ctx context.Context, txn *sql.Tx) error {
			_, err := txn.ExecContext(ctx, "ALTER TABLE dummy ADD COLUMN test3 TEXT;")
			if err != nil {
				return err
			}
			_, err = txn.ExecContext(ctx, "ALTER TABLE dummy ADD COLUMN test4 TEXT;")
			return err
		},
	},
}

var failMigration = sqlutil.Migration{
	Version: "iFail",
	Up: func(ctx context.Context, txn *sql.Tx) error {
		return fmt.Errorf("iFail")
	},
	Down: nil,
}

func Test_migrations_Up(t *testing.T) {
	withFail := make([]sqlutil.Migration, 0, len(dummyMigrations)+1)
	withFail = append(withFail, dummyMigrations...)
	withFail = append(withFail, failMigration)

	tests := []struct {
		name       string
		migrations []sqlutil.Migration
		wantResult map[string]struct{}
		wantErr    bool
	}{
		{
			name:       "dummy migration",
			migrations: dummyMigrations,
			wantResult: map[string]struct{}{
				"init":           {},
				"v2":             {},
				"multiple execs": {},
			},
		},
		{
			name:       "with fail",
			migrations: withFail,
			wantErr:    true,
		},
	}

	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		conStr, close := test.PrepareDBConnectionString(t, dbType)
		defer close()

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				driverName := sqlutil.SQLITE_DRIVER_NAME
				if dbType == test.DBTypePostgres {
					driverName = "pgx"
				}
				db, err := sql.Open(driverName, conStr)
				if err != nil {
					t.Errorf("unable to open database: %v", err)
				}
				m := sqlutil.NewMigrator(db)
				m.AddMigrations(tt.migrations...)
				if err = m.Up(ctx); (err != nil) != tt.wantErr {
					t.Errorf("Up() error = %v, wantErr %v", err, tt.wantErr)
				}
				result, err := m.ExecutedMigrations(ctx)
				if err != nil {
					t.Errorf("unable to get executed migrations: %v", err)
				}
				if !tt.wantErr && !reflect.DeepEqual(result, tt.wantResult) {
					t.Errorf("expected: %+v, got %v", tt.wantResult, result)
				}
			})
		}
	})
}

func Test_insertMigration(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		conStr, close := test.PrepareDBConnectionString(t, dbType)
		defer close()
		driverName := sqlutil.SQLITE_DRIVER_NAME
		if dbType == test.DBTypePostgres {
			driverName = "pgx"
		}

		db, err := sql.Open(driverName, conStr)
		if err != nil {
			t.Errorf("unable to open database: %v", err)
		}

		if err := sqlutil.InsertMigration(context.Background(), db, "testing"); err != nil {
			t.Fatalf("unable to insert migration: %s", err)
		}
		// Second insert should not return an error, as it was already executed.
		if err := sqlutil.InsertMigration(context.Background(), db, "testing"); err != nil {
			t.Fatalf("unable to insert migration: %s", err)
		}
	})
}

func Test_migrations_DendriteUpgradeIntegration(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		conStr, close := test.PrepareDBConnectionString(t, dbType)
		defer close()
		driverName := sqlutil.SQLITE_DRIVER_NAME
		if dbType == test.DBTypePostgres {
			driverName = "pgx"
		}

		db, err := sql.Open(driverName, conStr)
		if err != nil {
			t.Fatalf("unable to open database: %v", err)
		}
		defer db.Close()

		_, err = db.Exec(`
			CREATE TABLE db_migrations (
				version TEXT PRIMARY KEY NOT NULL,
				time TEXT NOT NULL,
				dendrite_version TEXT NOT NULL
			)
		`)
		if err != nil {
			t.Fatalf("unable to create legacy migrations table: %v", err)
		}
		const (
			legacyMigration = "dendrite migration"
			legacyTime      = "2026-01-01T00:00:00Z"
			legacyVersion   = "0.15.3"
			zendriteMig     = "zendrite migration"
		)
		_, err = db.Exec(
			`INSERT INTO db_migrations (version, time, dendrite_version) VALUES ($1, $2, $3)`,
			legacyMigration, legacyTime, legacyVersion,
		)
		if err != nil {
			t.Fatalf("unable to insert legacy migration: %v", err)
		}

		m := sqlutil.NewMigrator(db)
		m.AddMigrations(
			sqlutil.Migration{
				Version: legacyMigration,
				Up: func(context.Context, *sql.Tx) error {
					return fmt.Errorf("already-recorded Dendrite migration was executed")
				},
			},
			sqlutil.Migration{
				Version: zendriteMig,
				Up: func(ctx context.Context, txn *sql.Tx) error {
					_, err := txn.ExecContext(ctx, "CREATE TABLE zendrite_migration_ran (id INTEGER)")
					return err
				},
			},
		)

		for range 2 {
			if err = m.Up(context.Background()); err != nil {
				t.Fatalf("unable to migrate Dendrite database: %v", err)
			}
		}

		rows, err := db.Query("SELECT * FROM db_migrations LIMIT 0")
		if err != nil {
			t.Fatalf("unable to inspect migrations table: %v", err)
		}
		columns, err := rows.Columns()
		if rowsErr := rows.Err(); err == nil {
			err = rowsErr
		}
		if err != nil {
			t.Fatalf("unable to inspect migrations table columns: %v", err)
		}
		if err = rows.Close(); err != nil {
			t.Fatalf("unable to close migrations table inspection: %v", err)
		}

		if !reflect.DeepEqual(columns, []string{"version", "time", "zendrite_version"}) {
			t.Fatalf("unexpected migrations table columns: %v", columns)
		}

		type migrationRecord struct {
			time    string
			version string
		}
		records := make(map[string]migrationRecord)
		rows, err = db.Query("SELECT version, time, zendrite_version FROM db_migrations")
		if err != nil {
			t.Fatalf("unable to query migrated records: %v", err)
		}
		defer rows.Close()
		for rows.Next() {
			var name string
			var record migrationRecord
			if err = rows.Scan(&name, &record.time, &record.version); err != nil {
				t.Fatalf("unable to scan migrated record: %v", err)
			}
			records[name] = record
		}
		if err = rows.Err(); err != nil {
			t.Fatalf("unable to read migrated records: %v", err)
		}

		if got := records[legacyMigration]; got != (migrationRecord{time: legacyTime, version: legacyVersion}) {
			t.Fatalf("legacy migration metadata changed: %+v", got)
		}
		if got := records[zendriteMig]; got.time == "" || got.version == "" {
			t.Fatalf("new Zendrite migration was not recorded correctly: %+v", got)
		}
		if len(records) != 2 {
			t.Fatalf("unexpected migration records: %+v", records)
		}
	})
}
