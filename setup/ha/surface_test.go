package ha_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	"golang.org/x/crypto/bcrypt"

	fedstorage "codefloe.com/pat-s/zendrite/federationapi/storage"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	mediastorage "codefloe.com/pat-s/zendrite/mediaapi/storage"
	relaystorage "codefloe.com/pat-s/zendrite/relayapi/storage"
	roomstorage "codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/ha"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/mscs/msc2836"
	syncstorage "codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/test"
	userstorage "codefloe.com/pat-s/zendrite/userapi/storage"
)

var update = flag.Bool("update", false, "record the shared-state surface of a new upgrade level")

// level is one entry of the upgrade matrix in testdata/levels.json.
type level struct {
	ha.Contract
	// Surface is the digest of the shared-state surface the level was
	// qualified against; testdata/surface.txt holds the current one in full.
	Surface string `json:"surface"`
}

const (
	levelsFile  = "testdata/levels.json"
	surfaceFile = "testdata/surface.txt"
)

// TestContractSurface refuses a change to the state that candidates of
// different builds share, unless the build's upgrade contract declares it.
//
// A rolling upgrade runs an old and a new build against the same database and
// streams, one after the other. The contract's level says which builds may do
// that, so a change to the database schema, the migrations or the streams
// must come with a new level. This test records each level's surface and
// fails when the current surface differs from the one recorded for the
// current level. To declare a change, raise Level in contract.go, decide
// UpgradeFrom and RollbackTo, and run
//
//	go test ./setup/ha -run TestContractSurface -update
//
// Message formats are not captured here; a change to one that an older build
// cannot decode needs a new level and a raised RollbackTo just the same.
func TestContractSurface(t *testing.T) {
	current := sharedSurface(t)
	sum := sha256.Sum256([]byte(current))
	digest := hex.EncodeToString(sum[:])
	contract := ha.Release()

	var levels []level
	data, err := os.ReadFile(levelsFile)
	if err != nil && !os.IsNotExist(err) {
		t.Fatal(err)
	}
	if err == nil {
		require.NoError(t, json.Unmarshal(data, &levels))
	}
	var recorded *level
	for i := range levels {
		if levels[i].Level == contract.Level {
			recorded = &levels[i]
		}
	}
	if recorded != nil && recorded.Surface != digest {
		stored, _ := os.ReadFile(surfaceFile)
		t.Fatalf("the shared-state surface changed under upgrade level %d:\n%s\n"+
			"Raise Level in setup/ha/contract.go, and UpgradeFrom or RollbackTo if the change is not backward compatible, "+
			"then run: go test ./setup/ha -run TestContractSurface -update", contract.Level, lineDiff(string(stored), current))
	}
	if *update {
		if recorded == nil {
			if n := len(levels); n > 0 && levels[n-1].Level >= contract.Level {
				t.Fatalf("level %d must be higher than the last recorded level %d", contract.Level, levels[n-1].Level)
			}
			levels = append(levels, level{Contract: contract, Surface: digest})
		}
		encoded, err := json.MarshalIndent(levels, "", "  ")
		require.NoError(t, err)
		require.NoError(t, os.MkdirAll(filepath.Dir(levelsFile), 0o755))
		require.NoError(t, os.WriteFile(levelsFile, append(encoded, '\n'), 0o644)) //nolint:gosec // Checked-in test data.
		require.NoError(t, os.WriteFile(surfaceFile, []byte(current), 0o644))      //nolint:gosec // Checked-in test data.
		return
	}
	if recorded == nil {
		t.Fatalf("upgrade level %d is not recorded in %s; run: go test ./setup/ha -run TestContractSurface -update", contract.Level, levelsFile)
	}
	require.Equal(t, contract, recorded.Contract, "the contract in %s differs from setup/ha/contract.go", levelsFile)
	stored, err := os.ReadFile(surfaceFile)
	require.NoError(t, err)
	require.Equal(t, current, string(stored), "%s is stale; run: go test ./setup/ha -run TestContractSurface -update", surfaceFile)
}

// sharedSurface creates every table a candidate can use in a fresh
// PostgreSQL database, the way an activation does, and describes the
// resulting schema, the migrations recorded, and the streams.
func sharedSurface(t *testing.T) string {
	t.Helper()
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	t.Cleanup(cleanup)
	ctx := t.Context()
	opts := &config.DatabaseOptions{ConnectionString: config.DataSource(dsn)}
	cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
	local := func(spec.ServerName) bool { return true }
	must := func(_ any, err error) {
		t.Helper()
		require.NoError(t, err)
	}
	must(roomstorage.Open(ctx, cm, opts, caches))
	must(syncstorage.NewSyncServerDatasource(ctx, cm, opts))
	must(fedstorage.NewDatabase(ctx, cm, opts, caches, local))
	must(userstorage.NewUserDatabase(ctx, cm, opts, "localhost", bcrypt.MinCost, 1000, time.Minute, "notices"))
	must(userstorage.NewKeyDatabase(cm, opts))
	must(mediastorage.NewMediaAPIDatasource(cm, opts))
	must(relaystorage.NewDatabase(cm, opts, caches, local))
	must(msc2836.NewDatabase(cm, opts))

	// The ownership and compatibility records, as an activation creates them.
	haCfg := config.HA{Enabled: true, LeaseDuration: time.Hour, RenewDeadline: time.Hour, RetryPeriod: time.Hour, OwnershipTimeout: time.Hour, Compatibility: "surface"}
	gate := ha.NewPostgresGate(dsn, haCfg)
	require.NoError(t, gate.Acquire(ctx, ha.Candidate{Identity: "surface", LeaseName: "surface", LeaseNamespace: "surface"}))
	cfg := &config.Zendrite{}
	cfg.Global.HA = haCfg
	cfg.Global.DatabaseOptions.ConnectionString = config.DataSource(dsn)
	release, err := ha.PrepareStorage(ctx, cfg, gate)
	require.NoError(t, err)
	release()

	conn, err := pgx.Connect(ctx, dsn)
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.WithoutCancel(ctx)) }()
	var out strings.Builder
	describe := func(kind, query string) {
		t.Helper()
		rows, err := conn.Query(ctx, query)
		require.NoError(t, err)
		defer rows.Close()
		for rows.Next() {
			values, err := rows.Values()
			require.NoError(t, err)
			fields := make([]string, len(values))
			for i, v := range values {
				fields[i] = fmt.Sprint(v)
			}
			if kind == "column" && len(fields) == 5 && fields[0] == "syncapi_unpartialstated_rooms" && fields[1] == "created_at" {
				// PostgreSQL 14 changed extract(epoch ...) from a float result to numeric.
				// The catalog prints equivalent forms differently across supported majors.
				switch fields[4] {
				case "(EXTRACT(epoch FROM now()) * (1000)::numeric)", "(date_part('epoch'::text, now()) * (1000)::double precision)":
					fields[4] = "<epoch milliseconds>"
				}
			}
			// No trailing whitespace, which the pre-commit hooks would strip.
			fmt.Fprintf(&out, "%s\n", strings.TrimRight(kind+" "+strings.Join(fields, " | "), " "))
		}
		require.NoError(t, rows.Err())
	}
	describe("column", `SELECT table_name, column_name, data_type, is_nullable, coalesce(column_default, '')
		FROM information_schema.columns WHERE table_schema = current_schema() ORDER BY table_name, column_name`)
	describe("index", `SELECT tablename, indexname, indexdef FROM pg_indexes
		WHERE schemaname = current_schema() ORDER BY tablename, indexname`)
	// PostgreSQL 18 lists NOT NULL in pg_constraint, older majors do not. The column
	// rows above already record nullability, so leave these out to stay portable.
	describe("constraint", `SELECT conrelid::regclass::text, conname, pg_get_constraintdef(oid) FROM pg_constraint
		WHERE connamespace = current_schema()::regnamespace AND contype <> 'n' ORDER BY 1, 2`)
	describe("sequence", `SELECT sequence_name, data_type, increment FROM information_schema.sequences
		WHERE sequence_schema = current_schema() ORDER BY sequence_name`)
	describe("migration", `SELECT version FROM db_migrations ORDER BY version`)
	for _, stream := range jetstream.StreamDefinitions() {
		encoded, err := json.Marshal(stream)
		require.NoError(t, err)
		fmt.Fprintf(&out, "stream %s\n", encoded)
	}
	return out.String()
}

// lineDiff lists the lines only one of two surfaces has.
func lineDiff(before, after string) string {
	count := map[string]int{}
	for _, line := range strings.Split(before, "\n") {
		count[line]--
	}
	for _, line := range strings.Split(after, "\n") {
		count[line]++
	}
	var out []string
	for _, line := range strings.Split(before, "\n") {
		if count[line] < 0 {
			out = append(out, "- "+line)
		}
	}
	for _, line := range strings.Split(after, "\n") {
		if count[line] > 0 {
			out = append(out, "+ "+line)
		}
	}
	return strings.Join(out, "\n")
}
