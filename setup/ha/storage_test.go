package ha

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

func TestStorageCompatibilityAndMigrationSerialization(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer cleanup()
	cfg := &config.Zendrite{}
	cfg.Global.HA = config.HA{Enabled: true, Compatibility: "same-binary-and-config"}
	cfg.Global.DatabaseOptions.ConnectionString = config.DataSource(dsn)
	gate := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	release, err := PrepareStorage(t.Context(), cfg, gate)
	require.NoError(t, err)
	defer release()
	// Even another authorized startup cannot migrate concurrently.
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	_, err = PrepareStorage(ctx, cfg, gate)
	require.ErrorContains(t, err, "migration lock")
	release()
	again, err := PrepareStorage(t.Context(), cfg, gate)
	require.NoError(t, err)
	again()
	unsafe := *cfg
	unsafe.Global.DatabaseOptions.ConnectionString += " synchronous_commit=off"
	_, err = PrepareStorage(t.Context(), &unsafe, gate)
	require.ErrorContains(t, err, "synchronous_commit")
	cfg.Global.HA.Compatibility = "different-deployment"
	_, err = PrepareStorage(t.Context(), cfg, gate)
	require.ErrorIs(t, err, ErrIncompatibleBuild)
}

// withRelease runs a test as a build with another upgrade contract.
func withRelease(t *testing.T, c Contract) {
	t.Helper()
	previous := release
	release = c
	t.Cleanup(func() { release = previous })
}

// TestUpgradeContractRecord follows one deployment through an upgrade and a
// rollback, as standbys of each build see it before they take the Lease and
// as each activation records it.
func TestUpgradeContractRecord(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer cleanup()
	cfg := &config.Zendrite{}
	cfg.Global.HA = config.HA{Enabled: true, Compatibility: "deployment", Version: "v1"}
	cfg.Global.DatabaseOptions.ConnectionString = config.DataSource(dsn)
	haCfg := testHAConfig(time.Hour, time.Hour)
	haCfg.Compatibility = "deployment"
	activate := func(identity string) {
		t.Helper()
		gate := NewPostgresGate(dsn, haCfg)
		require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: identity, LeaseName: "matrix", LeaseNamespace: "example"}))
		release, err := PrepareStorage(t.Context(), cfg, gate)
		require.NoError(t, err)
		release()
	}
	standby := func() (Recorded, error) {
		t.Helper()
		return NewPostgresGate(dsn, haCfg).Prepare(t.Context())
	}

	// A fresh database admits any build, and a standby never creates the record.
	withRelease(t, Contract{Protocol: 1, Level: 1, UpgradeFrom: 1, RollbackTo: 1})
	recorded, err := standby()
	require.NoError(t, err)
	require.True(t, recorded.Empty())
	activate("v1-a")
	recorded, err = standby()
	require.NoError(t, err)
	require.Equal(t, Recorded{Fingerprint: "deployment", Protocol: 1, Level: 1, Floor: 1, Version: "v1"}, recorded)

	// A build that skips a level is refused before it would take the Lease,
	// and the record is untouched.
	withRelease(t, Contract{Protocol: 1, Level: 3, UpgradeFrom: 2, RollbackTo: 2})
	_, err = standby()
	require.ErrorIs(t, err, ErrIncompatibleBuild)

	// A compatible upgrade whose storage no longer supports level 1.
	withRelease(t, Contract{Protocol: 1, Level: 2, UpgradeFrom: 1, RollbackTo: 2})
	cfg.Global.HA.Version = "v2"
	_, err = standby()
	require.NoError(t, err)
	activate("v2-a")
	recorded, err = standby()
	require.NoError(t, err)
	require.Equal(t, 2, recorded.Level)
	require.Equal(t, 2, recorded.Floor)

	// The old build can no longer take over, as a standby or at activation.
	withRelease(t, Contract{Protocol: 1, Level: 1, UpgradeFrom: 1, RollbackTo: 1})
	_, err = standby()
	require.ErrorIs(t, err, ErrIncompatibleBuild)
	gate := NewPostgresGate(dsn, haCfg)
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "v1-b", LeaseName: "matrix", LeaseNamespace: "example"}))
	_, err = PrepareStorage(t.Context(), cfg, gate)
	require.ErrorIs(t, err, ErrIncompatibleBuild)
	recorded, err = NewPostgresGate(dsn, haCfg).Prepare(t.Context())
	require.ErrorIs(t, err, ErrIncompatibleBuild)
	require.Equal(t, 2, recorded.Level, "a refused activation must not rewrite the record")
}

// A deployment last activated by a build without an upgrade contract has a
// record without a level. It must be refused, so that the first upgrade to a
// build with a contract takes the offline path.
func TestRecordWithoutContractIsRefused(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer cleanup()
	conn, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	defer func() { _ = conn.Close(context.WithoutCancel(t.Context())) }()
	_, err = conn.Exec(t.Context(), "CREATE TABLE zendrite_ha_compatibility (id INTEGER PRIMARY KEY CHECK (id = 1), fingerprint TEXT NOT NULL)")
	require.NoError(t, err)
	_, err = conn.Exec(t.Context(), "INSERT INTO zendrite_ha_compatibility (id, fingerprint) VALUES (1, 'digest-of-binary-and-config')")
	require.NoError(t, err)
	haCfg := testHAConfig(time.Hour, time.Hour)
	haCfg.Compatibility = "deployment"
	_, err = NewPostgresGate(dsn, haCfg).Prepare(t.Context())
	require.ErrorIs(t, err, ErrIncompatibleBuild)
}

func TestLeaseCompatibility(t *testing.T) {
	for _, fingerprint := range []string{"expected", "different", ""} {
		t.Run("fingerprint="+fingerprint, func(t *testing.T) {
			client := fake.NewClientset(&coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: "matrix", Namespace: "test", Annotations: map[string]string{compatibilityAnnotation: fingerprint}}})
			failures := make(chan error, 1)
			lock := &permissionLock{Interface: &resourcelock.LeaseLock{LeaseMeta: metav1.ObjectMeta{Name: "matrix", Namespace: "test"}, Client: client.CoordinationV1()}, failures: failures, leases: client.CoordinationV1().Leases("test"), leaseName: "matrix", compatibility: "expected"}
			_, _, err := lock.Get(t.Context())
			if fingerprint == "expected" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "compatibility mismatch")
				require.Len(t, failures, 1)
			}
		})
	}
}

func TestLeaseBootstrapRetainsCompatibility(t *testing.T) {
	client := leaseClient()
	meta := metav1.ObjectMeta{Name: "matrix", Namespace: "test", Annotations: map[string]string{compatibilityAnnotation: "expected"}}
	lock := &permissionLock{
		Interface: &annotatedLeaseLock{LeaseLock: &resourcelock.LeaseLock{LeaseMeta: meta, Client: client.CoordinationV1(), LockConfig: resourcelock.ResourceLockConfig{Identity: "candidate"}}},
		failures:  make(chan error, 1), leases: client.CoordinationV1().Leases("test"), leaseName: "matrix", compatibility: "expected",
	}
	record := resourcelock.LeaderElectionRecord{HolderIdentity: "candidate", LeaseDurationSeconds: 30, AcquireTime: metav1.Now(), RenewTime: metav1.Now()}
	require.NoError(t, lock.Create(t.Context(), record))
	created, err := client.CoordinationV1().Leases("test").Get(t.Context(), "matrix", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "expected", created.Annotations[compatibilityAnnotation])
	// The first renewal must already have an initialized client-go resource version.
	require.NoError(t, lock.Update(t.Context(), record))
	observed, _, err := lock.Get(t.Context())
	require.NoError(t, err)
	require.Equal(t, "candidate", observed.HolderIdentity)
	// A mismatched candidate must not overwrite the existing compatibility record.
	lock.compatibility = "other"
	_, _, err = lock.Get(t.Context())
	require.ErrorContains(t, err, "compatibility mismatch")
	require.Error(t, lock.Create(t.Context(), record))
	stored, err := client.CoordinationV1().Leases("test").Get(t.Context(), "matrix", metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "expected", stored.Annotations[compatibilityAnnotation])
}

// TestPrepareStorageConnectionIsFenceable covers a connection that the sweep
// would otherwise miss: the dedicated migration connection holds the advisory
// lock through initialization and can commit schema changes, so it must carry
// the activation marker like every other application backend.
func TestPrepareStorageConnectionIsFenceable(t *testing.T) {
	dsn, cleanup := test.PrepareDBConnectionString(t, test.DBTypePostgres)
	defer cleanup()
	cfg := &config.Zendrite{}
	cfg.Global.HA = config.HA{Enabled: true, Compatibility: "same-binary-and-config"}
	cfg.Global.DatabaseOptions.ConnectionString = config.DataSource(dsn)

	gate := NewPostgresGate(dsn, testHAConfig(time.Hour, time.Hour))
	require.NoError(t, gate.Acquire(t.Context(), Candidate{Identity: "candidate-a", LeaseName: "matrix", LeaseNamespace: "example"}))
	release, err := PrepareStorage(t.Context(), cfg, gate)
	require.NoError(t, err)
	defer release()

	observer, err := pgx.Connect(t.Context(), dsn)
	require.NoError(t, err)
	defer func() { _ = observer.Close(context.WithoutCancel(t.Context())) }()
	var marked int
	require.NoError(t, observer.QueryRow(t.Context(),
		"SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = $1",
		gate.ApplicationName()).Scan(&marked))
	require.Positive(t, marked, "the migration connection must be visible to a successor's sweep")
}
