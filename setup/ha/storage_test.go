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
	gate := NewPostgresGate(dsn, 45*time.Second)
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
	cfg.Global.HA.Compatibility = "different-binary"
	_, err = PrepareStorage(t.Context(), cfg, gate)
	require.ErrorContains(t, err, "configuration or executable differs")
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

	gate := NewPostgresGate(dsn, 45*time.Second)
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
