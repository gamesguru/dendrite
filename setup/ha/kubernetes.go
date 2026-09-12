package ha

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordination "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// Run preserves standalone behavior without constructing any Kubernetes client
// when HA is disabled. A nil gate deliberately refuses HA application startup.
func Run(ctx context.Context, cfg config.HA, app Application, gate OwnershipGate, shutdownTimeout time.Duration) error {
	if !cfg.Enabled {
		return app.Run(ctx)
	}
	defer app.Stop()
	errs := &config.ConfigErrors{}
	cfg.Verify(errs)
	if len(*errs) != 0 {
		return errs
	}
	if shutdownTimeout <= 0 {
		return fmt.Errorf("HA shutdown timeout must be positive")
	}
	if ctx.Err() != nil {
		app.Stop()
		return nil
	}
	clientConfig, err := rest.InClusterConfig()
	if err != nil {
		return fmt.Errorf("HA requires an in-cluster Kubernetes service account: %w", err)
	}
	configureLogging(logrus.StandardLogger())
	client, err := newLeaseClient(clientConfig, cfg.RenewDeadline)
	if err != nil {
		return fmt.Errorf("create HA Kubernetes Lease client: %w", err)
	}
	return runKubernetes(ctx, cfg, client, app, gate, shutdownTimeout)
}

// newLeaseClient permits a slow request to use the renewal window. The retry
// period controls scheduling; the election context still bounds renewal work.
func newLeaseClient(clientConfig *rest.Config, renewDeadline time.Duration) (coordination.CoordinationV1Interface, error) {
	clientConfig = rest.CopyConfig(clientConfig)
	clientConfig.Timeout = renewDeadline
	return coordination.NewForConfig(clientConfig)
}

func runKubernetes(ctx context.Context, cfg config.HA, client coordination.CoordinationV1Interface, app Application, gate OwnershipGate, shutdownTimeout time.Duration) error {
	candidate := Candidate{Identity: cfg.CandidateID + "_" + uuid.NewString(), LeaseName: cfg.LeaseName, LeaseNamespace: cfg.LeaseNamespace}
	failures := make(chan error, 1)
	lock := &permissionLock{Interface: &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: candidate.LeaseName, Namespace: candidate.LeaseNamespace},
		Client:     client,
		LockConfig: resourcelock.ResourceLockConfig{Identity: candidate.Identity},
	}, failures: failures}
	acquired := make(chan context.Context, 1)
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: cfg.LeaseDuration, RenewDeadline: cfg.RenewDeadline, RetryPeriod: cfg.RetryPeriod,
		// Never release on cancellation: startup or protected work may still be running.
		// Expiry also does not prove fencing; every successor must pass OwnershipGate.
		ReleaseOnCancel: false,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leader context.Context) { acquired <- leader },
			OnStoppedLeading: func() {}, // Run completion is handled by control, including pre-acquisition cancellation.
		},
	})
	if err != nil {
		return fmt.Errorf("configure HA Lease election: %w", err)
	}
	return control(ctx, elector, acquired, failures, candidate, gate, app, shutdownTimeout)
}

// permissionLock turns permanent authorization failures into actionable terminal
// errors. Transient API failures retain client-go's acquisition/renewal retries.
type permissionLock struct {
	resourcelock.Interface
	failures chan<- error
}

func (l *permissionLock) report(err error) {
	if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
		select {
		case l.failures <- fmt.Errorf("HA Lease %s access denied: grant the service account get, create, update on coordination.k8s.io leases in this namespace (precreated Leases need get and update): %w", l.Describe(), err):
		default:
		}
	}
}

func (l *permissionLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	record, raw, err := l.Interface.Get(ctx)
	l.report(err)
	return record, raw, err
}

func (l *permissionLock) Create(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Create(ctx, record)
	l.report(err)
	return err
}

func (l *permissionLock) Update(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	err := l.Interface.Update(ctx, record)
	l.report(err)
	return err
}
