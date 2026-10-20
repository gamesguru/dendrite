package ha

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordination "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"

	"codefloe.com/pat-s/zendrite/internal/ownership"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
)

// Run preserves standalone behavior without constructing any Kubernetes client
// when HA is disabled. A nil gate deliberately refuses HA application startup.
func Run(ctx context.Context, cfg config.HA, metrics *config.Metrics, app Application, gate OwnershipGate, shutdownTimeout time.Duration) error {
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
	// Bind the status listener before electing. A standby never binds the
	// Matrix listeners, so this is the only surface its probes and metrics
	// have, and a candidate that cannot serve probes must fail to start
	// rather than be restarted for looking dead.
	status := NewStatus()
	if owner, ok := gate.(ownership.Owner); ok {
		status.TrackOwnership(owner)
	}
	if reporter, ok := app.(StateReporter); ok {
		status.Track(reporter)
	}
	listener, serve, err := listenStatus(cfg, status, metrics)
	if err != nil {
		return err
	}
	defer func() { _ = listener.Close() }()
	go serve()
	logrus.WithField("address", listener.Addr().String()).Info("HA status listener started")
	if p, ok := gate.(preparer); ok {
		status.ExpectPreparation(cfg.Version, Release())
		prepareCtx, stopPreparing := context.WithCancelCause(ctx)
		defer stopPreparing(nil)
		go prepare(prepareCtx, p, cfg, status)
	}
	return runKubernetes(ctx, cfg, client, app, gate, shutdownTimeout, status)
}

// preparer checks whether this candidate may take over the deployment.
type preparer interface {
	Prepare(context.Context) (Recorded, error)
}

// prepare reports, while this candidate is a standby, whether it may take
// over: whether it reaches the database and whether the deployment's upgrade
// contract admits its build. It only informs readiness and status; the Lease
// lock repeats the check before every acquisition, which is what enforces it.
func prepare(ctx context.Context, p preparer, cfg config.HA, status *Status) {
	contract := Release()
	logrus.WithFields(logrus.Fields{"version": cfg.Version, "contract": contract.String()}).Info("HA upgrade contract")
	ticker := time.NewTicker(cfg.RetryPeriod)
	defer ticker.Stop()
	for {
		switch status.Phase() {
		case PhaseStarting, PhaseStandby:
			recorded, err := p.Prepare(ctx)
			if ctx.Err() != nil {
				return
			}
			status.Prepared(cfg.Version, contract, recorded, err)
		case PhaseFencing, PhaseRecovering, PhaseActive, PhaseHandingOver, PhaseDraining, PhaseStopped:
			// Leadership is final: a candidate never returns to standby.
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// newLeaseClient permits a slow request to use the renewal window. The retry
// period controls scheduling; the election context still bounds renewal work.
func newLeaseClient(clientConfig *rest.Config, renewDeadline time.Duration) (coordination.CoordinationV1Interface, error) {
	clientConfig = rest.CopyConfig(clientConfig)
	clientConfig.Timeout = renewDeadline
	return coordination.NewForConfig(clientConfig)
}

func runKubernetes(ctx context.Context, cfg config.HA, client coordination.CoordinationV1Interface, app Application, gate OwnershipGate, shutdownTimeout time.Duration, status *Status) error {
	candidate := Candidate{Identity: cfg.CandidateID + "_" + uuid.NewString(), LeaseName: cfg.LeaseName, LeaseNamespace: cfg.LeaseNamespace}
	failures := make(chan error, 1)
	lock := &permissionLock{Interface: &annotatedLeaseLock{LeaseLock: &resourcelock.LeaseLock{
		LeaseMeta:  metav1.ObjectMeta{Name: candidate.LeaseName, Namespace: candidate.LeaseNamespace, Annotations: map[string]string{compatibilityAnnotation: cfg.Compatibility}},
		Client:     client,
		LockConfig: resourcelock.ResourceLockConfig{Identity: candidate.Identity},
	}}, failures: failures, leases: client.Leases(candidate.LeaseNamespace), compatibility: cfg.Compatibility, leaseName: candidate.LeaseName, identity: candidate.Identity}
	if a, ok := gate.(interface{ Acquirable(context.Context) error }); ok {
		lock.acquirable = a.Acquirable
	}
	acquired := make(chan context.Context, 1)
	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock:          lock,
		LeaseDuration: cfg.LeaseDuration, RenewDeadline: cfg.RenewDeadline, RetryPeriod: cfg.RetryPeriod,
		// Never release on cancellation: startup or protected work may still be
		// running. runKubernetes releases after a completed drain instead.
		// Neither release nor expiry proves fencing; every successor must pass OwnershipGate.
		ReleaseOnCancel: false,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(leader context.Context) { acquired <- leader },
			OnStoppedLeading: func() {}, // Run completion is handled by control, including pre-acquisition cancellation.
		},
	})
	if err != nil {
		return fmt.Errorf("configure HA Lease election: %w", err)
	}
	err = control(ctx, elector, acquired, failures, candidate, gate, app, shutdownTimeout, status)
	if err == nil && ctx.Err() != nil {
		releaseCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cfg.RenewDeadline)
		defer cancel()
		// A pull request this process left at the broker could otherwise
		// take the successor's first message for this room, which would then
		// wait the whole acknowledgement timeout before redelivery.
		jetstream.WaitForOwnedPulls(releaseCtx)
		releaseLease(releaseCtx, lock, candidate.Identity)
	}
	return err
}

// releaseLease hands the Lease back after a requested shutdown drained the
// application completely, so a standby takes over at its next retry instead
// of waiting for the Lease to expire. Control only reports such a shutdown
// without an error once the application has stopped, so no protected work of
// this process remains; a drain that overran the shutdown budget returns an
// error and keeps the Lease. The update is conditional on the Lease version
// read here, so it cannot clear a Lease another candidate has taken since.
func releaseLease(ctx context.Context, lock resourcelock.Interface, identity string) {
	record, _, err := lock.Get(ctx)
	if err != nil || record == nil || record.HolderIdentity != identity {
		return
	}
	now := metav1.NewTime(time.Now())
	err = lock.Update(ctx, resourcelock.LeaderElectionRecord{
		LeaseDurationSeconds: 1,
		RenewTime:            now,
		AcquireTime:          now,
		LeaderTransitions:    record.LeaderTransitions,
	})
	if err != nil {
		logrus.WithError(err).Warn("Could not release the HA Lease after shutdown; a successor waits for it to expire")
		return
	}
	logrus.Info("Released the HA Lease after a clean shutdown")
}

// permissionLock turns permanent authorization failures into actionable terminal
// errors. Transient API failures retain client-go's acquisition/renewal retries.
//
// It also refuses to take the Lease while acquirable fails. A candidate that
// cannot reach the database would hold the Lease through the whole ownership
// timeout without fencing, and then leave it to expire, keeping a healthy
// standby out for both. A build that the deployment's upgrade contract does
// not admit would retire the running owner and then fail to replace it.
// Renewals are never refused: a holder that loses the database gives the
// Lease up through its gate instead.
type permissionLock struct {
	resourcelock.Interface
	failures      chan<- error
	leases        coordination.LeaseInterface
	compatibility string
	leaseName     string
	identity      string
	acquirable    func(context.Context) error
	// holder is the holder identity last read from or written to the Lease,
	// which tells an acquiring Update from a renewing one: client-go sends
	// the same record for both.
	holder  atomic.Pointer[string]
	blocked atomic.Bool
}

// checkAcquirable reports why this candidate must not take the Lease now.
func (l *permissionLock) checkAcquirable(ctx context.Context) error {
	if l.acquirable == nil {
		return nil
	}
	err := l.acquirable(ctx)
	if err != nil {
		if !l.blocked.Swap(true) {
			if errors.Is(err, ErrIncompatibleBuild) {
				logrus.WithError(err).Error("Not acquiring the HA Lease: this build may not take over the running deployment")
			} else {
				logrus.WithError(err).Warn("Not acquiring the HA Lease while this candidate cannot reach the database")
			}
		}
		return fmt.Errorf("not acquiring the HA Lease: %w", err)
	}
	if l.blocked.Swap(false) {
		logrus.Info("This candidate may take over again; contending for the HA Lease")
	}
	return nil
}

func (l *permissionLock) observe(holder string) { l.holder.Store(&holder) }

func (l *permissionLock) holds() bool {
	holder := l.holder.Load()
	return holder != nil && *holder == l.identity
}

func (l *permissionLock) report(err error) {
	if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
		select {
		case l.failures <- fmt.Errorf("HA Lease %s access denied: grant the service account get, create, update on coordination.k8s.io leases in this namespace (precreated Leases need get/update and a matching compatibility annotation): %w", l.Describe(), err):
		default:
		}
	}
}

func (l *permissionLock) Get(ctx context.Context) (*resourcelock.LeaderElectionRecord, []byte, error) {
	record, raw, err := l.Interface.Get(ctx)
	if err == nil && record != nil {
		l.observe(record.HolderIdentity)
	}
	if err == nil && l.compatibility != "" {
		lease, readErr := l.leases.Get(ctx, l.leaseName, metav1.GetOptions{})
		if readErr != nil {
			err = readErr
		} else if lease.Annotations[compatibilityAnnotation] != l.compatibility {
			err = fmt.Errorf("HA Lease compatibility mismatch: configuration or executable differs, or the Lease lacks a compatibility record; coordinated offline migration required")
			select {
			case l.failures <- err:
			default:
			}
		}
	}
	l.report(err)
	return record, raw, err
}

func (l *permissionLock) Create(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	if err := l.checkAcquirable(ctx); err != nil {
		return err
	}
	err := l.Interface.Create(ctx, record)
	if err == nil {
		l.observe(record.HolderIdentity)
	}
	l.report(err)
	return err
}

func (l *permissionLock) Update(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	if !l.holds() {
		if err := l.checkAcquirable(ctx); err != nil {
			return err
		}
	}
	err := l.Interface.Update(ctx, record)
	if err == nil {
		l.observe(record.HolderIdentity)
	}
	// client-go calls Update both to renew and to acquire, so counting every
	// failure would inflate this on every healthy standby that loses a race.
	// Only a holder failing to renew is control-plane trouble, and a single
	// failure is not yet a lost lease: client-go retries until the deadline.
	if err != nil && record.HolderIdentity == l.identity {
		renewalFailures.Inc()
	}
	l.report(err)
	return err
}

const compatibilityAnnotation = "zendrite.codefloe.com/compatibility"

// annotatedLeaseLock creates compatibility metadata atomically with ownership.
// The client-go LeaseLock.Create implementation copies labels but drops LeaseMeta.Annotations.
type annotatedLeaseLock struct{ *resourcelock.LeaseLock }

func (l *annotatedLeaseLock) Create(ctx context.Context, record resourcelock.LeaderElectionRecord) error {
	_, err := l.Client.Leases(l.LeaseMeta.Namespace).Create(ctx, &coordinationv1.Lease{
		ObjectMeta: *l.LeaseMeta.DeepCopy(),
		Spec:       resourcelock.LeaderElectionRecordToLeaseSpec(&record),
	}, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	// Initialize client-go's private cached Lease, including its resource version,
	// so its normal renewal path preserves the annotation and conflict checks.
	_, _, err = l.Get(ctx)
	return err
}
