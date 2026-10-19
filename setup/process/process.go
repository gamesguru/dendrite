package process

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/getsentry/sentry-go"
	"github.com/sirupsen/logrus"
)

type contextKey struct{}

// ProcessContext owns application workers and their dependencies.
type ProcessContext struct {
	workerMu    sync.Mutex
	cleanup     []func()
	cleanupOnce sync.Once
	mu          sync.RWMutex
	wg          sync.WaitGroup      // used to wait for components to shutdown
	ctx         context.Context     // canceled when Stop is called
	shutdown    context.CancelFunc  // shut down Zendrite
	degraded    map[string]struct{} // reasons why the process is degraded
	// quiescing is closed when a planned handover begins, before shutdown.
	quiescing   chan struct{}
	quiesceOnce sync.Once
	// requests counts HTTP requests being served, so a planned handover can
	// wait for them before it cancels the application.
	requests atomic.Int64
}

func NewProcessContext() *ProcessContext {
	return NewProcessContextWithParent(context.Background())
}

// NewProcessContextWithParent binds all application work to the ownership lifetime.
func NewProcessContextWithParent(parent context.Context) *ProcessContext {
	ctx, shutdown := context.WithCancel(parent) //nolint:forbidigo
	p := &ProcessContext{
		ctx:       ctx,
		shutdown:  shutdown,
		wg:        sync.WaitGroup{},
		degraded:  make(map[string]struct{}),
		quiescing: make(chan struct{}),
	}
	context.AfterFunc(ctx, p.WaitForComponentsToFinish)
	return p
}

func (b *ProcessContext) Context() context.Context {
	return context.WithValue(b.ctx, contextKey{}, b)
}

func (b *ProcessContext) ComponentStarted() {
	b.wg.Add(1)
}

func (b *ProcessContext) ComponentFinished() {
	b.wg.Done()
}

func (b *ProcessContext) ShutdownZendrite() {
	b.workerMu.Lock()
	b.shutdown()
	b.workerMu.Unlock()
}

func (b *ProcessContext) WaitForShutdown() <-chan struct{} {
	return b.ctx.Done()
}

func (b *ProcessContext) WaitForComponentsToFinish() {
	//lint:ignore SA2001 This lock/unlock pair waits for a worker admitted before shutdown.
	b.workerMu.Lock()
	b.workerMu.Unlock() //nolint:staticcheck // Synchronize with a worker admitted just before parent cancellation.
	b.wg.Wait()
	b.cleanupOnce.Do(func() {
		b.workerMu.Lock()
		callbacks := b.cleanup
		b.workerMu.Unlock()
		for i := len(callbacks) - 1; i >= 0; i-- {
			callbacks[i]()
		}
	})
}

func (b *ProcessContext) Degraded(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.degraded[err.Error()]; !ok {
		logrus.WithError(err).Warn("Zendrite has entered a degraded state")
		sentry.CaptureException(err)
		b.degraded[err.Error()] = struct{}{}
	}
}

func (b *ProcessContext) IsDegraded() (bool, []string) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if len(b.degraded) == 0 {
		return false, nil
	}
	reasons := make([]string, 0, len(b.degraded))
	for reason := range b.degraded {
		reasons = append(reasons, reason)
	}
	return true, reasons
}

// Go registers a worker before launching it and refuses work after cancellation.
func (b *ProcessContext) Go(f func()) bool {
	b.workerMu.Lock()
	defer b.workerMu.Unlock()
	if b.ctx.Err() != nil {
		return false
	}
	b.wg.Add(1)
	go func() { defer b.wg.Done(); f() }()
	return true
}

// Go tracks work when ctx belongs to a ProcessContext, including derived contexts.
// Callers without a ProcessContext retain responsibility for joining their workers.
func Go(ctx context.Context, f func()) bool {
	if b, ok := ctx.Value(contextKey{}).(*ProcessContext); ok {
		return b.Go(f)
	}
	if ctx.Err() != nil {
		return false
	}
	go f()
	return true
}

// Defer registers dependency cleanup after all workers, including startup, finish.
// Register resources from a tracked worker before it returns.
func (b *ProcessContext) Defer(f func()) {
	b.workerMu.Lock()
	defer b.workerMu.Unlock()
	b.cleanup = append(b.cleanup, f)
}

// Sleep waits for a delay or cancellation and reports whether the delay elapsed.
func Sleep(ctx context.Context, delay time.Duration) bool {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return ctx.Err() == nil
	}
}

// Repeat runs a tracked maintenance task after an initial delay and then periodically.
func Repeat(ctx context.Context, initial, interval time.Duration, f func()) {
	Go(ctx, func() {
		if !Sleep(ctx, initial) {
			return
		}
		for ctx.Err() == nil {
			f()
			if !Sleep(ctx, interval) {
				return
			}
		}
	})
}

// Do registers synchronous application work, refusing admission after cancellation.
func (b *ProcessContext) Do(f func()) bool {
	b.workerMu.Lock()
	if b.ctx.Err() != nil {
		b.workerMu.Unlock()
		return false
	}
	b.wg.Add(1)
	b.workerMu.Unlock()
	defer b.wg.Done()
	f()
	return true
}

// Serve runs an HTTP request as application work, like Do, and counts it as
// in flight until it returns.
func (b *ProcessContext) Serve(f func()) bool {
	b.requests.Add(1)
	defer b.requests.Add(-1)
	return b.Do(f)
}

// InFlight reports how many HTTP requests are being served.
func (b *ProcessContext) InFlight() int64 { return b.requests.Load() }

// Quiesce announces a planned handover. The application keeps running and
// serving, but work that would otherwise wait for a long time, such as a
// long-polling sync, should answer now so that its client can reconnect to
// the successor.
func (b *ProcessContext) Quiesce() {
	b.quiesceOnce.Do(func() { close(b.quiescing) })
}

// Quiescing is closed once a planned handover has begun. Without an
// application owner it is nil, which never fires.
func Quiescing(ctx context.Context) <-chan struct{} {
	if b, ok := ctx.Value(contextKey{}).(*ProcessContext); ok {
		return b.quiescing
	}
	return nil
}

// Lifetime returns the application context for work that must outlive a request.
// Without an application owner it preserves the supplied cancellation boundary.
func Lifetime(ctx context.Context) context.Context {
	if b, ok := ctx.Value(contextKey{}).(*ProcessContext); ok {
		return b.Context()
	}
	return ctx
}

// Do joins a callback dispatched by an external library to the application lifetime.
func Do(ctx context.Context, f func()) bool {
	if b, ok := ctx.Value(contextKey{}).(*ProcessContext); ok {
		return b.Do(f)
	}
	if ctx.Err() != nil {
		return false
	}
	f()
	return true
}

// Stagger dispatches a batch on one tracked scheduler using one timer.
// Callbacks run serially and should dispatch blocking work to tracked workers.
func Stagger[T any](ctx context.Context, items []T, initial, interval time.Duration, dispatch func(T)) {
	if len(items) == 0 {
		return
	}
	next := time.Now().Add(initial)
	Go(ctx, func() {
		timer := time.NewTimer(time.Until(next))
		defer timer.Stop()
		for _, item := range items {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			}
			if ctx.Err() != nil {
				return
			}
			dispatch(item)
			next = next.Add(interval)
			timer.Reset(time.Until(next))
		}
	})
}
