package lifecycle

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/setup/process"
)

func await[T any](t *testing.T, ch <-chan T) T {
	t.Helper()
	select {
	case result := <-ch:
		return result
	case <-time.After(5 * time.Second):
		t.Fatal("lifecycle did not progress")
		var zero T
		return zero
	}
}

func TestActivation(t *testing.T) {
	for _, cancelDuringRecovery := range []bool{false, true} {
		name := "active shutdown"
		if cancelDuringRecovery {
			name = "cancel during recovery"
		}
		t.Run(name, func(t *testing.T) {
			var calls atomic.Int32
			var workerStopped atomic.Bool
			var cleaned atomic.Bool
			entered := make(chan struct{})
			recoverNow := make(chan struct{})
			h := New(func(p *process.ProcessContext) error {
				calls.Add(1)
				p.Defer(func() {
					if !workerStopped.Load() {
						t.Error("dependency closed before its worker stopped")
					}
					cleaned.Store(true)
				})
				p.Go(func() { <-p.WaitForShutdown(); workerStopped.Store(true) })
				close(entered)
				select {
				case <-recoverNow:
					return nil
				case <-p.WaitForShutdown():
					return p.Context().Err()
				}
			}, time.Second)
			if h.State() != Standby || h.Ready() || calls.Load() != 0 {
				t.Fatal("construction activated the application")
			}
			ownership, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			result := make(chan error, 1)
			go func() { result <- h.Run(ownership) }()
			await(t, entered)
			if h.Ready() || h.State() != Recovering {
				t.Fatal("recovery published readiness")
			}
			if err := h.Run(ownership); !errors.Is(err, ErrAlreadyStarted) {
				t.Fatalf("second activation: %v", err)
			}
			if !cancelDuringRecovery {
				close(recoverNow)
				deadline := time.After(time.Second)
				for !h.Ready() {
					select {
					case <-deadline:
						t.Fatal("never became ready")
					default:
						time.Sleep(time.Millisecond)
					}
				}
			}
			cancel(errors.New("ownership lost"))
			if h.Ready() {
				t.Fatal("canceled owner remained ready")
			}
			err := await(t, result)
			if err != nil {
				t.Fatalf("shutdown: %v", err)
			}
			if !workerStopped.Load() || !cleaned.Load() || h.State() != Stopped {
				t.Fatal("shutdown did not join and clean up")
			}
			if err := h.Run(t.Context()); !errors.Is(err, ErrAlreadyStarted) {
				t.Fatalf("restarted stopped activation: %v", err)
			}
			if calls.Load() != 1 {
				t.Fatal("initialized more than once")
			}
		})
	}
}

func TestInitializationFailure(t *testing.T) {
	failure := errors.New("recovery failed")
	var cleaned atomic.Bool
	h := New(func(p *process.ProcessContext) error {
		p.Defer(func() { cleaned.Store(true) })
		return failure
	}, time.Second)
	if err := h.Run(t.Context()); !errors.Is(err, failure) {
		t.Fatalf("lost startup error: %v", err)
	}
	if !cleaned.Load() || h.Ready() || h.State() != Stopped {
		t.Fatal("failed startup not cleaned up")
	}
}

func TestShutdownDeadline(t *testing.T) {
	for _, blockStartup := range []bool{false, true} {
		t.Run(map[bool]string{false: "worker", true: "startup"}[blockStartup], func(t *testing.T) {
			entered, release, cleaned := make(chan struct{}), make(chan struct{}), make(chan struct{})
			h := New(func(p *process.ProcessContext) error {
				p.Defer(func() { close(cleaned) })
				if !blockStartup {
					p.Go(func() { <-release })
				}
				close(entered)
				if blockStartup {
					<-release
				}
				return nil
			}, 10*time.Millisecond)
			result := make(chan error, 1)
			go func() { result <- h.Run(t.Context()) }()
			await(t, entered)
			h.Stop()
			if err := await(t, result); !errors.Is(err, ErrShutdownTimeout) {
				t.Fatalf("missing forced-exit signal: %v", err)
			}
			select {
			case <-cleaned:
				t.Fatal("closed dependencies while still in use")
			default:
			}
			close(release)
			await(t, cleaned)
			if h.Ready() {
				t.Fatal("timed-out activation became ready")
			}
		})
	}
}

func TestStopBeforeActivation(t *testing.T) {
	h := New(func(*process.ProcessContext) error { t.Fatal("started after stop"); return nil }, time.Second)
	h.Stop()
	if err := h.Run(t.Context()); !errors.Is(err, ErrAlreadyStarted) {
		t.Fatalf("start after stop: %v", err)
	}
}

func TestCancelledOwnership(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(nil)
	h := New(func(*process.ProcessContext) error { t.Fatal("initialized with canceled ownership"); return nil }, time.Second)
	if err := h.Run(ctx); err != nil {
		t.Fatal(err)
	}
	if h.State() != Stopped {
		t.Fatal(h.State())
	}
}

func TestStartupPanicCleansUp(t *testing.T) {
	var cleaned atomic.Bool
	h := New(func(p *process.ProcessContext) error {
		p.Defer(func() { cleaned.Store(true) })
		panic("legacy constructor failure")
	}, time.Second)
	if err := h.Run(t.Context()); err == nil {
		t.Fatal("startup panic was lost")
	} else if !strings.Contains(err.Error(), "TestStartupPanicCleansUp.func1") ||
		!strings.Contains(err.Error(), "lifecycle_test.go:") {
		t.Fatalf("startup panic lost its origin stack: %v", err)
	}
	if !cleaned.Load() || h.State() != Stopped {
		t.Fatal("panic skipped cleanup")
	}
}

func TestUnrelatedStartupCancellationIsFailure(t *testing.T) {
	h := New(func(*process.ProcessContext) error { return context.Canceled }, time.Second)
	if err := h.Run(t.Context()); !errors.Is(err, context.Canceled) {
		t.Fatalf("lost unrelated startup cancellation: %v", err)
	}
}

func TestStartupPanicCancellation(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cancel      bool
		logged      bool
		cause       error
		wantFailure bool
	}{
		{"error cancellation", true, false, context.Canceled, false},
		{"logged cancellation", true, true, context.Canceled, false},
		{"unrelated cancellation", false, true, context.Canceled, true},
		{"error during shutdown", true, false, errors.New("database failure"), true},
		{"logged error during shutdown", true, true, errors.New("database failure"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var cleaned atomic.Bool
			h := New(func(p *process.ProcessContext) error {
				p.Defer(func() { cleaned.Store(true) })
				if tc.cancel {
					p.ShutdownZendrite()
				}
				if tc.logged {
					logrus.WithError(tc.cause).Panic("startup failed")
				}
				panic(fmt.Errorf("startup failed: %w", tc.cause))
			}, time.Second)
			err := h.Run(t.Context())
			if tc.wantFailure {
				if !errors.Is(err, tc.cause) {
					t.Fatalf("lost panic cause: %v", err)
				}
			} else if err != nil {
				t.Fatalf("graceful cancellation failed: %v", err)
			}
			if !cleaned.Load() || h.State() != Stopped {
				t.Fatal("startup panic skipped cleanup")
			}
		})
	}
}
