package process

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestWorkerCleanupOrdering(t *testing.T) {
	p := NewProcessContextWithParent(t.Context())
	var stopped atomic.Bool
	var order []int
	p.Defer(func() { order = append(order, 1) })
	p.Defer(func() {
		if !stopped.Load() {
			t.Error("cleanup preceded worker exit")
		}
		order = append(order, 2)
	})
	child, cancel := context.WithCancelCause(p.Context())
	defer cancel(nil)
	if !Go(child, func() { <-child.Done(); stopped.Store(true) }) {
		t.Fatal("worker refused")
	}
	p.ShutdownZendrite()
	if p.Go(func() { t.Error("started after shutdown") }) || p.Do(func() { t.Error("admitted after shutdown") }) {
		t.Fatal("accepted new work")
	}
	p.WaitForComponentsToFinish()
	p.WaitForComponentsToFinish()
	if len(order) != 2 || order[0] != 2 || order[1] != 1 {
		t.Fatalf("cleanup order: %v", order)
	}
}

func TestConcurrentAdmissionAndShutdown(t *testing.T) {
	for range 100 {
		p := NewProcessContextWithParent(t.Context())
		var callers sync.WaitGroup
		for range 20 {
			callers.Go(func() { p.Go(func() { <-p.WaitForShutdown() }) })
		}
		p.ShutdownZendrite()
		p.WaitForComponentsToFinish()
		callers.Wait()
	}
}

func TestRepeatCancellation(t *testing.T) {
	p := NewProcessContextWithParent(t.Context())
	var calls atomic.Int32
	Repeat(p.Context(), time.Hour, time.Hour, func() { calls.Add(1) })
	p.ShutdownZendrite()
	p.WaitForComponentsToFinish()
	if calls.Load() != 0 {
		t.Fatal("maintenance ran after cancellation")
	}
}

func TestDegraded(t *testing.T) {
	p := NewProcessContext()
	p.Degraded(errors.New("storage unavailable"))
	degraded, reasons := p.IsDegraded()
	if !degraded || len(reasons) != 1 {
		t.Fatalf("degraded=%v reasons=%v", degraded, reasons)
	}
}

func TestBackgroundWorkRetainsApplicationLifetime(t *testing.T) {
	p := NewProcessContextWithParent(t.Context())
	request, cancel := context.WithCancelCause(p.Context())
	lifetime := Lifetime(request)
	cancel(nil)
	if lifetime.Err() != nil {
		t.Fatal("request cancellation canceled application background work")
	}
	ran := make(chan struct{})
	Repeat(lifetime, 0, time.Hour, func() { close(ran) })
	select {
	case <-ran:
	case <-time.After(time.Second):
		t.Fatal("background task did not run")
	}
	p.ShutdownZendrite()
	p.WaitForComponentsToFinish()
	if lifetime.Err() == nil {
		t.Fatal("background task escaped application cancellation")
	}
	if Do(lifetime, func() { t.Error("callback admitted after cancellation") }) {
		t.Fatal("admitted canceled callback")
	}
}

func TestAdmittedCallbackIsJoined(t *testing.T) {
	p := NewProcessContextWithParent(t.Context())
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var callbackFinished atomic.Bool
	p.Defer(func() {
		if !callbackFinished.Load() {
			t.Error("cleanup ran before callback finished")
		}
	})
	go func() {
		p.Do(func() { close(entered); <-release; callbackFinished.Store(true) })
		close(done)
	}()
	<-entered
	p.ShutdownZendrite()
	close(release)
	p.WaitForComponentsToFinish()
	<-done
}

func TestServeCountsRequestsInFlight(t *testing.T) {
	p := NewProcessContext()
	defer p.ShutdownZendrite()
	release := make(chan struct{})
	serving := make(chan struct{})
	go p.Serve(func() { close(serving); <-release })
	<-serving
	if got := p.InFlight(); got != 1 {
		t.Fatalf("in flight: %d, want 1", got)
	}
	close(release)
	deadline := time.Now().Add(5 * time.Second)
	for p.InFlight() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("a finished request is still counted")
		}
		time.Sleep(time.Millisecond)
	}
	p.ShutdownZendrite()
	if p.Serve(func() { t.Error("served after shutdown") }) || p.InFlight() != 0 {
		t.Fatal("a refused request must not stay counted")
	}
}

func TestQuiescing(t *testing.T) {
	if Quiescing(context.Background()) != nil {
		t.Fatal("work without an application owner is never quiesced")
	}
	p := NewProcessContext()
	defer p.ShutdownZendrite()
	quiescing := Quiescing(p.Context())
	select {
	case <-quiescing:
		t.Fatal("quiesced before a handover")
	default:
	}
	p.Quiesce()
	p.Quiesce()
	<-quiescing
}
