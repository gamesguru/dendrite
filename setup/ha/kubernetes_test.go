package ha

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
	ktesting "k8s.io/client-go/testing"

	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/lifecycle"
	"codefloe.com/pat-s/zendrite/setup/process"
)

func electionConfig() config.HA {
	c := config.HA{}
	c.Defaults()
	c.Enabled = true
	c.CandidateID = "pod"
	c.LeaseName = "homeserver"
	c.LeaseNamespace = "matrix"
	return c
}

// leaseClient adds API-server resourceVersion conflict semantics to the fake.
// Reactors run under the fake's mutex, so the compare and update are atomic.
func leaseClient() *fake.Clientset {
	client := fake.NewClientset()
	var revision int
	client.PrependReactor("update", "leases", func(action ktesting.Action) (bool, runtime.Object, error) {
		updateAction, ok := action.(ktesting.UpdateAction)
		if !ok {
			return true, nil, fmt.Errorf("expected update action, got %T", action)
		}
		update, ok := updateAction.GetObject().(*coordinationv1.Lease)
		if !ok {
			return true, nil, fmt.Errorf("expected Lease, got %T", updateAction.GetObject())
		}
		old, err := client.Tracker().Get(action.GetResource(), action.GetNamespace(), update.Name)
		if err != nil {
			return true, nil, err
		}
		oldLease, ok := old.(*coordinationv1.Lease)
		if !ok {
			return true, nil, fmt.Errorf("expected stored Lease, got %T", old)
		}
		if oldLease.ResourceVersion != update.ResourceVersion {
			return true, nil, apierrors.NewConflict(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, update.Name, errors.New("resourceVersion changed"))
		}
		revision++
		updated := update.DeepCopy()
		updated.ResourceVersion = strconv.Itoa(revision)
		err = client.Tracker().Update(action.GetResource(), updated, action.GetNamespace())
		return true, updated, err
	})
	return client
}

func TestKubernetesContention(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := leaseClient()
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		var starts atomic.Int32
		var fences atomic.Int32
		identities := make(chan string, 2)
		gate := gateFunc(func(_ context.Context, c Candidate) error {
			fences.Add(1)
			identities <- c.Identity
			return nil
		})
		done := make(chan error, 2)
		servers := make([]*lifecycle.Homeserver, 2)
		for i := range servers {
			server := lifecycle.New(func(*process.ProcessContext) error { starts.Add(1); return nil }, time.Second)
			servers[i] = server
			go func() {
				done <- runKubernetes(ctx, electionConfig(), client.CoordinationV1(), server, gate, time.Second, NewStatus())
			}()
		}
		time.Sleep(time.Minute)
		synctest.Wait()
		if starts.Load() != 1 || fences.Load() != 1 {
			t.Fatalf("starts=%d fences=%d", starts.Load(), fences.Load())
		}
		ready := 0
		for _, server := range servers {
			if server.Ready() {
				ready++
			}
		}
		if ready != 1 {
			t.Fatalf("ready candidates=%d", ready)
		}
		identity := <-identities
		lease, err := client.CoordinationV1().Leases("matrix").Get(ctx, "homeserver", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if identity == "pod" || *lease.Spec.HolderIdentity != identity {
			t.Fatalf("unexpected identity %q", identity)
		}
		cancel(nil)
		for range servers {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
		// Cancellation must not clear the Lease while protected work might be draining.
		lease, err = client.CoordinationV1().Leases("matrix").Get(t.Context(), "homeserver", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if *lease.Spec.HolderIdentity != identity {
			t.Fatal("leadership released on cancellation")
		}
	})
}

// reachableGate is a gate whose database reachability the test controls.
type reachableGate struct {
	gateFunc
	unreachable *atomic.Bool
}

func (g reachableGate) Reachable(context.Context) error {
	if g.unreachable.Load() {
		return errors.New("database unreachable")
	}
	return nil
}

// A candidate cut off from the database must leave the Lease to one that can
// fence, but a holder that loses the database keeps renewing: giving it up is
// the gate's decision, made only after the freshness window.
func TestKubernetesAcquiresOnlyWhenDatabaseReachable(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		client := leaseClient()
		ctx, cancel := context.WithCancelCause(t.Context())
		defer cancel(nil)
		fenced := make(chan string, 4)
		fence := gateFunc(func(_ context.Context, c Candidate) error {
			fenced <- c.Identity
			return nil
		})
		var firstUnreachable, secondUnreachable atomic.Bool
		firstUnreachable.Store(true)
		run := func(unreachable *atomic.Bool, done chan<- error) *lifecycle.Homeserver {
			server := lifecycle.New(func(*process.ProcessContext) error { return nil }, time.Second)
			go func() {
				done <- runKubernetes(ctx, electionConfig(), client.CoordinationV1(), server, reachableGate{fence, unreachable}, time.Second, NewStatus())
			}()
			return server
		}
		done := make(chan error, 2)
		// The cut-off candidate starts first, so without the check it would
		// take the free Lease.
		first := run(&firstUnreachable, done)
		time.Sleep(time.Second)
		second := run(&secondUnreachable, done)
		time.Sleep(time.Minute)
		synctest.Wait()
		if first.Ready() || !second.Ready() {
			t.Fatalf("cut-off candidate ready=%v, healthy candidate ready=%v", first.Ready(), second.Ready())
		}
		holder := <-fenced
		if len(fenced) != 0 {
			t.Fatalf("%d further candidates fenced", len(fenced))
		}

		// The holder now loses the database while the other candidate
		// regains it: the Lease must stay with the holder.
		secondUnreachable.Store(true)
		firstUnreachable.Store(false)
		time.Sleep(time.Minute)
		synctest.Wait()
		lease, err := client.CoordinationV1().Leases("matrix").Get(ctx, "homeserver", metav1.GetOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if *lease.Spec.HolderIdentity != holder || len(fenced) != 0 || !second.Ready() {
			t.Fatalf("holder=%q, want %q; further fences=%d", *lease.Spec.HolderIdentity, holder, len(fenced))
		}
		cancel(nil)
		for range 2 {
			if err := <-done; err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestKubernetesFailures(t *testing.T) {
	for _, scenario := range []string{"get denied", "create denied", "update denied", "API outage", "renewal failure"} {
		t.Run(scenario, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client := leaseClient()
				ctx, cancel := context.WithCancelCause(t.Context())
				defer cancel(nil)
				var unavailable atomic.Bool
				client.PrependReactor("*", "leases", func(action ktesting.Action) (bool, runtime.Object, error) {
					if scenario == action.GetVerb()+" denied" {
						return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "coordination.k8s.io", Resource: "leases"}, "homeserver", errors.New("RBAC denied"))
					}
					if scenario == "API outage" || unavailable.Load() {
						return true, nil, fmt.Errorf("API unavailable")
					}
					return false, nil, nil
				})
				var starts atomic.Int32
				server := lifecycle.New(func(*process.ProcessContext) error { starts.Add(1); return nil }, time.Second)
				done := make(chan error, 1)
				go func() {
					done <- runKubernetes(ctx, electionConfig(), client.CoordinationV1(), server, gateFunc(func(context.Context, Candidate) error { return nil }), time.Second, NewStatus())
				}()
				synctest.Wait()
				if scenario == "renewal failure" {
					if !server.Ready() {
						t.Fatal("leader did not activate")
					}
					unavailable.Store(true)
				}
				time.Sleep(time.Minute)
				synctest.Wait()
				if scenario == "API outage" {
					if starts.Load() != 0 {
						t.Fatal("API outage allowed startup")
					}
					select {
					case err := <-done:
						t.Fatalf("outage killed standby: %v", err)
					default:
					}
					cancel(nil)
					if err := <-done; err != nil {
						t.Fatal(err)
					}
				} else {
					err := <-done
					if err == nil {
						t.Fatal("expected failure")
					}
					if scenario == "renewal failure" && !errors.Is(err, ErrLeadershipLost) {
						t.Fatal(err)
					}
				}
				if server.Ready() {
					t.Fatal("failed candidate is ready")
				}
			})
		})
	}
}

// delayedTransport models API latency without wall-clock sleeps or network I/O.
type delayedTransport struct{ latency time.Duration }

func (d delayedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	timer := time.NewTimer(d.latency)
	defer timer.Stop()
	select {
	case <-req.Context().Done():
		return nil, req.Context().Err()
	case <-timer.C:
		return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(`{"apiVersion":"coordination.k8s.io/v1","kind":"Lease","metadata":{"name":"matrix"}}`)), Request: req}, nil
	}
}

func TestLeaseRequestRenewalWindow(t *testing.T) {
	for _, latency := range []time.Duration{2 * time.Second, 5 * time.Second} {
		t.Run(latency.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				// A request slower than RetryPeriod (1s) can still fit RenewDeadline (4s).
				original := &rest.Config{Host: "http://kubernetes.invalid", Timeout: time.Second, Transport: delayedTransport{latency: latency}}
				client, err := newLeaseClient(original, 4*time.Second)
				if err != nil {
					t.Fatal(err)
				}
				lease, err := client.Leases("matrix").Get(t.Context(), "matrix", metav1.GetOptions{})
				if latency < 4*time.Second {
					if err != nil {
						t.Fatalf("slow request inside renewal window failed: %v", err)
					}
					if lease.Name != "matrix" {
						t.Fatalf("unexpected lease: %+v", lease)
					}
				} else if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("request exceeded renewal window without timing out: %v", err)
				}
				if original.Timeout != time.Second {
					t.Fatal("mutated shared REST config")
				}
			})
		})
	}
}

func TestInvalidHAStopsApplication(t *testing.T) {
	server := lifecycle.New(func(*process.ProcessContext) error { t.Error("invalid HA initialized application"); return nil }, time.Second)
	if err := Run(t.Context(), config.HA{Enabled: true}, nil, server, nil, time.Second); err == nil {
		t.Fatal("invalid HA accepted")
	}
	if server.State() != lifecycle.Stopped {
		t.Fatal("early configuration failure did not stop application")
	}
}
