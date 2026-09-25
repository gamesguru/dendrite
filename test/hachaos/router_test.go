//go:build linux

package hachaos

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"sync"
	"time"
)

// router mirrors the chart's HAProxy routing layer: it checks the active-only
// health endpoint of every candidate every 500ms, starts every backend down,
// marks a backend down after one failed check and up after one success, and
// cuts in-flight requests to a backend as soon as it is marked down.
type router struct {
	listener net.Listener
	server   *http.Server
	timeline *timeline
	backends []*backend
	base     context.Context
	stopped  chan struct{}

	mu       sync.Mutex
	overlaps []time.Time
}

type backend struct {
	slot    *slot
	proxy   *httputil.ReverseProxy
	mu      sync.Mutex
	up      bool
	session context.Context
	cut     context.CancelCauseFunc
}

const routerCheckInterval = 500 * time.Millisecond

var errBackendDown = errors.New("backend marked down")

func startRouter(ctx context.Context, slots []*slot, tl *timeline) (*router, error) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	r := &router{listener: listener, timeline: tl, base: context.WithoutCancel(ctx), stopped: make(chan struct{})}
	for _, s := range slots {
		target, _ := url.Parse(s.httpURL())
		b := &backend{slot: s, proxy: httputil.NewSingleHostReverseProxy(target)}
		b.session, b.cut = context.WithCancelCause(r.base)
		r.backends = append(r.backends, b)
	}
	r.server = &http.Server{Handler: http.HandlerFunc(r.serve), ReadHeaderTimeout: 30 * time.Second}
	go func() { _ = r.server.Serve(listener) }()
	go r.check(context.WithoutCancel(ctx))
	return r, nil
}

func (r *router) url() string { return "http://" + r.listener.Addr().String() }

func (r *router) check(ctx context.Context) {
	ticker := time.NewTicker(routerCheckInterval)
	defer ticker.Stop()
	for {
		select {
		case <-r.stopped:
			return
		case <-ticker.C:
		}
		var wg sync.WaitGroup
		for _, b := range r.backends {
			wg.Add(1)
			go func() {
				defer wg.Done()
				probe, cancel := context.WithTimeout(ctx, routerCheckInterval)
				defer cancel()
				r.mark(b, b.slot.healthy(probe))
			}()
		}
		wg.Wait()
		up := 0
		for _, b := range r.backends {
			if b.isUp() {
				up++
			}
		}
		if up > 1 {
			r.mu.Lock()
			r.overlaps = append(r.overlaps, time.Now())
			r.mu.Unlock()
			r.timeline.add(eventRouter, "router", "%d backends passed the active-only check at once", up)
		}
	}
}

func (r *router) mark(b *backend, healthy bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if healthy == b.up {
		return
	}
	b.up = healthy
	if healthy {
		r.timeline.add(eventRouter, b.slot.name, "up")
		return
	}
	// on-marked-down shutdown-sessions: close streams so clients retry.
	b.cut(errBackendDown)
	b.session, b.cut = context.WithCancelCause(r.base)
	r.timeline.add(eventRouter, b.slot.name, "down")
}

func (b *backend) isUp() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.up
}

func (r *router) active() *backend {
	for _, b := range r.backends {
		if b.isUp() {
			return b
		}
	}
	return nil
}

// activeSlot reports the candidate currently receiving traffic, if any.
func (r *router) activeSlot() *slot {
	if b := r.active(); b != nil {
		return b.slot
	}
	return nil
}

func (r *router) serve(w http.ResponseWriter, req *http.Request) {
	b := r.active()
	if b == nil {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"errcode":"M_UNKNOWN","error":"no active candidate"}`))
		return
	}
	b.mu.Lock()
	session := b.session
	b.mu.Unlock()
	ctx, cancel := context.WithCancelCause(req.Context())
	defer cancel(nil)
	// The backend session is deliberately separate from the request: marking
	// the backend down must cut requests that are already in flight.
	stop := context.AfterFunc(session, func() { cancel(errBackendDown) }) //nolint:contextcheck // See above.
	defer stop()
	b.proxy.ServeHTTP(w, req.WithContext(ctx))
}

func (r *router) routingOverlaps() []time.Time {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]time.Time(nil), r.overlaps...)
}

func (r *router) stop() {
	close(r.stopped)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = r.server.Shutdown(ctx)
}
