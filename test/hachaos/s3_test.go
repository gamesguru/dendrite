//go:build linux

package hachaos

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path/filepath"
	"sync"
	"time"
)

// s3Service is an S3 server behind an auditing proxy. The proxy records every
// mutating request with the process that sent it, which is how the harness
// detects a retired owner writing media after its successor took over.
type s3Service struct {
	bin      string
	logs     string
	db       string
	backend  string
	timeline *timeline
	owners   *owners

	proc     *process
	listener net.Listener
	server   *http.Server

	mu      sync.Mutex
	cond    *sync.Cond
	faults  map[string]fault
	effects []httpEffect
}

// httpEffect is one mutating request observed at a protected boundary.
type httpEffect struct {
	At     time.Time `json:"at"`
	Owner  string    `json:"owner"`
	Method string    `json:"method"`
	Path   string    `json:"path"`
	Status int       `json:"status"`
}

type ownerKey struct{}

func ownerFrom(ctx context.Context) string {
	if owner, ok := ctx.Value(ownerKey{}).(string); ok {
		return owner
	}
	return unknownOwner
}

const s3Bucket = "media"

func startS3(ctx context.Context, dir, logs, bin string, o *owners, tl *timeline) (*s3Service, error) {
	port, err := freePort()
	if err != nil {
		return nil, err
	}
	s := &s3Service{bin: bin, logs: logs, db: filepath.Join(dir, "objects"), backend: fmt.Sprintf("127.0.0.1:%d", port), timeline: tl, owners: o, faults: map[string]fault{}}
	s.cond = sync.NewCond(&s.mu)
	if err = s.start(ctx); err != nil {
		return nil, err
	}
	if s.listener, err = net.Listen("tcp4", "127.0.0.1:0"); err != nil {
		return nil, err
	}
	target, _ := url.Parse("http://" + s.backend)
	reverse := httputil.NewSingleHostReverseProxy(target)
	s.server = &http.Server{
		Handler:           http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { s.serve(w, r, reverse) }),
		ReadHeaderTimeout: 30 * time.Second,
		ConnContext: func(ctx context.Context, c net.Conn) context.Context {
			return context.WithValue(ctx, ownerKey{}, o.of(c))
		},
	}
	go func() { _ = s.server.Serve(s.listener) }()
	return s, nil
}

func (s *s3Service) start(ctx context.Context) error {
	proc, err := startProcess(s.logs, processSpec{name: "s3", bin: s.bin, env: []string{roleEnv + "=s3", "HACHAOS_S3_ROOT=" + s.db, "HACHAOS_S3_ADDRESS=" + s.backend}})
	if err != nil {
		return err
	}
	s.proc = proc
	s.timeline.add(eventProc, "s3", "started")
	return waitFor(ctx, 30*time.Second, func() error {
		if proc.exited() {
			return errPermanent(fmt.Errorf("S3 server exited during startup"))
		}
		conn, err := net.DialTimeout("tcp", s.backend, time.Second)
		if err != nil {
			return err
		}
		return conn.Close()
	})
}

func (s *s3Service) endpoint() string { return "http://" + s.listener.Addr().String() }

func (s *s3Service) setFault(owner string, f fault) {
	s.mu.Lock()
	if f == pass {
		delete(s.faults, owner)
	} else {
		s.faults[owner] = f
	}
	s.cond.Broadcast()
	s.mu.Unlock()
	s.timeline.add(eventFault, "s3", "%s -> %s", owner, f)
}

func (s *s3Service) heal() {
	s.mu.Lock()
	s.faults = map[string]fault{}
	s.cond.Broadcast()
	s.mu.Unlock()
	s.timeline.add(eventFault, "s3", "healed")
}

func (s *s3Service) faultFor(owner string) fault {
	if f, ok := s.faults[owner]; ok {
		return f
	}
	return s.faults[allOwners]
}

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(status int) {
	r.status = status
	r.ResponseWriter.WriteHeader(status)
}

func (s *s3Service) serve(w http.ResponseWriter, r *http.Request, reverse *httputil.ReverseProxy) {
	owner := ownerFrom(r.Context())
	if !s.admit(r.Context(), owner) {
		if hijacker, ok := w.(http.Hijacker); ok {
			if conn, _, err := hijacker.Hijack(); err == nil {
				_ = conn.Close()
				return
			}
		}
		w.WriteHeader(http.StatusBadGateway)
		return
	}
	arrived := time.Now()
	recorder := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
	reverse.ServeHTTP(recorder, r)
	if r.Method == http.MethodPut || r.Method == http.MethodPost || r.Method == http.MethodDelete {
		s.mu.Lock()
		s.effects = append(s.effects, httpEffect{At: arrived, Owner: owner, Method: r.Method, Path: r.URL.Path, Status: recorder.status})
		s.mu.Unlock()
	}
}

// admit holds a request while its owner is partitioned, and reports whether
// it may proceed.
func (s *s3Service) admit(ctx context.Context, owner string) bool {
	stop := context.AfterFunc(ctx, func() {
		s.mu.Lock()
		s.cond.Broadcast()
		s.mu.Unlock()
	})
	defer stop()
	s.mu.Lock()
	defer s.mu.Unlock()
	for {
		switch s.faultFor(owner) {
		case pass:
			return true
		case reject:
			return false
		case blackhole, dropUpstream, dropDownstream:
		}
		if ctx.Err() != nil {
			return false
		}
		s.cond.Wait()
	}
}

func (s *s3Service) mutations() []httpEffect {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]httpEffect(nil), s.effects...)
}

func (s *s3Service) stop() {
	if s.server != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = s.server.Shutdown(ctx)
	}
	if s.proc != nil {
		s.proc.stop(5 * time.Second)
	}
}
