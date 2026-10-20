//go:build linux

package hachaos

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/goccy/go-yaml"
	"golang.org/x/crypto/bcrypt"

	"codefloe.com/pat-s/zendrite/setup/config"
)

const (
	haServerName     = "ha.hachaos.test:8448"
	remoteServerName = "remote.hachaos.test:8448"
)

// timings are the HA election settings under test.
type timings struct {
	Lease     time.Duration `json:"lease_duration"`
	Renew     time.Duration `json:"renew_deadline"`
	Retry     time.Duration `json:"retry_period"`
	Ownership time.Duration `json:"ownership_timeout"`
}

// referenceTimings are the documented defaults.
var referenceTimings = timings{Lease: 30 * time.Second, Renew: 20 * time.Second, Retry: 5 * time.Second, Ownership: 45 * time.Second}

// slot is one candidate position, the equivalent of a pod in a Deployment.
// Its supervisor restarts the process whenever it exits, as the kubelet
// restarts a container, so a candidate that loses ownership and exits comes
// back as a fresh standby.
type slot struct {
	index      int
	name       string
	dir        string
	config     string
	httpPort   int
	statusPort int
	// pprofPort serves the candidate's goroutine dumps, so a request that
	// hangs can be traced to where the server waits.
	pprofPort int
	env       []string
	logs      string
	owners    *owners
	timeline  *timeline

	mu        sync.Mutex
	bin       string
	proc      *process
	supervise bool
	restartIn time.Duration
	starts    int
	wake      chan struct{}
	done      chan struct{}
}

func (s *slot) httpURL() string   { return fmt.Sprintf("http://127.0.0.1:%d", s.httpPort) }
func (s *slot) statusURL() string { return fmt.Sprintf("http://127.0.0.1:%d", s.statusPort) }

// goroutines returns the running candidate's goroutine dump.
func (s *slot) goroutines(ctx context.Context) ([]byte, error) {
	if s.pprofPort == 0 {
		return nil, errors.New("no pprof listener")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/debug/pprof/goroutine?debug=2", s.pprofPort), nil)
	if err != nil {
		return nil, err
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return io.ReadAll(resp.Body)
}

func (s *slot) current() *process {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.proc
}

func (s *slot) setBinary(bin string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bin = bin
}

// start launches the process and keeps it running until halt is called.
func (s *slot) start() error {
	if err := s.launch(); err != nil {
		return err
	}
	s.mu.Lock()
	s.supervise = true
	s.wake = make(chan struct{}, 1)
	s.done = make(chan struct{})
	s.mu.Unlock()
	go s.superviseLoop()
	return nil
}

func (s *slot) launch() error {
	s.mu.Lock()
	bin := s.bin
	s.mu.Unlock()
	proc, err := startProcess(s.logs, processSpec{
		name: s.name, bin: bin, dir: s.dir, env: s.env,
		args: []string{"--config", s.config, "--http-bind-address", fmt.Sprintf("127.0.0.1:%d", s.httpPort), "--really-enable-open-registration"},
	})
	if err != nil {
		return err
	}
	s.owners.track(proc.pid(), s.name)
	s.mu.Lock()
	s.proc = proc
	s.starts++
	s.mu.Unlock()
	s.timeline.add(eventProc, s.name, "started pid %d", proc.pid())
	return nil
}

func (s *slot) superviseLoop() {
	defer close(s.done)
	for {
		proc := s.current()
		select {
		case <-proc.done:
		case <-s.wake:
			return
		}
		code, _ := proc.exitCode()
		s.owners.forget(proc.pid())
		s.timeline.add(eventProc, s.name, "pid %d exited with status %d", proc.pid(), code)
		s.mu.Lock()
		supervise, delay := s.supervise, s.restartIn
		s.mu.Unlock()
		if !supervise {
			return
		}
		select {
		case <-time.After(delay):
		case <-s.wake:
			return
		}
		if err := s.launch(); err != nil {
			s.timeline.add(eventProc, s.name, "restart failed: %v", err)
			return
		}
	}
}

// halt stops supervising and terminates the process: SIGTERM when graceful,
// otherwise SIGKILL.
func (s *slot) halt(graceful bool) {
	s.mu.Lock()
	wasSupervised := s.supervise
	s.supervise = false
	wake, done := s.wake, s.done
	s.mu.Unlock()
	if wasSupervised && wake != nil {
		select {
		case wake <- struct{}{}:
		default:
		}
	}
	if proc := s.current(); proc != nil {
		if graceful {
			proc.stop(45 * time.Second)
		} else {
			proc.kill()
		}
		s.owners.forget(proc.pid())
	}
	if done != nil {
		<-done
	}
}

// candidateStatus is the body of /_zendrite/monitor/status.
type candidateStatus struct {
	Phase    string         `json:"phase"`
	Leader   bool           `json:"leader"`
	Ready    bool           `json:"ready"`
	Reason   string         `json:"reason"`
	Boundary string         `json:"blocked_boundary"`
	Upgrade  *upgradeStatus `json:"upgrade"`
}

// upgradeStatus is a candidate's upgrade contract against the deployment.
type upgradeStatus struct {
	Version         string `json:"version"`
	Level           int    `json:"level"`
	DeploymentLevel int    `json:"deployment_level"`
	DeploymentFloor int    `json:"deployment_floor"`
	Prepared        bool   `json:"prepared"`
	Compatible      bool   `json:"compatible"`
	Reason          string `json:"reason"`
}

// prepared reports whether a standby may take over, as its readiness does.
func (st candidateStatus) prepared() bool {
	return st.Phase == "standby" && st.Reason == "" && st.Upgrade != nil && st.Upgrade.Prepared
}

var probeClient = &http.Client{Timeout: 500 * time.Millisecond}

func (s *slot) status(ctx context.Context) (candidateStatus, error) {
	var st candidateStatus
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.statusURL()+"/_zendrite/monitor/status", nil)
	if err != nil {
		return st, err
	}
	resp, err := probeClient.Do(req)
	if err != nil {
		return st, err
	}
	defer resp.Body.Close()
	err = json.NewDecoder(resp.Body).Decode(&st)
	return st, err
}

// health reports the active-only readiness probe's status code, which is
// what the router uses to select the backend, or zero if it failed.
func (s *slot) health(ctx context.Context) int {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.statusURL()+"/_zendrite/monitor/health", nil)
	if err != nil {
		return 0
	}
	resp, err := probeClient.Do(req)
	if err != nil {
		return 0
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// haConfig renders a candidate configuration. Everything that enters the HA
// compatibility fingerprint is identical across candidates; only the
// candidate identity, status address and local search workspace differ.
func haConfig(e *environment, s *slot) ([]byte, error) {
	cfg := &config.Zendrite{Version: config.Version}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	cfg.Global.ServerName = spec.ServerName(haServerName)
	cfg.Global.PrivateKeyPath = config.Path(e.haKey)
	cfg.Global.DatabaseOptions = config.DatabaseOptions{ConnectionString: config.DataSource(e.pg.dsn()), MaxOpenConnections: 40, MaxIdleConnections: 5, ConnMaxLifetimeSeconds: -1}
	cfg.Global.JetStream.Addresses = e.nats.candidateAddresses()
	cfg.Global.JetStream.TopicPrefix = topicPrefix
	cfg.Global.JetStream.Replicas = 3
	// Generated configurations skip TLS verification, which also forces TLS;
	// the harness brokers are plaintext.
	cfg.Global.JetStream.DisableTLSValidation = false
	cfg.Global.JetStream.StoragePath = config.Path(filepath.Join(e.dir, "shared"))
	cfg.Global.HA = config.HA{
		Enabled: true, Backend: "kubernetes", LeaseName: leaseName, LeaseNamespace: leaseNamespace, CandidateID: s.name,
		LeaseDuration: e.timings.Lease, RenewDeadline: e.timings.Renew, RetryPeriod: e.timings.Retry, OwnershipTimeout: e.timings.Ownership,
		StatusAddress: fmt.Sprintf("127.0.0.1:%d", s.statusPort),
		// The defaults, which a literal would otherwise leave disabled.
		HandoverGrace: 2 * time.Second, HandoverTimeout: 15 * time.Second,
	}
	commonConfig(cfg)
	cfg.MSCs.MSCs = e.settings.mscs
	cfg.MediaAPI.Storage.Type = "s3"
	cfg.MediaAPI.Storage.S3 = config.S3Storage{Endpoint: e.s3.endpoint(), Region: "us-east-1", Bucket: s3Bucket, AccessKeyID: "hachaos", SecretAccessKey: "hachaos", PathStyle: true}
	cfg.SyncAPI.Fulltext = config.Fulltext{Enabled: true, RebuildOnStartup: true, IndexPath: config.Path(filepath.Join(s.dir, "searchindex")), Language: "en"}
	return yaml.Marshal(cfg)
}

// remoteConfig renders the standalone federation peer, which keeps its own
// SQLite databases, embedded broker and local media so that faults injected
// into the HA dependencies never reach it.
func remoteConfig(e *environment, dir string) ([]byte, error) {
	cfg := &config.Zendrite{Version: config.Version}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: false})
	cfg.Global.ServerName = spec.ServerName(remoteServerName)
	cfg.Global.PrivateKeyPath = config.Path(e.remoteKey)
	for name, db := range map[string]*config.DatabaseOptions{
		"federationapi": &cfg.FederationAPI.Database, "keyserver": &cfg.KeyServer.Database, "mscs": &cfg.MSCs.Database,
		"mediaapi": &cfg.MediaAPI.Database, "roomserver": &cfg.RoomServer.Database, "syncapi": &cfg.SyncAPI.Database,
		"userapi": &cfg.UserAPI.AccountDatabase, "relayapi": &cfg.RelayAPI.Database,
	} {
		db.ConnectionString = config.DataSource("file:" + filepath.Join(dir, name+".db"))
	}
	cfg.Global.JetStream.StoragePath = config.Path(dir)
	cfg.Global.JetStream.TopicPrefix = "Remote"
	commonConfig(cfg)
	cfg.MSCs.MSCs = e.settings.mscs
	cfg.MediaAPI.Storage.Type = "local"
	cfg.MediaAPI.Storage.Local.BasePath = config.Path(filepath.Join(dir, "media"))
	cfg.SyncAPI.Fulltext = config.Fulltext{Enabled: false, IndexPath: config.Path(filepath.Join(dir, "searchindex")), Language: "en"}
	return yaml.Marshal(cfg)
}

func commonConfig(cfg *config.Zendrite) {
	cfg.Global.TrustedIDServers = nil
	cfg.Global.ReportStats.Enabled = false
	cfg.Global.Presence = config.PresenceOptions{}
	cfg.Global.Metrics.Enabled = true
	cfg.Global.Metrics.BasicAuth.Username = "metrics"
	cfg.Global.Metrics.BasicAuth.Password = "metrics"
	cfg.ClientAPI.RegistrationDisabled = false
	cfg.ClientAPI.RateLimiting.Enabled = false
	cfg.FederationAPI.DisableTLSValidation = true
	cfg.FederationAPI.DenyNetworkCIDRs = []string{}
	cfg.FederationAPI.AllowNetworkCIDRs = []string{"0.0.0.0/0", "::/0"}
	cfg.FederationAPI.KeyPerspectives = config.KeyPerspectives{}
	cfg.FederationAPI.PreferDirectFetch = true
	cfg.UserAPI.BCryptCost = bcrypt.MinCost
	cfg.MSCs.MSCs = nil
	cfg.Logging = []config.LogrusHook{{Type: "std", Level: "info"}}
}

func writeConfig(path string, body []byte) error {
	return os.WriteFile(path, body, 0o600)
}
