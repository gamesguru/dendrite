package base_test

import (
	"bytes"
	"context"
	"embed"
	"html/template"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	basepkg "codefloe.com/pat-s/zendrite/setup/base"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
)

//go:embed static/*.gotmpl
var staticContent embed.FS

// waitForListener polls until the listener started by SetupAndServeHTTP accepts
// connections. SetupAndServeHTTP binds asynchronously, so sleeping for a fixed
// duration races with the bind and makes the tests flaky on loaded machines.
func waitForListener(t *testing.T, network, address string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, err := net.Dial(network, address)
		if err == nil {
			_ = conn.Close()
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("listener on %s://%s never became ready: %s", network, address, err)
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func TestLandingPage_Tcp(t *testing.T) {
	// generate the expected result
	tmpl := template.Must(template.ParseFS(staticContent, "static/*.gotmpl"))
	expectedRes := &bytes.Buffer{}
	err := tmpl.ExecuteTemplate(expectedRes, "index.gotmpl", map[string]string{
		"Version": internal.VersionString(),
	})
	assert.NoError(t, err)

	processCtx := process.NewProcessContext()
	routers := httputil.NewRouters()
	cfg := config.Zendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})

	// hack: create a server and close it immediately, just to get a random port assigned
	s := httptest.NewServer(nil)
	s.Close()

	// start base with the listener and wait for it to be started
	address, err := config.HTTPAddress(s.URL)
	assert.NoError(t, err)
	go basepkg.SetupAndServeHTTP(processCtx, &cfg, routers, address, nil, nil)
	waitForListener(t, address.Network(), address.Address)

	// When hitting /, we should be redirected to /_matrix/static, which should contain the landing page
	req, err := http.NewRequest(http.MethodGet, s.URL, nil)
	assert.NoError(t, err)

	// do the request
	resp, err := s.Client().Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// read the response
	buf := &bytes.Buffer{}
	_, err = buf.ReadFrom(resp.Body)
	assert.NoError(t, err)

	// Using .String() for user friendly output
	assert.Equal(t, expectedRes.String(), buf.String(), "response mismatch")
}

func TestLandingPage_UnixSocket(t *testing.T) {
	// generate the expected result
	tmpl := template.Must(template.ParseFS(staticContent, "static/*.gotmpl"))
	expectedRes := &bytes.Buffer{}
	err := tmpl.ExecuteTemplate(expectedRes, "index.gotmpl", map[string]string{
		"Version": internal.VersionString(),
	})
	assert.NoError(t, err)

	processCtx := process.NewProcessContext()
	routers := httputil.NewRouters()
	cfg := config.Zendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})

	tempDir := t.TempDir()
	socket := path.Join(tempDir, "socket")
	// start base with the listener and wait for it to be started
	address, err := config.UnixSocketAddress(socket, "755")
	assert.NoError(t, err)
	go basepkg.SetupAndServeHTTP(processCtx, &cfg, routers, address, nil, nil)
	waitForListener(t, address.Network(), address.Address)

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", socket)
			},
		},
	}
	resp, err := client.Get("http://unix/")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// read the response
	buf := &bytes.Buffer{}
	_, err = buf.ReadFrom(resp.Body)
	assert.NoError(t, err)

	// Using .String() for user friendly output
	assert.Equal(t, expectedRes.String(), buf.String(), "response mismatch")
}

func TestStartHTTPBindFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()
	address, err := config.HTTPAddress("http://" + listener.Addr().String())
	require.NoError(t, err)
	p := process.NewProcessContextWithParent(t.Context())
	cfg := config.Zendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	err = basepkg.StartHTTP(p, &cfg, httputil.NewRouters(), address, nil, nil)
	require.Error(t, err, "bind failure must be visible to activation")
	p.ShutdownZendrite()
	p.WaitForComponentsToFinish()
}

func TestHTTPShutdownJoinsRequestsBeforeCleanup(t *testing.T) {
	p := process.NewProcessContextWithParent(t.Context())
	cfg := config.Zendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	routers := httputil.NewRouters()
	entered, canceled, release, handlerFinished := make(chan struct{}), make(chan struct{}), make(chan struct{}), make(chan struct{})
	routers.Client.HandleFunc("/lifecycle-test", func(w http.ResponseWriter, r *http.Request) {
		close(entered)
		<-r.Context().Done()
		close(canceled)
		<-release
		close(handlerFinished)
	}).Methods(http.MethodGet)
	p.Defer(func() {
		select {
		case <-handlerFinished:
		default:
			t.Error("dependencies closed before HTTP request finished")
		}
	})
	probe := httptest.NewServer(nil)
	probe.Close()
	address, err := config.HTTPAddress(probe.URL)
	require.NoError(t, err)
	require.NoError(t, basepkg.StartHTTP(p, &cfg, routers, address, nil, nil))
	requestDone := make(chan struct{})
	go func() {
		defer close(requestDone)
		response, err := http.Get(probe.URL + "/_matrix/client/lifecycle-test")
		if err == nil {
			_ = response.Body.Close()
		}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("request did not enter")
	}
	p.ShutdownZendrite()
	select {
	case <-canceled:
	case <-time.After(5 * time.Second):
		t.Fatal("request context not canceled")
	}
	close(release)
	p.WaitForComponentsToFinish()
	<-requestDone
}

func TestHTTPWaitsForRecoveryReadiness(t *testing.T) {
	p := process.NewProcessContextWithParent(t.Context())
	defer func() { p.ShutdownZendrite(); p.WaitForComponentsToFinish() }()
	cfg := config.Zendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	routers := httputil.NewRouters()
	var ready atomic.Bool
	var calls atomic.Int32
	routers.Client.HandleFunc("/recovery-test", func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	}).Methods(http.MethodGet)
	probe := httptest.NewServer(nil)
	probe.Close()
	address, err := config.HTTPAddress(probe.URL)
	require.NoError(t, err)
	require.NoError(t, basepkg.StartHTTP(p, &cfg, routers, address, nil, nil, ready.Load))
	for _, tc := range []struct {
		name   string
		ready  bool
		status int
		calls  int32
	}{
		{"recovering", false, http.StatusServiceUnavailable, 0},
		{"active", true, http.StatusOK, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ready.Store(tc.ready)
			response, err := http.Get(probe.URL + "/_matrix/client/recovery-test")
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, tc.status, response.StatusCode)
			require.Equal(t, tc.calls, calls.Load())
		})
	}
}
