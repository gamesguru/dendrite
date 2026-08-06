package base_test

import (
	"bytes"
	"context"
	"embed"
	"html/template"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path"
	"testing"
	"time"

	"github.com/element-hq/dendrite/internal"
	"github.com/element-hq/dendrite/internal/httputil"
	basepkg "github.com/element-hq/dendrite/setup/base"
	"github.com/element-hq/dendrite/setup/config"
	"github.com/element-hq/dendrite/setup/process"
	"github.com/stretchr/testify/assert"
)

//go:embed static/*.gotmpl
var staticContent embed.FS

func startBaseServer(
	t *testing.T,
	processCtx *process.ProcessContext,
	cfg *config.Dendrite,
	routers httputil.Routers,
	address config.ServerAddress,
) {
	t.Helper()

	go basepkg.SetupAndServeHTTP(processCtx, cfg, routers, address, nil, nil)
	t.Cleanup(func() {
		processCtx.ShutdownDendrite()
		processCtx.WaitForComponentsToFinish()
	})
}

func waitForResponse(t *testing.T, client *http.Client, method, url string, body io.Reader) *http.Response {
	t.Helper()

	deadline := time.Now().Add(2 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(method, url, body)
		if err != nil {
			t.Fatalf("failed to build request for %s: %v", url, err)
		}
		resp, err := client.Do(req)
		if err == nil {
			return resp
		}
		lastErr = err
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %v", url, lastErr)
	return nil
}

func readBody(t *testing.T, body io.ReadCloser) string {
	t.Helper()
	defer func() {
		assert.NoError(t, body.Close())
	}()

	buf := &bytes.Buffer{}
	_, err := buf.ReadFrom(body)
	assert.NoError(t, err)
	return buf.String()
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
	cfg := config.Dendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})

	// hack: create a server and close it immediately, just to get a random port assigned
	s := httptest.NewServer(nil)
	s.Close()

	// start base with the listener and wait for it to be started
	address, err := config.HTTPAddress(s.URL)
	assert.NoError(t, err)
	startBaseServer(t, processCtx, &cfg, routers, address)

	// When hitting /, we should be redirected to /_matrix/static, which should contain the landing page
	req, err := http.NewRequest(http.MethodGet, s.URL, nil)
	assert.NoError(t, err)

	// do the request
	resp, err := s.Client().Do(req)
	if err != nil {
		resp = waitForResponse(t, s.Client(), http.MethodGet, s.URL, nil)
	}
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Using .String() for user friendly output
	assert.Equal(t, expectedRes.String(), readBody(t, resp.Body), "response mismatch")
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
	cfg := config.Dendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})

	tempDir := t.TempDir()
	socket := path.Join(tempDir, "socket")
	// start base with the listener and wait for it to be started
	address, err := config.UnixSocketAddress(socket, "755")
	assert.NoError(t, err)
	startBaseServer(t, processCtx, &cfg, routers, address)

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(_ context.Context, _, _ string) (net.Conn, error) {
				return net.Dial("unix", socket)
			},
		},
	}
	resp := waitForResponse(t, client, http.MethodGet, "http://unix/", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	// Using .String() for user friendly output
	assert.Equal(t, expectedRes.String(), readBody(t, resp.Body), "response mismatch")
}

func TestLandingPage_ZeroValueRouters(t *testing.T) {
	tmpl := template.Must(template.ParseFS(staticContent, "static/*.gotmpl"))
	expectedRes := &bytes.Buffer{}
	err := tmpl.ExecuteTemplate(expectedRes, "index.gotmpl", map[string]string{
		"Version": internal.VersionString(),
	})
	assert.NoError(t, err)

	processCtx := process.NewProcessContext()
	cfg := config.Dendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})

	s := httptest.NewServer(nil)
	s.Close()

	address, err := config.HTTPAddress(s.URL)
	assert.NoError(t, err)
	startBaseServer(t, processCtx, &cfg, httputil.Routers{}, address)

	resp := waitForResponse(t, s.Client(), http.MethodGet, s.URL+"/_matrix/static/", nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	assert.Equal(t, expectedRes.String(), readBody(t, resp.Body), "response mismatch")
}
