// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package profiling

import (
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
)

func TestStartDisabledOrInvalid(t *testing.T) {
	for _, tc := range []struct {
		name    string
		cfg     config.Pyroscope
		wantErr bool
	}{
		{name: "disabled"},
		{name: "invalid", cfg: config.Pyroscope{Enabled: true}, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			profiler, err := Start(tc.cfg)
			require.Nil(t, profiler)
			require.Equal(t, tc.wantErr, err != nil)
		})
	}
}

func TestPyroscopeUploads(t *testing.T) {
	// CPU profiling is process-wide, so this test must not run in parallel.
	var mu sync.Mutex
	var names []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, http.MethodPost, r.Method)
		assert.Equal(t, "/ingest", r.URL.Path)
		username, password, ok := r.BasicAuth()
		assert.True(t, ok)
		assert.Equal(t, "profiles-user", username)
		assert.Equal(t, "profiles-password", password)
		assert.Equal(t, "test-tenant", r.Header.Get("X-Scope-OrgID"))
		body, err := io.ReadAll(r.Body)
		assert.NoError(t, err)
		assert.NotEmpty(t, body)
		mu.Lock()
		names = append(names, r.URL.Query().Get("name"))
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	var cfg config.Zendrite
	cfg.Defaults(config.DefaultOpts{})
	require.NoError(t, yaml.Unmarshal([]byte(`global:
  pyroscope:
    enabled: true
    application_name: zendrite-test
    basic_auth:
      username: profiles-user
      password: profiles-password
    tenant_id: test-tenant
    tags:
      instance: test-server
`), &cfg))
	cfg.Global.Pyroscope.ServerAddress = server.URL
	profiler, err := Start(cfg.Global.Pyroscope)
	require.NoError(t, err)
	require.NotNil(t, profiler)
	profiler.Flush(true)
	require.NoError(t, profiler.Stop())

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, names, "shutdown should upload remaining profiles")
	for _, name := range names {
		assert.Contains(t, name, "zendrite-test")
		assert.Contains(t, name, "instance=test-server")
	}
}
