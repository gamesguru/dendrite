// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package config

import (
	"strings"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/require"
)

func TestPyroscopeConfig(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		yaml    string
		wantErr bool
	}{
		{name: "omitted", yaml: "{}"},
		{name: "disabled", yaml: "global: {pyroscope: {server_address: invalid}}"},
		{name: "HTTP", yaml: "global: {pyroscope: {enabled: true, server_address: 'http://localhost:4040'}}"},
		{name: "HTTPS", yaml: "global: {pyroscope: {enabled: true, server_address: 'https://profiles.example.com/path'}}"},
		{name: "missing address", yaml: "global: {pyroscope: {enabled: true}}", wantErr: true},
		{name: "relative address", yaml: "global: {pyroscope: {enabled: true, server_address: '/profiles'}}", wantErr: true},
		{name: "missing host", yaml: "global: {pyroscope: {enabled: true, server_address: 'http://:4040'}}", wantErr: true},
		{name: "unsupported scheme", yaml: "global: {pyroscope: {enabled: true, server_address: 'ftp://localhost'}}", wantErr: true},
		{name: "malformed URL", yaml: "global: {pyroscope: {enabled: true, server_address: 'http://%'}}", wantErr: true},
		{name: "empty application", yaml: "global: {pyroscope: {enabled: true, server_address: 'http://localhost:4040', application_name: ''}}", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cfg Zendrite
			cfg.Defaults(DefaultOpts{})
			require.False(t, cfg.Global.Pyroscope.Enabled)
			require.Equal(t, "zendrite", cfg.Global.Pyroscope.ApplicationName)
			require.NoError(t, yaml.Unmarshal([]byte(tc.yaml), &cfg))
			var errs ConfigErrors
			cfg.Global.Verify(&errs)
			var profilingErrors []string
			for _, err := range errs {
				if strings.Contains(err, "global.pyroscope") {
					profilingErrors = append(profilingErrors, err)
				}
			}
			require.Equal(t, tc.wantErr, len(profilingErrors) > 0, "%v", errs)
		})
	}
}
