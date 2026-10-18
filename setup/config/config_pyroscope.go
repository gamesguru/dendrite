// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package config

import "net/url"

// Pyroscope configures opt-in continuous CPU and memory profiling.
type Pyroscope struct {
	Enabled         bool   `yaml:"enabled"`
	ApplicationName string `yaml:"application_name"`
	ServerAddress   string `yaml:"server_address"`
	BasicAuth       struct {
		Username string `yaml:"username"`
		Password string `yaml:"password"`
	} `yaml:"basic_auth"`
	TenantID string            `yaml:"tenant_id"`
	Tags     map[string]string `yaml:"tags"`
}

// Defaults sets the default application name and disables profiling.
func (c *Pyroscope) Defaults() {
	c.Enabled = false
	c.ApplicationName = "zendrite"
}

// Verify checks the profiling configuration when enabled.
func (c *Pyroscope) Verify(configErrs *ConfigErrors) {
	if !c.Enabled {
		return
	}
	checkNotEmpty(configErrs, "global.pyroscope.application_name", c.ApplicationName)
	u, err := url.Parse(c.ServerAddress)
	if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
		configErrs.Add("global.pyroscope.server_address must be an absolute HTTP or HTTPS URL")
	}
}
