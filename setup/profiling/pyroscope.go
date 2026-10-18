// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

// Package profiling configures continuous profiling for Zendrite.
package profiling

import (
	"fmt"

	"github.com/grafana/pyroscope-go"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// Start starts a profiler, or returns nil when profiling is disabled.
// The caller must call Flush(true), then Stop on a non-nil profiler on shutdown.
func Start(cfg config.Pyroscope) (*pyroscope.Profiler, error) {
	if !cfg.Enabled {
		return nil, nil
	}
	var configErrs config.ConfigErrors
	cfg.Verify(&configErrs)
	if len(configErrs) > 0 {
		return nil, fmt.Errorf("invalid Pyroscope configuration: %w", configErrs)
	}
	profiler, err := pyroscope.Start(pyroscope.Config{
		ApplicationName:   cfg.ApplicationName,
		ServerAddress:     cfg.ServerAddress,
		BasicAuthUser:     cfg.BasicAuth.Username,
		BasicAuthPassword: cfg.BasicAuth.Password,
		TenantID:          cfg.TenantID,
		Tags:              cfg.Tags,
		Logger:            logrus.StandardLogger(),
		ProfileTypes: []pyroscope.ProfileType{
			pyroscope.ProfileCPU,
			pyroscope.ProfileAllocObjects,
			pyroscope.ProfileAllocSpace,
			pyroscope.ProfileInuseObjects,
			pyroscope.ProfileInuseSpace,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("start Pyroscope: %w", err)
	}
	return profiler, nil
}
