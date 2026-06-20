// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package userapi

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRegisteredUsersGauge(t *testing.T) {
	registry := prometheus.NewPedanticRegistry()
	gauge := newRegisteredUsersGauge()
	gauge.Set(42)
	require.NoError(t, registry.Register(gauge))

	metricFamilies, err := registry.Gather()
	require.NoError(t, err)
	require.Len(t, metricFamilies, 1)
	assert.Equal(t, "zendrite_clientapi_reg_users_total", metricFamilies[0].GetName())
	require.Len(t, metricFamilies[0].Metric, 1)
	assert.Equal(t, float64(42), metricFamilies[0].Metric[0].GetGauge().GetValue())
}
