// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
)

// wantFociJSON is the shape clients parse: a flat livekit_service_url on the
// focus object, per element-hq/lk-jwt-service's documented well-known block
// and matrix-js-sdk's isLivekitTransportConfig. Anything nested is ignored by
// clients, which then fail with MISSING_MATRIX_RTC_TRANSPORT.
var wantFociJSON = []any{
	map[string]any{
		"type":                "livekit",
		"livekit_service_url": "https://new.example.com/livekit/jwt",
	},
	map[string]any{
		"type":                "livekit",
		"livekit_service_url": "https://old.example.com/livekit/jwt",
	},
}

// testFoci pairs the flat config field with the deprecated nested one, which
// must still reach clients in the flat wire shape.
func testFoci() []config.RTCFocus {
	return []config.RTCFocus{
		{Type: "livekit", LiveKitServiceURL: "https://new.example.com/livekit/jwt"},
		{Type: "livekit", LiveKit: &config.LiveKitFocus{ServiceURL: "https://old.example.com/livekit/jwt"}},
	}
}

func TestWellKnownRTCFoci(t *testing.T) {
	t.Parallel()

	var cfg config.Zendrite
	cfg.Defaults(config.DefaultOpts{})
	cfg.Global.WellKnownClientName = "https://matrix.example.com"
	cfg.Global.RTCFoci = testFoci()

	routers := httputil.NewRouters()
	Setup(routers, &cfg, nil, nil, &discoveryUserAPI{}, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

	req := httptest.NewRequest(http.MethodGet, "/.well-known/matrix/client", nil)
	rec := httptest.NewRecorder()
	routers.WellKnown.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var got map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, wantFociJSON, got["org.matrix.msc4143.rtc_foci"])
}

// TestWellKnownWithoutRTCFoci keeps the key out of the response entirely when
// no focus is configured, rather than advertising an empty list.
func TestWellKnownWithoutRTCFoci(t *testing.T) {
	t.Parallel()

	var cfg config.Zendrite
	cfg.Defaults(config.DefaultOpts{})
	cfg.Global.WellKnownClientName = "https://matrix.example.com"

	routers := httputil.NewRouters()
	Setup(routers, &cfg, nil, nil, &discoveryUserAPI{}, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

	req := httptest.NewRequest(http.MethodGet, "/.well-known/matrix/client", nil)
	rec := httptest.NewRecorder()
	routers.WellKnown.ServeHTTP(rec, req)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var got map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.NotContains(t, got, "org.matrix.msc4143.rtc_foci")
}

func TestGetRTCTransports(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		foci []config.RTCFocus
		want any
	}{
		{name: "configured foci", foci: testFoci(), want: wantFociJSON},
		{name: "no foci", foci: nil, want: []any{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			response := GetRTCTransports(tc.foci)
			require.Equal(t, http.StatusOK, response.Code)

			data, err := json.Marshal(response.JSON)
			require.NoError(t, err)

			var got map[string]any
			require.NoError(t, json.Unmarshal(data, &got))
			assert.Equal(t, map[string]any{"rtc_transports": tc.want}, got)
		})
	}
}
