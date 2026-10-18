// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"encoding/json"
	"maps"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// Any user API call panics: discovery must work without authentication or storage.
type discoveryUserAPI struct {
	userapi.ClientUserAPI
}

func TestVersions(t *testing.T) {
	t.Parallel()

	baseFeatures := map[string]bool{
		"org.matrix.e2e_cross_signing":  true,
		"org.matrix.msc2285.stable":     true,
		"org.matrix.msc3916.stable":     true,
		"org.matrix.simplified_msc3575": true,
		"org.matrix.msc4140":            false,
		"org.matrix.msc4143":            false,
		"org.matrix.msc4354":            true,
		"org.matrix.msc4222":            true,
		"uk.tcpip.msc4133":              true,
		"uk.tcpip.msc4133.stable":       true,
		"com.beeper.msc4169":            true,
	}
	tests := []struct {
		name     string
		mscs     []string
		rtcFoci  []config.RTCFocus
		maxDelay time.Duration
		features map[string]bool
	}{
		{name: "defaults"},
		{
			name: "configured MSCs",
			mscs: []string{"msc2444", "msc2753", "msc2836", "msc3814"},
			features: map[string]bool{
				"org.matrix.msc2444": true,
				"org.matrix.msc2753": true,
				"org.matrix.msc2836": true,
				"org.matrix.msc3814": true,
			},
		},
		{
			name: "OIDC implies discovery and scopes",
			mscs: []string{"msc3861"},
			features: map[string]bool{
				"org.matrix.msc3861": true,
				"org.matrix.msc2965": true,
				"org.matrix.msc2967": true,
			},
		},
		{
			name: "RTC configured",
			rtcFoci: []config.RTCFocus{{
				Type:    "livekit",
				LiveKit: &config.LiveKitFocus{ServiceURL: "https://rtc.example.com"},
			}},
			features: map[string]bool{"org.matrix.msc4143": true},
		},
		{
			name:     "delayed events enabled",
			maxDelay: 24 * time.Hour,
			features: map[string]bool{"org.matrix.msc4140": true},
		},
		{
			name: "unknown and inactive entries cannot advertise support",
			mscs: []string{"msc9999", "msc2965", "msc2967", "msc4143", "msc4115", "msc3916", "msc3575"},
		},
		{
			name:     "duplicate entries",
			mscs:     []string{"msc2753", "msc2753"},
			features: map[string]bool{"org.matrix.msc2753": true},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var cfg config.Zendrite
			cfg.Defaults(config.DefaultOpts{})
			cfg.MSCs.MSCs = tc.mscs
			cfg.Global.RTCFoci = tc.rtcFoci
			cfg.ClientAPI.DelayedEvents.MaxDelay = tc.maxDelay
			// Values without the MSC enabled must not activate OIDC discovery.
			cfg.MSCs.MSC3861.Issuer = "https://auth.example.com"
			routers := httputil.NewRouters()
			Setup(routers, &cfg, nil, nil, &discoveryUserAPI{}, nil, nil, nil, nil, nil, nil, nil, nil, caching.DisableMetrics)

			wantFeatures := maps.Clone(baseFeatures)
			maps.Copy(wantFeatures, tc.features)
			for _, authorization := range []string{"", "Bearer invalid"} {
				req := httptest.NewRequest(http.MethodGet, "/_matrix/client/versions", nil)
				req.Header.Set("Authorization", authorization)
				rec := httptest.NewRecorder()
				routers.Client.ServeHTTP(rec, req)
				if rec.Code != http.StatusOK {
					t.Fatalf("GET /versions: status %d, body %s", rec.Code, rec.Body.String())
				}
				var response struct {
					Versions         []string        `json:"versions"`
					UnstableFeatures map[string]bool `json:"unstable_features"`
				}
				if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				wantVersions := []string{"r0.0.1", "r0.1.0", "r0.2.0", "r0.3.0", "r0.4.0", "r0.5.0", "r0.6.1", "v1.0", "v1.1", "v1.2", "v1.3", "v1.4", "v1.5", "v1.6", "v1.7", "v1.8", "v1.9", "v1.10", "v1.11", "v1.12", "v1.13"}
				if !reflect.DeepEqual(response.Versions, wantVersions) {
					t.Errorf("versions = %v, want %v", response.Versions, wantVersions)
				}
				if !maps.Equal(response.UnstableFeatures, wantFeatures) {
					t.Errorf("unstable_features = %v, want %v", response.UnstableFeatures, wantFeatures)
				}
			}

			req := httptest.NewRequest(http.MethodOptions, "/_matrix/client/versions", nil)
			rec := httptest.NewRecorder()
			routers.Client.ServeHTTP(rec, req)
			if rec.Code != http.StatusOK || rec.Header().Get("Access-Control-Allow-Origin") != "*" {
				t.Errorf("OPTIONS /versions: status %d, headers %v", rec.Code, rec.Header())
			}
		})
	}
}
