// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package config

import (
	"encoding/json"
	"testing"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRTCFocus_MarshalJSON_LiveKit checks the wire shape produced for
// clients against element-hq/lk-jwt-service's documented well-known shape
// (https://github.com/element-hq/lk-jwt-service, "Advertising the service"):
// a flat "livekit_service_url" field on the focus object.
func TestRTCFocus_MarshalJSON_LiveKit(t *testing.T) {
	t.Parallel()

	focus := RTCFocus{
		Type:              "livekit",
		LiveKitServiceURL: "https://matrix.example.com/livekit/jwt",
	}

	data, err := json.Marshal(focus)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, map[string]any{
		"type":                "livekit",
		"livekit_service_url": "https://matrix.example.com/livekit/jwt",
	}, got)
}

// TestRTCFocus_MarshalJSON_DeprecatedNestedField covers configs predating
// the flat livekit_service_url field: the nested `livekit:` block still
// works, producing the same flat wire shape.
func TestRTCFocus_MarshalJSON_DeprecatedNestedField(t *testing.T) {
	t.Parallel()

	focus := RTCFocus{
		Type:    "livekit",
		LiveKit: &LiveKitFocus{ServiceURL: "https://matrix.example.com/livekit/jwt"},
	}

	data, err := json.Marshal(focus)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, map[string]any{
		"type":                "livekit",
		"livekit_service_url": "https://matrix.example.com/livekit/jwt",
	}, got)
}

// TestRTCFocus_MarshalJSON_FlatFieldTakesPrecedence guards the migration
// path: if both the new and deprecated fields are set, the flat one wins.
func TestRTCFocus_MarshalJSON_FlatFieldTakesPrecedence(t *testing.T) {
	t.Parallel()

	focus := RTCFocus{
		Type:              "livekit",
		LiveKitServiceURL: "https://new.example.com/livekit/jwt",
		LiveKit:           &LiveKitFocus{ServiceURL: "https://old.example.com/livekit/jwt"},
	}

	data, err := json.Marshal(focus)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, "https://new.example.com/livekit/jwt", got["livekit_service_url"])
}

func TestRTCFocus_MarshalJSON_NoLiveKitConfig(t *testing.T) {
	t.Parallel()

	focus := RTCFocus{Type: "livekit"}

	data, err := json.Marshal(focus)
	require.NoError(t, err)

	var got map[string]any
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, map[string]any{"type": "livekit"}, got)
}

// TestRTCFocus_UnmarshalYAML covers the config side of both spellings: the
// flat livekit_service_url field and the deprecated nested livekit block.
func TestRTCFocus_UnmarshalYAML(t *testing.T) {
	t.Parallel()

	var global Global
	require.NoError(t, yaml.Unmarshal([]byte(`
rtc_foci:
  - type: livekit
    livekit_service_url: "https://new.example.com/livekit/jwt"
  - type: livekit
    livekit:
      service_url: "https://old.example.com/livekit/jwt"
  - type: livekit
    livekit_service_url: "https://new.example.com/livekit/jwt"
    livekit:
      service_url: "https://old.example.com/livekit/jwt"
`), &global))

	require.Len(t, global.RTCFoci, 3)
	assert.Equal(t, "https://new.example.com/livekit/jwt", global.RTCFoci[0].serviceURL())
	assert.Equal(t, "https://old.example.com/livekit/jwt", global.RTCFoci[1].serviceURL())
	assert.Equal(t, "https://new.example.com/livekit/jwt", global.RTCFoci[2].serviceURL())
}

// TestRTCFocus_UnmarshalJSON guards the wire shape against being write-only:
// a focus must decode from the JSON it produces, so that tests and tooling
// reading a well-known response see the service URL.
func TestRTCFocus_UnmarshalJSON(t *testing.T) {
	t.Parallel()

	data, err := json.Marshal(RTCFocus{
		Type:    "livekit",
		LiveKit: &LiveKitFocus{ServiceURL: "https://matrix.example.com/livekit/jwt"},
	})
	require.NoError(t, err)

	var got RTCFocus
	require.NoError(t, json.Unmarshal(data, &got))

	assert.Equal(t, RTCFocus{
		Type:              "livekit",
		LiveKitServiceURL: "https://matrix.example.com/livekit/jwt",
	}, got)
}

// TestRTCFocus_Verify checks that validation accepts either spelling and
// still rejects a "livekit" focus with no service URL at all.
func TestRTCFocus_Verify(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		focus   RTCFocus
		wantErr string
	}{
		{
			name:  "flat field",
			focus: RTCFocus{Type: "livekit", LiveKitServiceURL: "https://matrix.example.com/livekit/jwt"},
		},
		{
			name:  "deprecated nested field",
			focus: RTCFocus{Type: "livekit", LiveKit: &LiveKitFocus{ServiceURL: "https://matrix.example.com/livekit/jwt"}},
		},
		{
			name:    "no service url",
			focus:   RTCFocus{Type: "livekit"},
			wantErr: `global.rtc_foci[0].livekit_service_url is required for type "livekit"`,
		},
		{
			name:    "empty nested service url",
			focus:   RTCFocus{Type: "livekit", LiveKit: &LiveKitFocus{}},
			wantErr: `global.rtc_foci[0].livekit_service_url is required for type "livekit"`,
		},
		{
			name:    "no type",
			focus:   RTCFocus{LiveKitServiceURL: "https://matrix.example.com/livekit/jwt"},
			wantErr: "global.rtc_foci[0].type is required",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var global Global
			global.Defaults(DefaultOpts{Generate: true, SingleDatabase: true})
			global.ServerName = "localhost"
			global.PrivateKeyPath = "matrix_key.pem"
			global.RTCFoci = []RTCFocus{tc.focus}

			errs := &ConfigErrors{}
			global.Verify(errs)

			if tc.wantErr == "" {
				assert.Empty(t, *errs)
				return
			}
			assert.Contains(t, []string(*errs), tc.wantErr)
		})
	}
}
