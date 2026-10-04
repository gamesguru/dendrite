// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
)

type profileFederation struct {
	fclient.FederationClient
	t       *testing.T
	profile string
	err     error
}

func (f *profileFederation) DoRequestAndParseResponse(_ context.Context, req *http.Request, result any) error {
	require.Equal(f.t, http.MethodGet, req.Method)
	require.Equal(f.t, "remote.test", req.URL.Host)
	require.Equal(f.t, "/_matrix/federation/v1/query/profile", req.URL.Path)
	require.Equal(f.t, "@bob:remote.test", req.URL.Query().Get("user_id"))
	require.Empty(f.t, req.URL.Query().Get("field"))
	require.Contains(f.t, req.Header.Get("Authorization"), "X-Matrix")
	if f.err != nil {
		return f.err
	}
	return json.Unmarshal([]byte(f.profile), result)
}

func TestRemoteProfileFields(t *testing.T) {
	var cfg config.Zendrite
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	fed := &profileFederation{
		t:       t,
		profile: `{"displayname":"Bob","avatar_url":"mxc://remote.test/bob","m.tz":"Europe/Paris","org.example.job":{"title":"Engineer"}}`,
	}
	req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v3/profile/@bob:remote.test", nil)
	res := GetProfile(req, nil, &cfg.ClientAPI, "@bob:remote.test", nil, fed)
	require.Equal(t, http.StatusOK, res.Code)
	body, err := json.Marshal(res.JSON)
	require.NoError(t, err)
	require.JSONEq(t, fed.profile, string(body))

	for key, want := range map[string]string{
		"displayname":     `{"displayname":"Bob"}`,
		"avatar_url":      `{"avatar_url":"mxc://remote.test/bob"}`,
		"m.tz":            `{"m.tz":"Europe/Paris"}`,
		"org.example.job": `{"org.example.job":{"title":"Engineer"}}`,
	} {
		res = GetProfileField(req, nil, &cfg.ClientAPI, "@bob:remote.test", key, nil, fed)
		require.Equal(t, http.StatusOK, res.Code, key)
		body, err = json.Marshal(res.JSON)
		require.NoError(t, err)
		require.JSONEq(t, want, string(body), key)
	}

	res = GetProfileField(req, nil, &cfg.ClientAPI, "@bob:remote.test", "org.example.unset", nil, fed)
	require.Equal(t, http.StatusNotFound, res.Code)

	// A display name that is not a string is left out.
	fed.profile = `{"displayname":1,"m.tz":"Europe/Paris"}`
	res = GetProfile(req, nil, &cfg.ClientAPI, "@bob:remote.test", nil, fed)
	require.Equal(t, http.StatusOK, res.Code)
	body, err = json.Marshal(res.JSON)
	require.NoError(t, err)
	require.JSONEq(t, `{"m.tz":"Europe/Paris"}`, string(body))

	// A remote 404 means the user has no profile.
	fed.err = spec.HTTPError{Code: http.StatusNotFound}
	res = GetProfile(req, nil, &cfg.ClientAPI, "@bob:remote.test", nil, fed)
	require.Equal(t, http.StatusNotFound, res.Code)
}
