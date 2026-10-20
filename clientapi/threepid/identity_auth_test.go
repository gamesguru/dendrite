// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package threepid

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type openIDUserAPI struct {
	userapi.ClientUserAPI
	users []string
}

func (a *openIDUserAPI) PerformOpenIDTokenCreation(_ context.Context, req *userapi.PerformOpenIDTokenCreationRequest, res *userapi.PerformOpenIDTokenCreationResponse) error {
	a.users = append(a.users, req.UserID)
	res.Token = userapi.OpenIDToken{Token: "openid-token", UserID: req.UserID, ExpiresAtMS: time.Now().Add(time.Hour).UnixMilli()}
	return nil
}

func identityAuthConfig() *config.ClientAPI {
	cfg := &config.Zendrite{}
	cfg.Defaults(config.DefaultOpts{Generate: true, SingleDatabase: true})
	cfg.Global.ServerName = "hs.example"
	cfg.Global.TrustedIDServers = []string{"id.example"}
	return &cfg.ClientAPI
}

func jsonResponse(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

// TestServerToken covers how the homeserver authenticates to the v2 identity
// API when the client gave it no token: it registers with an OpenID token for
// the server notices user, reuses the token it gets, and registers again once
// when the identity server no longer accepts it.
func TestServerToken(t *testing.T) {
	t.Cleanup(func() { forgetServerToken("id.example", "token-1"); forgetServerToken("id.example", "token-2") })
	userAPI := &openIDUserAPI{}
	var registrations []map[string]any
	var seen []string
	rejected := false
	client := fclient.NewClient(fclient.WithTransport(identityTransport(func(req *http.Request) (*http.Response, error) {
		if req.URL.Path == "/_matrix/identity/v2/account/register" {
			var body map[string]any
			require.NoError(t, json.NewDecoder(req.Body).Decode(&body))
			require.Empty(t, req.Header.Get("Authorization"))
			registrations = append(registrations, body)
			return jsonResponse(200, fmt.Sprintf(`{"token":"token-%d"}`, len(registrations))), nil
		}
		seen = append(seen, req.Header.Get("Authorization"))
		if req.Header.Get("Authorization") == "Bearer token-1" && rejected {
			return jsonResponse(401, `{"errcode":"M_UNAUTHORIZED","error":"expired"}`), nil
		}
		return jsonResponse(200, `{"sid":"sid"}`), nil
	})))
	auth := IdentityAuth{UserAPI: userAPI, Config: identityAuthConfig(), Client: client}
	request := EmailAssociationRequest{IDServer: "id.example", Secret: "secret", Email: "alice@example.com"}

	for range 2 {
		_, err := CreateSession(t.Context(), request, auth)
		require.NoError(t, err)
	}
	require.Len(t, registrations, 1, "the token is reused")
	assert.Equal(t, []string{"@_server:hs.example"}, userAPI.users)
	assert.Equal(t, "openid-token", registrations[0]["access_token"])
	assert.Equal(t, "Bearer", registrations[0]["token_type"])
	assert.Equal(t, "hs.example", registrations[0]["matrix_server_name"])
	assert.InDelta(t, time.Hour.Seconds(), registrations[0]["expires_in"], 5)

	rejected = true
	_, err := CreateSession(t.Context(), request, auth)
	require.NoError(t, err)
	require.Len(t, registrations, 2, "a rejected token is replaced")
	assert.Equal(t, []string{"Bearer token-1", "Bearer token-1", "Bearer token-1", "Bearer token-2"}, seen)

	// A client token is used as it is, and never replaced.
	seen = nil
	auth.Token = "client-token"
	_, err = CreateSession(t.Context(), request, auth)
	require.NoError(t, err)
	assert.Equal(t, []string{"Bearer client-token"}, seen)
	require.Len(t, registrations, 2)

	// Untrusted identity servers are not contacted.
	request.IDServer = "untrusted.example"
	_, err = CreateSession(t.Context(), request, auth)
	assert.ErrorAs(t, err, &ErrNotTrusted{})
}
