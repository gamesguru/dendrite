// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package threepid

import (
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/setup/config"
)

type identityTransport func(*http.Request) (*http.Response, error)

func (f identityTransport) RoundTrip(req *http.Request) (*http.Response, error) { return f(req) }

func TestCheckAssociation(t *testing.T) {
	cfg := &config.ClientAPI{Matrix: &config.Global{TrustedIDServers: []string{"id.example"}}}
	for _, tc := range []struct {
		name            string
		code            int
		body            string
		verified, fails bool
	}{
		{"validated", 200, `{"medium":"email","address":"a@example.com"}`, true, false},
		{"unvalidated", 400, `{"errcode":"M_SESSION_NOT_VALIDATED"}`, false, false},
		{"HTTP failure with valid JSON", 500, `{"medium":"email","address":"a@example.com"}`, false, true},
		{"empty", 200, `{}`, false, true},
		{"invalid medium", 200, `{"medium":"other","address":"a"}`, false, true},
		{"malformed", 200, `{`, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := fclient.NewClient(fclient.WithTransport(identityTransport(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: tc.code, Body: io.NopCloser(strings.NewReader(tc.body))}, nil
			})))
			auth := IdentityAuth{Token: "token", Config: cfg, Client: client}
			verified, _, _, err := CheckAssociation(t.Context(), Credentials{SID: "sid", Secret: "secret", IDServer: "id.example"}, auth)
			require.Equal(t, tc.verified, verified)
			if tc.fails {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidationSessions(t *testing.T) {
	rememberSession("session", "secret", "id.example")
	require.Equal(t, "id.example", SessionServer("session", "secret"))
	require.Empty(t, SessionServer("session", "wrong"))
	validationSessions.Lock()
	validationSessions.entries[sessionKey("session", "secret")] = validationSession{server: "id.example", expires: time.Now().Add(-time.Second)}
	validationSessions.Unlock()
	require.Empty(t, SessionServer("session", "secret"))
	rememberSession("another", "secret", "id.example")
	validationSessions.Lock()
	_, exists := validationSessions.entries[sessionKey("session", "secret")]
	validationSessions.Unlock()
	require.False(t, exists)
}

func TestCreateSessionErrorDetails(t *testing.T) {
	cfg := &config.ClientAPI{Matrix: &config.Global{TrustedIDServers: []string{"id.example"}}}
	client := fclient.NewClient(fclient.WithTransport(identityTransport(func(*http.Request) (*http.Response, error) {
		return &http.Response{StatusCode: 403, Body: io.NopCloser(strings.NewReader(`{"errcode":"M_FORBIDDEN","error":"Verification disabled"}`))}, nil
	})))
	auth := IdentityAuth{Token: "token", Config: cfg, Client: client}
	sid, err := CreateSession(t.Context(), EmailAssociationRequest{IDServer: "id.example", Secret: "secret", Email: "alice@example.com"}, auth)
	require.Empty(t, sid)
	require.ErrorContains(t, err, "id.example (HTTP 403)")
	require.ErrorContains(t, err, "M_FORBIDDEN")
	require.ErrorContains(t, err, "Verification disabled")
	var remote *IdentityServerError
	require.ErrorAs(t, err, &remote)
	require.Equal(t, 403, remote.Code)
}
