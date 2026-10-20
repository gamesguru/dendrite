// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package threepid

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib/fclient"

	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// IdentityAuth authenticates requests to the v2 identity service API, which
// replaced the unauthenticated v1 API removed by MSC2713.
type IdentityAuth struct {
	// Token is the client's identity server access token (`id_access_token`).
	// Without one, the homeserver uses its own, see ServerToken.
	Token   string
	UserAPI userapi.ClientUserAPI
	Config  *config.ClientAPI
	Client  *fclient.Client
}

// serverTokens caches the homeserver's own access token per identity server.
var serverTokens = struct {
	sync.Mutex
	tokens map[string]string
}{tokens: make(map[string]string)}

// serverTokenUser is the user the homeserver registers with identity servers
// as: the server notices user, which exists for this kind of task.
func serverTokenUser(cfg *config.ClientAPI) string {
	return fmt.Sprintf("@%s:%s", cfg.Matrix.ServerNotices.LocalPart, cfg.Matrix.ServerName)
}

// ServerToken returns the homeserver's own access token for an identity
// server, registering an account there first if it has none. Registration
// presents an OpenID token for the server notices user, which the identity
// server checks with this homeserver over federation.
func (a IdentityAuth) ServerToken(ctx context.Context, server string) (string, error) {
	serverTokens.Lock()
	token, ok := serverTokens.tokens[server]
	serverTokens.Unlock()
	if ok {
		return token, nil
	}

	var openID userapi.PerformOpenIDTokenCreationResponse
	if err := a.UserAPI.PerformOpenIDTokenCreation(ctx, &userapi.PerformOpenIDTokenCreationRequest{
		UserID: serverTokenUser(a.Config),
	}, &openID); err != nil {
		return "", fmt.Errorf("creating an OpenID token: %w", err)
	}
	var registered struct {
		Token string `json:"token"`
	}
	if _, err := a.send(ctx, server, "", http.MethodPost, "/_matrix/identity/v2/account/register", nil, map[string]any{
		"access_token":       openID.Token.Token,
		"token_type":         "Bearer",
		"matrix_server_name": string(a.Config.Matrix.ServerName),
		"expires_in":         int64(time.Until(time.UnixMilli(openID.Token.ExpiresAtMS)).Seconds()),
	}, &registered); err != nil {
		return "", fmt.Errorf("registering with identity server %s: %w", server, err)
	}
	if registered.Token == "" {
		return "", fmt.Errorf("identity server %s returned no access token", server)
	}

	serverTokens.Lock()
	serverTokens.tokens[server] = registered.Token
	serverTokens.Unlock()
	return registered.Token, nil
}

func forgetServerToken(server, token string) {
	serverTokens.Lock()
	defer serverTokens.Unlock()
	if serverTokens.tokens[server] == token {
		delete(serverTokens.tokens, server)
	}
}

// do sends an authenticated request to the v2 API of a trusted identity
// server and decodes a 200 response into out. With the homeserver's own
// token, a 401 response registers again once, since identity servers may
// expire or revoke tokens.
func (a IdentityAuth) do(ctx context.Context, server, method, path string, query url.Values, body, out any) error {
	if err := isTrusted(server, a.Config); err != nil {
		return err
	}
	if a.Token != "" {
		_, err := a.send(ctx, server, a.Token, method, path, query, body, out)
		return err
	}
	for attempt := 0; ; attempt++ {
		token, err := a.ServerToken(ctx, server)
		if err != nil {
			return err
		}
		code, err := a.send(ctx, server, token, method, path, query, body, out)
		if code != http.StatusUnauthorized || attempt > 0 {
			return err
		}
		forgetServerToken(server, token)
	}
}

// send sends one request to an identity server, authenticated with token if
// it is not empty, and returns the response's status code.
func (a IdentityAuth) send(ctx context.Context, server, token, method, path string, query url.Values, body, out any) (int, error) {
	requestURL := url.URL{Scheme: "https", Host: server, Path: path, RawQuery: query.Encode()}
	var reader io.Reader
	if body != nil {
		encoded, err := json.Marshal(body)
		if err != nil {
			return 0, err
		}
		reader = bytes.NewReader(encoded)
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL.String(), reader)
	if err != nil {
		return 0, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := a.Client.DoHTTPRequest(ctx, req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, identityError(resp)
	}
	if out == nil {
		return resp.StatusCode, nil
	}
	if err = json.NewDecoder(resp.Body).Decode(out); err != nil {
		return resp.StatusCode, errors.Join(errors.New("decoding the identity server response"), err)
	}
	return resp.StatusCode, nil
}
