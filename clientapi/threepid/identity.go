// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package threepid

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// IdentityServerError preserves Matrix errors returned by the identity server.
type IdentityServerError struct {
	Code        int
	MatrixError spec.MatrixError
	Body        json.RawMessage
}

func (e *IdentityServerError) Error() string { return e.MatrixError.Error() }

func identityError(resp *http.Response) error {
	var body json.RawMessage
	var matrixError spec.MatrixError
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fmt.Errorf("identity server returned HTTP %d", resp.StatusCode)
	}
	if err := json.Unmarshal(body, &matrixError); err != nil || matrixError.ErrCode == "" {
		return fmt.Errorf("identity server returned HTTP %d", resp.StatusCode)
	}
	return &IdentityServerError{Code: resp.StatusCode, MatrixError: matrixError, Body: body}
}

// Bind publishes an association between a validated third-party identifier
// and a Matrix user ID on the identity server.
func Bind(ctx context.Context, creds Credentials, userID string, auth IdentityAuth) (GetValidatedResponse, error) {
	var result GetValidatedResponse
	body := map[string]string{"sid": creds.SID, "client_secret": creds.Secret, "mxid": userID}
	if err := auth.do(ctx, creds.IDServer, http.MethodPost, "/_matrix/identity/v2/3pid/bind", nil, body, &result); err != nil {
		return result, err
	}
	if result.Address == "" || (result.Medium != "email" && result.Medium != "msisdn") {
		return result, fmt.Errorf("invalid identity server bind response")
	}
	return result, nil
}

// Unbind signs a request on behalf of the authenticated Matrix user.
func Unbind(ctx context.Context, server, userID, medium, address string, cfg *config.ClientAPI, client *fclient.Client) (string, error) {
	if err := isTrusted(server, cfg); err != nil {
		return "", err
	}
	_, domain, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return "", err
	}
	identity, err := cfg.Matrix.SigningIdentityFor(domain)
	if err != nil {
		return "", err
	}
	request := fclient.NewFederationRequest(http.MethodPost, domain, spec.ServerName(server), "/_matrix/identity/v2/3pid/unbind")
	if err = request.SetContent(map[string]any{"mxid": userID, "threepid": map[string]string{"medium": medium, "address": address}}); err != nil {
		return "", err
	}
	if err = request.Sign(domain, identity.KeyID, identity.PrivateKey); err != nil {
		return "", err
	}
	req, err := request.HTTPRequest()
	if err != nil {
		return "", err
	}
	req.URL.Scheme = "https"
	resp, err := client.DoHTTPRequest(ctx, req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	switch resp.StatusCode {
	case http.StatusOK:
		return "success", nil
	case http.StatusBadRequest, http.StatusNotFound, http.StatusNotImplemented:
		return "no-support", nil
	default:
		return "", identityError(resp)
	}
}
