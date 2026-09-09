// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package threepid

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"

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

// Bind publishes an association using the authenticated identity service v2 API.
func Bind(ctx context.Context, creds Credentials, token, userID string, cfg *config.ClientAPI, client *fclient.Client) (GetValidatedResponse, error) {
	var result GetValidatedResponse
	if err := isTrusted(creds.IDServer, cfg); err != nil {
		return result, err
	}
	body, err := json.Marshal(map[string]string{"sid": creds.SID, "client_secret": creds.Secret, "mxid": userID})
	if err != nil {
		return result, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://"+creds.IDServer+"/_matrix/identity/v2/3pid/bind", bytes.NewReader(body))
	if err != nil {
		return result, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := client.DoHTTPRequest(ctx, req)
	if err != nil {
		return result, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return result, identityError(resp)
	}
	if err = json.NewDecoder(resp.Body).Decode(&result); err != nil {
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
