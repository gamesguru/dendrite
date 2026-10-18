// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package threepid

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// EmailAssociationRequest represents the body of POST /account/3pid/email/requestToken
// and POST /register/email/requestToken.
type EmailAssociationRequest struct {
	IDServer      string `json:"id_server"`
	IDAccessToken string `json:"id_access_token"`
	Secret        string `json:"client_secret"`
	Email         string `json:"email"`
	SendAttempt   int    `json:"send_attempt"`
	NextLink      string `json:"next_link,omitempty"`
}

// EmailAssociationCheckRequest represents the body of the deprecated POST /account/3pid.
type EmailAssociationCheckRequest struct {
	Creds Credentials `json:"three_pid_creds"`
	Bind  bool        `json:"bind"`
}

// Credentials represents the ThreePidCredentials of POST /account/3pid, and
// the body of POST /account/3pid/add and /account/3pid/bind.
type Credentials struct {
	SID           string `json:"sid"`
	IDServer      string `json:"id_server"`
	IDAccessToken string `json:"id_access_token"`
	Secret        string `json:"client_secret"`
}

type SID struct {
	SID string `json:"sid"`
}

// CreateSession asks an identity server to send a validation email, and
// returns the ID of its validation session.
// Returns an error if there was a problem sending the request or decoding the
// response, or if the identity server responded with a non-OK status.
func CreateSession(ctx context.Context, req EmailAssociationRequest, auth IdentityAuth) (string, error) {
	body := map[string]any{
		"client_secret": req.Secret,
		"email":         req.Email,
		"send_attempt":  req.SendAttempt,
	}
	if req.NextLink != "" {
		body["next_link"] = req.NextLink
	}
	var sid SID
	if err := auth.do(ctx, req.IDServer, http.MethodPost, "/_matrix/identity/v2/validate/email/requestToken", nil, body, &sid); err != nil {
		var remote *IdentityServerError
		if errors.As(err, &remote) {
			return "", fmt.Errorf("could not create a session on the server %s (HTTP %d): %w", req.IDServer, remote.Code, err)
		}
		return "", fmt.Errorf("could not create a session on the server %s: %w", req.IDServer, err)
	}
	if sid.SID == "" {
		return "", fmt.Errorf("identity server returned an empty session ID")
	}
	rememberSession(sid.SID, req.Secret, req.IDServer)
	return sid.SID, nil
}

type GetValidatedResponse struct {
	Medium      string `json:"medium"`
	ValidatedAt int64  `json:"validated_at"`
	Address     string `json:"address"`
}

// CheckAssociation checks the status of an ongoing association validation on an
// identity server.
// Returns a boolean set to true if the association has been validated, false if not.
// If the association has been validated, also returns the related third-party
// identifier and its medium.
// Returns an error if there was a problem sending the request or decoding the
// response, or if the identity server responded with an error other than
// M_SESSION_NOT_VALIDATED.
func CheckAssociation(ctx context.Context, creds Credentials, auth IdentityAuth) (bool, string, string, error) {
	var respBody GetValidatedResponse
	query := url.Values{"sid": {creds.SID}, "client_secret": {creds.Secret}}
	err := auth.do(ctx, creds.IDServer, http.MethodGet, "/_matrix/identity/v2/3pid/getValidated3pid", query, nil, &respBody)
	var remote *IdentityServerError
	switch {
	case errors.As(err, &remote) && remote.MatrixError.ErrCode == spec.ErrorSessionNotValidated:
		return false, "", "", nil
	case err != nil:
		return false, "", "", err
	}
	if respBody.Address == "" || (respBody.Medium != "email" && respBody.Medium != "msisdn") {
		return false, "", "", fmt.Errorf("invalid identity server validation response")
	}
	return true, respBody.Address, respBody.Medium, nil
}

// isTrusted checks if a given identity server is part of the list of trusted
// identity servers in the configuration file.
// Returns an error if the server isn't trusted.
func isTrusted(idServer string, cfg *config.ClientAPI) error {
	for _, server := range cfg.Matrix.TrustedIDServers {
		if idServer == server {
			return nil
		}
	}
	return ErrNotTrusted{}
}
