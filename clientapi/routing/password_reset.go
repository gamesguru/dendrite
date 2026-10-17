// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"fmt"
	"net/http"

	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/clientapi/threepid"
	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

// loginTypeEmailIdentity is the UIA stage that proves control of an email
// address through an identity server validation session.
const loginTypeEmailIdentity = "m.login.email.identity"

// Error codes the spec uses that gomatrixserverlib does not define.
const (
	// ErrorThreePIDNotFound is returned when no account has the email address.
	errorThreePIDNotFound spec.MatrixErrorCode = "M_THREEPID_NOT_FOUND"
	// ErrorUnauthorized marks a UIA stage that was submitted but not passed.
	errorUnauthorized spec.MatrixErrorCode = "M_UNAUTHORIZED"
)

// RequestPasswordResetEmailToken implements
// POST /account/password/email/requestToken.
//
// The validation email is always sent through client_api.email_validation_server.
// A client-supplied id_server is ignored: password reset trusts the identity
// server to vouch for the address, so it must be one the admin chose.
func RequestPasswordResetEmailToken(req *http.Request, userAPI api.ClientUserAPI, cfg *config.ClientAPI, client *fclient.Client) util.JSONResponse {
	var body threepid.EmailAssociationRequest
	if resErr := httputil.UnmarshalJSONRequest(req, &body); resErr != nil {
		return *resErr
	}
	if cfg.EmailValidationServer == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MatrixError{ErrCode: "M_THREEPID_MEDIUM_NOT_SUPPORTED", Err: "Password reset by email is not configured on this homeserver"},
		}
	}
	if body.Email == "" || body.Secret == "" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("email and client_secret are required")}
	}
	body.Email = authtypes.CanonicalThreePIDAddress("email", body.Email)
	body.IDServer = cfg.EmailValidationServer
	body.IDAccessToken = ""

	var owner api.QueryLocalpartForThreePIDResponse
	if err := userAPI.QueryLocalpartForThreePID(req.Context(), &api.QueryLocalpartForThreePIDRequest{ThreePID: body.Email, Medium: "email"}, &owner); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("QueryLocalpartForThreePID failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if owner.Localpart == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MatrixError{ErrCode: errorThreePIDNotFound, Err: "Email not found"},
		}
	}

	auth := threepid.IdentityAuth{UserAPI: userAPI, Config: cfg, Client: client}
	sid, err := threepid.CreateSession(req.Context(), body, auth)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("threepid.CreateSession failed")
		return threePIDError(err, body.IDServer)
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: reqTokenResponse{SID: sid}}
}

// RequestPasswordResetMSISDNToken implements
// POST /account/password/msisdn/requestToken, which Zendrite does not support.
func RequestPasswordResetMSISDNToken() util.JSONResponse {
	return util.JSONResponse{
		Code: http.StatusBadRequest,
		JSON: spec.MatrixError{ErrCode: "M_THREEPID_MEDIUM_NOT_SUPPORTED", Err: "Phone numbers are not supported on this homeserver"},
	}
}

type passwordResetRequest struct {
	NewPassword   string            `json:"new_password"`
	LogoutDevices *bool             `json:"logout_devices"`
	Auth          passwordResetAuth `json:"auth"`
}

type passwordResetAuth struct {
	Type    string `json:"type"`
	Session string `json:"session"`
	// ThreePIDCreds is the stable name; threepidCreds predates it.
	ThreePIDCreds       *threepid.Credentials `json:"threepid_creds"`
	LegacyThreePIDCreds *threepid.Credentials `json:"threepidCreds"`
}

// passwordResetChallenge is a user-interactive authentication response that
// can also carry an error, for a stage that was submitted but not passed.
type passwordResetChallenge struct {
	userInteractiveResponse
	ErrCode spec.MatrixErrorCode `json:"errcode,omitempty"`
	Err     string               `json:"error,omitempty"`
}

// PasswordReset implements POST /account/password without an access token:
// a user who forgot their password proves control of an email address on
// their account and sets a new password.
func PasswordReset(req *http.Request, userAPI api.ClientUserAPI, cfg *config.ClientAPI, client *fclient.Client) util.JSONResponse {
	if cfg.EmailValidationServer == "" {
		// Without email validation, changing the password needs a session.
		return util.JSONResponse{Code: http.StatusUnauthorized, JSON: spec.MissingToken("Missing access token")}
	}
	var r passwordResetRequest
	if resErr := httputil.UnmarshalJSONRequest(req, &r); resErr != nil {
		return *resErr
	}
	sessionID := r.Auth.Session
	if sessionID == "" {
		sessionID = util.RandomString(sessionIDLength)
	}
	challenge := func(errCode spec.MatrixErrorCode, message string) util.JSONResponse {
		return util.JSONResponse{
			Code: http.StatusUnauthorized,
			JSON: passwordResetChallenge{
				userInteractiveResponse: newUserInteractiveResponse(
					sessionID, []authtypes.Flow{{Stages: []authtypes.LoginType{loginTypeEmailIdentity}}}, nil,
				),
				ErrCode: errCode,
				Err:     message,
			},
		}
	}
	if r.Auth.Type != loginTypeEmailIdentity {
		return challenge("", "")
	}
	creds := r.Auth.ThreePIDCreds
	if creds == nil {
		creds = r.Auth.LegacyThreePIDCreds
	}
	if creds == nil || creds.SID == "" || creds.Secret == "" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("auth.threepid_creds needs sid and client_secret")}
	}
	// Only the configured identity server may vouch for the address.
	if creds.IDServer != "" && creds.IDServer != cfg.EmailValidationServer {
		return challenge(spec.ErrorThreePIDAuthFailed, "Only sessions from this homeserver's requestToken endpoint are accepted")
	}
	if err := internal.ValidatePassword(r.NewPassword); err != nil {
		return *internal.PasswordResponse(err)
	}

	auth := threepid.IdentityAuth{UserAPI: userAPI, Config: cfg, Client: client}
	validated, address, medium, err := threepid.CheckAssociation(req.Context(), threepid.Credentials{
		SID: creds.SID, Secret: creds.Secret, IDServer: cfg.EmailValidationServer,
	}, auth)
	switch {
	case err != nil:
		util.GetLogger(req.Context()).WithError(err).Warn("password reset: validation check failed")
		return challenge(errorUnauthorized, "Unable to check the email validation")
	case !validated:
		// Clients poll until the user follows the link in the email.
		return challenge(errorUnauthorized, "The email address has not been validated yet")
	case medium != "email":
		return challenge(spec.ErrorThreePIDAuthFailed, "Only email addresses can reset a password")
	}

	var owner api.QueryLocalpartForThreePIDResponse
	if err = userAPI.QueryLocalpartForThreePID(req.Context(), &api.QueryLocalpartForThreePIDRequest{
		ThreePID: authtypes.CanonicalThreePIDAddress("email", address), Medium: "email",
	}, &owner); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("QueryLocalpartForThreePID failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if owner.Localpart == "" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MatrixError{ErrCode: errorThreePIDNotFound, Err: "Email not found"}}
	}
	sessions.addCompletedSessionStage(sessionID, loginTypeEmailIdentity)

	var updated api.PerformPasswordUpdateResponse
	if err = userAPI.PerformPasswordUpdate(req.Context(), &api.PerformPasswordUpdateRequest{
		Localpart: owner.Localpart, ServerName: owner.ServerName, Password: r.NewPassword,
	}, &updated); err != nil || !updated.PasswordUpdated {
		util.GetLogger(req.Context()).WithError(err).Error("PerformPasswordUpdate failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	util.GetLogger(req.Context()).WithField("localpart", owner.Localpart).Info("Password reset by email")

	// logout_devices defaults to true. The reset comes from no device, so
	// every device and pusher goes.
	if r.LogoutDevices == nil || *r.LogoutDevices {
		userID := fmt.Sprintf("@%s:%s", owner.Localpart, owner.ServerName)
		if err = userAPI.PerformDeviceDeletion(req.Context(), &api.PerformDeviceDeletionRequest{UserID: userID}, &api.PerformDeviceDeletionResponse{}); err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("PerformDeviceDeletion failed")
			return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
		}
		// Pushers keep the ID of the session that created them, which is
		// never negative, so -1 removes them all.
		if err = userAPI.PerformPusherDeletion(req.Context(), &api.PerformPusherDeletionRequest{
			Localpart: owner.Localpart, ServerName: owner.ServerName, SessionID: -1,
		}, &struct{}{}); err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("PerformPusherDeletion failed")
			return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
		}
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
}
