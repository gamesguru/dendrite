// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// accountModeration is one of the account moderation actions of MSC4323:
// locking (MSC3939) or suspending (MSC3823) an account.
type accountModeration struct {
	// field is the JSON field of the request and response.
	field string
	// verb names the action in error messages.
	verb string
	// get reports whether the action applies to the account.
	get func(*userapi.Account) bool
	// set applies or lifts the action.
	set func(ctx context.Context, userAPI userapi.ClientUserAPI, localpart string, serverName spec.ServerName, on bool) error
}

var (
	lockModeration = accountModeration{
		field: "locked",
		verb:  "lock",
		get:   func(acc *userapi.Account) bool { return acc.Locked },
		set: func(ctx context.Context, userAPI userapi.ClientUserAPI, localpart string, serverName spec.ServerName, on bool) error {
			return userAPI.PerformAccountLock(ctx, localpart, serverName, on)
		},
	}
	suspendModeration = accountModeration{
		field: "suspended",
		verb:  "suspend",
		get:   func(acc *userapi.Account) bool { return acc.Suspended },
		set: func(ctx context.Context, userAPI userapi.ClientUserAPI, localpart string, serverName spec.ServerName, on bool) error {
			return userAPI.PerformAccountSuspension(ctx, localpart, serverName, on)
		},
	}
)

// moderationTarget resolves the local, active account that the admin lock
// and suspend endpoints act on, or the error response for the user ID.
func moderationTarget(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI, userID string) (*userapi.Account, *util.JSONResponse) {
	parsed, err := spec.NewUserID(userID, true)
	if err != nil || !cfg.Matrix.IsLocalServerName(parsed.Domain()) {
		return nil, &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The user ID does not belong to this server"),
		}
	}
	var res userapi.QueryAccountByLocalpartResponse
	err = userAPI.QueryAccountByLocalpart(req.Context(), &userapi.QueryAccountByLocalpartRequest{
		Localpart:  parsed.Local(),
		ServerName: parsed.Domain(),
	}, &res)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && res.Account.Deactivated) {
		return nil, &util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The user does not exist or is deactivated"),
		}
	}
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("userAPI.QueryAccountByLocalpart failed")
		return nil, &util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	return res.Account, nil
}

// AdminGetModeration implements GET /_matrix/client/v1/admin/lock/{userId}
// and GET /_matrix/client/v1/admin/suspend/{userId} (MSC4323).
func AdminGetModeration(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI, m accountModeration, userID string) util.JSONResponse {
	account, resErr := moderationTarget(req, cfg, userAPI, userID)
	if resErr != nil {
		return *resErr
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: map[string]bool{m.field: m.get(account)}}
}

// AdminSetModeration implements PUT /_matrix/client/v1/admin/lock/{userId}
// and PUT /_matrix/client/v1/admin/suspend/{userId} (MSC4323).
// Administrators can neither moderate themselves nor other administrators.
func AdminSetModeration(
	req *http.Request, cfg *config.ClientAPI, device *userapi.Device, userAPI userapi.ClientUserAPI,
	m accountModeration, userID string,
) util.JSONResponse {
	var body map[string]json.RawMessage
	if resErr := httputil.UnmarshalJSONRequest(req, &body); resErr != nil {
		return *resErr
	}
	var on bool
	if raw, ok := body[m.field]; !ok {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("Missing " + m.field)}
	} else if err := json.Unmarshal(raw, &on); err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON(m.field + " must be a boolean")}
	}
	account, resErr := moderationTarget(req, cfg, userAPI, userID)
	if resErr != nil {
		return *resErr
	}
	if on {
		if account.UserID == device.UserID {
			return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("You cannot " + m.verb + " your own account")}
		}
		if account.AccountType == userapi.AccountTypeAdmin {
			return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("You cannot " + m.verb + " another administrator")}
		}
	}
	if err := m.set(req.Context(), userAPI, account.Localpart, account.ServerName, on); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("failed to " + m.verb + " account")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: map[string]bool{m.field: on}}
}
