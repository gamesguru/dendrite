// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"database/sql"
	"errors"
	"net/http"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type adminLockRequest struct {
	Locked *bool `json:"locked"`
}

type adminLockResponse struct {
	Locked bool `json:"locked"`
}

// lockTarget resolves the local, active account that GET and PUT
// /admin/lock/{userId} act on, or the error response for the user ID.
func lockTarget(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI, userID string) (*userapi.Account, *util.JSONResponse) {
	parsed, err := spec.NewUserID(userID, true)
	if err != nil || !cfg.Matrix.IsLocalServerName(parsed.Domain()) {
		return nil, &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("The user ID does not belong to this server"),
		}
	}
	notFound := &util.JSONResponse{
		Code: http.StatusNotFound,
		JSON: spec.NotFound("The user does not exist or is deactivated"),
	}
	var res userapi.QueryAccountByLocalpartResponse
	err = userAPI.QueryAccountByLocalpart(req.Context(), &userapi.QueryAccountByLocalpartRequest{
		Localpart:  parsed.Local(),
		ServerName: parsed.Domain(),
	}, &res)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, notFound
	}
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("userAPI.QueryAccountByLocalpart failed")
		return nil, &util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	deactivated, err := userAPI.IsAccountDeactivated(req.Context(), res.Account.Localpart, res.Account.ServerName)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("userAPI.IsAccountDeactivated failed")
		return nil, &util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if deactivated {
		return nil, notFound
	}
	return res.Account, nil
}

// AdminGetLock implements GET /_matrix/client/v1/admin/lock/{userId}
// (MSC4323), which reports whether an account is locked (MSC3939).
func AdminGetLock(req *http.Request, cfg *config.ClientAPI, userAPI userapi.ClientUserAPI, userID string) util.JSONResponse {
	account, resErr := lockTarget(req, cfg, userAPI, userID)
	if resErr != nil {
		return *resErr
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: adminLockResponse{Locked: account.Locked}}
}

// AdminSetLock implements PUT /_matrix/client/v1/admin/lock/{userId}
// (MSC4323), which locks or unlocks an account (MSC3939). Administrators can
// neither lock themselves nor other administrators.
func AdminSetLock(req *http.Request, cfg *config.ClientAPI, device *userapi.Device, userAPI userapi.ClientUserAPI, userID string) util.JSONResponse {
	var body adminLockRequest
	if resErr := httputil.UnmarshalJSONRequest(req, &body); resErr != nil {
		return *resErr
	}
	if body.Locked == nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("Missing locked")}
	}
	account, resErr := lockTarget(req, cfg, userAPI, userID)
	if resErr != nil {
		return *resErr
	}
	if *body.Locked {
		if account.UserID == device.UserID {
			return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("You cannot lock your own account")}
		}
		if account.AccountType == userapi.AccountTypeAdmin {
			return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("You cannot lock another administrator")}
		}
	}
	if err := userAPI.PerformAccountLock(req.Context(), account.Localpart, account.ServerName, *body.Locked); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("userAPI.PerformAccountLock failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: adminLockResponse{Locked: *body.Locked}}
}
