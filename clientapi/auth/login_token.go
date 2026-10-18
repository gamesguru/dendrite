// Copyright 2024 New Vector Ltd.
// Copyright 2021 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package auth

import (
	"context"
	"net/http"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/setup/config"
	uapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// LoginTypeToken describes how to authenticate with a login token.
type LoginTypeToken struct {
	UserAPI uapi.LoginTokenInternalAPI
	Config  *config.ClientAPI
}

// Name implements Type.
func (t *LoginTypeToken) Name() string {
	return authtypes.LoginTypeToken
}

// LoginFromJSON implements Type. The token is deleted before the login
// proceeds, so that it can only be used for a single login, even when several
// requests present it at once.
func (t *LoginTypeToken) LoginFromJSON(ctx context.Context, reqBytes []byte) (*Login, LoginCleanupFunc, *util.JSONResponse) {
	var r loginTokenRequest
	if err := httputil.UnmarshalJSON(reqBytes, &r); err != nil {
		return nil, nil, err
	}

	var res uapi.QueryLoginTokenResponse
	if err := t.UserAPI.QueryLoginToken(ctx, &uapi.QueryLoginTokenRequest{Token: r.Token}, &res); err != nil {
		util.GetLogger(ctx).WithError(err).Error("UserAPI.QueryLoginToken failed")
		return nil, nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if res.Data == nil {
		return nil, nil, &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("invalid login token"),
		}
	}

	var delRes uapi.PerformLoginTokenDeletionResponse
	if err := t.UserAPI.PerformLoginTokenDeletion(ctx, &uapi.PerformLoginTokenDeletionRequest{Token: r.Token}, &delRes); err != nil {
		util.GetLogger(ctx).WithError(err).Error("UserAPI.PerformLoginTokenDeletion failed")
		return nil, nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !delRes.Consumed {
		// Another login used the token first.
		return nil, nil, &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("invalid login token"),
		}
	}

	r.Identifier.Type = "m.id.user"
	r.Identifier.User = res.Data.UserID

	return &r.Login, func(context.Context, *util.JSONResponse) {}, nil
}

// loginTokenRequest struct to hold the possible parameters from an HTTP request.
type loginTokenRequest struct {
	Login
	Token string `json:"token"`
}
