// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"io"
	"net/http"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type getLoginTokenResponse struct {
	LoginToken  string `json:"login_token"`
	ExpiresInMS int64  `json:"expires_in_ms"`
}

// GetLoginToken implements POST /_matrix/client/v1/login/get_token (MSC3882):
// after the user authenticates again, it returns a single-use m.login.token
// that signs in a new client as the same user. Verify asks for the password on
// every call, so each new client needs explicit consent.
func GetLoginToken(
	req *http.Request, userInteractiveAuth *auth.UserInteractive, userAPI userapi.ClientUserAPI, device *userapi.Device,
) util.JSONResponse {
	ctx := req.Context()

	bodyBytes, err := io.ReadAll(req.Body)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The request body could not be read: " + err.Error()),
		}
	}

	login, errRes := userInteractiveAuth.Verify(ctx, bodyBytes, device)
	if errRes != nil {
		return *errRes
	}
	if login.Username() != device.UserID {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("unable to create a login token for another user"),
		}
	}

	var res userapi.PerformLoginTokenCreationResponse
	if err = userAPI.PerformLoginTokenCreation(ctx, &userapi.PerformLoginTokenCreationRequest{
		Data: userapi.LoginTokenData{UserID: device.UserID},
	}, &res); err != nil {
		util.GetLogger(ctx).WithError(err).Error("userAPI.PerformLoginTokenCreation failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: getLoginTokenResponse{
			LoginToken:  res.Metadata.Token,
			ExpiresInMS: time.Until(res.Metadata.Expiration).Milliseconds(),
		},
	}
}
