// Copyright 2017-2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"errors"
	"fmt"
	"net/http"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	appserviceAPI "codefloe.com/pat-s/zendrite/appservice/api"
	"codefloe.com/pat-s/zendrite/clientapi/auth/authtypes"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// GetProfile implements GET /_matrix/federation/v1/query/profile.
func GetProfile(
	httpReq *http.Request,
	userAPI userapi.FederationUserAPI,
	cfg *config.FederationAPI,
) util.JSONResponse {
	userID, field := httpReq.FormValue("user_id"), httpReq.FormValue("field")

	// httpReq.FormValue will return an empty string if value is not found
	if userID == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("The request body did not contain required argument 'user_id'."),
		}
	}

	_, domain, err := cfg.Matrix.SplitLocalID('@', userID)
	if err != nil {
		util.GetLogger(httpReq.Context()).WithError(err).Error("gomatrixserverlib.SplitID failed")
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam(fmt.Sprintf("Domain %q does not match this server", domain)),
		}
	}

	profile, err := userAPI.QueryProfile(httpReq.Context(), userID)
	if err != nil {
		if errors.Is(err, appserviceAPI.ErrProfileNotExists) {
			return util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("The user does not exist or does not have a profile."),
			}
		}
		util.GetLogger(httpReq.Context()).WithError(err).Error("userAPI.QueryProfile failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	fields := profile.ClientFields()
	if field == "" {
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: fields,
		}
	}

	// Any profile field may be requested (MSC4133). An unset display name or
	// avatar URL is reported as an empty object, as before.
	value, ok := fields[field]
	switch {
	case ok:
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: map[string]any{field: value},
		}
	case field == authtypes.ProfileFieldDisplayName, field == authtypes.ProfileFieldAvatarURL:
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: struct{}{},
		}
	}
	return util.JSONResponse{
		Code: http.StatusNotFound,
		JSON: spec.NotFound("The requested profile key does not exist."),
	}
}
