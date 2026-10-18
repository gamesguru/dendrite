// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/gomatrix"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

// KnockRoomByIDOrAlias implements POST /_matrix/client/v3/knock/{roomIdOrAlias}.
func KnockRoomByIDOrAlias(
	req *http.Request,
	device *api.Device,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	profileAPI api.ClientUserAPI,
	roomIDOrAlias string,
) util.JSONResponse {
	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("Invalid user ID")}
	}

	var body struct {
		Reason string `json:"reason"`
	}
	rawBody, err := io.ReadAll(req.Body)
	if err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("Unable to read the request body")}
	}
	if len(bytes.TrimSpace(rawBody)) > 0 {
		if err = json.Unmarshal(rawBody, &body); err != nil {
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.NotJSON("The request body is not valid JSON: " + err.Error())}
		}
	}

	knockReq := roomserverAPI.PerformKnockRequest{
		RoomIDOrAlias: roomIDOrAlias,
		UserID:        *userID,
		Content:       map[string]any{},
	}
	if body.Reason != "" {
		knockReq.Content["reason"] = body.Reason
	}
	// Spec 1.12 renamed server_name to via; accept both.
	servers := req.URL.Query()["via"]
	if len(servers) == 0 {
		servers = req.URL.Query()["server_name"]
	}
	for _, serverName := range servers {
		knockReq.ServerNames = append(knockReq.ServerNames, spec.ServerName(serverName))
	}

	// Include the profile, as joins do, so the room can show who knocked.
	if profile, profileErr := profileAPI.QueryProfile(req.Context(), device.UserID); profileErr == nil {
		if profile.DisplayName != "" {
			knockReq.Content["displayname"] = profile.DisplayName
		}
		if profile.AvatarURL != "" {
			knockReq.Content["avatar_url"] = profile.AvatarURL
		}
	}

	roomID, err := rsAPI.PerformKnock(req.Context(), &knockReq)
	var errInvalidID roomserverAPI.ErrInvalidID
	var errNotAllowed roomserverAPI.ErrNotAllowed
	var errHTTP *gomatrix.HTTPError
	switch {
	case err == nil:
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: struct {
				RoomID string `json:"room_id"`
			}{roomID},
		}
	case errors.As(err, &errInvalidID):
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam(errInvalidID.Error())}
	case errors.As(err, &errNotAllowed):
		return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden(errNotAllowed.Error())}
	case errors.As(err, &errHTTP) && errHTTP.Code >= 400 && json.Valid([]byte(errHTTP.Message)):
		// Pass the remote server's answer through, e.g. a 403 because the
		// room does not allow knocking.
		return util.JSONResponse{Code: errHTTP.Code, JSON: json.RawMessage(errHTTP.Message)}
	case errors.As(err, &errHTTP):
		util.GetLogger(req.Context()).WithError(err).Warn("Knock failed over federation")
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: spec.Unknown("Unable to knock on the room through any server"),
		}
	default:
		if resp := httputil.MatrixErrorResponse(err); resp != nil {
			return *resp
		}
		util.GetLogger(req.Context()).WithError(err).Error("PerformKnock failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
}
