// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"encoding/json"
	"net/http"

	appserviceAPI "github.com/element-hq/dendrite/appservice/api"
	"github.com/element-hq/dendrite/clientapi/httputil"
	"github.com/element-hq/dendrite/internal/eventutil"
	roomserverAPI "github.com/element-hq/dendrite/roomserver/api"
	"github.com/element-hq/dendrite/userapi/api"
	"github.com/matrix-org/gomatrix"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
)

func JoinRoomByIDOrAlias(
	joinCtx context.Context,
	device *api.Device,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	profileAPI api.ClientUserAPI,
	roomIDOrAlias string,
	content map[string]interface{},
	serverNames []spec.ServerName,
) util.JSONResponse {
	// Prepare to ask the roomserver to perform the room join.
	if content == nil {
		content = map[string]interface{}{}
	}
	joinReq := roomserverAPI.PerformJoinRequest{
		RoomIDOrAlias: roomIDOrAlias,
		UserID:        device.UserID,
		IsGuest:       device.AccountType == api.AccountTypeGuest,
		Content:       content,
		ServerNames:   serverNames,
	}

	// Work out our localpart for the client profile request.

	// Request our profile content to populate the request content with.
	profile, err := profileAPI.QueryProfile(joinCtx, device.UserID)

	switch err {
	case nil:
		joinReq.Content["displayname"] = profile.DisplayName
		joinReq.Content["avatar_url"] = profile.AvatarURL
	case appserviceAPI.ErrProfileNotExists:
		util.GetLogger(joinCtx).Error("Unable to query user profile, no profile found.")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("Unable to query user profile, no profile found."),
		}
	default:
	}

	roomID, _, err := rsAPI.PerformJoin(joinCtx, &joinReq)
	switch e := err.(type) {
	case nil: // success case
		return util.JSONResponse{
			Code: http.StatusOK,
			// TODO: Put the response struct somewhere internal.
			JSON: struct {
				RoomID string `json:"room_id"`
			}{roomID},
		}
	case roomserverAPI.ErrInvalidID:
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.Unknown(e.Error()),
		}
	case roomserverAPI.ErrNotAllowed:
		jsonErr := spec.Forbidden(e.Error())
		if device.AccountType == api.AccountTypeGuest {
			jsonErr = spec.GuestAccessForbidden(e.Error())
		}
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: jsonErr,
		}
	case *gomatrix.HTTPError: // this ensures we proxy responses over federation to the client
		return util.JSONResponse{
			Code: e.Code,
			JSON: json.RawMessage(e.Message),
		}
	case eventutil.ErrRoomNoExists:
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound(e.Error()),
		}
	default:
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
}

func joinRequestContentAndServers(req *http.Request) (map[string]interface{}, []spec.ServerName) {
	content := map[string]interface{}{}
	_ = httputil.UnmarshalJSONRequest(req, &content)

	var serverNames []spec.ServerName
	if via, ok := req.URL.Query()["via"]; ok {
		for _, serverName := range via {
			serverNames = append(serverNames, spec.ServerName(serverName))
		}
	} else if queryServerNames, ok := req.URL.Query()["server_name"]; ok {
		for _, serverName := range queryServerNames {
			serverNames = append(serverNames, spec.ServerName(serverName))
		}
	}

	return content, serverNames
}

func asyncJoinResponse(roomIDOrAlias string) util.JSONResponse {
	var roomID string
	if _, err := spec.NewRoomID(roomIDOrAlias); err == nil {
		roomID = roomIDOrAlias
	}
	return util.JSONResponse{
		Code: http.StatusAccepted,
		JSON: struct {
			RoomID  string `json:"room_id,omitempty"`
			Joining bool   `json:"joining"`
		}{
			RoomID:  roomID,
			Joining: true,
		},
	}
}
