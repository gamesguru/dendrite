// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/gomatrix"
	"github.com/matrix-org/util"

	appserviceAPI "codefloe.com/pat-s/dendrite/appservice/api"
	"codefloe.com/pat-s/dendrite/clientapi/httputil"
	"codefloe.com/pat-s/dendrite/internal/eventutil"
	roomserverAPI "codefloe.com/pat-s/dendrite/roomserver/api"
	"codefloe.com/pat-s/dendrite/userapi/api"
)

func JoinRoomByIDOrAlias(
	req *http.Request,
	device *api.Device,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	profileAPI api.ClientUserAPI,
	roomIDOrAlias string,
) util.JSONResponse {
	// MSC3706: Trace join timing for diagnostics
	joinStartTime := time.Now()
	logger := util.GetLogger(req.Context()).WithFields(map[string]any{
		"room_id_or_alias": roomIDOrAlias,
		"user_id":          device.UserID,
		"trace":            "join_timing",
	})
	logger.Debug("Join request received")

	// Prepare to ask the roomserver to perform the room join.
	joinReq := roomserverAPI.PerformJoinRequest{
		RoomIDOrAlias: roomIDOrAlias,
		UserID:        device.UserID,
		IsGuest:       device.AccountType == api.AccountTypeGuest,
		Content:       map[string]any{},
	}

	// Check to see if any ?via= or ?server_name= query parameters
	// were given in the request.
	if serverNames, ok := req.URL.Query()["via"]; ok {
		for _, serverName := range serverNames {
			joinReq.ServerNames = append(
				joinReq.ServerNames,
				spec.ServerName(serverName),
			)
		}
	} else if serverNames, ok := req.URL.Query()["server_name"]; ok {
		for _, serverName := range serverNames {
			joinReq.ServerNames = append(
				joinReq.ServerNames,
				spec.ServerName(serverName),
			)
		}
	}

	// If content was provided in the request then include that
	// in the request. It'll get used as a part of the membership
	// event content.
	_ = httputil.UnmarshalJSONRequest(req, &joinReq.Content)

	// Work out our localpart for the client profile request.

	// Request our profile content to populate the request content with.
	profile, err := profileAPI.QueryProfile(req.Context(), device.UserID)

	switch {
	case err == nil:
		joinReq.Content["displayname"] = profile.DisplayName
		joinReq.Content["avatar_url"] = profile.AvatarURL
	case errors.Is(err, appserviceAPI.ErrProfileNotExists):
		util.GetLogger(req.Context()).Error("Unable to query user profile, no profile found.")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("Unable to query user profile, no profile found."),
		}
	default:
	}

	// Ask the roomserver to perform the join.
	done := make(chan util.JSONResponse, 1)
	go func() { //nolint:contextcheck
		defer close(done)
		roomID, _, err := rsAPI.PerformJoin(req.Context(), &joinReq)
		var response util.JSONResponse

		var errInvalidID roomserverAPI.ErrInvalidID
		var errNotAllowed roomserverAPI.ErrNotAllowed
		var errHTTP *gomatrix.HTTPError
		var errRoomNoExists eventutil.ErrRoomNoExists
		switch {
		case err == nil: // success case
			response = util.JSONResponse{
				Code: http.StatusOK,
				// TODO: Put the response struct somewhere internal.
				JSON: struct {
					RoomID string `json:"room_id"`
				}{roomID},
			}
		case errors.As(err, &errInvalidID):
			response = util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam(errInvalidID.Error()),
			}
		case errors.As(err, &errNotAllowed):
			jsonErr := spec.Forbidden(errNotAllowed.Error())
			if device.AccountType == api.AccountTypeGuest {
				jsonErr = spec.GuestAccessForbidden(errNotAllowed.Error())
			}
			response = util.JSONResponse{
				Code: http.StatusForbidden,
				JSON: jsonErr,
			}
		case errors.As(err, &errHTTP): // this ensures we proxy responses over federation to the client
			response = util.JSONResponse{
				Code: errHTTP.Code,
				JSON: json.RawMessage(errHTTP.Message),
			}
		case errors.As(err, &errRoomNoExists):
			response = util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound(errRoomNoExists.Error()),
			}
		default:
			// Check if this is already a Matrix error and preserve its error code
			if resp := httputil.MatrixErrorResponse(err); resp != nil {
				response = *resp
			} else {
				response = util.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
		}
		done <- response
	}()

	// Wait either for the join to finish, or for us to hit a reasonable
	// timeout, at which point we'll just return a 200 to placate clients.
	timer := time.NewTimer(time.Second * 20)
	select {
	case <-timer.C:
		logger.WithFields(map[string]any{
			"duration_ms": time.Since(joinStartTime).Milliseconds(),
			"result":      "timeout_202",
		}).Debug("Join request timeout - returning 202 (join continues in background)")
		return util.JSONResponse{
			Code: http.StatusAccepted,
			JSON: spec.Unknown("The room join will continue in the background."),
		}
	case result := <-done:
		// Stop and drain the timer
		if !timer.Stop() {
			<-timer.C
		}
		logger.WithFields(map[string]any{
			"duration_ms": time.Since(joinStartTime).Milliseconds(),
			"result_code": result.Code,
		}).Debug("Join request completed")
		return result
	}
}
