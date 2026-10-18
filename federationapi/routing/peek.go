// Copyright 2020-2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"errors"
	"net/http"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
)

// Peek implements the SS /peek API, handling inbound peeks.
func Peek(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	roomID, peekID string,
	remoteVersions []gomatrixserverlib.RoomVersion,
) util.JSONResponse {
	// A repeated peek renews the existing one: the inbound peek is replaced.
	roomVersion, err := rsAPI.QueryRoomVersionForRoom(httpReq.Context(), roomID)
	if err != nil {
		// The room version is only missing for a room this server does not know.
		util.GetLogger(httpReq.Context()).WithError(err).WithField("room_id", roomID).Debug("Refusing a peek into an unknown room")
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Room not found"),
		}
	}

	// Check that the room that the peeking server is trying to peek is actually
	// one of the room versions that they listed in their supported ?ver= in
	// the peek URL.
	remoteSupportsVersion := false
	for _, v := range remoteVersions {
		if v == roomVersion {
			remoteSupportsVersion = true
			break
		}
	}
	// If it isn't, stop trying to peek the room.
	if !remoteSupportsVersion {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.IncompatibleRoomVersion(string(roomVersion)),
		}
	}

	// tell the peeking server to renew every hour; the interval is in milliseconds
	renewalInterval := time.Hour.Milliseconds()

	var response api.PerformInboundPeekResponse
	err = rsAPI.PerformInboundPeek(
		httpReq.Context(),
		&api.PerformInboundPeekRequest{
			RoomID:          roomID,
			PeekID:          peekID,
			ServerName:      request.Origin(),
			RenewalInterval: renewalInterval,
		},
		&response,
	)
	var notAllowed api.ErrNotAllowed
	switch {
	case errors.As(err, &notAllowed):
		// PerformInboundPeek checks history visibility and encryption.
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(notAllowed.Error()),
		}
	case err != nil:
		util.GetLogger(httpReq.Context()).WithError(err).WithField("room_id", roomID).Error("PerformInboundPeek failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if !response.RoomExists {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Room not found"),
		}
	}

	respPeek := fclient.RespPeek{
		StateEvents: types.NewEventJSONsFromHeaderedEvents(response.StateEvents),
		AuthEvents:  types.NewEventJSONsFromHeaderedEvents(response.AuthChainEvents),
		RoomVersion: response.RoomVersion,
		// The roomserver may answer with a cached, nested HeaderedEvent,
		// whose JSON carries internal keys the peeking server rejects.
		LatestEvent:     response.LatestEvent.Unwrap(),
		RenewalInterval: renewalInterval,
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: respPeek,
	}
}
