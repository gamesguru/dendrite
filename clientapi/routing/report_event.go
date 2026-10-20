// Copyright 2024 New Vector Ltd.
// Copyright 2023 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"net/http"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	userAPI "codefloe.com/pat-s/zendrite/userapi/api"
)

type reportEventRequest struct {
	Reason string `json:"reason"`
	Score  int64  `json:"score"`
}

func ReportEvent(
	req *http.Request,
	device *userAPI.Device,
	roomID, eventID string,
	rsAPI api.ClientRoomserverAPI,
) util.JSONResponse {
	defer req.Body.Close()

	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.NotFound("You don't have permission to report this event, bad userID"),
		}
	}
	// The requesting user must be a member of the room
	errRes := checkMemberInRoom(req.Context(), rsAPI, *deviceUserID, roomID)
	if errRes != nil {
		return util.JSONResponse{
			Code: http.StatusNotFound, // Spec demands this...
			JSON: spec.NotFound("The event was not found or you are not joined to the room."),
		}
	}

	// Parse the request
	report := reportEventRequest{}
	if resErr := httputil.UnmarshalJSONRequest(req, &report); resErr != nil {
		return *resErr
	}

	queryRes := &api.QueryEventsByIDResponse{}
	if err = rsAPI.QueryEventsByID(req.Context(), &api.QueryEventsByIDRequest{
		RoomID:   roomID,
		EventIDs: []string{eventID},
	}, queryRes); err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{Err: err.Error()},
		}
	}

	// No event was found or it was already redacted
	if len(queryRes.Events) == 0 || queryRes.Events[0].Redacted() {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The event was not found or you are not joined to the room."),
		}
	}

	_, err = rsAPI.InsertReportedEvent(req.Context(), roomID, eventID, device.UserID, report.Reason, report.Score)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{Err: err.Error()},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

type reportRoomRequest struct {
	Reason *string `json:"reason"`
}

// ReportRoom implements POST /rooms/{roomId}/report (MSC4151). The reporter
// need not be in the room, but the room must be known to this server.
func ReportRoom(
	req *http.Request,
	device *userAPI.Device,
	roomID string,
	rsAPI api.ClientRoomserverAPI,
) util.JSONResponse {
	var report reportRoomRequest
	if resErr := httputil.UnmarshalJSONRequest(req, &report); resErr != nil {
		return *resErr
	}
	if report.Reason == nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("Missing reason")}
	}
	if _, err := rsAPI.QueryRoomVersionForRoom(req.Context(), roomID); err != nil {
		return util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("The room was not found.")}
	}
	if _, err := rsAPI.InsertReportedRoom(req.Context(), roomID, device.UserID, *report.Reason); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("rsAPI.InsertReportedRoom failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
}
