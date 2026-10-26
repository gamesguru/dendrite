// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/clientapi/producers"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

// GetAccountData implements GET /user/{userId}/[rooms/{roomid}/]account_data/{type}.
func GetAccountData(
	req *http.Request, userAPI api.ClientUserAPI, device *api.Device,
	userID string, roomID string, dataType string,
) util.JSONResponse {
	if userID != device.UserID {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not match the current user"),
		}
	}

	dataReq := api.QueryAccountDataRequest{
		UserID:   userID,
		DataType: dataType,
		RoomID:   roomID,
	}
	dataRes := api.QueryAccountDataResponse{}
	if err := userAPI.QueryAccountData(req.Context(), &dataReq, &dataRes); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("userAPI.QueryAccountData failed")
		return util.ErrorResponse(fmt.Errorf("userAPI.QueryAccountData: %w", err))
	}

	var data json.RawMessage
	var ok bool
	if roomID != "" {
		data, ok = dataRes.RoomAccountData[roomID][dataType]
	} else {
		data, ok = dataRes.GlobalAccountData[dataType]
	}
	// MSC3391 defines an empty object as deleted account data. Keep the
	// tombstone in storage so /sync can deliver it, but make it appear absent
	// to callers of the GET endpoint.
	if ok && !isEmptyAccountData(data) {
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: data,
		}
	}

	return util.JSONResponse{
		Code: http.StatusNotFound,
		JSON: spec.NotFound("data not found"),
	}
}

// SaveAccountData implements PUT /user/{userId}/[rooms/{roomId}/]account_data/{type}.
func SaveAccountData(
	req *http.Request, userAPI api.ClientUserAPI, device *api.Device,
	userID string, roomID string, dataType string, syncProducer *producers.SyncAPIProducer,
) util.JSONResponse {
	if userID != device.UserID {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID does not match the current user"),
		}
	}

	defer req.Body.Close()

	if req.Body == http.NoBody {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("Content not JSON"),
		}
	}

	// The spec reserves these types for the server: a 405 with M_BAD_JSON
	// rejects them for both global and room account data (MSC4010).
	if dataType == "m.fully_read" || dataType == "m.push_rules" {
		return util.JSONResponse{
			Code: http.StatusMethodNotAllowed,
			JSON: spec.BadJSON(fmt.Sprintf("Cannot set %s through this API.", dataType)),
		}
	}
	if strings.HasPrefix(dataType, threePIDBindingPrefix) {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(fmt.Sprintf("Unable to modify %q using this API", dataType)),
		}
	}

	body, err := io.ReadAll(req.Body)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("io.ReadAll failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var data map[string]json.RawMessage
	if err := json.Unmarshal(body, &data); err != nil || data == nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("Account data content must be a JSON object"),
		}
	}

	dataReq := api.InputAccountDataRequest{
		UserID:      userID,
		DataType:    dataType,
		RoomID:      roomID,
		AccountData: json.RawMessage(body),
	}
	dataRes := api.InputAccountDataResponse{}
	if err := userAPI.InputAccountData(req.Context(), &dataReq, &dataRes); err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("userAPI.InputAccountData failed")
		return util.ErrorResponse(err)
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// DeleteAccountData implements MSC3391's DELETE account_data endpoint. It
// stores the empty-object tombstone so /sync can report the deletion.
func DeleteAccountData(
	req *http.Request, userAPI api.ClientUserAPI, device *api.Device,
	userID string, roomID string, dataType string,
) util.JSONResponse {
	if userID != device.UserID {
		return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("userID does not match the current user")}
	}
	if dataType == "m.fully_read" || dataType == "m.push_rules" {
		return util.JSONResponse{Code: http.StatusMethodNotAllowed, JSON: spec.BadJSON(fmt.Sprintf("Cannot delete %s through this API.", dataType))}
	}
	if strings.HasPrefix(dataType, threePIDBindingPrefix) {
		return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden(fmt.Sprintf("Unable to modify %q using this API", dataType))}
	}
	dataReq := api.InputAccountDataRequest{UserID: userID, DataType: dataType, RoomID: roomID, AccountData: json.RawMessage("{}")}
	if err := userAPI.InputAccountData(req.Context(), &dataReq, &api.InputAccountDataResponse{}); err != nil {
		return util.ErrorResponse(err)
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: struct{}{}}
}

// isEmptyAccountData reports whether data is an MSC3391 deletion tombstone.
func isEmptyAccountData(data json.RawMessage) bool {
	switch strings.TrimSpace(string(data)) {
	case "", "null", "{}":
		return true
	default:
		return false
	}
}

type fullyReadEvent struct {
	EventID string `json:"event_id"`
}

// SaveReadMarker implements POST /rooms/{roomId}/read_markers.
func SaveReadMarker(
	req *http.Request,
	userAPI api.ClientUserAPI, rsAPI roomserverAPI.ClientRoomserverAPI,
	syncProducer *producers.SyncAPIProducer, device *api.Device, roomID string,
) util.JSONResponse {
	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("userID for this device is invalid"),
		}
	}

	// Verify that the user is a member of this room
	resErr := checkMemberInRoom(req.Context(), rsAPI, *deviceUserID, roomID)
	if resErr != nil {
		return *resErr
	}

	var r eventutil.ReadMarkerJSON
	resErr = httputil.UnmarshalJSONRequest(req, &r)
	if resErr != nil {
		return *resErr
	}

	if r.FullyRead != "" {
		data, err := json.Marshal(fullyReadEvent{EventID: r.FullyRead})
		if err != nil {
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		dataReq := api.InputAccountDataRequest{
			UserID:      device.UserID,
			DataType:    "m.fully_read",
			RoomID:      roomID,
			AccountData: data,
		}
		dataRes := api.InputAccountDataResponse{}
		if err := userAPI.InputAccountData(req.Context(), &dataReq, &dataRes); err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("userAPI.InputAccountData failed")
			return util.ErrorResponse(err)
		}
	}

	// Handle the read receipts that may be included in the read marker.
	if r.Read != "" {
		return setReceipt(req, userAPI, syncProducer, device, roomID, "m.read", r.Read, "")
	}
	if r.ReadPrivate != "" {
		return setReceipt(req, userAPI, syncProducer, device, roomID, "m.read.private", r.ReadPrivate, "")
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
