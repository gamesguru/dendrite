// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/clientapi/producers"
	"codefloe.com/pat-s/zendrite/internal/receipts"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

func SetReceipt(req *http.Request, rsAPI receipts.EventQuerier, userAPI userapi.ClientUserAPI, syncProducer *producers.SyncAPIProducer, device *userapi.Device, roomID, receiptType, eventID string) util.JSONResponse {
	var body map[string]json.RawMessage
	if req.Body != nil {
		data, err := io.ReadAll(req.Body)
		if err != nil {
			return util.ErrorResponse(err)
		}
		if len(data) > 0 {
			if err = json.Unmarshal(data, &body); err != nil || body == nil {
				return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("Expected a JSON object")}
			}
		}
	}
	var threadID string
	if raw, present := body["thread_id"]; present {
		if err := json.Unmarshal(raw, &threadID); err != nil || threadID == "" || (threadID != "main" && (!strings.HasPrefix(threadID, "$") || len(threadID) < 2)) {
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("thread_id must be main or a thread root event ID")}
		}
		if receiptType != "m.read" && receiptType != "m.read.private" {
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("thread_id is incompatible with this receipt type")}
		}
		actual, err := receipts.ThreadID(req.Context(), rsAPI, roomID, eventID)
		// A thread root is itself in the main timeline but may still carry a
		// receipt for its own thread (MSC3771); any other mismatch is rejected.
		if errors.Is(err, receipts.ErrUnknownEvent) || (err == nil && actual != threadID && (actual != "main" || threadID != eventID)) {
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("event_id is not related to thread_id")}
		}
		if err != nil {
			return util.ErrorResponse(err)
		}
	}
	return setReceipt(req, userAPI, syncProducer, device, roomID, receiptType, eventID, threadID)
}

func setReceipt(req *http.Request, userAPI userapi.ClientUserAPI, syncProducer *producers.SyncAPIProducer, device *userapi.Device, roomID, receiptType, eventID, threadID string) util.JSONResponse {
	timestamp := spec.AsTimestamp(time.Now())
	logrus.WithFields(logrus.Fields{
		"roomID":      roomID,
		"receiptType": receiptType,
		"eventID":     eventID,
		"userId":      device.UserID,
		"timestamp":   timestamp,
	}).Debug("Setting receipt")

	switch receiptType {
	case "m.read", "m.read.private":
		if err := syncProducer.SendReceipt(req.Context(), device.UserID, roomID, eventID, receiptType, threadID, timestamp); err != nil {
			return util.ErrorResponse(err)
		}

	case "m.fully_read":
		data, err := json.Marshal(fullyReadEvent{EventID: eventID})
		if err != nil {
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		dataReq := userapi.InputAccountDataRequest{
			UserID:      device.UserID,
			DataType:    "m.fully_read",
			RoomID:      roomID,
			AccountData: data,
		}
		dataRes := userapi.InputAccountDataResponse{}
		if err := userAPI.InputAccountData(req.Context(), &dataReq, &dataRes); err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("userAPI.InputAccountData failed")
			return util.ErrorResponse(err)
		}

	default:
		return util.MessageResponse(400, fmt.Sprintf("Receipt type '%s' not known", receiptType)) //nolint:mnd
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}
