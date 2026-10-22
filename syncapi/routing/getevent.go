// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"net/http"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// GetEvent implements
//
//	GET /_matrix/client/r0/rooms/{roomId}/event/{eventId}
//
// https://spec.matrix.org/v1.4/client-server-api/#get_matrixclientv3roomsroomideventeventid
func GetEvent(
	req *http.Request,
	device *userapi.Device,
	rawRoomID string,
	eventID string,
	cfg *config.SyncAPI,
	syncDB storage.Database,
	rsAPI api.SyncRoomserverAPI,
) util.JSONResponse {
	ctx := req.Context()
	db, err := syncDB.NewDatabaseSnapshot(ctx)
	logger := util.GetLogger(ctx).WithFields(logrus.Fields{
		"event_id": eventID,
		"room_id":  rawRoomID,
	})
	if err != nil {
		logger.WithError(err).Error("GetEvent: syncDB.NewDatabaseTransaction failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	defer db.Rollback() //nolint:errcheck

	roomID, err := spec.NewRoomID(rawRoomID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("invalid room ID"),
		}
	}

	events, err := db.Events(ctx, []string{eventID})
	if err != nil {
		logger.WithError(err).Error("GetEvent: syncDB.Events failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// The requested event does not exist in our database
	if len(events) == 0 {
		logger.Debugf("GetEvent: requested event doesn't exist locally")
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The event was not found or you do not have permission to read this event"),
		}
	}

	// The roomserver is the authority on an event's verdict, and it can change after
	// the sync API stored the event: one accepted while a room had partial state may
	// be rejected once the full state is known. It also accepts a redaction before
	// the sync consumer has necessarily applied it to its event copy, so prefer its
	// redacted event in that short window and /event does not expose the old content
	// after the redaction request has completed.
	var roomEvents api.QueryEventsByIDResponse
	if err = rsAPI.QueryEventsByID(ctx, &api.QueryEventsByIDRequest{
		RoomID:          rawRoomID,
		EventIDs:        []string{eventID},
		ExcludeRejected: true,
	}, &roomEvents); err != nil {
		logger.WithError(err).Error("GetEvent: rsAPI.QueryEventsByID failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	var roomEvent *rstypes.HeaderedEvent
	for _, candidate := range roomEvents.Events {
		if candidate.EventID() == eventID {
			roomEvent = candidate
			break
		}
	}
	if roomEvent == nil {
		logger.Debugf("GetEvent: the roomserver has rejected the requested event or does not have it")
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The event was not found or you do not have permission to read this event"),
		}
	}
	if !events[0].Redacted() && roomEvent.Redacted() {
		// Keep the sync copy's history visibility, which the
		// roomserver's event does not carry.
		copied := *events[0]
		copied.PDU = roomEvent.PDU
		events[0] = &copied
	}

	// If the request is coming from an appservice, get the user from the request
	rawUserID := device.UserID
	if asUserID := req.FormValue("user_id"); device.AppserviceID != "" && asUserID != "" {
		rawUserID = asUserID
	}

	userID, err := spec.NewUserID(rawUserID, true)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("invalid device.UserID")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Apply history visibility to determine if the user is allowed to view the event
	events, err = internal.ApplyHistoryVisibilityFilter(ctx, db, rsAPI, events, nil, *userID, "event")
	if err != nil {
		logger.WithError(err).Error("GetEvent: internal.ApplyHistoryVisibilityFilter failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// We only ever expect there to be one event
	if len(events) != 1 {
		// 0 events -> not allowed to view event; > 1 events -> something that shouldn't happen
		logger.WithField("event_count", len(events)).Debug("GetEvent: can't return the requested event")
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("The event was not found or you do not have permission to read this event"),
		}
	}

	clientEvent, err := synctypes.ToClientEvent(events[0], synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).WithField("senderID", events[0].SenderID()).WithField("roomID", *roomID).Error("Failed converting to ClientEvent")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	bundler, err := internal.NewRelationBundler(ctx, db, rsAPI, *userID)
	if err == nil {
		err = bundler.BundleEvent(ctx, events[0], clientEvent, synctypes.FormatAll)
	}
	if err != nil {
		logger.WithError(err).Error("GetEvent: failed to bundle relations")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: *clientEvent,
	}
}
