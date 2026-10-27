// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"errors"
	"net/http"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/clientapi/auth"
	"codefloe.com/pat-s/zendrite/clientapi/httputil"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/internal/transactions"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type redactionContent struct {
	Reason  string `json:"reason"`
	Redacts string `json:"redacts"`
}

type redactionResponse struct {
	EventID string `json:"event_id"`
}

func SendRedaction(
	req *http.Request, device *userapi.Device, roomID, eventID string, cfg *config.ClientAPI,
	rsAPI roomserverAPI.ClientRoomserverAPI,
	txnID *string,
	txnCache *transactions.Cache,
) util.JSONResponse {
	deviceUserID, userIDErr := spec.NewUserID(device.UserID, true)
	if userIDErr != nil {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID doesn't have power level to redact"),
		}
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("RoomID is invalid"),
		}
	}
	senderID, queryErr := rsAPI.QuerySenderIDForUser(req.Context(), *validRoomID, *deviceUserID)
	if queryErr != nil {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("userID doesn't have power level to redact"),
		}
	}

	resErr := checkMemberInRoom(req.Context(), rsAPI, *deviceUserID, roomID)
	if resErr != nil {
		return *resErr
	}

	// if user is member of room, and sender ID is nil, then this user doesn't have a pseudo ID for some reason,
	// which is unexpected.
	if senderID == nil {
		util.GetLogger(req.Context()).WithField("userID", *deviceUserID).WithField("roomID", roomID).Error("missing sender ID for user, despite having membership")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if txnID != nil {
		// Try to fetch response from transactionsCache
		if res, ok := txnCache.FetchTransaction(device, *txnID, req.URL); ok {
			return *res
		}
	}

	if resErr = checkRedactionAllowed(req.Context(), rsAPI, roomID, eventID, *senderID, device.Suspended); resErr != nil {
		return *resErr
	}

	var r redactionContent
	resErr = httputil.UnmarshalJSONRequest(req, &r)
	if resErr != nil {
		return *resErr
	}

	// create the new event and set all the fields we can
	proto := gomatrixserverlib.ProtoEvent{
		SenderID: string(*senderID),
		RoomID:   roomID,
		Type:     spec.MRoomRedaction,
		Redacts:  eventID,
	}

	// Room version 11 expects the "redacts" field on the
	// content field, so add it here as well
	r.Redacts = eventID

	err = proto.SetContent(r)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("proto.SetContent failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	identity, err := rsAPI.SigningIdentityFor(req.Context(), *validRoomID, *deviceUserID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var queryRes roomserverAPI.QueryLatestEventsAndStateResponse
	e, err := eventutil.QueryAndBuildEvent(req.Context(), &proto, &identity, time.Now(), rsAPI, &queryRes)
	if errors.Is(err, eventutil.ErrRoomNoExists{}) {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Room does not exist"),
		}
	}
	domain := device.UserDomain()
	if err = roomserverAPI.SendEvents(context.Background(), rsAPI, roomserverAPI.KindNew, []*types.HeaderedEvent{e}, device.UserDomain(), domain, domain, nil, false); err != nil { //nolint:contextcheck
		util.GetLogger(req.Context()).WithError(err).Errorf("failed to SendEvents")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	res := util.JSONResponse{
		Code: http.StatusOK,
		JSON: redactionResponse{
			EventID: e.EventID(),
		},
	}

	// Add response to transactionsCache
	if txnID != nil {
		txnCache.AddTransaction(device, *txnID, req.URL, &res)
	}

	return res
}

// checkRedactionAllowed reports whether senderID may redact eventID in
// roomID. It is used both by /redact and by /send/m.room.redaction (MSC4169):
// the roomserver also accepts redactions between users of the same server, so
// this check is what stops one local user from redacting another's events.
func checkRedactionAllowed(
	ctx context.Context, rsAPI roomserverAPI.ClientRoomserverAPI, roomID, eventID string, senderID spec.SenderID, suspended bool,
) *util.JSONResponse {
	ev := roomserverAPI.GetEvent(ctx, rsAPI, roomID, eventID)
	// The event may be unknown to us (e.g. we joined partway through the room's
	// history). We then can't tell who sent it, so only the power level check
	// below applies and the redaction is still sent out over federation.
	if ev != nil && ev.RoomID().String() != roomID {
		return &util.JSONResponse{
			Code: 400, //nolint:mnd
			JSON: spec.NotFound("cannot redact event in another room"),
		}
	}

	// Suspended users may only redact their own events (MSC3823).
	if suspended && (ev == nil || ev.SenderID() != senderID) {
		res := auth.UserSuspendedResponse()
		return &res
	}

	privileged := isPrivilegedCreator(ctx, rsAPI, roomID, senderID)

	// "Users may redact their own events, and any user with a power level greater than or equal
	// to the redact power level of the room may redact events there"
	// https://matrix.org/docs/spec/client_server/r0.6.1#put-matrix-client-r0-rooms-roomid-redact-eventid-txnid
	allowedToRedact := (ev != nil && ev.SenderID() == senderID) || privileged
	if !allowedToRedact {
		plEvent := roomserverAPI.GetStateEvent(ctx, rsAPI, roomID, gomatrixserverlib.StateKeyTuple{
			EventType: spec.MRoomPowerLevels,
			StateKey:  "",
		})
		if plEvent == nil {
			return &util.JSONResponse{
				Code: 403, //nolint:mnd
				JSON: spec.Forbidden("You don't have permission to redact this event, no power_levels event in this room."),
			}
		}
		pl, plErr := plEvent.PowerLevels()
		if plErr != nil {
			return &util.JSONResponse{
				Code: 403, //nolint:mnd
				JSON: spec.Forbidden(
					"You don't have permission to redact this event, the power_levels event for this room is malformed so auth checks cannot be performed.",
				),
			}
		}
		allowedToRedact = pl.UserLevel(senderID) >= pl.Redact
	}
	if !allowedToRedact {
		return &util.JSONResponse{
			Code: 403, //nolint:mnd
			JSON: spec.Forbidden("You don't have permission to redact this event, power level too low."),
		}
	}
	return nil
}
