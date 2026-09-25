// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
)

// MakeKnock implements GET /_matrix/federation/v1/make_knock/{roomID}/{userID}.
func MakeKnock(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	roomID spec.RoomID, userID spec.UserID,
	remoteVersions []gomatrixserverlib.RoomVersion,
) util.JSONResponse {
	ctx := httpReq.Context()
	if userID.Domain() != request.Origin() {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("The knock must be sent by the server of the user"),
		}
	}

	inRoomRes := api.QueryServerJoinedToRoomResponse{}
	if err := rsAPI.QueryServerJoinedToRoom(ctx, &api.QueryServerJoinedToRoomRequest{
		ServerName: request.Destination(),
		RoomID:     roomID.String(),
	}, &inRoomRes); err != nil {
		util.GetLogger(ctx).WithError(err).Error("rsAPI.QueryServerJoinedToRoom failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if !inRoomRes.RoomExists || !inRoomRes.IsInRoom {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound(fmt.Sprintf("Local server not currently joined to room: %s", roomID.String())),
		}
	}

	roomVersion := inRoomRes.RoomVersion
	if !slices.Contains(remoteVersions, roomVersion) {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.IncompatibleRoomVersion(string(roomVersion)),
		}
	}
	verImpl, err := gomatrixserverlib.GetRoomVersion(roomVersion)
	if err != nil || roomVersion == gomatrixserverlib.RoomVersionPseudoIDs {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.IncompatibleRoomVersion(string(roomVersion)),
		}
	}
	if verImpl.CheckKnockingAllowed(string(roomVersion), "", "", spec.Knock, spec.Leave) != nil {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden(fmt.Sprintf("Room version %s does not support knocking", roomVersion)),
		}
	}

	identity, err := cfg.Matrix.SigningIdentityFor(request.Destination())
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound(fmt.Sprintf("Server name %q does not exist", request.Destination())),
		}
	}

	senderID := userID.String()
	proto := gomatrixserverlib.ProtoEvent{
		SenderID: senderID,
		RoomID:   roomID.String(),
		Type:     spec.MRoomMember,
		StateKey: &senderID,
	}
	if err = proto.SetContent(gomatrixserverlib.MemberContent{Membership: spec.Knock}); err != nil {
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	queryRes := api.QueryLatestEventsAndStateResponse{}
	event, err := eventutil.QueryAndBuildEvent(ctx, &proto, identity, time.Now(), rsAPI, &queryRes)
	if err != nil {
		var errNoExists eventutil.ErrRoomNoExists
		if errors.As(err, &errNoExists) {
			return util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("Room does not exist")}
		}
		util.GetLogger(ctx).WithError(err).Error("eventutil.QueryAndBuildEvent failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}

	// Refuse knocks the room would reject, e.g. when the join rule is not
	// knock or the user is banned.
	stateEvents := make([]gomatrixserverlib.PDU, len(queryRes.StateEvents))
	for i := range queryRes.StateEvents {
		stateEvents[i] = queryRes.StateEvents[i].PDU
	}
	provider, err := gomatrixserverlib.NewAuthEvents(stateEvents)
	if err == nil {
		err = gomatrixserverlib.Allowed(event.PDU, provider, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})
	}
	if err != nil {
		return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden(err.Error())}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: map[string]any{
			"event":        proto,
			"room_version": roomVersion,
		},
	}
}

// SendKnock implements PUT /_matrix/federation/v1/send_knock/{roomID}/{eventID}.
//
//nolint:gocyclo
func SendKnock(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	keys gomatrixserverlib.JSONVerifier,
	roomID spec.RoomID, eventID string,
) util.JSONResponse {
	ctx := httpReq.Context()
	inRoomRes := api.QueryServerJoinedToRoomResponse{}
	if err := rsAPI.QueryServerJoinedToRoom(ctx, &api.QueryServerJoinedToRoomRequest{
		ServerName: request.Destination(),
		RoomID:     roomID.String(),
	}, &inRoomRes); err != nil {
		util.GetLogger(ctx).WithError(err).Error("rsAPI.QueryServerJoinedToRoom failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if !inRoomRes.RoomExists || !inRoomRes.IsInRoom {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound(fmt.Sprintf("Local server not currently joined to room: %s", roomID.String())),
		}
	}
	verImpl, err := gomatrixserverlib.GetRoomVersion(inRoomRes.RoomVersion)
	if err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.UnsupportedRoomVersion(err.Error())}
	}

	event, err := verImpl.NewEventFromUntrustedJSON(request.Content())
	if err != nil {
		var badJSONErr gomatrixserverlib.BadJSONError
		if errors.As(err, &badJSONErr) {
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON(err.Error())}
		}
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.NotJSON("The request body could not be decoded into valid JSON. " + err.Error()),
		}
	}
	if event.RoomID().String() != roomID.String() {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The room ID in the request path must match the room ID in the knock event JSON"),
		}
	}
	if event.EventID() != eventID {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The event ID in the request path must match the event ID in the knock event JSON"),
		}
	}
	if event.Type() != spec.MRoomMember || event.StateKey() == nil || !event.StateKeyEquals(string(event.SenderID())) {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The knock event must be a membership event for its sender"),
		}
	}
	if membership, memErr := event.Membership(); memErr != nil || membership != spec.Knock {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The membership in the event content must be set to knock"),
		}
	}

	sender, err := rsAPI.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
	if err != nil || sender == nil {
		return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("The sender of the knock is invalid")}
	}
	if sender.Domain() != request.Origin() {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("The sender does not match the server that originated the request"),
		}
	}

	// Check that the event is signed by the server sending the request.
	redacted, err := verImpl.RedactEventJSON(event.JSON())
	if err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.BadJSON("The event JSON could not be redacted")}
	}
	verifyResults, err := keys.VerifyJSONs(ctx, []gomatrixserverlib.VerifyJSONRequest{{
		ServerName:           sender.Domain(),
		Message:              redacted,
		AtTS:                 event.OriginServerTS(),
		ValidityCheckingFunc: gomatrixserverlib.StrictValiditySignatureCheck,
	}})
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("keys.VerifyJSONs failed")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	if verifyResults[0].Error != nil {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("The knock must be signed by the server it originated on"),
		}
	}

	// We are responsible for sending the knock to the other servers in the
	// room, so set SendAsServer to our server name.
	var response api.InputRoomEventsResponse
	rsAPI.InputRoomEvents(ctx, &api.InputRoomEventsRequest{
		InputRoomEvents: []api.InputRoomEvent{{
			Kind:         api.KindNew,
			Event:        &types.HeaderedEvent{PDU: event},
			SendAsServer: string(cfg.Matrix.ServerName),
		}},
	}, &response)
	if response.ErrMsg != "" {
		util.GetLogger(ctx).WithField("not_allowed", response.NotAllowed).WithField("error", response.ErrMsg).Error("failed to input knock event")
		if response.NotAllowed {
			return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden(response.ErrMsg)}
		}
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}

	// MSC4311: the room state goes back as full PDUs including m.room.create.
	state, err := gomatrixserverlib.GenerateStrippedState(ctx, roomID, rsAPI.StateQuerier())
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("failed to generate knock room state")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespSendKnock{KnockRoomState: state},
	}
}
