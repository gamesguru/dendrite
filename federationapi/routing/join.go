// Copyright 2017-2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	federationAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
)

const sendJoinHeroLimit = 5

// MakeJoin implements the /make_join API.
func MakeJoin(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	roomID spec.RoomID, userID spec.UserID,
	remoteVersions []gomatrixserverlib.RoomVersion,
) util.JSONResponse {
	roomVersion, err := rsAPI.QueryRoomVersionForRoom(httpReq.Context(), roomID.String())
	if err != nil {
		util.GetLogger(httpReq.Context()).WithError(err).Error("failed obtaining room version")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	req := api.QueryServerJoinedToRoomRequest{
		ServerName: request.Destination(),
		RoomID:     roomID.String(),
	}
	res := api.QueryServerJoinedToRoomResponse{}
	if err = rsAPI.QueryServerJoinedToRoom(httpReq.Context(), &req, &res); err != nil {
		util.GetLogger(httpReq.Context()).WithError(err).Error("rsAPI.QueryServerJoinedToRoom failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	createJoinTemplate := func(proto *gomatrixserverlib.ProtoEvent) (gomatrixserverlib.PDU, []gomatrixserverlib.PDU, error) { //nolint:contextcheck
		identity, signErr := cfg.Matrix.SigningIdentityFor(request.Destination())
		if signErr != nil {
			util.GetLogger(httpReq.Context()).WithError(signErr).Errorf("obtaining signing identity for %s failed", request.Destination())
			return nil, nil, spec.NotFound(fmt.Sprintf("Server name %q does not exist", request.Destination()))
		}

		queryRes := api.QueryLatestEventsAndStateResponse{
			RoomVersion: roomVersion,
		}
		event, signErr := eventutil.QueryAndBuildEvent(httpReq.Context(), proto, identity, time.Now(), rsAPI, &queryRes)
		if signErr != nil {
			var errNoExists eventutil.ErrRoomNoExists
			var badJSON gomatrixserverlib.BadJSONError
			switch {
			case errors.As(signErr, &errNoExists):
				util.GetLogger(httpReq.Context()).WithError(signErr).Error("eventutil.BuildEvent failed")
				return nil, nil, spec.NotFound("Room does not exist")
			case errors.As(signErr, &badJSON):
				util.GetLogger(httpReq.Context()).WithError(signErr).Error("eventutil.BuildEvent failed")
				return nil, nil, spec.BadJSON(badJSON.Error())
			default:
				util.GetLogger(httpReq.Context()).WithError(signErr).Error("eventutil.BuildEvent failed")
				return nil, nil, spec.InternalServerError{}
			}
		}

		stateEvents := make([]gomatrixserverlib.PDU, len(queryRes.StateEvents))
		for i, stateEvent := range queryRes.StateEvents {
			stateEvents[i] = stateEvent.PDU
		}
		return event, stateEvents, nil
	}

	roomQuerier := api.JoinRoomQuerier{
		TargetRoomID: roomID.String(),
		Roomserver:   rsAPI,
	}

	senderIDPtr, err := rsAPI.QuerySenderIDForUser(httpReq.Context(), roomID, userID)
	if err != nil {
		util.GetLogger(httpReq.Context()).WithError(err).Error("rsAPI.QuerySenderIDForUser failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	var senderID spec.SenderID
	if senderIDPtr == nil {
		senderID = spec.SenderID(userID.String())
	} else {
		senderID = *senderIDPtr
	}

	input := gomatrixserverlib.HandleMakeJoinInput{
		Context:           httpReq.Context(),
		UserID:            userID,
		SenderID:          senderID,
		RoomID:            roomID,
		RoomVersion:       roomVersion,
		RemoteVersions:    remoteVersions,
		RequestOrigin:     request.Origin(),
		LocalServerName:   cfg.Matrix.ServerName,
		LocalServerInRoom: res.RoomExists && res.IsInRoom,
		RoomQuerier:       &roomQuerier,
		UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) { //nolint:contextcheck
			return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
		},
		BuildEventTemplate: createJoinTemplate,
	}
	response, internalErr := gomatrixserverlib.HandleMakeJoin(input)
	internalErr = roomQuerier.RestrictedJoinError(internalErr)
	if internalErr != nil {
		var internalServerErr spec.InternalServerError
		var matrixErr spec.MatrixError
		var incompatibleErr spec.IncompatibleRoomVersionError
		switch {
		case errors.As(internalErr, &internalServerErr):
			util.GetLogger(httpReq.Context()).WithError(internalErr).Error("failed to handle make_join request")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		case errors.As(internalErr, &matrixErr):
			util.GetLogger(httpReq.Context()).WithError(internalErr).Error("failed to handle make_join request")
			code := http.StatusInternalServerError
			switch matrixErr.ErrCode {
			case spec.ErrorForbidden:
				code = http.StatusForbidden
			case spec.ErrorNotFound:
				code = http.StatusNotFound
			case api.ErrorUnableToAuthoriseJoin, api.ErrorUnableToGrantJoin, spec.ErrorBadJSON:
				code = http.StatusBadRequest
			}

			return util.JSONResponse{
				Code: code,
				JSON: matrixErr,
			}
		case errors.As(internalErr, &incompatibleErr):
			util.GetLogger(httpReq.Context()).WithError(internalErr).Error("failed to handle make_join request")
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: incompatibleErr,
			}
		default:
			util.GetLogger(httpReq.Context()).WithError(internalErr).Error("failed to handle make_join request")
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown("unknown error"),
			}
		}
	}

	if response == nil {
		util.GetLogger(httpReq.Context()).Error("gmsl.HandleMakeJoin returned invalid response")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: map[string]any{
			"event":        response.JoinTemplateEvent,
			"room_version": response.RoomVersion,
		},
	}
}

// SendJoin implements the /send_join API.
func SendJoin(
	httpReq *http.Request,
	request *fclient.FederationRequest,
	cfg *config.FederationAPI,
	rsAPI api.FederationRoomserverAPI,
	fsAPI federationAPI.ClientFederationAPI,
	keys gomatrixserverlib.JSONVerifier,
	roomID spec.RoomID,
	eventID string,
) util.JSONResponse {
	roomVersion, err := rsAPI.QueryRoomVersionForRoom(httpReq.Context(), roomID.String())
	if err != nil {
		util.GetLogger(httpReq.Context()).WithError(err).Error("rsAPI.QueryRoomVersionForRoom failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	input := gomatrixserverlib.HandleSendJoinInput{
		Context:           httpReq.Context(),
		RoomID:            roomID,
		EventID:           eventID,
		JoinEvent:         request.Content(),
		RoomVersion:       roomVersion,
		RequestOrigin:     request.Origin(),
		LocalServerName:   cfg.Matrix.ServerName,
		KeyID:             cfg.Matrix.KeyID,
		PrivateKey:        cfg.Matrix.PrivateKey,
		Verifier:          keys,
		MembershipQuerier: &api.MembershipQuerier{Roomserver: rsAPI},
		UserIDQuerier: func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) { //nolint:contextcheck
			return rsAPI.QueryUserIDForSender(httpReq.Context(), roomID, senderID)
		},
		StoreSenderIDFromPublicID: func(ctx context.Context, senderID spec.SenderID, userIDRaw string, roomID spec.RoomID) error {
			userID, userErr := spec.NewUserID(userIDRaw, true)
			if userErr != nil {
				return userErr
			}
			return rsAPI.StoreUserRoomPublicKey(ctx, senderID, *userID, roomID)
		},
	}
	response, joinErr := gomatrixserverlib.HandleSendJoin(input)
	if joinErr != nil {
		var internalServerErr spec.InternalServerError
		var matrixErr spec.MatrixError
		switch {
		case errors.As(joinErr, &internalServerErr):
			util.GetLogger(httpReq.Context()).WithError(joinErr)
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		case errors.As(joinErr, &matrixErr):
			util.GetLogger(httpReq.Context()).WithError(joinErr)
			code := http.StatusInternalServerError
			switch matrixErr.ErrCode {
			case spec.ErrorForbidden:
				code = http.StatusForbidden
			case spec.ErrorNotFound:
				code = http.StatusNotFound
			case spec.ErrorUnsupportedRoomVersion:
				code = http.StatusInternalServerError
			case spec.ErrorBadJSON:
				code = http.StatusBadRequest
			}

			return util.JSONResponse{
				Code: code,
				JSON: matrixErr,
			}
		default:
			util.GetLogger(httpReq.Context()).WithError(joinErr)
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.Unknown("unknown error"),
			}
		}
	}

	if response == nil {
		util.GetLogger(httpReq.Context()).Error("gmsl.HandleMakeJoin returned invalid response")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Fetch the state and auth chain. We do this before we send the events
	// on, in case this fails.
	var stateAndAuthChainResponse api.QueryStateAndAuthChainResponse
	err = rsAPI.QueryStateAndAuthChain(httpReq.Context(), &api.QueryStateAndAuthChainRequest{
		PrevEventIDs: response.JoinEvent.PrevEventIDs(),
		AuthEventIDs: response.JoinEvent.AuthEventIDs(),
		RoomID:       roomID.String(),
		ResolveState: true,
	}, &stateAndAuthChainResponse)
	if err != nil {
		util.GetLogger(httpReq.Context()).WithError(err).Error("rsAPI.QueryStateAndAuthChain failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if !stateAndAuthChainResponse.RoomExists {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Room does not exist"),
		}
	}
	if !stateAndAuthChainResponse.StateKnown {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("State not known"),
		}
	}
	signedURI, parseErr := url.ParseRequestURI(request.RequestURI())
	partialState := parseErr == nil && signedURI.Query().Get("omit_members") == "true"
	var serversInRoom []string
	if partialState {
		// Capture the room's existing hosts before the joining server's event is
		// persisted, so servers_in_room does not include the requester itself.
		var serverRes federationAPI.QueryJoinedHostServerNamesInRoomResponse
		if err := fsAPI.QueryJoinedHostServerNamesInRoom(httpReq.Context(), &federationAPI.QueryJoinedHostServerNamesInRoomRequest{
			RoomID: roomID.String(),
		}, &serverRes); err != nil {
			util.GetLogger(httpReq.Context()).WithError(err).Error("fsAPI.QueryJoinedHostServerNamesInRoom failed")
			return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
		}
		serversInRoom = make([]string, 0, len(serverRes.ServerNames))
		for _, serverName := range serverRes.ServerNames {
			serversInRoom = append(serversInRoom, string(serverName))
		}
	}

	// Send the events to the room server.
	// We are responsible for notifying other servers that the user has joined
	// the room, so set SendAsServer to cfg.Matrix.ServerName
	if !response.AlreadyJoined {
		var rsResponse api.InputRoomEventsResponse
		rsAPI.InputRoomEvents(httpReq.Context(), &api.InputRoomEventsRequest{
			InputRoomEvents: []api.InputRoomEvent{
				{
					Kind:          api.KindNew,
					Event:         &types.HeaderedEvent{PDU: response.JoinEvent},
					SendAsServer:  string(cfg.Matrix.ServerName),
					TransactionID: nil,
				},
			},
		}, &rsResponse)
		if rsResponse.ErrMsg != "" {
			util.GetLogger(httpReq.Context()).WithField(logrus.ErrorKey, rsResponse.ErrMsg).Error("SendEvents failed")
			if rsResponse.NotAllowed {
				return util.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.Forbidden(rsResponse.ErrMsg),
				}
			}
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	// sort events deterministically by depth (lower is earlier)
	// We also do this because sytest's basic federation server isn't good at using the correct
	// state if these lists are randomized, resulting in flakey tests. :(
	sort.Sort(eventsByDepth(stateAndAuthChainResponse.StateEvents))
	sort.Sort(eventsByDepth(stateAndAuthChainResponse.AuthChainEvents))

	// https://matrix.org/docs/spec/server_server/latest#put-matrix-federation-v1-send-join-roomid-eventid
	stateEvents := stateAndAuthChainResponse.StateEvents
	authEvents := stateAndAuthChainResponse.AuthChainEvents
	if partialState {
		joiningUserID := ""
		if stateKey := response.JoinEvent.StateKey(); stateKey != nil {
			joiningUserID = *stateKey
		}
		stateEvents = partialSendJoinState(stateEvents, joiningUserID)
		stateEventIDs := make(map[string]struct{}, len(stateEvents))
		for _, event := range stateEvents {
			stateEventIDs[event.EventID()] = struct{}{}
		}
		filteredAuthEvents := make([]*types.HeaderedEvent, 0, len(authEvents))
		for _, event := range authEvents {
			if _, includedInState := stateEventIDs[event.EventID()]; !includedInState {
				filteredAuthEvents = append(filteredAuthEvents, event)
			}
		}
		authEvents = filteredAuthEvents
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: fclient.RespSendJoin{
			StateEvents:    types.NewEventJSONsFromHeaderedEvents(stateEvents),
			AuthEvents:     types.NewEventJSONsFromHeaderedEvents(authEvents),
			Origin:         cfg.Matrix.ServerName,
			Event:          response.JoinEvent.JSON(),
			MembersOmitted: partialState,
			ServersInRoom:  serversInRoom,
		},
	}
}

// partialSendJoinState keeps non-membership state and, for unnamed rooms, up to
// five room heroes for an MSC3706 send_join response. Heroes are selected from
// current joined and invited members, falling back to departed members when
// none remain. The joining user's existing membership event is retained.
func partialSendJoinState(state []*types.HeaderedEvent, joiningUserID string) []*types.HeaderedEvent {
	var joined, invited, departed []*types.HeaderedEvent
	var joiningMembership *types.HeaderedEvent
	hasNameOrAlias := false
	for _, event := range state {
		if event.Type() == spec.MRoomName || event.Type() == spec.MRoomCanonicalAlias {
			hasNameOrAlias = true
		}
		if event.Type() != spec.MRoomMember || event.StateKey() == nil {
			continue
		}
		if *event.StateKey() == joiningUserID {
			joiningMembership = event
			continue
		}
		membership, err := event.Membership()
		if err != nil {
			continue
		}
		switch membership {
		case spec.Join:
			joined = append(joined, event)
		case spec.Invite:
			invited = append(invited, event)
		case spec.Leave, spec.Ban:
			departed = append(departed, event)
		}
	}
	var members []*types.HeaderedEvent
	if !hasNameOrAlias {
		if len(joined) > 0 || len(invited) > 0 {
			members = make([]*types.HeaderedEvent, 0, len(joined)+len(invited))
			members = append(members, joined...)
			members = append(members, invited...)
		} else {
			members = departed
		}
	}
	// Pick the oldest members first to match /sync's SelectRoomHeroes, which
	// orders by added_at. That column lives in the sync API's database, so the
	// event's depth (then event ID) stands in for it here.
	sort.Slice(members, func(i, j int) bool {
		if members[i].Depth() == members[j].Depth() {
			return members[i].EventID() < members[j].EventID()
		}
		return members[i].Depth() < members[j].Depth()
	})
	if len(members) > sendJoinHeroLimit {
		members = members[:sendJoinHeroLimit]
	}
	heroes := make(map[string]struct{}, len(members)+1)
	if joiningMembership != nil {
		heroes[joiningMembership.EventID()] = struct{}{}
	}
	for _, event := range members {
		heroes[event.EventID()] = struct{}{}
	}
	filtered := make([]*types.HeaderedEvent, 0, len(state))
	for _, event := range state {
		if event.Type() == spec.MRoomMember {
			if _, ok := heroes[event.EventID()]; !ok {
				continue
			}
		}
		filtered = append(filtered, event)
	}
	return filtered
}

type eventsByDepth []*types.HeaderedEvent

func (e eventsByDepth) Len() int {
	return len(e)
}

func (e eventsByDepth) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e eventsByDepth) Less(i, j int) bool {
	return e[i].Depth() < e[j].Depth()
}
