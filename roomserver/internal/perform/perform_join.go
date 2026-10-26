// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform

import (
	"context"
	"crypto/ed25519"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/getsentry/sentry-go"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	fsAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	rsAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/internal/helpers"
	"codefloe.com/pat-s/zendrite/roomserver/internal/input"
	"codefloe.com/pat-s/zendrite/roomserver/internal/query"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
)

type Joiner struct {
	Cfg   *config.RoomServer
	FSAPI fsAPI.RoomserverFederationAPI
	RSAPI rsAPI.RoomserverInternalAPI
	DB    storage.Database

	Inputer *input.Inputer
	Queryer *query.Queryer

	PurgeTracker     *PurgeTracker
	PurgeWaitTimeout time.Duration
}

// PerformJoin handles joining matrix rooms, including over federation by talking to the federationapi.
func (r *Joiner) PerformJoin(
	ctx context.Context,
	req *rsAPI.PerformJoinRequest,
) (roomID string, joinedVia spec.ServerName, err error) {
	// MSC3706: Trace join timing for diagnostics
	joinStartTime := time.Now()
	logger := logrus.WithContext(ctx).WithFields(logrus.Fields{
		"room_id": req.RoomIDOrAlias,
		"user_id": req.UserID,
		"servers": req.ServerNames,
		"trace":   "join_timing",
	})
	logger.Debug("Roomserver join request started")
	roomID, joinedVia, err = r.performJoin(context.Background(), req) //nolint:contextcheck
	if err != nil {
		logger.WithFields(logrus.Fields{
			"duration_ms": time.Since(joinStartTime).Milliseconds(),
			"result":      "error",
		}).WithError(err).Error("Roomserver join failed")
		sentry.CaptureException(err)
		return "", "", err
	}
	logger.WithFields(logrus.Fields{
		"duration_ms": time.Since(joinStartTime).Milliseconds(),
		"joined_via":  joinedVia,
		"result":      "success",
	}).Debug("Roomserver join completed successfully")

	return roomID, joinedVia, nil
}

func (r *Joiner) performJoin(
	ctx context.Context,
	req *rsAPI.PerformJoinRequest,
) (string, spec.ServerName, error) {
	_, domain, err := gomatrixserverlib.SplitID('@', req.UserID)
	if err != nil {
		return "", "", rsAPI.ErrInvalidID{Err: fmt.Errorf("supplied user ID %q in incorrect format", req.UserID)}
	}
	if !r.Cfg.Matrix.IsLocalServerName(domain) {
		return "", "", rsAPI.ErrInvalidID{Err: fmt.Errorf("user %q does not belong to this homeserver", req.UserID)}
	}
	if strings.HasPrefix(req.RoomIDOrAlias, "!") {
		return r.performJoinRoomByID(ctx, req)
	}
	if strings.HasPrefix(req.RoomIDOrAlias, "#") {
		return r.performJoinRoomByAlias(ctx, req)
	}
	return "", "", rsAPI.ErrInvalidID{Err: fmt.Errorf("room ID or alias %q is invalid", req.RoomIDOrAlias)}
}

func (r *Joiner) performJoinRoomByAlias(
	ctx context.Context,
	req *rsAPI.PerformJoinRequest,
) (string, spec.ServerName, error) {
	if req.LocalOnly {
		_, domain, err := gomatrixserverlib.SplitID('#', req.RoomIDOrAlias)
		if err != nil {
			return "", "", rsAPI.ErrInvalidID{Err: fmt.Errorf("alias %q is not in the correct format", req.RoomIDOrAlias)}
		}
		if !r.Cfg.Matrix.IsLocalServerName(domain) {
			return "", "", errLocalJoinOnly
		}
	}
	roomID, servers, err := resolveRoomAlias(ctx, r.Cfg, r.FSAPI, r.RSAPI, req.RoomIDOrAlias)
	if err != nil {
		return "", "", err
	}
	req.ServerNames = append(req.ServerNames, servers...)

	// If we do, then pluck out the room ID and continue the join.
	req.RoomIDOrAlias = roomID
	return r.performJoinRoomByID(ctx, req)
}

// resolveRoomAlias looks up the room ID for an alias, locally or over
// federation, and returns servers which are likely to be in the room.
func resolveRoomAlias(
	ctx context.Context,
	cfg *config.RoomServer,
	fedAPI fsAPI.RoomserverFederationAPI,
	rsapi rsAPI.RoomserverInternalAPI,
	alias string,
) (string, []spec.ServerName, error) {
	// Get the domain part of the room alias.
	_, domain, err := gomatrixserverlib.SplitID('#', alias)
	if err != nil {
		return "", nil, rsAPI.ErrInvalidID{Err: fmt.Errorf("alias %q is not in the correct format", alias)}
	}
	servers := []spec.ServerName{domain}

	// Check if this alias matches our own server configuration. If it
	// doesn't then we'll need to ask a remote server.
	var roomID string
	if !cfg.Matrix.IsLocalServerName(domain) {
		// The alias isn't owned by us, so we will need to look it up
		// using a remote server.
		dirReq := fsAPI.PerformDirectoryLookupRequest{
			RoomAlias:  alias,  // the room alias to lookup
			ServerName: domain, // the server to ask
		}
		dirRes := fsAPI.PerformDirectoryLookupResponse{}
		err = fedAPI.PerformDirectoryLookup(ctx, &dirReq, &dirRes)
		if err != nil {
			logrus.WithError(err).Errorf("error looking up alias %q", alias)
			return "", nil, fmt.Errorf("looking up alias %q over federation failed: %w", alias, err)
		}
		roomID = dirRes.RoomID
		servers = append(servers, dirRes.ServerNames...)
	} else {
		getRoomReq := rsAPI.GetRoomIDForAliasRequest{
			Alias:              alias,
			IncludeAppservices: true,
		}
		getRoomRes := rsAPI.GetRoomIDForAliasResponse{}
		// Otherwise, look up if we know this room alias locally.
		err = rsapi.GetRoomIDForAlias(ctx, &getRoomReq, &getRoomRes)
		if err != nil {
			return "", nil, fmt.Errorf("lookup room alias %q failed: %w", alias, err)
		}
		roomID = getRoomRes.RoomID
	}

	// If the room ID is empty then we failed to look up the alias.
	if roomID == "" {
		return "", nil, spec.NotFound(fmt.Sprintf("alias %q not found", alias))
	}
	return roomID, servers, nil
}

// TODO: Break this function up a bit & move to GMSL
//
//nolint:gocyclo
func (r *Joiner) performJoinRoomByID(
	ctx context.Context,
	req *rsAPI.PerformJoinRequest,
) (string, spec.ServerName, error) {
	// MSC3706: Trace join timing for diagnostics
	decisionStartTime := time.Now()
	traceLogger := logrus.WithContext(ctx).WithFields(logrus.Fields{
		"room_id": req.RoomIDOrAlias,
		"user_id": req.UserID,
		"trace":   "join_timing",
	})

	// The original client request ?server_name=... may include this HS so filter that out so we
	// don't attempt to make_join with ourselves
	for i := 0; i < len(req.ServerNames); i++ {
		if r.Cfg.Matrix.IsLocalServerName(req.ServerNames[i]) {
			// delete this entry
			req.ServerNames = append(req.ServerNames[:i], req.ServerNames[i+1:]...)
			i--
		}
	}

	roomID, err := spec.NewRoomID(req.RoomIDOrAlias)
	if err != nil {
		return "", "", rsAPI.ErrInvalidID{Err: fmt.Errorf("room ID %q is invalid: %w", req.RoomIDOrAlias, err)}
	}

	// Hold off if the room is currently being purged so the join doesn't
	// race the per-component purge fanout. NATS message order guarantees
	// downstream consumers see the purge before any subsequent rejoin
	// events; we just need to wait until the purge fanout message has been
	// produced.
	if err := r.PurgeTracker.WaitFor(ctx, roomID.String(), r.PurgeWaitTimeout); err != nil {
		return "", "", &spec.MatrixError{
			ErrCode: spec.ErrorUnknown,
			Err:     fmt.Sprintf("room %q is being purged; please retry shortly", roomID.String()),
		}
	}

	// Force a federated join if we aren't in the room and we've been
	// given some server names to try joining by.
	inRoomReq := &rsAPI.QueryServerJoinedToRoomRequest{
		RoomID: req.RoomIDOrAlias,
	}
	inRoomRes := &rsAPI.QueryServerJoinedToRoomResponse{}
	if err = r.Queryer.QueryServerJoinedToRoom(ctx, inRoomReq, inRoomRes); err != nil {
		return "", "", fmt.Errorf("r.Queryer.QueryServerJoinedToRoom: %w", err)
	}
	serverInRoom := inRoomRes.IsInRoom
	if req.LocalOnly && !serverInRoom {
		return "", "", errLocalJoinOnly
	}
	forceFederatedJoin := len(req.ServerNames) > 0 && !serverInRoom

	userID, err := spec.NewUserID(req.UserID, true)
	if err != nil {
		return "", "", rsAPI.ErrInvalidID{Err: fmt.Errorf("user ID %q is invalid: %w", req.UserID, err)}
	}

	// Look up the room NID for the supplied room ID.
	var senderID spec.SenderID
	checkInvitePending := false
	info, err := r.DB.RoomInfo(ctx, req.RoomIDOrAlias)
	if err == nil && info != nil {
		switch info.RoomVersion {
		case gomatrixserverlib.RoomVersionPseudoIDs:
			senderIDPtr, queryErr := r.Queryer.QuerySenderIDForUser(ctx, *roomID, *userID)
			if queryErr == nil {
				checkInvitePending = true
			}
			if senderIDPtr == nil {
				// create user room key if needed
				key, keyErr := r.RSAPI.GetOrCreateUserRoomPrivateKey(ctx, *userID, *roomID)
				if keyErr != nil {
					util.GetLogger(ctx).WithError(keyErr).Error("GetOrCreateUserRoomPrivateKey failed")
					return "", "", fmt.Errorf("GetOrCreateUserRoomPrivateKey failed: %w", keyErr)
				}
				senderID = spec.SenderIDFromPseudoIDKey(key)
			} else {
				senderID = *senderIDPtr
			}
		default:
			checkInvitePending = true
			senderID = spec.SenderID(userID.String())
		}
	}

	userDomain := userID.Domain()

	// Force a federated join if we're dealing with a pending invite
	// and we aren't in the room, or our copy of the room has not seen the
	// invite yet: /invite can arrive before the inviting server's /send
	// carrying the same event, and a local join would then be refused.
	if checkInvitePending {
		isInvitePending, inviteSender, _, inviteEvent, inviteErr := helpers.IsInvitePending(ctx, r.DB, req.RoomIDOrAlias, senderID)
		if inviteErr == nil && isInvitePending && (!serverInRoom || !r.stateHasInvite(ctx, req.RoomIDOrAlias, *userID)) {
			inviter, queryErr := r.RSAPI.QueryUserIDForSender(ctx, *roomID, inviteSender)
			if queryErr != nil {
				return "", "", fmt.Errorf("r.RSAPI.QueryUserIDForSender: %w", queryErr)
			}

			// If we were invited by someone from another server then we can
			// assume they are in the room so we can join via them.
			if inviter != nil && !r.Cfg.Matrix.IsLocalServerName(inviter.Domain()) {
				req.ServerNames = append(req.ServerNames, inviter.Domain())
				forceFederatedJoin = true
				memberEvent := gjson.Parse(string(inviteEvent.JSON()))
				// only set unsigned if we've got a content.membership, which we _should_
				if memberEvent.Get("content.membership").Exists() {
					req.Unsigned = map[string]any{
						"prev_sender": memberEvent.Get("sender").Str,
						"prev_content": map[string]any{
							"is_direct":  memberEvent.Get("content.is_direct").Bool(),
							"membership": memberEvent.Get("content.membership").Str,
						},
					}
				}
			}
		}
	}

	// A user the room bans cannot rejoin it, so refuse at once rather than ask another
	// server, which may not yet know of the ban (a partial state join in particular).
	if info != nil && senderID != "" {
		var membershipRes rsAPI.QueryMembershipForUserResponse
		if banErr := r.Queryer.QueryMembershipForSenderID(ctx, *roomID, senderID, &membershipRes); banErr == nil {
			if membershipRes.Membership == spec.Ban {
				return "", "", rsAPI.ErrNotAllowed{Err: fmt.Errorf("user %s is banned from the room", req.UserID)}
			}
			// A rejoin after leaving uses the stale local DAG. If the room has
			// other servers and the client named one, join through it so the
			// new join event is anchored to the room's current forward
			// extremities rather than the old branch that the user left.
			if membershipRes.Membership == spec.Leave && len(req.ServerNames) > 0 {
				if servers := r.otherServersInRoom(ctx, req.RoomIDOrAlias); len(servers) > 0 {
					forceFederatedJoin = true
				}
			}
		}
	}

	// MSC3706: Force federated join if room is in partial state and user is not already joined,
	// so that authorization happens with complete state on the remote server. Even a public
	// room needs this: the partial state may omit a ban of the user, which only a server
	// that knows their membership can enforce.
	if info != nil && !forceFederatedJoin && len(req.ServerNames) > 0 {
		isPartialState, partialStateErr := r.DB.IsRoomPartialState(ctx, info.RoomNID)
		if partialStateErr == nil && isPartialState {
			// Check if the user is already joined - if so, they can proceed with local operations
			membershipRes := &rsAPI.QueryMembershipForUserResponse{}
			_ = r.Queryer.QueryMembershipForSenderID(ctx, *roomID, senderID, membershipRes)
			if !membershipRes.IsInRoom {
				logrus.WithFields(logrus.Fields{
					"room_id": req.RoomIDOrAlias,
					"user_id": req.UserID,
				}).Info("Forcing federated join due to partial state room")
				forceFederatedJoin = true
			}
		}
	}

	// If a guest is trying to join a room, check that the room has a m.room.guest_access event
	if req.IsGuest {
		var guestAccessEvent *types.HeaderedEvent
		guestAccess := "forbidden"
		guestAccessEvent, err = r.DB.GetStateEvent(ctx, req.RoomIDOrAlias, spec.MRoomGuestAccess, "")
		if (err != nil && !errors.Is(err, sql.ErrNoRows)) || guestAccessEvent == nil {
			logrus.WithError(err).Warn("unable to get m.room.guest_access event, defaulting to 'forbidden'")
		}
		if guestAccessEvent != nil {
			guestAccess = gjson.GetBytes(guestAccessEvent.Content(), "guest_access").String()
		}

		// Servers MUST only allow guest users to join rooms if the m.room.guest_access state event
		// is present on the room and has the guest_access value can_join.
		if guestAccess != "can_join" {
			return "", "", rsAPI.ErrNotAllowed{Err: fmt.Errorf("guest access is forbidden")}
		}
	}

	// If we should do a forced federated join then do that.
	var joinedVia spec.ServerName
	if forceFederatedJoin {
		traceLogger.WithFields(logrus.Fields{
			"decision_ms":    time.Since(decisionStartTime).Milliseconds(),
			"federated":      true,
			"server_in_room": serverInRoom,
			"server_count":   len(req.ServerNames),
		}).Debug("Join decision: federated join")
		joinedVia, err = r.performFederatedJoinRoomByID(ctx, req)
		return req.RoomIDOrAlias, joinedVia, err
	}

	// Try to construct an actual join event from the template.
	// If this succeeds then it is a sign that the room already exists
	// locally on the homeserver.
	// TODO: Check what happens if the room exists on the server
	// but everyone has since left. I suspect it does the wrong thing.

	var buildRes rsAPI.QueryLatestEventsAndStateResponse
	identity := r.Cfg.Matrix.SigningIdentity

	// at this point we know we have an existing room
	if inRoomRes.RoomVersion == gomatrixserverlib.RoomVersionPseudoIDs {
		var pseudoIDKey ed25519.PrivateKey
		pseudoIDKey, err = r.RSAPI.GetOrCreateUserRoomPrivateKey(ctx, *userID, *roomID)
		if err != nil {
			util.GetLogger(ctx).WithError(err).Error("GetOrCreateUserRoomPrivateKey failed")
			return "", "", err
		}

		mapping := &gomatrixserverlib.MXIDMapping{
			UserRoomKey: spec.SenderIDFromPseudoIDKey(pseudoIDKey),
			UserID:      userID.String(),
		}

		// Sign the mapping with the server identity
		if err = mapping.Sign(identity.ServerName, identity.KeyID, identity.PrivateKey); err != nil {
			return "", "", err
		}
		req.Content["mxid_mapping"] = mapping

		// sign the event with the pseudo ID key
		identity = fclient.SigningIdentity{
			ServerName: spec.ServerName(spec.SenderIDFromPseudoIDKey(pseudoIDKey)),
			KeyID:      "ed25519:1",
			PrivateKey: pseudoIDKey,
		}
	}

	senderIDString := string(senderID)

	// Prepare the template for the join event.
	proto := gomatrixserverlib.ProtoEvent{
		Type:     spec.MRoomMember,
		SenderID: senderIDString,
		StateKey: &senderIDString,
		RoomID:   req.RoomIDOrAlias,
		Redacts:  "",
	}
	if err = proto.SetUnsigned(struct{}{}); err != nil {
		return "", "", fmt.Errorf("eb.SetUnsigned: %w", err)
	}

	// It is possible for the request to include some "content" for the
	// event. We'll always overwrite the "membership" key, but the rest,
	// like "display_name" or "avatar_url", will be kept if supplied.
	if req.Content == nil {
		req.Content = map[string]any{}
	}
	req.Content["membership"] = spec.Join
	if authorisedVia, aerr := r.populateAuthorisedViaUserForRestrictedJoin(ctx, req, senderID); aerr != nil {
		// Check if this is a M_FORBIDDEN error (user not in allowed spaces/rooms).
		// These should return HTTP 403 so appservice bridges can retry with an invite.
		var matrixErr spec.MatrixError
		if errors.As(aerr, &matrixErr) && matrixErr.ErrCode == spec.ErrorForbidden {
			return "", "", rsAPI.ErrNotAllowed{Err: aerr}
		}
		// No local user can authorize the restricted join, but another server
		// in the room may have one: join through it instead. Only choose
		// fallback servers ourselves when the client did not name any; an
		// explicit ?server_name= list must be honoured so a refused join is
		// reported instead of silently succeeding through another server.
		if len(req.ServerNames) == 0 && errors.As(aerr, &matrixErr) && (matrixErr.ErrCode == rsAPI.ErrorUnableToAuthoriseJoin || matrixErr.ErrCode == rsAPI.ErrorUnableToGrantJoin) {
			if servers := r.otherServersInRoom(ctx, req.RoomIDOrAlias); len(servers) > 0 {
				delete(req.Content, "join_authorised_via_users_server") //nolint:misspell // Matrix spec uses British spelling
				req.ServerNames = append(req.ServerNames, servers...)
				joinedVia, err = r.performFederatedJoinRoomByID(ctx, req)
				return req.RoomIDOrAlias, joinedVia, err
			}
		}
		// Return remaining errors to the client API for status mapping.
		return "", "", aerr
	} else if authorisedVia != "" {
		req.Content["join_authorised_via_users_server"] = authorisedVia //nolint:misspell // Matrix spec uses British spelling
	}
	if err = proto.SetContent(req.Content); err != nil {
		return "", "", fmt.Errorf("eb.SetContent: %w", err)
	}
	event, err := eventutil.QueryAndBuildEvent(ctx, &proto, &identity, time.Now(), r.RSAPI, &buildRes)

	switch {
	case err == nil:
		// The room join is local. Send the new join event into the
		// roomserver. First of all check that the user isn't already
		// a member of the room. This is best-effort (as in we won't
		// fail if we can't find the existing membership) because there
		// is really no harm in just sending another membership event.
		traceLogger.WithFields(logrus.Fields{
			"decision_ms": time.Since(decisionStartTime).Milliseconds(),
			"federated":   false,
		}).Debug("Join decision: local join")
		membershipRes := &rsAPI.QueryMembershipForUserResponse{}
		_ = r.Queryer.QueryMembershipForSenderID(ctx, *roomID, senderID, membershipRes)

		// If we haven't already joined the room then send an event
		// into the room changing our membership status.
		if !membershipRes.RoomExists || !membershipRes.IsInRoom {
			inputReq := rsAPI.InputRoomEventsRequest{
				InputRoomEvents: []rsAPI.InputRoomEvent{
					{
						Kind:         rsAPI.KindNew,
						Event:        event,
						SendAsServer: string(userDomain),
					},
				},
			}
			inputRes := rsAPI.InputRoomEventsResponse{}
			r.Inputer.InputRoomEvents(ctx, &inputReq, &inputRes)
			if err = inputRes.Err(); err != nil {
				return "", "", rsAPI.ErrNotAllowed{Err: errors.New(rsAPI.InputRejectionReason(err))}
			}
		}
	case errors.As(err, new(eventutil.ErrRoomNoExists)):
		// The room doesn't exist locally. If the room ID looks like it should
		// be ours then this probably means that we've nuked our database at
		// some point. Either way, without via servers there is nothing to try
		// over federation, so surface the not-found state directly instead of
		// letting the federation API fail with a generic "Unknown HTTP error".
		if len(req.ServerNames) == 0 {
			return "", "", eventutil.ErrRoomNoExists{}
		}

		// Perform a federated room join.
		joinedVia, err = r.performFederatedJoinRoomByID(ctx, req)
		return req.RoomIDOrAlias, joinedVia, err
	default:
		// Something else went wrong.
		return "", "", fmt.Errorf("error joining local room: %w", err)
	}

	// By this point, if req.RoomIDOrAlias contained an alias, then
	// it will have been overwritten with a room ID by performJoinRoomByAlias.
	// We should now include this in the response so that the CS API can
	// return the right room ID.
	return req.RoomIDOrAlias, userDomain, nil
}

// errLocalJoinOnly refuses a join that only federation could complete to a
// user who may only join rooms this server is in.
var errLocalJoinOnly = rsAPI.ErrNotAllowed{Err: errors.New("this account may only join rooms on this server")}

func (r *Joiner) performFederatedJoinRoomByID(
	ctx context.Context,
	req *rsAPI.PerformJoinRequest,
) (spec.ServerName, error) {
	// Every federated join passes through here, so this also covers a pending
	// remote invite, a partial-state room and a restricted join that only a
	// remote server can authorize.
	if req.LocalOnly {
		return "", errLocalJoinOnly
	}
	// Try joining by all of the supplied server names.
	fedReq := fsAPI.PerformJoinRequest{
		RoomID:      req.RoomIDOrAlias, // the room ID to try and join
		UserID:      req.UserID,        // the user ID joining the room
		ServerNames: req.ServerNames,   // the server to try joining with
		Content:     req.Content,       // the membership event content
		Unsigned:    req.Unsigned,      // the unsigned event content, if any
	}
	fedRes := fsAPI.PerformJoinResponse{}
	r.FSAPI.PerformJoin(ctx, &fedReq, &fedRes)
	if fedRes.LastError != nil {
		return "", fedRes.LastError
	}
	return fedRes.JoinedVia, nil
}

func (r *Joiner) populateAuthorisedViaUserForRestrictedJoin(
	ctx context.Context,
	joinReq *rsAPI.PerformJoinRequest,
	senderID spec.SenderID,
) (string, error) {
	roomID, err := spec.NewRoomID(joinReq.RoomIDOrAlias)
	if err != nil {
		return "", err
	}

	return r.Queryer.QueryRestrictedJoinAllowed(ctx, *roomID, senderID)
}

// stateHasInvite reports whether the room's current state already holds an
// invite for the user.
func (r *Joiner) stateHasInvite(ctx context.Context, roomID string, userID spec.UserID) bool {
	res := rsAPI.QueryMembershipForUserResponse{}
	if err := r.Queryer.QueryMembershipForUser(ctx, &rsAPI.QueryMembershipForUserRequest{RoomID: roomID, UserID: userID}, &res); err != nil {
		return false
	}
	return res.Membership == spec.Invite
}

// otherServersInRoom lists the other servers joined to a room.
func (r *Joiner) otherServersInRoom(ctx context.Context, roomID string) []spec.ServerName {
	res := fsAPI.QueryJoinedHostServerNamesInRoomResponse{}
	if err := r.FSAPI.QueryJoinedHostServerNamesInRoom(ctx, &fsAPI.QueryJoinedHostServerNamesInRoomRequest{
		RoomID:             roomID,
		ExcludeSelf:        true,
		ExcludeBlacklisted: true,
	}, &res); err != nil {
		logrus.WithError(err).WithField("room_id", roomID).Warn("Failed to list the servers in the room")
		return nil
	}
	return res.ServerNames
}
