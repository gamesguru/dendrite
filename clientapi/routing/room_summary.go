// Copyright 2025 Jackmaninov
// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"encoding/json"
	"net/http"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/caching"
	rsAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// RoomSummaryResponse is the response body of the stable room summary API
// (GET /_matrix/client/v1/room_summary/{roomIdOrAlias}, Matrix v1.15).
type RoomSummaryResponse struct {
	RoomID           string   `json:"room_id"`
	RoomType         string   `json:"room_type,omitempty"`
	Name             string   `json:"name,omitempty"`
	Topic            string   `json:"topic,omitempty"`
	AvatarURL        string   `json:"avatar_url,omitempty"`
	CanonicalAlias   string   `json:"canonical_alias,omitempty"`
	NumJoinedMembers int      `json:"num_joined_members"`
	GuestCanJoin     bool     `json:"guest_can_join"`
	WorldReadable    bool     `json:"world_readable"`
	JoinRule         string   `json:"join_rule,omitempty"`
	AllowedRoomIDs   []string `json:"allowed_room_ids,omitempty"`
	Encryption       string   `json:"encryption,omitempty"`
	Membership       string   `json:"membership,omitempty"`
	RoomVersion      string   `json:"room_version,omitempty"`
}

// unstableRoomSummaryResponse is the response body of the experimental MSC3266
// endpoints. It carries the stable fields plus the im.nheko.summary.* names
// that pre-v1.15 clients expect.
type unstableRoomSummaryResponse struct {
	RoomSummaryResponse
	UnstableEncryption  string `json:"im.nheko.summary.encryption,omitempty"`
	UnstableRoomVersion string `json:"im.nheko.summary.room_version,omitempty"`
}

// GetRoomSummary implements the stable room summary API.
// GET /_matrix/client/v1/room_summary/{roomIdOrAlias}
// Supports both authenticated and unauthenticated requests.
// Unauthenticated requests can only access public, knockable or world-readable rooms.
func GetRoomSummary(
	req *http.Request,
	device *userapi.Device, // May be nil for unauthenticated requests
	roomIDOrAlias string,
	roomserverAPI rsAPI.ClientRoomserverAPI,
	fsAPI api.FederationInternalAPI, // May be nil when federation is unavailable
	serverName spec.ServerName,
	cache caching.RoomSummaryCache,
) util.JSONResponse {
	summary, errResp := getRoomSummary(req, device, roomIDOrAlias, roomserverAPI, fsAPI, serverName, cache)
	if errResp != nil {
		return *errResp
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: summary,
	}
}

// GetRoomSummaryUnstable implements the experimental MSC3266 room summary API.
// GET /_matrix/client/unstable/im.nheko.summary/summary/{roomIdOrAlias}
// GET /_matrix/client/unstable/im.nheko.summary/rooms/{roomIdOrAlias}/summary
// It behaves exactly like GetRoomSummary but additionally returns the
// im.nheko.summary.* field names for clients that predate Matrix v1.15.
func GetRoomSummaryUnstable(
	req *http.Request,
	device *userapi.Device,
	roomIDOrAlias string,
	roomserverAPI rsAPI.ClientRoomserverAPI,
	fsAPI api.FederationInternalAPI,
	serverName spec.ServerName,
	cache caching.RoomSummaryCache,
) util.JSONResponse {
	summary, errResp := getRoomSummary(req, device, roomIDOrAlias, roomserverAPI, fsAPI, serverName, cache)
	if errResp != nil {
		return *errResp
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: unstableRoomSummaryResponse{
			RoomSummaryResponse: summary,
			UnstableEncryption:  summary.Encryption,
			UnstableRoomVersion: summary.RoomVersion,
		},
	}
}

// getRoomSummary resolves the room, checks access and builds the summary.
// It returns either a summary or a JSON error response.
func getRoomSummary(
	req *http.Request,
	device *userapi.Device,
	roomIDOrAlias string,
	roomserverAPI rsAPI.ClientRoomserverAPI,
	fsAPI api.FederationInternalAPI,
	serverName spec.ServerName,
	cache caching.RoomSummaryCache,
) (RoomSummaryResponse, *util.JSONResponse) {
	ctx := req.Context()
	authenticated := device != nil

	// Parse via query parameters for federation
	vias := req.URL.Query()["via"]

	// Parse and validate room ID or alias
	roomID, jsonErr := parseRoomIDOrAlias(ctx, roomIDOrAlias, roomserverAPI)
	if jsonErr != nil {
		return RoomSummaryResponse{}, jsonErr
	}

	// Try to fetch room state locally first
	stateRes := &rsAPI.QueryBulkStateContentResponse{}
	err := roomserverAPI.QueryBulkStateContent(ctx, &rsAPI.QueryBulkStateContentRequest{
		RoomIDs:        []string{roomID},
		AllowWildcards: true,
		StateTuples: []gomatrixserverlib.StateKeyTuple{
			{EventType: spec.MRoomName, StateKey: ""},
			{EventType: spec.MRoomTopic, StateKey: ""},
			{EventType: spec.MRoomAvatar, StateKey: ""},
			{EventType: spec.MRoomCanonicalAlias, StateKey: ""},
			{EventType: spec.MRoomJoinRules, StateKey: ""},
			{EventType: spec.MRoomGuestAccess, StateKey: ""},
			{EventType: spec.MRoomHistoryVisibility, StateKey: ""},
			{EventType: spec.MRoomCreate, StateKey: ""},
			{EventType: spec.MRoomEncryption, StateKey: ""},
			{EventType: spec.MRoomMember, StateKey: "*"}, // Wildcard for member count
		},
	}, stateRes)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("QueryBulkStateContent failed")
		return RoomSummaryResponse{}, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Check if room exists locally
	roomState, roomExistsLocally := stateRes.Rooms[roomID]

	// If room doesn't exist locally, try federation
	if !roomExistsLocally {
		if summary := fetchRoomSummaryViaFederation(ctx, fsAPI, serverName, roomID, vias); summary != nil {
			return *summary, nil
		}

		// Federation failed, return 404
		return RoomSummaryResponse{}, &util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Room not found"),
		}
	}

	// Room exists locally - check access control
	var membership string

	if device != nil {
		// Authenticated request - get user ID and check full access
		userID, err := spec.NewUserID(device.UserID, true)
		if err != nil {
			util.GetLogger(ctx).WithError(err).Error("UserID is invalid")
			return RoomSummaryResponse{}, &util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("Device UserID is invalid"),
			}
		}

		// Check access control (world-readable, public, knockable, or user membership)
		canAccess, userMembership := checkRoomAccess(ctx, roomserverAPI, roomID, *userID, roomState)
		if !canAccess {
			// Return 404 instead of 403 to not leak room existence
			return RoomSummaryResponse{}, &util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("Room not found"),
			}
		}
		membership = userMembership
	} else {
		// Unauthenticated request - only allow public/knockable/world-readable rooms
		if !checkUnauthenticatedAccess(roomState) {
			// Return 404 instead of 403 to not leak room existence
			return RoomSummaryResponse{}, &util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("Room not found"),
			}
		}
		// Don't include membership for unauthenticated requests
		membership = ""
	}

	// Check cache for room summary (without user-specific membership)
	if cache != nil {
		if cachedResponse, ok := cache.GetRoomSummary(roomID, authenticated); ok {
			response := fromCacheResponse(cachedResponse)
			response.Membership = membership
			return response, nil
		}
	}

	// Cache miss - query room version and build response
	roomVersion := getRoomVersion(ctx, roomserverAPI, roomID)

	// Build response (without membership for caching)
	response := buildRoomSummaryResponse(roomID, roomState, "", roomVersion)
	response.AllowedRoomIDs = getAllowedRoomIDs(ctx, roomserverAPI, roomID, response.JoinRule)

	// Store in cache (without user-specific membership)
	if cache != nil {
		cache.StoreRoomSummary(roomID, authenticated, toCacheResponse(response))
	}

	// Add user's membership to response if authenticated
	response.Membership = membership

	return response, nil
}

// fetchRoomSummaryViaFederation attempts to fetch room summary via federation
// Returns nil if federation is unavailable, fails, or the room is not accessible.
func fetchRoomSummaryViaFederation(
	ctx context.Context,
	fsAPI api.FederationInternalAPI,
	serverName spec.ServerName,
	roomID string,
	vias []string,
) *RoomSummaryResponse {
	if fsAPI == nil {
		return nil
	}

	// Extract server name from room ID if no via parameters provided
	if len(vias) == 0 {
		_, domain, err := gomatrixserverlib.SplitID('!', roomID)
		if err != nil {
			return nil
		}
		vias = []string{string(domain)}
	}

	// Try each via server in sequence
	for _, via := range vias {
		if via == string(serverName) {
			// Skip our own server
			continue
		}

		// Call federation hierarchy endpoint
		res, err := fsAPI.RoomHierarchies(
			ctx,
			serverName,
			spec.ServerName(via),
			roomID,
			false, // suggestedOnly = false to get all room info
		)
		if err != nil {
			util.GetLogger(ctx).WithError(err).Warnf("Failed to fetch room hierarchy from %s", via)
			continue
		}

		// Convert federation hierarchy response to room summary
		summary := convertHierarchyToSummary(res.Room)
		return &summary
	}

	// All federation attempts failed
	return nil
}

// convertHierarchyToSummary converts a federation hierarchy room to a room summary response.
func convertHierarchyToSummary(room fclient.RoomHierarchyRoom) RoomSummaryResponse {
	summary := RoomSummaryResponse{
		RoomID:           room.RoomID,
		Name:             room.Name,
		Topic:            room.Topic,
		AvatarURL:        room.AvatarURL,
		CanonicalAlias:   room.CanonicalAlias,
		NumJoinedMembers: room.JoinedMembersCount,
		GuestCanJoin:     room.GuestCanJoin,
		WorldReadable:    room.WorldReadable,
		JoinRule:         room.JoinRule,
	}

	if room.RoomType != nil {
		summary.RoomType = *room.RoomType
	}

	// Add allowed room IDs for restricted rooms
	if len(room.AllowedRoomIDs) > 0 {
		summary.AllowedRoomIDs = room.AllowedRoomIDs
	}

	// Note: the federation hierarchy response does not carry membership,
	// encryption, or room_version, so those stay empty for remote rooms.

	return summary
}

// parseRoomIDOrAlias resolves a room alias to room ID, or validates a room ID.
func parseRoomIDOrAlias(ctx context.Context, roomIDOrAlias string, roomserverAPI rsAPI.ClientRoomserverAPI) (string, *util.JSONResponse) {
	// Try parsing as room ID first
	if roomID, err := spec.NewRoomID(roomIDOrAlias); err == nil {
		return roomID.String(), nil
	}

	// Try parsing as room alias - validate it has correct format
	_, _, err := gomatrixserverlib.SplitID('#', roomIDOrAlias)
	if err != nil {
		return "", &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Invalid room ID or alias"),
		}
	}

	// Resolve alias to room ID
	queryReq := &rsAPI.GetRoomIDForAliasRequest{
		Alias:              roomIDOrAlias,
		IncludeAppservices: true,
	}
	queryRes := &rsAPI.GetRoomIDForAliasResponse{}
	if err := roomserverAPI.GetRoomIDForAlias(ctx, queryReq, queryRes); err != nil {
		util.GetLogger(ctx).WithError(err).Error("GetRoomIDForAlias failed")
		return "", &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	if queryRes.RoomID == "" {
		return "", &util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Room alias not found"),
		}
	}

	return queryRes.RoomID, nil
}

// checkRoomAccess determines if the user can access the room summary
// Returns (canAccess, membership).
func checkRoomAccess(
	ctx context.Context,
	roomserverAPI rsAPI.ClientRoomserverAPI,
	roomID string,
	userID spec.UserID,
	roomState map[gomatrixserverlib.StateKeyTuple]string,
) (bool, string) {
	// Get user's membership state (we'll need this regardless)
	membership := getUserMembership(ctx, roomserverAPI, roomID, userID)

	// World-readable, public and knockable rooms can be previewed by anyone
	if isPubliclyPreviewable(roomState) {
		return true, membership
	}

	// Allow access if the user is or was a member of the room (join, invite, knock, leave, ban).
	// This matches Synapse behavior: you can see the summary of rooms you have been in.
	switch membership {
	case spec.Join, spec.Invite, spec.Knock, spec.Leave, spec.Ban:
		return true, membership
	}

	// No access - not previewable and the user was never in the room
	return false, ""
}

// checkUnauthenticatedAccess determines if an unauthenticated user can access the room summary.
// Only allows access to world-readable rooms and rooms with a public, knock or
// knock_restricted join rule, as permitted by the specification.
func checkUnauthenticatedAccess(
	roomState map[gomatrixserverlib.StateKeyTuple]string,
) bool {
	return isPubliclyPreviewable(roomState)
}

// isPubliclyPreviewable reports whether the room may be summarized without being a member:
// it is world-readable or has a public, knock or knock_restricted join rule.
func isPubliclyPreviewable(roomState map[gomatrixserverlib.StateKeyTuple]string) bool {
	histVisKey := gomatrixserverlib.StateKeyTuple{
		EventType: spec.MRoomHistoryVisibility,
		StateKey:  "",
	}
	if visibility, ok := roomState[histVisKey]; ok && visibility == string(gomatrixserverlib.HistoryVisibilityWorldReadable) {
		return true
	}

	// QueryBulkStateContent returns the extracted join_rule value directly (e.g., "public")
	joinRuleKey := gomatrixserverlib.StateKeyTuple{
		EventType: spec.MRoomJoinRules,
		StateKey:  "",
	}
	switch roomState[joinRuleKey] {
	case spec.Public, spec.Knock, spec.KnockRestricted:
		return true
	}

	return false
}

// getUserMembership gets the current membership state for a user in a room.
func getUserMembership(ctx context.Context, roomserverAPI rsAPI.ClientRoomserverAPI, roomID string, userID spec.UserID) string {
	var membershipRes rsAPI.QueryMembershipForUserResponse
	err := roomserverAPI.QueryMembershipForUser(ctx, &rsAPI.QueryMembershipForUserRequest{
		RoomID: roomID,
		UserID: userID,
	}, &membershipRes)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("QueryMembershipForUser failed")
		return ""
	}

	return membershipRes.Membership
}

// getRoomVersion queries the room version.
func getRoomVersion(ctx context.Context, roomserverAPI rsAPI.ClientRoomserverAPI, roomID string) string {
	roomVersion, err := roomserverAPI.QueryRoomVersionForRoom(ctx, roomID)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("QueryRoomVersionForRoom failed")
		return ""
	}

	return string(roomVersion)
}

// getAllowedRoomIDs returns the room IDs listed in the join rules of a
// restricted or knock_restricted room, or nil for any other join rule.
// QueryBulkStateContent only yields the join_rule value, so the full event is
// fetched here.
func getAllowedRoomIDs(ctx context.Context, roomserverAPI rsAPI.ClientRoomserverAPI, roomID string, joinRule string) []string {
	if joinRule != spec.Restricted && joinRule != spec.KnockRestricted {
		return nil
	}

	joinRuleTuple := gomatrixserverlib.StateKeyTuple{
		EventType: spec.MRoomJoinRules,
		StateKey:  "",
	}
	var res rsAPI.QueryCurrentStateResponse
	err := roomserverAPI.QueryCurrentState(ctx, &rsAPI.QueryCurrentStateRequest{
		RoomID:      roomID,
		StateTuples: []gomatrixserverlib.StateKeyTuple{joinRuleTuple},
	}, &res)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("QueryCurrentState failed")
		return nil
	}
	joinRuleEv := res.StateEvents[joinRuleTuple]
	if joinRuleEv == nil {
		return nil
	}

	var content gomatrixserverlib.JoinRuleContent
	if err := json.Unmarshal(joinRuleEv.Content(), &content); err != nil {
		util.GetLogger(ctx).WithError(err).Warn("failed to parse join rules content")
		return nil
	}

	allowedRoomIDs := make([]string, 0, len(content.Allow))
	for _, allow := range content.Allow {
		if allow.Type != spec.MRoomMembership || allow.RoomID == "" {
			continue
		}
		allowedRoomIDs = append(allowedRoomIDs, allow.RoomID)
	}
	if len(allowedRoomIDs) == 0 {
		return nil
	}
	return allowedRoomIDs
}

// buildRoomSummaryResponse constructs the response from room state.
func buildRoomSummaryResponse(
	roomID string,
	roomState map[gomatrixserverlib.StateKeyTuple]string,
	membership string,
	roomVersion string,
) RoomSummaryResponse {
	response := RoomSummaryResponse{
		RoomID:        roomID,
		Membership:    membership,
		RoomVersion:   roomVersion,
		GuestCanJoin:  false,
		WorldReadable: false,
	}

	// Extract state content values
	for tuple, content := range roomState {
		switch tuple.EventType {
		case spec.MRoomName:
			response.Name = content

		case spec.MRoomTopic:
			response.Topic = content

		case spec.MRoomAvatar:
			response.AvatarURL = content

		case spec.MRoomCanonicalAlias:
			response.CanonicalAlias = content

		case spec.MRoomJoinRules:
			// ExtractContentValue returns just the join_rule value (e.g., "public", "invite", "restricted")
			response.JoinRule = content

		case spec.MRoomGuestAccess:
			response.GuestCanJoin = content == "can_join"

		case spec.MRoomHistoryVisibility:
			response.WorldReadable = content == string(gomatrixserverlib.HistoryVisibilityWorldReadable)

		case spec.MRoomCreate:
			// Parse create event for room type
			var createContent struct {
				Type string `json:"type"`
			}
			if err := json.Unmarshal([]byte(content), &createContent); err == nil {
				response.RoomType = createContent.Type
			}

		case spec.MRoomEncryption:
			// ExtractContentValue returns just the algorithm value (e.g., "m.megolm.v1.aes-sha2")
			response.Encryption = content

		case spec.MRoomMember:
			// Count joined members
			if content == spec.Join {
				response.NumJoinedMembers++
			}
		}
	}

	return response
}

// toCacheResponse converts routing.RoomSummaryResponse to caching.RoomSummaryResponse.
func toCacheResponse(r RoomSummaryResponse) caching.RoomSummaryResponse {
	return caching.RoomSummaryResponse{
		RoomID:           r.RoomID,
		RoomType:         r.RoomType,
		Name:             r.Name,
		Topic:            r.Topic,
		AvatarURL:        r.AvatarURL,
		CanonicalAlias:   r.CanonicalAlias,
		NumJoinedMembers: r.NumJoinedMembers,
		GuestCanJoin:     r.GuestCanJoin,
		WorldReadable:    r.WorldReadable,
		JoinRule:         r.JoinRule,
		AllowedRoomIDs:   r.AllowedRoomIDs,
		Encryption:       r.Encryption,
		Membership:       r.Membership,
		RoomVersion:      r.RoomVersion,
	}
}

// fromCacheResponse converts caching.RoomSummaryResponse to routing.RoomSummaryResponse.
func fromCacheResponse(r caching.RoomSummaryResponse) RoomSummaryResponse {
	return RoomSummaryResponse{
		RoomID:           r.RoomID,
		RoomType:         r.RoomType,
		Name:             r.Name,
		Topic:            r.Topic,
		AvatarURL:        r.AvatarURL,
		CanonicalAlias:   r.CanonicalAlias,
		NumJoinedMembers: r.NumJoinedMembers,
		GuestCanJoin:     r.GuestCanJoin,
		WorldReadable:    r.WorldReadable,
		JoinRule:         r.JoinRule,
		AllowedRoomIDs:   r.AllowedRoomIDs,
		Encryption:       r.Encryption,
		Membership:       r.Membership,
		RoomVersion:      r.RoomVersion,
	}
}
