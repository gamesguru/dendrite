// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strconv"

	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/internal/reconciliation"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

const (
	// MSC4521Prefix is the unstable federation prefix used by the
	// reconciliation profile.
	MSC4521Prefix = "/_matrix/federation/unstable/tk.nutra.msc0501"

	maxReconciliationBuckets = 128
	maxBucketSketchCapacity  = 32
	maxBucketDepth           = 32
)

// roomReconciliationState loads the room's event set and frame, returning an
// error response if the room is unknown or unsupported.
func roomReconciliationState(
	ctx context.Context, rsAPI roomserverAPI.FederationRoomserverAPI, roomID string, serverName spec.ServerName,
) (*roomserverAPI.QueryRoomReconciliationStateResponse, *util.JSONResponse) {
	var res roomserverAPI.QueryRoomReconciliationStateResponse
	if err := rsAPI.QueryRoomReconciliationState(ctx, &roomserverAPI.QueryRoomReconciliationStateRequest{
		RoomID: roomID, ServerName: serverName,
	}, &res); err != nil {
		if errors.Is(err, types.ErrRoomNotFound) {
			return nil, &util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound("Room not found"),
			}
		}
		util.GetLogger(ctx).WithError(err).Error("failed to query room reconciliation state")
		return nil, &util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("Failed to query room reconciliation state"),
		}
	}
	if _, ok := reconciliation.EventIDFormat(res.RoomVersion); !ok {
		return nil, &util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MatrixError{
				ErrCode: "M_UNSUPPORTED_ROOM_VERSION",
				Err:     "MSC4521 requires reference-hash event IDs (room version 3 or later)",
			},
		}
	}
	return &res, nil
}

// checkReconciliationAccess verifies that the requesting server is joined to
// the room and not blocked by server ACLs.
func checkReconciliationAccess(
	ctx context.Context, rsAPI roomserverAPI.FederationRoomserverAPI,
	request *fclient.FederationRequest, roomID string,
) *util.JSONResponse {
	if res := ErrorIfLocalServerNotInRoom(ctx, rsAPI, roomID); res != nil {
		return res
	}
	joined := roomserverAPI.QueryServerJoinedToRoomRequest{
		ServerName: request.Origin(),
		RoomID:     roomID,
	}
	var joinedRes roomserverAPI.QueryServerJoinedToRoomResponse
	if err := rsAPI.QueryServerJoinedToRoom(ctx, &joined, &joinedRes); err != nil {
		res := util.ErrorResponse(err)
		return &res
	}
	if !joinedRes.RoomExists || !joinedRes.IsInRoom {
		return &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Server is not joined to this room"),
		}
	}
	if roomserverAPI.IsServerBannedFromRoom(ctx, rsAPI, roomID, request.Origin()) {
		return &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("Server is blocked by the room's server ACLs"),
		}
	}
	return nil
}

// frameID is the SHA-256 of the canonical JSON array of sorted, deduplicated
// frame event IDs, base64url encoded.
func frameID(eventIDs []string) string {
	unique := make(map[string]struct{}, len(eventIDs))
	sorted := make([]string, 0, len(eventIDs))
	for _, eventID := range eventIDs {
		if _, ok := unique[eventID]; ok {
			continue
		}
		unique[eventID] = struct{}{}
		sorted = append(sorted, eventID)
	}
	sort.Strings(sorted)
	encoded, _ := json.Marshal(sorted) //nolint:errchkjson // a []string always marshals
	sum := sha256.Sum256(encoded)
	return base64.RawURLEncoding.EncodeToString(sum[:])
}

func digestResponse(
	state *roomserverAPI.QueryRoomReconciliationStateResponse,
	snapshot *reconciliation.RoomSnapshot,
	withStrata bool,
) map[string]any {
	response := map[string]any{
		"digest":              snapshot.Digest,
		"digest_type":         reconciliation.DigestType,
		"known_event_count":   snapshot.Count,
		"frame_id":            frameID(state.ForwardExtremities),
		"frame_event_ids":     state.ForwardExtremities,
		"extremity_event_ids": state.ForwardExtremities,
	}
	if withStrata {
		response["strata"] = snapshot.Strata
	}
	return response
}

// RoomDigest implements GET room_digest/{roomID}.
func RoomDigest(
	httpReq *http.Request, request *fclient.FederationRequest,
	rsAPI roomserverAPI.FederationRoomserverAPI, roomID string, withStrata bool,
) util.JSONResponse {
	if res := checkReconciliationAccess(httpReq.Context(), rsAPI, request, roomID); res != nil {
		return *res
	}
	state, resErr := roomReconciliationState(httpReq.Context(), rsAPI, roomID, request.Origin())
	if resErr != nil {
		return *resErr
	}
	format, _ := reconciliation.EventIDFormat(state.RoomVersion)
	snapshot, err := reconciliation.Snapshot(state.EventIDs, format)
	if err != nil {
		util.GetLogger(httpReq.Context()).WithError(err).Error("MSC4521 snapshot failed")
		return util.ErrorResponse(err)
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: digestResponse(state, snapshot, withStrata)}
}

type roomDiffSketchBucket struct {
	Depth    uint64          `json:"depth"`
	Prefix   json.RawMessage `json:"prefix"`
	Capacity uint64          `json:"capacity"`
}

type roomDiffRequest struct {
	Mode                 string                 `json:"mode"`
	DigestType           string                 `json:"digest_type"`
	FrameID              string                 `json:"frame_id"`
	FrameNegotiation     bool                   `json:"frame_negotiation"`
	FrameEventIDs        []string               `json:"frame_event_ids"`
	LocalDigest          string                 `json:"local_digest"`
	LocalKnownEventCount uint64                 `json:"local_known_event_count"`
	Requests             []roomDiffSketchBucket `json:"requests"`
	LocalSketches        []string               `json:"local_sketches"`
	Scope                *string                `json:"scope"`
	StateAt              *string                `json:"state_at"`
}

func parseBucketPrefix(raw json.RawMessage) (uint64, bool) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, false
	}
	var number uint64
	if err := json.Unmarshal(raw, &number); err == nil {
		return number, true
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		number, err := strconv.ParseUint(text, 10, 64)
		return number, err == nil
	}
	return 0, false
}

// RoomDiff implements POST room_diff/{roomID}.
func RoomDiff(
	httpReq *http.Request, request *fclient.FederationRequest,
	rsAPI roomserverAPI.FederationRoomserverAPI, roomID string,
) util.JSONResponse {
	if res := checkReconciliationAccess(httpReq.Context(), rsAPI, request, roomID); res != nil {
		return *res
	}
	var body roomDiffRequest
	if err := json.Unmarshal(request.Content(), &body); err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON("The request body could not be decoded into valid JSON: " + err.Error()),
		}
	}
	badParam := func(message string) util.JSONResponse {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam(message)}
	}
	if body.Mode != "sketch" {
		return badParam("Only `sketch` mode is supported")
	}
	if body.DigestType != reconciliation.DigestType {
		return badParam("Unsupported `digest_type`")
	}
	scope := "event_set"
	if body.Scope != nil {
		scope = *body.Scope
	}
	if scope != "event_set" {
		return badParam("Only the `event_set` scope is supported")
	}
	if body.FrameNegotiation && len(body.FrameEventIDs) == 0 {
		return badParam("`frame_event_ids` is required when `frame_negotiation` is true")
	}
	if body.FrameID != "" {
		if raw, err := base64.RawURLEncoding.DecodeString(body.FrameID); err != nil || len(raw) != sha256.Size {
			return badParam("Invalid `frame_id`")
		}
	}
	for _, eventID := range body.FrameEventIDs {
		if len(eventID) < 2 || eventID[0] != '$' {
			return badParam("Invalid event ID in `frame_event_ids`")
		}
	}
	if len(body.Requests) != len(body.LocalSketches) {
		return badParam("`local_sketches` length must match `requests` length")
	}
	if len(body.Requests) == 0 || len(body.Requests) > maxReconciliationBuckets {
		return badParam("Invalid number of bucket requests")
	}
	requests := make([]reconciliation.BucketRequest, len(body.Requests))
	// Buckets must form an ordered antichain: each covers a range of the
	// 32-bit hash space, and ranges may neither overlap nor be out of order.
	var coveredUpTo uint64
	for i, bucket := range body.Requests {
		prefix, ok := parseBucketPrefix(bucket.Prefix)
		if !ok {
			return badParam(fmt.Sprintf("Invalid `requests[%d].prefix`", i))
		}
		if bucket.Depth > maxBucketDepth || bucket.Capacity == 0 || bucket.Capacity > maxBucketSketchCapacity {
			return badParam(fmt.Sprintf("Invalid `requests[%d]`", i))
		}
		if prefix >= uint64(1)<<bucket.Depth {
			return badParam(fmt.Sprintf("`requests[%d].prefix` is out of range for its depth", i))
		}
		start := prefix << (maxBucketDepth - bucket.Depth)
		if start < coveredUpTo {
			return badParam(fmt.Sprintf("`requests[%d]` overlaps or precedes an earlier bucket", i))
		}
		coveredUpTo = (prefix + 1) << (maxBucketDepth - bucket.Depth)
		requests[i] = reconciliation.BucketRequest{
			Depth:    uint8(bucket.Depth),
			Prefix:   prefix,
			Capacity: int(bucket.Capacity),
		}
	}

	state, resErr := roomReconciliationState(httpReq.Context(), rsAPI, roomID, request.Origin())
	if resErr != nil {
		return *resErr
	}
	currentFrameID := frameID(state.ForwardExtremities)
	requestedFrame := make(map[string]struct{}, len(body.FrameEventIDs))
	for _, eventID := range body.FrameEventIDs {
		requestedFrame[eventID] = struct{}{}
	}
	commonFrame := make([]string, 0, len(state.ForwardExtremities))
	for _, eventID := range state.ForwardExtremities {
		if _, ok := requestedFrame[eventID]; ok {
			commonFrame = append(commonFrame, eventID)
		}
	}
	frameStatus := "not_requested"
	if body.FrameNegotiation {
		if len(commonFrame) == 0 {
			frameStatus = "none"
		} else {
			frameStatus = "common"
		}
	}
	if body.FrameNegotiation && len(commonFrame) == 0 {
		return util.JSONResponse{Code: http.StatusOK, JSON: map[string]any{
			"digest_type":                reconciliation.DigestType,
			"missing_event_ids":          []string{},
			"requester_only_short_ids":   []string{},
			"remote_known_event_count":   len(state.EventIDs),
			"remote_extremity_event_ids": state.ForwardExtremities,
			"frame_id":                   currentFrameID,
			"frame_status":               frameStatus,
			"negotiated_frame_event_ids": commonFrame,
			"scope":                      scope,
			"state_at":                   body.StateAt,
			"sketch_status":              "not_applicable",
			"truncated":                  false,
		}}
	}

	format, _ := reconciliation.EventIDFormat(state.RoomVersion)
	result, err := reconciliation.BucketDiff(
		state.EventIDs, format, requests, body.LocalSketches,
		body.LocalDigest, body.LocalKnownEventCount,
	)
	if err != nil {
		if errors.Is(err, reconciliation.ErrInvalidRequest) {
			return badParam("Invalid reconciliation request: " + err.Error())
		}
		util.GetLogger(httpReq.Context()).WithError(err).Error("MSC4521 bucket diff failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.Unknown("Failed to compute the reconciliation difference"),
		}
	}

	missing := result.MissingEventIDs
	if missing == nil {
		missing = []string{}
	}
	requesterOnly := result.RequesterOnlyShortIDs
	if requesterOnly == nil {
		requesterOnly = []string{}
	}
	response := map[string]any{
		"digest_type":                reconciliation.DigestType,
		"missing_event_ids":          missing,
		"requester_only_short_ids":   requesterOnly,
		"remote_known_event_count":   result.KnownEventCount,
		"remote_extremity_event_ids": state.ForwardExtremities,
		"frame_id":                   currentFrameID,
		"frame_status":               frameStatus,
		"scope":                      scope,
		"state_at":                   body.StateAt,
		"sketch_status":              result.Status,
		"truncated":                  result.Status == "capacity_exceeded",
	}
	if body.FrameNegotiation {
		response["negotiated_frame_event_ids"] = commonFrame
	}
	if result.Status == "decoded" {
		response["expected_requester_side_accumulator"] = result.ExpectedRequesterSideAccumulator
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: response}
}
