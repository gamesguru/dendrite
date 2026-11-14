// Copyright 2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strconv"

	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/reconciliation"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

const (
	reconcileMaxRounds    = 8
	reconcileRootCapacity = 32
	reconcileMaxBuckets   = 128
	reconcileMaxDepth     = 32
)

type remoteReconciliationDigest struct {
	Digest          string   `json:"digest"`
	DigestType      string   `json:"digest_type"`
	KnownEventCount uint64   `json:"known_event_count"`
	FrameID         string   `json:"frame_id"`
	FrameEventIDs   []string `json:"frame_event_ids"`
}

type remoteReconciliationDiff struct {
	DigestType                       string   `json:"digest_type"`
	MissingEventIDs                  []string `json:"missing_event_ids"`
	RequesterOnlyShortIDs            []string `json:"requester_only_short_ids"`
	ExpectedRequesterSideAccumulator string   `json:"expected_requester_side_accumulator"`
	RemoteKnownEventCount            uint64   `json:"remote_known_event_count"`
	FrameID                          string   `json:"frame_id"`
	FrameStatus                      string   `json:"frame_status"`
	SketchStatus                     string   `json:"sketch_status"`
	Truncated                        bool     `json:"truncated"`
}

type reconciliationBucketRequest struct {
	Depth    uint64 `json:"depth"`
	Prefix   string `json:"prefix"`
	Capacity uint64 `json:"capacity"`
}

// AdminReconcileRoom runs one explicit MSC4521 exchange against serverName for
// roomID, fetching any events the remote holds that this server is missing.
func AdminReconcileRoom(
	req *http.Request, device *userapi.Device, rsAPI roomserverAPI.ClientRoomserverAPI,
	federation fclient.FederationClient, cfg *config.Zendrite,
) util.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(httputil.Vars(req))
	if err != nil {
		return util.ErrorResponse(err)
	}
	roomID, serverName := vars["roomID"], vars["serverName"]
	if roomID == "" || serverName == "" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Expecting server name and room ID."),
		}
	}
	if !reconciliation.Enabled || !cfg.FederationAPI.MSC4521Enabled {
		return util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("MSC4521 reconciliation is not enabled on this server"),
		}
	}
	roomVersion, err := rsAPI.QueryRoomVersionForRoom(req.Context(), roomID)
	if err != nil {
		return util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("Room not found")}
	}
	format, ok := reconciliation.EventIDFormat(roomVersion)
	if !ok {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MatrixError{
				ErrCode: "M_UNSUPPORTED_ROOM_VERSION",
				Err:     "MSC4521 requires reference-hash event IDs (room version 3 or later)",
			},
		}
	}

	logger := logrus.WithFields(logrus.Fields{
		"userID": device.UserID, "serverName": serverName, "roomID": roomID,
	})
	identity := cfg.Global.SigningIdentity
	origin := cfg.Global.ServerName
	destination := spec.ServerName(serverName)

	remoteDigest, err := fetchRemoteDigest(req.Context(), federation, identity, origin, destination, roomID)
	if err != nil {
		logger.WithError(err).Warn("MSC4521 remote digest failed")
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: spec.Unknown("Could not fetch remote reconciliation digest: " + err.Error()),
		}
	}
	if remoteDigest.DigestType != reconciliation.DigestType {
		return util.JSONResponse{
			Code: http.StatusBadGateway,
			JSON: spec.Unknown("Remote server does not speak " + reconciliation.DigestType),
		}
	}

	requests := []reconciliation.BucketRequest{{Depth: 0, Prefix: 0, Capacity: reconcileRootCapacity}}
	fetched := make([]string, 0)
	failed := make(map[string]string)
	seen := make(map[string]struct{})
	requesterOnly := []string{}
	rounds := 0
	status := "diverged"

	for rounds = 0; rounds < reconcileMaxRounds; rounds++ {
		state, resErr := roomReconciliationStateFromAPI(req.Context(), rsAPI, roomID)
		if resErr != nil {
			return *resErr
		}
		localSnapshot, err := reconciliation.Snapshot(state.EventIDs, format)
		if err != nil {
			return util.ErrorResponse(err)
		}
		if localSnapshot.Digest == remoteDigest.Digest && localSnapshot.Count == remoteDigest.KnownEventCount {
			status = "synchronized"
			break
		}
		if len(requests) == 0 {
			status = "capacity_exceeded"
			break
		}
		sketches, err := reconciliation.BucketSketches(state.EventIDs, format, requests)
		if err != nil {
			return util.ErrorResponse(err)
		}
		diff, err := postRemoteDiff(
			req.Context(), federation, identity, origin, destination, roomID,
			requests, sketches, localSnapshot, remoteDigest,
		)
		if err != nil {
			logger.WithError(err).Warn("MSC4521 remote diff failed")
			return util.JSONResponse{
				Code: http.StatusBadGateway,
				JSON: spec.Unknown("Could not fetch remote reconciliation diff: " + err.Error()),
			}
		}
		if diff.SketchStatus == "capacity_exceeded" {
			requests = splitBuckets(requests)
			continue
		}
		if diff.SketchStatus != "decoded" {
			status = diff.SketchStatus
			break
		}
		requesterOnly = diff.RequesterOnlyShortIDs
		if len(diff.MissingEventIDs) == 0 {
			// Nothing more to pull. Any remaining difference is on our side.
			status = "diverged"
			break
		}
		pulled := 0
		for _, eventID := range diff.MissingEventIDs {
			if _, ok := seen[eventID]; ok {
				continue
			}
			seen[eventID] = struct{}{}
			outcome, err := rsAPI.PerformAdminFetchEvent(req.Context(), roomID, eventID, destination)
			if err != nil {
				failed[eventID] = err.Error()
				continue
			}
			if outcome == "accepted" {
				fetched = append(fetched, eventID)
				pulled++
			} else {
				failed[eventID] = outcome
			}
		}
		requests = []reconciliation.BucketRequest{{Depth: 0, Prefix: 0, Capacity: reconcileRootCapacity}}
		if pulled == 0 {
			status = "no_progress"
			break
		}
	}

	response := map[string]any{
		"status":         status,
		"rounds":         rounds,
		"remote_digest":  remoteDigest.Digest,
		"remote_count":   remoteDigest.KnownEventCount,
		"fetched":        fetched,
		"failed":         failed,
		"requester_only": requesterOnly,
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: response}
}

func roomReconciliationStateFromAPI(
	ctx context.Context, rsAPI roomserverAPI.ClientRoomserverAPI, roomID string,
) (*roomserverAPI.QueryRoomReconciliationStateResponse, *util.JSONResponse) {
	var state roomserverAPI.QueryRoomReconciliationStateResponse
	if err := rsAPI.QueryRoomReconciliationState(ctx, &roomserverAPI.QueryRoomReconciliationStateRequest{
		RoomID: roomID,
	}, &state); err != nil {
		return nil, &util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("Room not found")}
	}
	return &state, nil
}

func fetchRemoteDigest(
	ctx context.Context, federation fclient.FederationClient,
	identity fclient.SigningIdentity, origin, destination spec.ServerName, roomID string,
) (*remoteReconciliationDigest, error) {
	path := "/_matrix/federation/unstable/tk.nutra.msc0501/room_digest/" + url.PathEscape(roomID)
	var response remoteReconciliationDigest
	if err := doSignedFederationRequest(ctx, federation, identity, http.MethodGet, origin, destination, path, nil, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func postRemoteDiff(
	ctx context.Context, federation fclient.FederationClient,
	identity fclient.SigningIdentity, origin, destination spec.ServerName, roomID string,
	requests []reconciliation.BucketRequest, sketches []string,
	local *reconciliation.RoomSnapshot, remote *remoteReconciliationDigest,
) (*remoteReconciliationDiff, error) {
	wire := make([]reconciliationBucketRequest, len(requests))
	for i, request := range requests {
		wire[i] = reconciliationBucketRequest{
			Depth:    uint64(request.Depth),
			Prefix:   strconv.FormatUint(request.Prefix, 10),
			Capacity: uint64(request.Capacity),
		}
	}
	body := map[string]any{
		"mode":                    "sketch",
		"digest_type":             reconciliation.DigestType,
		"frame_id":                remote.FrameID,
		"frame_negotiation":       false,
		"frame_event_ids":         remote.FrameEventIDs,
		"local_digest":            local.Digest,
		"local_known_event_count": local.Count,
		"requests":                wire,
		"local_sketches":          sketches,
		"scope":                   "event_set",
	}
	path := "/_matrix/federation/unstable/tk.nutra.msc0501/room_diff/" + url.PathEscape(roomID)
	var response remoteReconciliationDiff
	if err := doSignedFederationRequest(ctx, federation, identity, http.MethodPost, origin, destination, path, body, &response); err != nil {
		return nil, err
	}
	return &response, nil
}

func doSignedFederationRequest(
	ctx context.Context, federation fclient.FederationClient,
	identity fclient.SigningIdentity, method string, origin, destination spec.ServerName,
	path string, body any, result any,
) error {
	request := fclient.NewFederationRequest(method, origin, destination, path)
	if body != nil {
		if err := request.SetContent(body); err != nil {
			return fmt.Errorf("set content: %w", err)
		}
	}
	if err := request.Sign(identity.ServerName, identity.KeyID, identity.PrivateKey); err != nil {
		return fmt.Errorf("sign: %w", err)
	}
	httpReq, err := request.HTTPRequest()
	if err != nil {
		return fmt.Errorf("http request: %w", err)
	}
	return federation.DoRequestAndParseResponse(ctx, httpReq, result)
}

// splitBuckets halves every bucket into its two children, up to the depth and
// count limits.
func splitBuckets(requests []reconciliation.BucketRequest) []reconciliation.BucketRequest {
	next := make([]reconciliation.BucketRequest, 0, len(requests)*2)
	for _, request := range requests {
		if request.Depth >= reconcileMaxDepth || len(next)+2 > reconcileMaxBuckets {
			continue
		}
		next = append(next,
			reconciliation.BucketRequest{Depth: request.Depth + 1, Prefix: request.Prefix << 1, Capacity: request.Capacity},
			reconciliation.BucketRequest{Depth: request.Depth + 1, Prefix: request.Prefix<<1 | 1, Capacity: request.Capacity},
		)
	}
	sort.Slice(next, func(i, j int) bool {
		if next[i].Depth == next[j].Depth {
			return next[i].Prefix < next[j].Prefix
		}
		return next[i].Depth < next[j].Depth
	})
	return next
}
