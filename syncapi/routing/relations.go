// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// A page holds at most relationsMaxLimit children, and recurse=true follows
// relationsRecursionDepth levels of relations; the spec asks for at least three.
const (
	relationsMaxLimit       = 50
	relationsRecursionDepth = 3
)

type RelationsResponse struct {
	Chunk          []synctypes.ClientEvent `json:"chunk"`
	NextBatch      string                  `json:"next_batch,omitempty"`
	PrevBatch      string                  `json:"prev_batch,omitempty"`
	RecursionDepth *int                    `json:"recursion_depth,omitempty"`
}

type relationsParams struct {
	backwards bool
	limit     int
	recurse   *bool
}

func parseRelationsParams(req *http.Request) (relationsParams, error) {
	q := req.URL.Query()
	params := relationsParams{backwards: true, limit: relationsMaxLimit}
	switch q.Get("dir") {
	case "", "b":
	case "f":
		params.backwards = false
	default:
		return params, fmt.Errorf("dir must be b or f")
	}
	if l := q.Get("limit"); l != "" {
		limit, err := strconv.Atoi(l)
		if err != nil || limit < 0 {
			return params, fmt.Errorf("limit must be a non-negative integer")
		}
		if limit > 0 {
			params.limit = min(limit, relationsMaxLimit)
		}
	}
	for _, name := range []string{"recurse", "org.matrix.msc3981.recurse"} {
		if !q.Has(name) {
			continue
		}
		recurse, err := strconv.ParseBool(q.Get(name))
		if err != nil {
			return params, fmt.Errorf("%s must be a boolean", name)
		}
		params.recurse = &recurse
	}
	return params, nil
}

// parseRelationsToken accepts this endpoint's tokens as well as /messages and
// /sync tokens. An empty token is the start of the pagination in the given
// direction. For a stream token it also returns the parsed token: a decoded
// stream position bounds the page by stream order, which the converted
// topology token alone cannot express.
func parseRelationsToken(ctx context.Context, snapshot storage.DatabaseTransaction, roomID, token string, backwards bool) (types.TopologyToken, *types.StreamingToken, error) {
	if token == "" {
		if backwards {
			return types.TopologyToken{Depth: math.MaxInt64, PDUPosition: math.MaxInt64}, nil, nil
		}
		return types.TopologyToken{Depth: -1, PDUPosition: -1}, nil, nil
	}
	if topology, err := types.NewTopologyTokenFromString(token); err == nil {
		return topology, nil, nil
	}
	stream, err := types.NewStreamTokenFromString(token)
	if err != nil {
		return types.TopologyToken{}, nil, errInvalidRelationsToken
	}
	topology, err := snapshot.StreamToTopologicalPosition(ctx, roomID, stream.PDUPosition, backwards)
	return topology, &stream, err
}

var errInvalidRelationsToken = errors.New("invalid pagination token")

// Relations implements GET /_matrix/client/v1/rooms/{roomId}/relations/{eventId}
// and its relType and eventType forms, returning children in topology order.
//
//nolint:gocyclo
func Relations(
	req *http.Request, device *userapi.Device,
	syncDB storage.Database,
	rsAPI api.SyncRoomserverAPI,
	rawRoomID, eventID, relType, eventType string,
) util.JSONResponse {
	ctx := req.Context()
	roomID, err := spec.NewRoomID(rawRoomID)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("invalid room ID"),
		}
	}
	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Error("device.UserID invalid")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	params, err := parseRelationsParams(req)
	if err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam(err.Error())}
	}

	snapshot, err := syncDB.NewDatabaseSnapshot(ctx)
	if err != nil {
		logrus.WithError(err).Error("Failed to get snapshot for relations")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	var succeeded bool
	defer sqlutil.EndTransactionWithCheck(snapshot, &succeeded, &err)

	// A missing parent and one the user may not see are indistinguishable.
	parents, err := snapshot.Events(ctx, []string{eventID})
	if err != nil {
		return util.ErrorResponse(err)
	}
	if len(parents) == 1 && parents[0].RoomID().String() == roomID.String() {
		parents, err = internal.ApplyHistoryVisibilityFilter(ctx, snapshot, rsAPI, parents, nil, *userID, "relations")
		if err != nil {
			return util.ErrorResponse(err)
		}
	} else {
		parents = nil
	}
	if len(parents) == 0 {
		return util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("Event not found.")}
	}

	q := req.URL.Query()
	from, fromStream, err := parseRelationsToken(ctx, snapshot, roomID.String(), q.Get("from"), params.backwards)
	var to types.TopologyToken
	if err == nil {
		// A to token bounds the opposite end of the page.
		to, _, err = parseRelationsToken(ctx, snapshot, roomID.String(), q.Get("to"), !params.backwards)
	}
	if errors.Is(err, errInvalidRelationsToken) {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam(err.Error())}
	}
	if err != nil {
		return util.ErrorResponse(err)
	}
	res, err := relationsPage(ctx, snapshot, rsAPI, *userID, *roomID, eventID, relType, eventType, params, from, to, fromStream)
	if err != nil {
		return util.ErrorResponse(err)
	}
	res.PrevBatch = q.Get("from")
	succeeded = true
	return util.JSONResponse{Code: http.StatusOK, JSON: res}
}

func relationsPage(
	ctx context.Context, snapshot storage.DatabaseTransaction, rsAPI api.SyncRoomserverAPI,
	userID spec.UserID, roomID spec.RoomID, eventID, relType, eventType string,
	params relationsParams, from, to types.TopologyToken, fromStream *types.StreamingToken,
) (*RelationsResponse, error) {
	maxDepth := 1
	if params.recurse != nil && *params.recurse {
		maxDepth = relationsRecursionDepth
	}
	// One extra entry tells whether there is another page.
	//
	// A stream token bounds pagination by stream order. Relations are selected
	// by topology position, so children stored after the token can share a
	// shallower position; drop them as /messages does. Dropping entries from a
	// fixed-size batch could hide older eligible relations beyond it, so keep
	// reading further batches until the page is full or the range is exhausted.
	bounded := params.backwards && fromStream != nil
	var entries []types.TopologicalRelation
	cursor := from
	for {
		batch, err := snapshot.RelationsInTopologicalRange(ctx, roomID.String(), eventID, relType, eventType, maxDepth, cursor, to, params.backwards, params.limit+1)
		if err != nil {
			return nil, err
		}
		for _, entry := range batch {
			if !bounded || entry.Position.PDUPosition <= fromStream.PDUPosition {
				entries = append(entries, entry)
			}
		}
		if !bounded || len(entries) > params.limit || len(batch) < params.limit+1 {
			break
		}
		// Tokens name the cursor after an event, so step back over the last one.
		cursor = batch[len(batch)-1].Position
		cursor.Decrement()
	}
	res := &RelationsResponse{Chunk: []synctypes.ClientEvent{}}
	if params.recurse != nil {
		res.RecursionDepth = &maxDepth
	}
	if len(entries) > params.limit {
		entries = entries[:params.limit]
		next := entries[len(entries)-1].Position
		if params.backwards {
			// Tokens name the cursor after an event, so step back over the last one.
			next.Decrement()
		}
		res.NextBatch = next.String()
	}

	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.EventID)
	}
	loaded, err := snapshot.Events(ctx, ids)
	if err != nil {
		return nil, err
	}
	byID := make(map[string]*rstypes.HeaderedEvent, len(loaded))
	for _, event := range loaded {
		byID[event.EventID()] = event
	}
	events := make([]*rstypes.HeaderedEvent, 0, len(entries))
	for _, entry := range entries {
		if event, ok := byID[entry.EventID]; ok {
			events = append(events, event)
		}
	}
	events, err = internal.ApplyHistoryVisibilityFilter(ctx, snapshot, rsAPI, events, nil, userID, "relations")
	if err != nil {
		return nil, err
	}
	ignores, err := snapshot.IgnoresForUser(ctx, userID.String())
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	res.Chunk = make([]synctypes.ClientEvent, 0, len(events))
	shown := make([]*rstypes.HeaderedEvent, 0, len(events))
	for _, event := range events {
		sender, err := rsAPI.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
		if err != nil {
			return nil, err
		}
		if sender != nil && ignores != nil {
			if _, ignored := ignores.List[sender.String()]; ignored {
				continue
			}
		}
		clientEvent, err := synctypes.ToClientEvent(event.PDU, synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})
		if err != nil {
			util.GetLogger(ctx).WithError(err).WithField("event_id", event.EventID()).Error("Failed converting to ClientEvent")
			continue
		}
		res.Chunk = append(res.Chunk, *clientEvent)
		shown = append(shown, event)
	}

	watermark, err := snapshot.MaxStreamPositionForRelations(ctx)
	if err != nil {
		return nil, err
	}
	bundler := internal.NewRelationBundlerAt(snapshot, rsAPI, userID, ignores, watermark)
	return res, bundler.Bundle(ctx, shown, res.Chunk, synctypes.FormatAll)
}
