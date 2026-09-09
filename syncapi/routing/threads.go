// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

const threadPageSize = 100

// ThreadsResponse contains thread roots with their bundled aggregations.
type ThreadsResponse struct {
	Chunk     []synctypes.ClientEvent `json:"chunk"`
	NextBatch string                  `json:"next_batch,omitempty"`
}

type threadCursor struct {
	Room      string               `json:"r"`
	User      string               `json:"u"`
	Include   string               `json:"i"`
	Watermark types.StreamPosition `json:"w"`
	Before    types.TopologyToken  `json:"b"`
}

func parseThreadParams(req *http.Request, roomID, userID string) (threadCursor, int, error) {
	q := req.URL.Query()
	include := q.Get("include")
	if include == "" {
		include = "all"
	}
	if include != "all" && include != "participated" {
		return threadCursor{}, 0, fmt.Errorf("include must be all or participated")
	}
	limit := threadPageSize
	if q.Has("limit") {
		n, err := strconv.Atoi(q.Get("limit"))
		if err != nil || n <= 0 {
			return threadCursor{}, 0, fmt.Errorf("limit must be a positive integer")
		}
		limit = min(n, threadPageSize)
	}
	cursor := threadCursor{Room: roomID, User: userID, Include: include, Before: types.TopologyToken{Depth: math.MaxInt64, PDUPosition: math.MaxInt64}}
	if q.Has("from") {
		data, err := base64.RawURLEncoding.DecodeString(q.Get("from"))
		if err != nil {
			return cursor, 0, fmt.Errorf("invalid thread pagination token")
		}
		if err = json.Unmarshal(data, &cursor); err != nil || cursor.Room != roomID || cursor.User != userID || cursor.Include != include || cursor.Watermark <= 0 || cursor.Before.Depth < 0 || cursor.Before.PDUPosition <= 0 {
			return cursor, 0, fmt.Errorf("invalid thread pagination token")
		}
	}
	return cursor, limit, nil
}

// Threads lists room thread roots ordered by the latest visible reply.
func Threads(req *http.Request, device *userapi.Device, syncDB storage.Database, rsAPI api.SyncRoomserverAPI, rawRoomID string) util.JSONResponse {
	roomID, err := spec.NewRoomID(rawRoomID)
	if err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("invalid room ID")}
	}
	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return util.ErrorResponse(err)
	}
	cursor, limit, err := parseThreadParams(req, rawRoomID, device.UserID)
	if err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam(err.Error())}
	}
	snapshot, err := syncDB.NewDatabaseSnapshot(req.Context())
	if err != nil {
		return util.ErrorResponse(err)
	}
	succeeded := false
	defer sqlutil.EndTransactionWithCheck(snapshot, &succeeded, &err)
	allowed, err := canViewThreads(req.Context(), snapshot, rsAPI, *roomID, *userID)
	if err != nil {
		return util.ErrorResponse(err)
	}
	if !allowed {
		return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("cannot view this room")}
	}
	maxRelation, err := snapshot.MaxStreamPositionForRelations(req.Context())
	if err != nil {
		return util.ErrorResponse(err)
	}
	if req.URL.Query().Has("from") && cursor.Watermark > maxRelation {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("unknown thread pagination token")}
	}
	if !req.URL.Query().Has("from") {
		cursor.Watermark = maxRelation
	}
	ignores, err := snapshot.IgnoresForUser(req.Context(), device.UserID)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return util.ErrorResponse(err)
	}
	reader := threadReader{snapshot: snapshot, rsAPI: rsAPI, userID: *userID, roomID: rawRoomID, watermark: cursor.Watermark, ignores: ignores}
	response, err := reader.page(req.Context(), cursor, limit)
	if err != nil {
		return util.ErrorResponse(err)
	}
	succeeded = true
	return util.JSONResponse{Code: http.StatusOK, JSON: response}
}

func canViewThreads(ctx context.Context, snapshot storage.DatabaseTransaction, rsAPI api.SyncRoomserverAPI, roomID spec.RoomID, userID spec.UserID) (bool, error) {
	var membership api.QueryMembershipForUserResponse
	if err := rsAPI.QueryMembershipForUser(ctx, &api.QueryMembershipForUserRequest{RoomID: roomID.String(), UserID: userID}, &membership); err != nil {
		return false, err
	}
	if !membership.RoomExists || membership.Membership == spec.Ban || membership.IsRoomForgotten {
		return false, nil
	}
	if membership.HasBeenInRoom {
		return true, nil
	}
	visibility, err := snapshot.GetStateEvent(ctx, roomID.String(), spec.MRoomHistoryVisibility, "")
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return visibility != nil && gjson.GetBytes(visibility.Content(), "history_visibility").Str == spec.WorldReadable, nil
}

type threadReader struct {
	snapshot  storage.DatabaseTransaction
	rsAPI     api.SyncRoomserverAPI
	userID    spec.UserID
	roomID    string
	watermark types.StreamPosition
	ignores   *types.IgnoredUsers
}

func (r *threadReader) page(ctx context.Context, cursor threadCursor, limit int) (*ThreadsResponse, error) {
	result := &ThreadsResponse{Chunk: []synctypes.ClientEvent{}}
	before := cursor.Before
	summaries := make(map[string]*threadSummary)
	var last types.TopologyToken
	for {
		entries, err := r.snapshot.ThreadRelations(ctx, r.roomID, r.watermark, before, threadPageSize)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			before = entry.Position
			summary, ok := summaries[entry.RootEventID]
			if !ok {
				summary, err = r.root(ctx, entry.RootEventID)
				if err != nil {
					return nil, err
				}
				summaries[entry.RootEventID] = summary
			}
			if summary == nil {
				continue
			}
			// Compare the latest eligible reply with the cursor before emitting. This
			// prevents older replies from resurrecting roots returned on earlier pages.
			if (cursor.Include == "participated" && !summary.participated) || summary.position.Depth > entry.Position.Depth || (summary.position.Depth == entry.Position.Depth && summary.position.PDUPosition > entry.Position.PDUPosition) {
				summaries[entry.RootEventID] = nil
				continue
			}
			if summary.latestID != entry.EventID {
				continue
			}
			summaries[entry.RootEventID] = nil

			if len(result.Chunk) == limit {
				cursor.Before = last
				data, err := json.Marshal(cursor)
				if err != nil {
					return nil, err
				}
				result.NextBatch = base64.RawURLEncoding.EncodeToString(data)
				return result, nil
			}
			result.Chunk = append(result.Chunk, *summary.event)
			last = entry.Position
		}
		if len(entries) < threadPageSize {
			return result, nil
		}
	}
}

func (r *threadReader) visible(ctx context.Context, events []*rstypes.HeaderedEvent) ([]*rstypes.HeaderedEvent, error) {
	sameRoom := make([]*rstypes.HeaderedEvent, 0, len(events))
	for _, event := range events {
		if event.RoomID().String() == r.roomID {
			sameRoom = append(sameRoom, event)
		}
	}
	return internal.ApplyHistoryVisibilityFilter(ctx, r.snapshot, r.rsAPI, sameRoom, nil, r.userID, "threads")
}

func (r *threadReader) clientEvent(ctx context.Context, event *rstypes.HeaderedEvent) (*synctypes.ClientEvent, error) {
	return synctypes.ToClientEvent(event.PDU, synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return r.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})
}

func (r *threadReader) ignored(userID string) bool {
	if r.ignores == nil {
		return false
	}
	_, ok := r.ignores.List[userID]
	return ok
}

type threadSummary struct {
	event        *synctypes.ClientEvent
	latestID     string
	position     types.TopologyToken
	participated bool
}

func (r *threadReader) root(ctx context.Context, rootID string) (*threadSummary, error) {
	roots, err := r.snapshot.Events(ctx, []string{rootID})
	if err != nil {
		return nil, err
	}
	roots, err = r.visible(ctx, roots)
	if err != nil || len(roots) == 0 {
		return nil, err
	}
	root := roots[0]
	if gjson.GetBytes(root.Content(), `m\.relates_to.rel_type`).Exists() {
		return nil, nil
	}
	clientRoot, err := r.clientEvent(ctx, root)
	if err != nil {
		return nil, err
	}
	participated := clientRoot.Sender == r.userID.String()
	var latest *rstypes.HeaderedEvent
	var latestStream types.StreamPosition
	count := 0
	err = r.eachRelation(ctx, rootID, "m.thread", func(event *rstypes.HeaderedEvent, pos types.StreamPosition, sender string) error {
		count++
		participated = participated || sender == r.userID.String()
		if latest == nil || event.Depth() > latest.Depth() || (event.Depth() == latest.Depth() && pos > latestStream) {
			latest, latestStream = event, pos
		}
		return nil
	})
	if err != nil {
		return nil, err
	}

	if latest == nil {
		return nil, nil
	}
	if r.ignored(clientRoot.Sender) {
		// Events() returns a freshly decoded PDU, so redaction cannot mutate shared state.
		root.Redact()
		clientRoot, err = r.clientEvent(ctx, root)
		if err != nil {
			return nil, err
		}
	}
	clientLatest, err := r.clientEvent(ctx, latest)
	if err != nil {
		return nil, err
	}
	if err = r.bundleRelations(ctx, root, clientRoot); err != nil {
		return nil, err
	}
	if err = r.bundleRelations(ctx, latest, clientLatest); err != nil {
		return nil, err
	}
	bundle := struct {
		LatestEvent  *synctypes.ClientEvent `json:"latest_event"`
		Count        int                    `json:"count"`
		Participated bool                   `json:"current_user_participated"`
	}{clientLatest, count, participated}
	unsigned := clientRoot.Unsigned
	if len(unsigned) == 0 {
		unsigned = []byte(`{}`)
	}
	clientRoot.Unsigned, err = sjson.SetBytes(unsigned, `m\.relations.m\.thread`, bundle)
	return &threadSummary{event: clientRoot, latestID: latest.EventID(), position: types.TopologyToken{Depth: types.StreamPosition(latest.Depth()), PDUPosition: latestStream}, participated: participated}, err
}

// eachRelation filters before aggregating so neither IDs nor counts reveal hidden events.
func (r *threadReader) eachRelation(ctx context.Context, eventID, relType string, visit func(*rstypes.HeaderedEvent, types.StreamPosition, string) error) error {
	var from types.StreamPosition
	for {
		replies, _, next, err := r.snapshot.RelationsFor(ctx, r.roomID, eventID, relType, "", from, r.watermark, false, threadPageSize)
		if err != nil {
			return err
		}
		events := make([]*rstypes.HeaderedEvent, 0, len(replies))
		positions := make(map[string]types.StreamPosition, len(replies))
		for _, reply := range replies {
			events = append(events, reply.HeaderedEvent)
			positions[reply.EventID()] = reply.StreamPosition
		}
		events, err = r.visible(ctx, events)
		if err != nil {
			return err
		}
		for _, event := range events {
			sender, err := r.rsAPI.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
			if err != nil {
				return err
			}
			if sender == nil || r.ignored(sender.String()) {
				continue
			}
			if err = visit(event, positions[event.EventID()], sender.String()); err != nil {
				return err
			}
		}
		if next == "" {
			return nil
		}
		from, err = types.NewStreamPositionFromString(next)
		if err != nil {
			return err
		}
	}
}

// bundleRelations includes references and the latest valid edit on roots and
// latest replies. It deliberately does not recurse into edits or references.
func (r *threadReader) bundleRelations(ctx context.Context, event *rstypes.HeaderedEvent, client *synctypes.ClientEvent) error {
	unsigned := client.Unsigned
	if len(unsigned) == 0 {
		unsigned = []byte(`{}`)
	}
	// Never reuse aggregations received from another server or for another user.
	var err error
	unsigned, err = sjson.DeleteBytes(unsigned, `m\.relations`)
	if err != nil {
		return err
	}
	client.Unsigned = unsigned
	if event.StateKey() != nil {
		return nil
	}
	var references []map[string]string
	err = r.eachRelation(ctx, event.EventID(), "m.reference", func(child *rstypes.HeaderedEvent, _ types.StreamPosition, _ string) error {
		references = append(references, map[string]string{"event_id": child.EventID()})
		return nil
	})
	if err != nil {
		return err
	}
	if len(references) > 0 {
		client.Unsigned, err = sjson.SetBytes(client.Unsigned, `m\.relations.m\.reference`, map[string]any{"chunk": references})
		if err != nil {
			return err
		}
	}
	if event.Redacted() || r.ignored(client.Sender) || gjson.GetBytes(event.Content(), `m\.relates_to.rel_type`).Str == "m.replace" {
		return nil
	}
	var edit *rstypes.HeaderedEvent
	err = r.eachRelation(ctx, event.EventID(), "m.replace", func(child *rstypes.HeaderedEvent, _ types.StreamPosition, sender string) error {
		if child.StateKey() != nil || child.Type() != event.Type() || sender != client.Sender || child.Redacted() {
			return nil
		}
		if child.Type() != "m.room.encrypted" && !gjson.GetBytes(child.Content(), `m\.new_content`).IsObject() {
			return nil
		}
		if edit == nil || child.OriginServerTS() > edit.OriginServerTS() || (child.OriginServerTS() == edit.OriginServerTS() && child.EventID() > edit.EventID()) {
			edit = child
		}
		return nil
	})
	if err != nil || edit == nil {
		return err
	}
	clientEdit, err := r.clientEvent(ctx, edit)
	if err != nil {
		return err
	}
	clientEdit.Unsigned, err = sjson.DeleteBytes(clientEdit.Unsigned, `m\.relations`)
	if err != nil {
		return err
	}
	client.Unsigned, err = sjson.SetBytes(client.Unsigned, `m\.relations.m\.replace`, clientEdit)
	return err
}
