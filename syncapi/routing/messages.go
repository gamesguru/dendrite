// Copyright 2018-2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"path"
	"sort"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/sync"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type messagesReq struct {
	ctx              context.Context
	db               storage.Database
	snapshot         storage.DatabaseTransaction
	rsAPI            api.SyncRoomserverAPI
	cfg              *config.SyncAPI
	roomID           string
	from             *types.TopologyToken
	to               *types.TopologyToken
	device           *userapi.Device
	deviceUserID     spec.UserID
	wasToProvided    bool
	backwardOrdering bool
	filter           *synctypes.RoomEventFilter
	didBackfill      bool
}

type messagesResp struct {
	Start       string                  `json:"start"`
	StartStream string                  `json:"start_stream,omitempty"` // NOTSPEC: used by Cerulean, so clients can hit /messages then immediately /sync with a latest sync token
	End         string                  `json:"end,omitempty"`
	Chunk       []synctypes.ClientEvent `json:"chunk"`
	State       []synctypes.ClientEvent `json:"state,omitempty"`
}

// OnIncomingMessagesRequest implements the /messages endpoint from the
// client-server API.
// See: https://matrix.org/docs/spec/client_server/latest.html#get-matrix-client-r0-rooms-roomid-messages
//
//nolint:gocyclo
func OnIncomingMessagesRequest(
	req *http.Request, db storage.Database, roomID string, device *userapi.Device,
	rsAPI api.SyncRoomserverAPI,
	cfg *config.SyncAPI,
	srp *sync.RequestPool,
	lazyLoadCache caching.LazyLoadCache,
) util.JSONResponse {
	var err error

	deviceUserID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("device.UserID invalid")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// NewDatabaseTransaction is used here instead of NewDatabaseSnapshot as we
	// expect to be able to write to the database in response to a /messages
	// request that requires backfilling from the roomserver or federation.
	snapshot, err := db.NewDatabaseTransaction(req.Context())
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	var succeeded bool
	defer sqlutil.EndTransactionWithCheck(snapshot, &succeeded, &err)

	// check if the user has already forgotten about this room
	membershipResp, err := getMembershipForUser(req.Context(), roomID, device.UserID, rsAPI)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !membershipResp.RoomExists {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("room does not exist"),
		}
	}

	if membershipResp.IsRoomForgotten {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("user already forgot about this room"),
		}
	}

	filter, err := parseRoomEventFilter(req)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("unable to parse filter"),
		}
	}

	// Extract parameters from the request's URL.
	// Pagination tokens.
	var fromStream *types.StreamingToken
	fromQuery := req.URL.Query().Get("from")
	toQuery := req.URL.Query().Get("to")

	// Direction to return events from.
	dir := req.URL.Query().Get("dir")
	if dir != "b" && dir != "f" {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.MissingParam("Bad or missing dir query parameter (should be either 'b' or 'f')"),
		}
	}
	// A boolean is easier to handle in this case, especially since dir is sure
	// to have one of the two accepted values (so dir == "f" <=> !backwardOrdering).
	backwardOrdering := (dir == "b")

	emptyFromSupplied := fromQuery == ""
	if emptyFromSupplied {
		// If "from" isn't provided, it defaults to either the earliest stream
		// position (if we're going forward) or to the latest one (if we're
		// going backward).

		var from types.TopologyToken
		if backwardOrdering {
			from = types.TopologyToken{Depth: math.MaxInt64, PDUPosition: math.MaxInt64}
		} else {
			// go 1 earlier than the first event so we correctly fetch the earliest event
			// this is because Database.GetEventsInTopologicalRange is exclusive of the lower-bound.
			from = types.TopologyToken{}
		}
		fromQuery = from.String()
	}

	from, err := types.NewTopologyTokenFromString(fromQuery)
	if err != nil {
		var streamToken types.StreamingToken
		if streamToken, err = types.NewStreamTokenFromString(fromQuery); err != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.InvalidParam("Invalid from parameter: " + err.Error()),
			}
		} else {
			fromStream = &streamToken
			from, err = snapshot.StreamToTopologicalPosition(req.Context(), roomID, streamToken.PDUPosition, backwardOrdering)
			if err != nil {
				logrus.WithError(err).Errorf("Failed to get topological position for streaming token %v", streamToken)
				return util.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
		}
	}

	// Pagination tokens. To is optional, and its default value depends on the
	// direction ("b" or "f").
	var to types.TopologyToken
	wasToProvided := true
	if len(toQuery) > 0 {
		to, err = types.NewTopologyTokenFromString(toQuery)
		if err != nil {
			var streamToken types.StreamingToken
			if streamToken, err = types.NewStreamTokenFromString(toQuery); err != nil {
				return util.JSONResponse{
					Code: http.StatusBadRequest,
					JSON: spec.InvalidParam("Invalid to parameter: " + err.Error()),
				}
			} else {
				to, err = snapshot.StreamToTopologicalPosition(req.Context(), roomID, streamToken.PDUPosition, !backwardOrdering)
				if err != nil {
					logrus.WithError(err).Errorf("Failed to get topological position for streaming token %v", streamToken)
					return util.JSONResponse{
						Code: http.StatusInternalServerError,
						JSON: spec.InternalServerError{},
					}
				}
			}
		}
	} else {
		// If "to" isn't provided, it defaults to either the earliest stream
		// position (if we're going backward) or to the latest one (if we're
		// going forward).
		to = types.TopologyToken{Depth: math.MaxInt64, PDUPosition: math.MaxInt64}
		if backwardOrdering {
			// go 1 earlier than the first event so we correctly fetch the earliest event
			// this is because Database.GetEventsInTopologicalRange is exclusive of the lower-bound.
			to = types.TopologyToken{}
		}
		wasToProvided = false
	}

	// If the user already left the room, grep events from before that
	if membershipResp.Membership == spec.Leave {
		var token types.TopologyToken
		token, err = snapshot.EventPositionInTopology(req.Context(), membershipResp.EventID)
		if err != nil {
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
			}
		}
		if backwardOrdering {
			from = token
		}
	}

	mReq := messagesReq{
		ctx:              req.Context(),
		db:               db,
		snapshot:         snapshot,
		rsAPI:            rsAPI,
		cfg:              cfg,
		roomID:           roomID,
		from:             &from,
		to:               &to,
		wasToProvided:    wasToProvided,
		filter:           filter,
		backwardOrdering: backwardOrdering,
		device:           device,
		deviceUserID:     *deviceUserID,
	}

	clientEvents, start, end, err := mReq.retrieveEvents(req.Context(), rsAPI)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("mreq.retrieveEvents failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// If start and end are equal we've reached the beginning of the room or are stuck.
	// A filtered-empty page (len==0 but valid end) must preserve its end token so the
	// client can continue paginating; retrieveEvents returns emptyToken for true end-of-history.
	if start == end {
		end = types.TopologyToken{}
	}

	util.GetLogger(req.Context()).WithFields(logrus.Fields{
		"request_from":   from.String(),
		"request_to":     to.String(),
		"limit":          filter.Limit,
		"backwards":      backwardOrdering,
		"response_start": start.String(),
		"response_end":   end.String(),
		"backfilled":     mReq.didBackfill,
	}).Info("Responding")

	res := messagesResp{
		Chunk: clientEvents,
		Start: start.String(),
		End:   end.String(),
	}
	if filter.LazyLoadMembers {
		membershipEvents, err := applyLazyLoadMembers(req.Context(), device, snapshot, roomID, clientEvents, lazyLoadCache)
		if err != nil {
			util.GetLogger(req.Context()).WithError(err).Error("failed to apply lazy loading")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		res.State = append(res.State, synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(membershipEvents), synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) { //nolint:contextcheck
			return rsAPI.QueryUserIDForSender(req.Context(), roomID, senderID)
		})...)
	}

	if fromStream != nil {
		res.StartStream = fromStream.String()
	}

	// Respond with the events.
	succeeded = true
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: res,
	}
}

func getMembershipForUser(ctx context.Context, roomID, userID string, rsAPI api.SyncRoomserverAPI) (resp api.QueryMembershipForUserResponse, err error) {
	fullUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return resp, err
	}
	req := api.QueryMembershipForUserRequest{
		RoomID: roomID,
		UserID: *fullUserID,
	}
	if err := rsAPI.QueryMembershipForUser(ctx, &req, &resp); err != nil {
		return api.QueryMembershipForUserResponse{}, err
	}

	return resp, nil
}

// retrieveEvents retrieves events from the local database for a request on
// /messages. If there's not enough events to retrieve, it asks another
// homeserver in the room for older events.
// Returns an error if there was an issue talking to the database or with the
// remote homeserver.
func (r *messagesReq) retrieveEvents(ctx context.Context, rsAPI api.SyncRoomserverAPI) (
	clientEvents []synctypes.ClientEvent, start,
	end types.TopologyToken, err error,
) {
	emptyToken := types.TopologyToken{}
	// Retrieve the events from the local database.
	// streamEvents has already been filtered by the client's sender/type filter (SelectEvents).
	// topologyStart/end come from the unfiltered topology range, so end remains valid for
	// pagination even when the filter removes all events from a page.
	streamEvents, topologyStart, end, err := r.snapshot.GetEventsInTopologicalRange(r.ctx, r.from, r.to, r.roomID, r.filter, r.backwardOrdering) //nolint:contextcheck
	if err != nil {
		err = fmt.Errorf("GetEventsInRange: %w", err)
		return []synctypes.ClientEvent{}, *r.from, emptyToken, err
	}
	end.Decrement()

	// topologyHadEvents distinguishes "no events in topology range" (true end of history
	// or backward extremity) from "events existed but the filter removed all of them".
	topologyHadEvents := topologyStart != emptyToken

	var events []*rstypes.HeaderedEvent
	util.GetLogger(r.ctx).WithFields(logrus.Fields{ //nolint:contextcheck
		"start":     r.from,
		"end":       r.to,
		"backwards": r.backwardOrdering,
	}).Infof("Fetched %d events locally", len(streamEvents))

	if len(streamEvents) == 0 && !topologyHadEvents {
		// The topology query itself found no events: we are at a backward extremity or the
		// beginning of the room. Check for backfill opportunities.
		if events, err = r.handleEmptyEventsSlice(); err != nil { //nolint:contextcheck
			return []synctypes.ClientEvent{}, *r.from, emptyToken, err
		}
	} else if len(streamEvents) > 0 {
		if events, err = r.handleNonEmptyEventsSlice(streamEvents); err != nil { //nolint:contextcheck
			return []synctypes.ClientEvent{}, *r.from, emptyToken, err
		}
	}
	// If topologyHadEvents && len(streamEvents) == 0: all events in this page were removed
	// by the sender/type filter. events stays empty. We return the topology end token below
	// so the client can paginate past this range and find matching events further back.

	// If we didn't get any event, we don't need to proceed any further.
	if len(events) == 0 {
		if !topologyHadEvents {
			// True end of history (or backfill returned nothing): signal client to stop.
			return []synctypes.ClientEvent{}, *r.from, emptyToken, nil
		}
		// Filtered-empty page: return the topology end token so the client continues.
		return []synctypes.ClientEvent{}, *r.from, end, nil
	}

	// Apply room history visibility filter
	startTime := time.Now()
	filteredEvents, err := internal.ApplyHistoryVisibilityFilter(r.ctx, r.snapshot, r.rsAPI, events, nil, r.deviceUserID, "messages") //nolint:contextcheck
	if err != nil {
		return []synctypes.ClientEvent{}, *r.from, *r.to, nil
	}
	logrus.WithFields(logrus.Fields{
		"duration":      time.Since(startTime),
		"room_id":       r.roomID,
		"events_before": len(events),
		"events_after":  len(filteredEvents),
	}).Debug("applied history visibility (messages)")

	// No events left after applying history visibility; return the topology end token
	// so the client can continue paginating (same logic as the sender/type filter case above).
	if len(filteredEvents) == 0 {
		return []synctypes.ClientEvent{}, *r.from, end, nil
	}

	// If we backfilled in the process of getting events, we need
	// to re-fetch the start/end positions
	if r.didBackfill {
		_, end, err = r.getStartEnd(filteredEvents)
		if err != nil {
			return []synctypes.ClientEvent{}, *r.from, *r.to, err
		}
	}

	// Sort the events to ensure we send them in the right order.
	if r.backwardOrdering {
		if events[len(events)-1].Type() == spec.MRoomCreate {
			// NOTSPEC: We've hit the beginning of the room so there's really nowhere
			// else to go. This seems to fix Element iOS from looping on /messages endlessly.
			end = types.TopologyToken{}
		}

		// This reverses the array from old->new to new->old
		reversed := func(in []*rstypes.HeaderedEvent) []*rstypes.HeaderedEvent {
			out := make([]*rstypes.HeaderedEvent, len(in))
			for i := 0; i < len(in); i++ {
				out[i] = in[len(in)-i-1]
			}
			return out
		}
		filteredEvents = reversed(filteredEvents)
	}

	start = *r.from

	return synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(filteredEvents), synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	}), start, end, nil
}

func (r *messagesReq) getStartEnd(events []*rstypes.HeaderedEvent) (start, end types.TopologyToken, err error) {
	if r.backwardOrdering {
		start = *r.from
		if events[len(events)-1].Type() == spec.MRoomCreate {
			// NOTSPEC: We've hit the beginning of the room so there's really nowhere
			// else to go. This seems to fix Element iOS from looping on /messages endlessly.
			end = types.TopologyToken{}
		} else {
			end, err = r.snapshot.EventPositionInTopology(
				r.ctx, events[0].EventID(),
			)
			// A stream/topological position is a cursor located between two events.
			// While they are identified in the code by the event on their right (if
			// we consider a left to right chronological order), tokens need to refer
			// to them by the event on their left, therefore we need to decrement the
			// end position we send in the response if we're going backward.
			end.Decrement()
		}
	} else {
		start = *r.from
		end, err = r.snapshot.EventPositionInTopology(
			r.ctx, events[len(events)-1].EventID(),
		)
	}
	if err != nil {
		err = fmt.Errorf("EventPositionInTopology: for end event %s: %w", events[len(events)-1].EventID(), err)
		return
	}
	return
}

// handleEmptyEventsSlice handles the case where the initial request to the
// database returned an empty slice of events. It does so by checking whether
// the set is empty because we've reached a backward extremity, and if that is
// the case, by retrieving as much events as requested by backfilling from
// another homeserver.
// Returns an error if there was an issue talking with the database or
// backfilling.
func (r *messagesReq) handleEmptyEventsSlice() (
	events []*rstypes.HeaderedEvent, err error,
) {
	backwardExtremities, err := r.snapshot.BackwardExtremitiesForRoom(r.ctx, r.roomID)

	// Check if we have backward extremities for this room.
	if len(backwardExtremities) > 0 {
		// If so, retrieve as much events as needed through backfilling.
		events, err = r.backfill(r.roomID, backwardExtremities, r.filter.Limit)
		if err != nil {
			return
		}
		r.didBackfill = true
		// Backfilled events come from federation and are not pre-filtered by the
		// database query; apply the client's sender/type filter here.
		events = filterHeaderedEvents(events, r.filter)
	} else {
		// If not, it means the slice was empty because we reached the room's
		// creation, so return an empty slice.
		events = []*rstypes.HeaderedEvent{}
	}

	return
}

// handleNonEmptyEventsSlice handles the case where the initial request to the
// database returned a non-empty slice of events. It does so by checking whether
// events are missing from the expected result, and retrieve missing events
// through backfilling if needed.
// Returns an error if there was an issue while backfilling.
func (r *messagesReq) handleNonEmptyEventsSlice(streamEvents []types.StreamEvent) (
	events []*rstypes.HeaderedEvent, err error,
) {
	// Check if we have enough events.
	isSetLargeEnough := len(streamEvents) >= r.filter.Limit
	if !isSetLargeEnough {
		// it might be fine we don't have up to 'limit' events, let's find out
		if r.backwardOrdering {
			if r.wasToProvided {
				// The condition in the SQL query is a strict "greater than" so
				// we need to check against to-1.
				streamPos := streamEvents[len(streamEvents)-1].StreamPosition
				isSetLargeEnough = (r.to.PDUPosition-1 == streamPos)
			}
		} else {
			streamPos := streamEvents[0].StreamPosition
			isSetLargeEnough = (r.from.PDUPosition-1 == streamPos)
		}
	}

	// Check if the slice contains a backward extremity.
	backwardExtremities, err := r.snapshot.BackwardExtremitiesForRoom(r.ctx, r.roomID)
	if err != nil {
		return
	}

	// Backfill is needed if we've reached a backward extremity and need more
	// events. It's only needed if the direction is backward.
	if len(backwardExtremities) > 0 && !isSetLargeEnough && r.backwardOrdering {
		var pdus []*rstypes.HeaderedEvent
		// Only ask the remote server for enough events to reach the limit.
		pdus, err = r.backfill(r.roomID, backwardExtremities, r.filter.Limit-len(streamEvents))
		if err != nil {
			return
		}
		r.didBackfill = true
		// Backfilled events come from federation and are not pre-filtered by the
		// database query; apply the client's sender/type filter here.
		pdus = filterHeaderedEvents(pdus, r.filter)
		// Append the PDUs to the list to send back to the client.
		events = append(events, pdus...)
	}

	// Append the events ve previously retrieved locally.
	events = append(events, r.snapshot.StreamEventsToEvents(r.ctx, nil, streamEvents, r.rsAPI)...)
	sort.Sort(eventsByDepth(events))

	return
}

type eventsByDepth []*rstypes.HeaderedEvent

func (e eventsByDepth) Len() int {
	return len(e)
}

func (e eventsByDepth) Swap(i, j int) {
	e[i], e[j] = e[j], e[i]
}

func (e eventsByDepth) Less(i, j int) bool {
	return e[i].Depth() < e[j].Depth()
}

// matchesGlob reports whether name matches the glob pattern, where * matches
// any sequence of characters. This implements Matrix filter wildcards per the
// client-server API spec. Event types contain no '/' so path.Match's path
// separator does not interfere with the match.
func matchesGlob(pattern, name string) bool {
	ok, _ := path.Match(pattern, name)
	return ok
}

// filterHeaderedEvents filters events by the sender and type constraints in
// filter, mirroring the SQL filtering applied to locally stored events.
// Used to apply client-specified filters to backfilled events from federation,
// which bypass the database query.
func filterHeaderedEvents(events []*rstypes.HeaderedEvent, filter *synctypes.RoomEventFilter) []*rstypes.HeaderedEvent {
	if filter == nil {
		return events
	}
	if filter.Senders == nil && filter.NotSenders == nil && filter.Types == nil && filter.NotTypes == nil {
		return events
	}
	filtered := make([]*rstypes.HeaderedEvent, 0, len(events))
	for _, ev := range events {
		sender := ev.UserID.String()

		if filter.Senders != nil {
			found := false
			for _, s := range *filter.Senders {
				if s == sender {
					found = true
					break
				}
			}
			if !found {
				continue
			}
		}

		if filter.NotSenders != nil {
			excluded := false
			for _, s := range *filter.NotSenders {
				if s == sender {
					excluded = true
					break
				}
			}
			if excluded {
				continue
			}
		}

		eventType := ev.Type()

		if filter.Types != nil {
			matched := false
			for _, pattern := range *filter.Types {
				if matchesGlob(pattern, eventType) {
					matched = true
					break
				}
			}
			if !matched {
				continue
			}
		}

		if filter.NotTypes != nil {
			excluded := false
			for _, pattern := range *filter.NotTypes {
				if matchesGlob(pattern, eventType) {
					excluded = true
					break
				}
			}
			if excluded {
				continue
			}
		}

		filtered = append(filtered, ev)
	}
	return filtered
}

// backfill performs a backfill request over the federation on another
// homeserver in the room.
// See: https://matrix.org/docs/spec/server_server/latest#get-matrix-federation-v1-backfill-roomid
// It also stores the PDUs retrieved from the remote homeserver's response to
// the database.
// Returns with an empty string if the remote homeserver didn't return with any
// event, or if there is no remote homeserver to contact.
// Returns an error if there was an issue with retrieving the list of servers in
// the room or sending the request.
func (r *messagesReq) backfill(roomID string, backwardsExtremities map[string][]string, limit int) ([]*rstypes.HeaderedEvent, error) {
	var res api.PerformBackfillResponse
	err := r.rsAPI.PerformBackfill(context.Background(), &api.PerformBackfillRequest{
		RoomID:               roomID,
		BackwardsExtremities: backwardsExtremities,
		Limit:                limit,
		ServerName:           r.cfg.Matrix.ServerName,
		VirtualHost:          r.device.UserDomain(),
	}, &res)
	if err != nil {
		return nil, fmt.Errorf("PerformBackfill failed: %w", err)
	}
	util.GetLogger(r.ctx).WithField("new_events", len(res.Events)).Info("Storing new events from backfill")

	// TODO: we should only be inserting events into the database from the roomserver's kafka output stream.
	// Currently, this can race with live events for the room and cause problems. It's also just a bit unclear
	// when you have multiple entry points to write events.

	// we have to order these by depth, starting with the lowest because otherwise the topology tokens
	// will skip over events that have the same depth but different stream positions due to the query which is:
	//  - anything less than the depth OR
	//  - anything with the same depth and a lower stream position.
	sort.Sort(eventsByDepth(res.Events))

	// Store the events in the database, while marking them as unfit to show
	// up in responses to sync requests.
	if res.HistoryVisibility == "" {
		res.HistoryVisibility = gomatrixserverlib.HistoryVisibilityShared
	}
	events := res.Events
	for i := range events {
		events[i].Visibility = res.HistoryVisibility
		_, err = r.db.WriteEvent(
			context.Background(),
			events[i],
			[]*rstypes.HeaderedEvent{},
			[]string{},
			[]string{},
			nil, true,
			events[i].Visibility,
		)
		if err != nil {
			return nil, err
		}
	}

	// we may have got more than the requested limit so resize now
	if len(events) > limit {
		// last `limit` events
		events = events[len(events)-limit:]
	}

	return events, nil
}
