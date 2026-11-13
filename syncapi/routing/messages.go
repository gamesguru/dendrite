// Copyright 2018-2024 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"path"
	"sort"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
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

// maxBackfillReloads bounds how often one request reads a page again after a
// backfill. Each reread follows a backfill that stored events, so a chain of
// gaps makes progress; the bound only stops a marker that never clears.
const maxBackfillReloads = 4

// maxForwardBackfillRounds bounds how many federation batches one forward
// /messages request asks for while closing a gap ahead of its cursor. Forward
// pagination moves the token past everything it returns, so a gap left partly
// filled below those events could never be walked again: the fill has to close
// the gap before the page is served. A gap still open after the bound is
// reported as an error rather than a short page, so the client retries with the
// same cursor and the next request continues from the events already stored.
const maxForwardBackfillRounds = 10

// forwardBackfillBatch is the number of events requested per federation
// backfill while closing a forward gap. It is independent of the page limit,
// which can be as low as 10 and would need hundreds of round trips. It mirrors
// the roomserver's maxBackfillEvents.
const forwardBackfillBatch = 100

// errForwardBackfillIncomplete is returned when a forward page cannot close the
// gap ahead of its cursor within maxForwardBackfillRounds. The events fetched so
// far are stored, so retrying the same cursor makes progress.
var errForwardBackfillIncomplete = errors.New("forward backfill did not close the history gap within one request")

type messagesReq struct {
	ctx               context.Context
	db                storage.Database
	snapshot          storage.DatabaseTransaction
	rsAPI             api.SyncRoomserverAPI
	cfg               *config.SyncAPI
	roomID            string
	from              *types.TopologyToken
	fromStream        *types.StreamingToken
	to                *types.TopologyToken
	device            *userapi.Device
	deviceUserID      spec.UserID
	wasToProvided     bool
	backwardOrdering  bool
	filter            *synctypes.RoomEventFilter
	didBackfill       bool
	backfillHadEvents bool
	backfillEnd       *types.TopologyToken
	// reloads counts how many times the page was read again after a backfill.
	reloads int
	// forwardBackfillRounds counts the federation batches fetched for this
	// request while closing a gap ahead of a forward cursor.
	forwardBackfillRounds int
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

	// The roomserver reports a join before the sync API has stored it, so a
	// first page requested right after joining would look empty. Wait for it
	// before the request transaction below is opened, so no connection is held
	// while we poll.
	if req.URL.Query().Get("from") == "" && membershipResp.Membership == spec.Join {
		internal.WaitForMembershipInTopology(req.Context(), db, membershipResp.EventID)
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
		fromStream:       fromStream,
		to:               &to,
		wasToProvided:    wasToProvided,
		filter:           filter,
		backwardOrdering: backwardOrdering,
		device:           device,
		deviceUserID:     *deviceUserID,
	}

	clientEvents, start, end, err := mReq.retrieveEvents(req.Context(), rsAPI)
	if errors.Is(err, errForwardBackfillIncomplete) {
		// Progress is stored, so a retry continues where this request stopped.
		util.GetLogger(req.Context()).WithError(err).Warn("mreq.retrieveEvents: history gap not yet closed")
		return util.JSONResponse{
			Code: http.StatusTooManyRequests,
			JSON: spec.LimitExceeded("History is still being fetched, retry shortly", 2000),
		}
	}
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

// extremityBranchEvents returns page events older than extremityDepth which
// are ancestors of the given backwards-extremity event. Walking prev_events
// keeps a depth boundary scoped to the DAG branch that owns that extremity.
//
// A page which contains the extremity itself is left alone. The older events
// on it are newer than the gap unless the gap sits just below the extremity,
// and handleNonEmptyEventsSlice keeps any backfilled gap event that sorts at
// or above the page's end, so the cursor cannot pass one. Holding the page
// back instead truncates it at the extremity, and the empty-page backfill that
// follows moves the cursor past the events below, dropping them from the walk.
func (r *messagesReq) extremityBranchEvents(
	ctx context.Context, extremityID string, page []types.StreamEvent, extremityDepth int64,
) (map[string]struct{}, error) {
	for _, ev := range page {
		if ev.EventID() == extremityID {
			return nil, nil
		}
	}

	candidates := make(map[string]struct{})
	minDepth := extremityDepth
	for _, ev := range page {
		if ev.Depth() < extremityDepth {
			candidates[ev.EventID()] = struct{}{}
			if ev.Depth() < minDepth {
				minDepth = ev.Depth()
			}
		}
	}
	if len(candidates) == 0 {
		return nil, nil
	}

	ancestors := make(map[string]struct{})
	seen := make(map[string]struct{})
	queue := []string{extremityID}
	for len(queue) > 0 {
		batch := queue
		queue = nil
		unseen := batch[:0]
		for _, eventID := range batch {
			if _, ok := seen[eventID]; ok {
				continue
			}
			seen[eventID] = struct{}{}
			unseen = append(unseen, eventID)
		}
		if len(unseen) == 0 {
			continue
		}
		events, err := r.snapshot.Events(ctx, unseen)
		if err != nil {
			return nil, fmt.Errorf("load events for backwards extremity %s: %w", extremityID, err)
		}
		for _, ev := range events {
			if _, ok := candidates[ev.EventID()]; ok {
				ancestors[ev.EventID()] = struct{}{}
			}
			if ev.Depth() > minDepth {
				queue = append(queue, ev.PrevEventIDs()...)
			}
		}
	}
	return ancestors, nil
}

// openBackwardExtremities returns the extremities whose gap is still open: at
// least one prev event they were recorded with is absent from the database.
// Markers are only removed when a backfill covers them, so one can outlive its
// gap. All prev events are looked up in a single query. An extremity recorded
// with no prev events is kept.
func (r *messagesReq) openBackwardExtremities(
	ctx context.Context, backwardExtremities map[string][]string,
) (map[string]struct{}, error) {
	var all []string
	for _, prevIDs := range backwardExtremities {
		all = append(all, prevIDs...)
	}
	have := make(map[string]struct{}, len(all))
	if len(all) > 0 {
		stored, err := r.snapshot.Events(ctx, util.UniqueStrings(all))
		if err != nil {
			return nil, fmt.Errorf("load prev events of the backwards extremities: %w", err)
		}
		for _, ev := range stored {
			have[ev.EventID()] = struct{}{}
		}
	}
	open := make(map[string]struct{}, len(backwardExtremities))
	for extremityID, prevIDs := range backwardExtremities {
		if len(prevIDs) == 0 {
			open[extremityID] = struct{}{}
			continue
		}
		for _, id := range prevIDs {
			if _, ok := have[id]; !ok {
				open[extremityID] = struct{}{}
				break
			}
		}
	}
	return open, nil
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
	var streamEvents []types.StreamEvent
	var topologyStart types.TopologyToken
	if r.fromStream != nil && r.backwardOrdering {
		// A stream token bounds the page by stream order alone, as in Synapse:
		// events persisted before it can be deeper in the DAG than the event at
		// the token, for instance after a network partition heals.
		streamEvents, topologyStart, end, err = r.snapshot.GetEventsBeforeStreamPosition(r.ctx, r.fromStream.PDUPosition, r.to, r.roomID, r.filter, true) //nolint:contextcheck
	} else {
		streamEvents, topologyStart, end, err = r.snapshot.GetEventsInTopologicalRange(r.ctx, r.from, r.to, r.roomID, r.filter, r.backwardOrdering) //nolint:contextcheck
	}
	if err != nil {
		err = fmt.Errorf("GetEventsInRange: %w", err)
		return []synctypes.ClientEvent{}, *r.from, emptyToken, err
	}
	topologyHadEvents := len(streamEvents) > 0
	// A stream token is a position in stream order, not topology order. Events
	// can be persisted in a different order than their DAG depth (for example
	// room creation state events that arrive after the client's sync token), so
	// constrain the page by the token's stream position as well.
	if r.fromStream != nil {
		filtered := streamEvents[:0]
		for _, ev := range streamEvents {
			if r.backwardOrdering {
				// Backfilled events are stored after the token was issued, so their
				// stream positions are above it, but they belong to older history
				// (no deeper than the token's own position in the topology).
				if ev.StreamPosition <= r.fromStream.PDUPosition ||
					(ev.ExcludeFromSync && (r.from == nil || r.from.Depth == 0 || ev.Depth() <= int64(r.from.Depth))) {
					filtered = append(filtered, ev)
				}
			} else if ev.StreamPosition > r.fromStream.PDUPosition {
				filtered = append(filtered, ev)
			}
		}
		streamEvents = filtered
	}
	// A forward page can only backfill the gap that sits immediately ahead of
	// its cursor. Forward pagination moves the token past every event it
	// returns, so a gap left partly filled below them would be skipped for good;
	// the fill therefore closes the gap before the page is served, and the page
	// is read again to include the new events. Backfill is skipped when the
	// client supplied a `to` bound, and for stream tokens, whose position is in
	// stream order rather than topology order.
	if !r.backwardOrdering && r.fromStream == nil && !r.wasToProvided && len(streamEvents) > 0 {
		filled, fErr := r.fillForwardGap(ctx, streamEvents[0].Depth())
		if fErr != nil {
			return []synctypes.ClientEvent{}, *r.from, emptyToken, fErr
		}
		if filled {
			// The gap is closed; read the page again so the events just stored
			// are returned in order instead of being skipped over.
			return r.retrieveEvents(ctx, rsAPI)
		}
		// A gap can also sit inside the page. Serve only the events before it;
		// the next request then starts at the extremity and fills it above.
		streamEvents, end, err = r.truncateForwardPageAtGap(ctx, streamEvents, end)
		if err != nil {
			return []synctypes.ClientEvent{}, *r.from, emptyToken, err
		}
	}
	decrementEnd := r.backwardOrdering
	extremityFilteredEmpty := false
	// withheld holds the page set aside because its first event sits on an
	// extremity branch, so it can still be returned if backfill finds nothing.
	var withheld []types.StreamEvent
	if r.backwardOrdering && len(streamEvents) > 0 {
		if backwardExtremities, bErr := r.snapshot.BackwardExtremitiesForRoom(ctx, r.roomID); bErr == nil && len(backwardExtremities) > 0 {
			excluded := make(map[string]struct{})
			openExtremities, openErr := r.openBackwardExtremities(ctx, backwardExtremities)
			if openErr != nil {
				return nil, *r.from, emptyToken, openErr
			}
			for extremityID := range backwardExtremities {
				if _, open := openExtremities[extremityID]; !open {
					// Every prev event this marker was waiting for is stored now
					// (for instance fetched with /event), so no gap is left here.
					// Holding back the events below it would skip them.
					continue
				}
				tok, err := r.snapshot.EventPositionInTopology(ctx, extremityID)
				if err != nil {
					continue
				}
				extremityDepth := int64(tok.Depth)
				if int64(r.from.Depth) >= extremityDepth {
					branchEvents, branchErr := r.extremityBranchEvents(ctx, extremityID, streamEvents, extremityDepth)
					if branchErr != nil {
						return nil, *r.from, emptyToken, branchErr
					}
					for eventID := range branchEvents {
						excluded[eventID] = struct{}{}
					}
				}
			}
			if len(excluded) > 0 {
				// A topology token is a single linear cursor, so stop at the first
				// excluded event. Advancing farther would skip that event on the
				// next request, including when another branch has older events.
				filtered := streamEvents[:0]
				for _, ev := range streamEvents {
					if _, shouldExclude := excluded[ev.EventID()]; shouldExclude {
						if len(filtered) > 0 {
							end, err = r.snapshot.EventPositionInTopology(ctx, filtered[len(filtered)-1].EventID())
							if err != nil {
								return nil, *r.from, emptyToken, err
							}
						} else {
							// Keep the page so it can be returned if backfill has
							// nothing to fill the extremity with.
							withheld = append([]types.StreamEvent(nil), streamEvents...)
							// Leave this event at the cursor so a subsequent request can
							// retry it after the extremity has been backfilled.
							end, err = r.snapshot.EventPositionInTopology(ctx, ev.EventID())
							if err != nil {
								return nil, *r.from, emptyToken, err
							}
							decrementEnd = false
							extremityFilteredEmpty = true
						}
						break
					}
					filtered = append(filtered, ev)
				}
				streamEvents = filtered
			}
		}
	}
	if decrementEnd {
		// A forward token already sits at the last event returned, and the
		// forward range excludes the events up to its stream position.
		end.Decrement()
	}

	// topologyHadEvents distinguishes "no events in topology range" (true end of history
	// or backward extremity) from "events existed but the filter removed all of them".
	topologyHadEvents = topologyHadEvents && topologyStart != emptyToken && !extremityFilteredEmpty

	var events []*rstypes.HeaderedEvent
	util.GetLogger(r.ctx).WithFields(logrus.Fields{ //nolint:contextcheck
		"start":     r.from,
		"end":       r.to,
		"backwards": r.backwardOrdering,
	}).Infof("Fetched %d events locally", len(streamEvents))

	if len(streamEvents) == 0 && !topologyHadEvents {
		// The topology query itself found no events: we are at a backward extremity or the
		// beginning of the room. Check for backfill opportunities.
		if events, err = r.handleEmptyEventsSlice(); err != nil {
			return []synctypes.ClientEvent{}, *r.from, emptyToken, err
		}
		if extremityFilteredEmpty && r.backfillHadEvents && r.reloads < maxBackfillReloads {
			// The page was set aside because an extremity sat above older stored
			// events. This covers the case where the page lies entirely below the
			// extremity; a page that contains the extremity is left intact by
			// extremityBranchEvents instead. The backfill has filled the gap and
			// cleared the marker, so read the page again from the same cursor.
			// Returning only the
			// backfilled events would move the cursor below the stored events
			// that were set aside and skip them.
			r.reloads++
			// The second read decides for itself whether it backfilled anything.
			r.backfillHadEvents = false
			r.backfillEnd = nil
			return r.retrieveEvents(ctx, rsAPI)
		}
		if extremityFilteredEmpty && len(events) == 0 && !r.backfillHadEvents && len(withheld) > 0 {
			// Backfill found nothing to fill the extremity, so the withheld events
			// are all there is: return them rather than ending pagination here.
			sort.Slice(withheld, func(i, j int) bool {
				if withheld[i].Depth() == withheld[j].Depth() {
					return withheld[i].StreamPosition < withheld[j].StreamPosition
				}
				return withheld[i].Depth() < withheld[j].Depth()
			})
			events = r.snapshot.StreamEventsToEvents(r.ctx, nil, withheld, r.rsAPI) //nolint:contextcheck
			topologyHadEvents = true
		}
	} else if len(streamEvents) > 0 {
		if events, err = r.handleNonEmptyEventsSlice(streamEvents); err != nil {
			return []synctypes.ClientEvent{}, *r.from, emptyToken, err
		}
	}
	// If topologyHadEvents && len(streamEvents) == 0: all events in this page were removed
	// by the sender/type filter. events stays empty. We return the topology end token below
	// so the client can paginate past this range and find matching events further back.

	// If we didn't get any event, we don't need to proceed any further.
	if len(events) == 0 {
		if r.backfillHadEvents && r.backfillEnd != nil {
			return []synctypes.ClientEvent{}, *r.from, *r.backfillEnd, nil
		}
		if !topologyHadEvents {
			// True end of history (or backfill returned nothing): signal client to stop.
			return []synctypes.ClientEvent{}, *r.from, emptyToken, nil
		}
		// Filtered-empty page: return the topology end token so the client continues.
		return []synctypes.ClientEvent{}, *r.from, end, nil
	}
	if len(r.filter.RelationTypes) > 0 || len(r.filter.NotRelationTypes) > 0 {
		events = filterHeaderedEvents(events, r.filter)
		if len(events) == 0 {
			if r.backfillHadEvents && r.backfillEnd != nil {
				return []synctypes.ClientEvent{}, *r.from, *r.backfillEnd, nil
			}
			return []synctypes.ClientEvent{}, *r.from, end, nil
		}
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
		end, err = r.getEnd(filteredEvents)
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

	clientEvents = synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(filteredEvents), synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})
	bundler, err := internal.NewRelationBundler(ctx, r.snapshot, rsAPI, r.deviceUserID)
	if err != nil {
		return []synctypes.ClientEvent{}, *r.from, *r.to, err
	}
	if err = bundler.Bundle(ctx, filteredEvents, clientEvents, synctypes.FormatAll); err != nil {
		return []synctypes.ClientEvent{}, *r.from, *r.to, err
	}
	return clientEvents, start, end, nil
}

func (r *messagesReq) getEnd(events []*rstypes.HeaderedEvent) (end types.TopologyToken, err error) {
	if r.backwardOrdering {
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

// reachableBackwardExtremities keeps the extremities whose gap can hold events
// older than the request. Backfilling from an extremity deeper than the
// requested position fills a different part of the history and returns events
// newer than the client asked from, which the client then places on the wrong
// side of its timeline. A cursor at depth 0 sits before every stored event, so
// every gap is older and none is dropped. An extremity whose position cannot be
// read is kept.
func (r *messagesReq) reachableBackwardExtremities(
	backwardExtremities map[string][]string,
) map[string][]string {
	reachable := backwardExtremities
	// Depth 0 is a cursor before every stored event (for instance a prev_batch
	// of a room joined over federation): all history is older than it.
	if r.backwardOrdering && r.from != nil && r.from.Depth != 0 {
		reachable = make(map[string][]string, len(backwardExtremities))
		for extremityID, prevs := range backwardExtremities {
			pos, err := r.snapshot.EventPositionInTopology(r.ctx, extremityID)
			if err != nil || pos.Depth <= r.from.Depth {
				reachable[extremityID] = prevs
			}
		}
	}
	if !r.backwardOrdering {
		return reachable
	}
	return deepestExtremities(r.ctx, r.snapshot, reachable)
}

// deepestExtremities keeps only the extremities at the greatest known depth.
// A room can hold many open gaps; walking backwards must fill the one nearest
// the cursor first. Backfilling from an older gap returns a block of old
// history and leaves the newer hole between it and the cursor, which the walk
// then never reaches. Extremities whose depth is unknown are kept only when no
// depth is known at all.
func deepestExtremities(
	ctx context.Context, snapshot storage.DatabaseTransaction, extremities map[string][]string,
) map[string][]string {
	if len(extremities) < 2 {
		return extremities
	}
	best := types.StreamPosition(0)
	depths := make(map[string]types.StreamPosition, len(extremities))
	for extremityID := range extremities {
		pos, err := snapshot.EventPositionInTopology(ctx, extremityID)
		if err != nil {
			continue
		}
		depths[extremityID] = pos.Depth
		if pos.Depth > best {
			best = pos.Depth
		}
	}
	if len(depths) == 0 {
		return extremities
	}
	deepest := make(map[string][]string)
	for extremityID, depth := range depths {
		if depth == best {
			deepest[extremityID] = extremities[extremityID]
		}
	}
	return deepest
}

// forwardGapExtremity returns the open backward extremity that marks the gap
// immediately ahead of a forward cursor, together with its recorded prev
// events. A stored event whose prev event is missing always gets a backwards
// extremity marker when it is written, so the first event after a hole is the
// marker for that hole.
//
// Only extremities between the cursor and the page's first event are eligible:
// the gap they mark is exactly the one the page would jump over. An extremity
// deeper in the room belongs to a gap the walk has not reached yet, and filling
// it here would store events newer than the page's own range. Extremities at or
// behind the cursor are history the walk already passed.
func (r *messagesReq) forwardGapExtremity(ctx context.Context, firstEventDepth int64) (string, []string, error) {
	backwardExtremities, err := r.snapshot.BackwardExtremitiesForRoom(ctx, r.roomID)
	if err != nil || len(backwardExtremities) == 0 {
		return "", nil, err
	}
	open, err := r.openBackwardExtremities(ctx, backwardExtremities)
	if err != nil {
		return "", nil, err
	}
	bestID := ""
	bestDepth := int64(math.MaxInt64)
	for extremityID := range open {
		pos, posErr := r.snapshot.EventPositionInTopology(ctx, extremityID)
		if posErr != nil {
			continue
		}
		depth := int64(pos.Depth)
		if r.from != nil && depth <= int64(r.from.Depth) {
			continue
		}
		if depth > firstEventDepth {
			continue
		}
		if depth < bestDepth {
			bestID, bestDepth = extremityID, depth
		}
	}
	if bestID == "" {
		return "", nil, nil
	}
	return bestID, backwardExtremities[bestID], nil
}

// truncateForwardPageAtGap cuts a forward page before the first event, after
// the page's first, that is an open backward extremity (its prev events are
// missing, so a hole lies below it). The returned end token sits at the last
// event kept, so forward pagination resumes at the extremity. The first event
// is never cut, so every page makes progress.
func (r *messagesReq) truncateForwardPageAtGap(
	ctx context.Context, events []types.StreamEvent, end types.TopologyToken,
) ([]types.StreamEvent, types.TopologyToken, error) {
	if len(events) < 2 {
		return events, end, nil
	}
	backwardExtremities, err := r.snapshot.BackwardExtremitiesForRoom(ctx, r.roomID)
	if err != nil || len(backwardExtremities) == 0 {
		return events, end, err
	}
	open, err := r.openBackwardExtremities(ctx, backwardExtremities)
	if err != nil || len(open) == 0 {
		return events, end, err
	}
	for i := 1; i < len(events); i++ {
		if _, isOpen := open[events[i].EventID()]; !isOpen {
			continue
		}
		newEnd, posErr := r.snapshot.EventPositionInTopology(ctx, events[i-1].EventID())
		if posErr != nil {
			return events, end, posErr
		}
		return events[:i], newEnd, nil
	}
	return events, end, nil
}

// fillForwardGap closes the history gap between a forward cursor and the first
// event the page would return, fetching from federation in batches. It reports
// whether it stored any event, so the caller knows to read the page again.
//
// Each batch stores up to forwardBackfillBatch events and moves the extremity
// marker down the chain towards the cursor. Filling stops once no open
// extremity is left ahead of the cursor, or when the remote servers have no
// more events (the page is then served from what is stored).
func (r *messagesReq) fillForwardGap(ctx context.Context, firstEventDepth int64) (bool, error) {
	filled := false
	for r.forwardBackfillRounds < maxForwardBackfillRounds {
		extremityID, prevs, err := r.forwardGapExtremity(ctx, firstEventDepth)
		if err != nil {
			return filled, err
		}
		if extremityID == "" {
			return filled, nil
		}
		r.forwardBackfillRounds++
		pdus, bfErr := r.backfill(r.roomID, map[string][]string{extremityID: prevs}, forwardBackfillBatch)
		if bfErr != nil {
			if ctx.Err() != nil {
				return filled, bfErr
			}
			if isTerminalBackfillFailure(bfErr) {
				// No server has these events: the gap is as filled as it can
				// get and the rest of the history is genuinely unreachable.
				util.GetLogger(ctx).WithError(bfErr).Debug("Forward backfill found no events on the attempted servers")
				return filled, nil
			}
			// A transient failure must not look like the end of history, and it
			// must not let the cursor move past the unfilled gap either.
			util.GetLogger(ctx).WithError(bfErr).Warn("Forward backfill failed; returning an error so the client can retry")
			return filled, bfErr
		}
		if len(pdus) == 0 {
			return filled, nil
		}
		filled = true
	}

	// The last batch may have closed the gap exactly at the bound, so only
	// report failure when an open gap is still ahead of the cursor.
	extremityID, _, err := r.forwardGapExtremity(ctx, firstEventDepth)
	if err != nil {
		return filled, err
	}
	if extremityID == "" {
		return filled, nil
	}
	return filled, errForwardBackfillIncomplete
}

// handleEmptyEventsSlice handles the case where the initial request to the
// database returned an empty slice of events. It does so by checking whether
// the set is empty because we've reached a backward extremity, and if that is
// the case, by retrieving as much events as requested by backfilling from
// another homeserver.
// Returns an error if there was an issue talking with the database or a
// transient failure prevented the backfill. A definitive "no server has these
// events" answer is not an error: it is the end of the history this server can
// reach and is returned as an empty page.
func (r *messagesReq) handleEmptyEventsSlice() (
	events []*rstypes.HeaderedEvent, err error,
) {
	backwardExtremities, err := r.snapshot.BackwardExtremitiesForRoom(r.ctx, r.roomID)
	if err == nil {
		backwardExtremities = r.reachableBackwardExtremities(backwardExtremities)
	}

	// Check if we have backward extremities for this room.
	if len(backwardExtremities) > 0 {
		// If so, retrieve as much events as needed through backfilling.
		events, err = r.backfill(r.roomID, backwardExtremities, r.filter.Limit)
		if err != nil {
			if r.ctx.Err() != nil {
				return
			}
			if isTerminalBackfillFailure(err) {
				// Every server answered that it has no earlier events, so the
				// client has reached the end of the history this server can
				// reach. There is nothing to retry.
				util.GetLogger(r.ctx).WithError(err).Debug("Backfill found no events on the attempted servers")
				return []*rstypes.HeaderedEvent{}, nil
			}
			// A transient failure must not look like the end of history: the
			// client would stop paginating and never retry the extremity.
			util.GetLogger(r.ctx).WithError(err).Warn("Backfill failed; returning an error so the client can retry")
			return nil, err
		}
		r.didBackfill = true
		r.backfillHadEvents = len(events) > 0
		if r.backfillHadEvents {
			backfillEnd, positionErr := r.getEnd(events)
			if positionErr != nil {
				return nil, positionErr
			}
			r.backfillEnd = &backfillEnd
		}
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

// isTerminalBackfillFailure reports whether attempted backfill servers
// definitively lacked the requested events, rather than a transient failure.
func isTerminalBackfillFailure(err error) bool {
	var unavailable *api.BackfillUnavailableError
	return errors.As(err, &unavailable) && !unavailable.Retryable
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
	backwardExtremities = r.reachableBackwardExtremities(backwardExtremities)

	// A full page can still end at a backward extremity: backfill stores at most
	// maxBackfillEvents per request, so the oldest events of a long gap may not
	// have been fetched yet. Fetch them now so the next page does not skip them.
	// Events backfilled at or above this page's oldest event are newer than the
	// token it returns, so a later page would start below them and never return
	// them; keep those on this page.
	var backfilledAbovePageEnd []*rstypes.HeaderedEvent
	if len(backwardExtremities) > 0 && isSetLargeEnough && r.backwardOrdering {
		for i := range streamEvents {
			if _, ok := backwardExtremities[streamEvents[i].EventID()]; !ok {
				continue
			}
			pdus, bfErr := r.backfill(r.roomID, backwardExtremities, r.filter.Limit)
			if bfErr != nil {
				if r.ctx.Err() != nil {
					return nil, bfErr
				}
				util.GetLogger(r.ctx).WithError(bfErr).Warn("Backfill at backward extremity failed")
			} else {
				// Backfilled events come from federation and are not pre-filtered
				// by the database query, so apply the client's filter here too.
				pdus = filterHeaderedEvents(pdus, r.filter)
				pageEndDepth := streamEvents[0].Depth()
				for _, ev := range streamEvents[1:] {
					if ev.Depth() < pageEndDepth {
						pageEndDepth = ev.Depth()
					}
				}
				onPage := make(map[string]struct{}, len(streamEvents))
				for _, ev := range streamEvents {
					onPage[ev.EventID()] = struct{}{}
				}
				// The page can exceed the limit by the gap events kept here. A
				// client tolerates a long page, and dropping them would skip them.
				for _, ev := range pdus {
					if ev.Depth() < pageEndDepth {
						continue
					}
					if _, duplicate := onPage[ev.EventID()]; duplicate {
						continue
					}
					onPage[ev.EventID()] = struct{}{}
					backfilledAbovePageEnd = append(backfilledAbovePageEnd, ev)
				}
				if len(backfilledAbovePageEnd) > 0 {
					r.didBackfill = true
				}
			}
			break
		}
	}

	// Backfill is needed if we've reached a backward extremity and need more
	// events. It's only needed if the direction is backward.
	if len(backwardExtremities) > 0 && !isSetLargeEnough && r.backwardOrdering {
		var pdus []*rstypes.HeaderedEvent
		// Only ask the remote server for enough events to reach the limit.
		pdus, err = r.backfill(r.roomID, backwardExtremities, r.filter.Limit-len(streamEvents))
		switch {
		case err != nil && r.ctx.Err() != nil:
			return
		case err != nil:
			// Backfill is best effort: fall back to the events we have locally.
			util.GetLogger(r.ctx).WithError(err).Warn("Backfill failed, returning only local events")
			err = nil
		default:
			r.didBackfill = true
			if len(pdus) > 0 {
				sort.Sort(eventsByDepth(pdus))
				r.backfillHadEvents = true
				backfillEnd, positionErr := r.getEnd(pdus)
				if positionErr != nil {
					return nil, positionErr
				}
				r.backfillEnd = &backfillEnd
			}
			// Backfilled events come from federation and are not pre-filtered by the
			// database query; apply the client's sender/type filter here.
			pdus = filterHeaderedEvents(pdus, r.filter)
			// Append the PDUs to the list to send back to the client.
			events = append(events, pdus...)
		}
	}

	// Append the events ve previously retrieved locally.
	localEvents := r.snapshot.StreamEventsToEvents(r.ctx, nil, streamEvents, r.rsAPI)
	if r.backwardOrdering && len(events) > 0 {
		// Backfill returns only the newest events of the gap. Local events older than
		// those sit below the gap (e.g. from an earlier stay in the room); returning
		// them would put the end token past the gap and skip the stored events in
		// between, so leave them for a later page.
		oldest := events[0].Depth()
		kept := localEvents[:0]
		for _, ev := range localEvents {
			if ev.Depth() >= oldest {
				kept = append(kept, ev)
			}
		}
		localEvents = kept
	}
	events = append(events, localEvents...)
	events = append(events, backfilledAbovePageEnd...)
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
	if filter.Senders == nil && filter.NotSenders == nil && filter.Types == nil && filter.NotTypes == nil && len(filter.RelationTypes) == 0 && len(filter.NotRelationTypes) == 0 {
		return events
	}
	filtered := make([]*rstypes.HeaderedEvent, 0, len(events))
	for _, ev := range events {
		if len(filter.RelationTypes) > 0 || len(filter.NotRelationTypes) > 0 {
			var content struct {
				RelatesTo struct {
					RelType string `json:"rel_type"`
				} `json:"m.relates_to"`
			}
			err := json.Unmarshal(ev.Content(), &content)
			if err != nil && len(filter.RelationTypes) > 0 {
				continue
			}
			relationType := content.RelatesTo.RelType
			if len(filter.RelationTypes) > 0 {
				matched := false
				for _, allowed := range filter.RelationTypes {
					if relationType == allowed {
						matched = true
						break
					}
				}
				if !matched {
					continue
				}
			}
			excluded := false
			for _, excludedType := range filter.NotRelationTypes {
				if relationType == excludedType {
					excluded = true
					break
				}
			}
			if excluded {
				continue
			}
		}
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
	return backfillEvents(r.ctx, r.rsAPI, r.db, r.cfg.Matrix.ServerName, r.device.UserDomain(), roomID, backwardsExtremities, limit)
}

// backfillEvents asks the roomserver to backfill events over federation and
// stores them as backfilled events, which keeps them out of sync responses.
// It continues independently of ctx cancellation and deadlines. The result contains
// At most limit events are returned in ascending depth order after storing the full batch. The
// limit must be nonnegative. Backfill and event-write errors propagate; backward-extremity
// cleanup failures do not fail the request. Earlier writes may remain when a
// later write fails.
func backfillEvents(
	ctx context.Context, rsAPI api.SyncRoomserverAPI, db storage.Database,
	serverName, virtualHost spec.ServerName,
	roomID string, backwardsExtremities map[string][]string, limit int,
) ([]*rstypes.HeaderedEvent, error) {
	// Finish the backfill even if the client goes away, so the events are stored.
	ctx = context.WithoutCancel(ctx)

	// Backward-extremity rows persist failed cleanup work: if a previous lookup
	// or purge failed, retry it on the next /messages backfill attempt.
	var pendingPrevIDs []string
	for _, prevIDs := range backwardsExtremities {
		pendingPrevIDs = append(pendingPrevIDs, prevIDs...)
	}
	retryBackwardExtremityCleanup(ctx, db, roomID, pendingPrevIDs)

	var res api.PerformBackfillResponse
	err := rsAPI.PerformBackfill(ctx, &api.PerformBackfillRequest{
		RoomID:               roomID,
		BackwardsExtremities: backwardsExtremities,
		Limit:                limit,
		ServerName:           serverName,
		VirtualHost:          virtualHost,
	}, &res)
	if err != nil {
		return nil, fmt.Errorf("PerformBackfill failed: %w", err)
	}
	util.GetLogger(ctx).WithField("new_events", len(res.Events)).Info("Storing new events from backfill")

	// TODO: we should only be inserting events into the database from the roomserver's JetStream output stream.
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
		// The roomserver reports the visibility in force before each event.
		// Fall back to the batch value when it could not be determined.
		if events[i].Visibility == "" {
			events[i].Visibility = res.HistoryVisibility
		}
		_, err = db.WriteEvent(
			ctx,
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

	// WriteEvent adds a backward extremity for every prev event it could not
	// find, but it cannot remove extremities belonging to other events which
	// cited those prev events as missing. Now that the whole batch is stored,
	// those extremities are stale: the /messages branch filter would hide the
	// events we just backfilled. Drop markers for every prev event we now know.
	prevEventIDs := make(map[string]struct{})
	for _, ev := range events {
		for _, prevID := range ev.PrevEventIDs() {
			prevEventIDs[prevID] = struct{}{}
		}
	}
	if len(prevEventIDs) > 0 {
		ids := make([]string, 0, len(prevEventIDs))
		for eventID := range prevEventIDs {
			ids = append(ids, eventID)
		}
		retryBackwardExtremityCleanup(ctx, db, roomID, ids)
	}

	// we may have got more than the requested limit so resize now
	if len(events) > limit {
		// last `limit` events
		events = events[len(events)-limit:]
	}

	return events, nil
}

// retryBackwardExtremityCleanup removes markers for candidate events that are
// now stored, in an independent write transaction. Empty candidates do nothing. Lookup or purge
// failures are not returned; the markers themselves persist failed work for a
// later backfill.
func retryBackwardExtremityCleanup(ctx context.Context, db storage.Database, roomID string, candidateIDs []string) {
	if len(candidateIDs) == 0 {
		return
	}
	candidateIDs = util.UniqueStrings(candidateIDs)
	// Read on a fresh view so events just written by the backfill are visible.
	stored, err := db.Events(ctx, candidateIDs)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Warn("Failed to look up events for backward-extremity cleanup")
		return
	}
	storedIDs := make([]string, 0, len(stored))
	for _, ev := range stored {
		storedIDs = append(storedIDs, ev.EventID())
	}
	if len(storedIDs) == 0 {
		return
	}
	err = db.PurgeBackwardExtremitiesForEvents(ctx, roomID, storedIDs)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Warn("Failed to purge stale backward extremities")
	}
}
