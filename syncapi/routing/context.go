// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/dendrite/internal/caching"
	"codefloe.com/pat-s/dendrite/internal/sqlutil"
	roomserver "codefloe.com/pat-s/dendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/dendrite/roomserver/types"
	"codefloe.com/pat-s/dendrite/syncapi/internal"
	"codefloe.com/pat-s/dendrite/syncapi/storage"
	"codefloe.com/pat-s/dendrite/syncapi/synctypes"
	"codefloe.com/pat-s/dendrite/syncapi/types"
	userapi "codefloe.com/pat-s/dendrite/userapi/api"
)

type ContextRespsonse struct {
	End          string                  `json:"end"`
	Event        *synctypes.ClientEvent  `json:"event,omitempty"`
	EventsAfter  []synctypes.ClientEvent `json:"events_after,omitempty"`
	EventsBefore []synctypes.ClientEvent `json:"events_before,omitempty"`
	Start        string                  `json:"start"`
	State        []synctypes.ClientEvent `json:"state,omitempty"`
}

func Context(
	req *http.Request, device *userapi.Device,
	rsAPI roomserver.SyncRoomserverAPI,
	syncDB storage.Database,
	roomID, eventID string,
	lazyLoadCache caching.LazyLoadCache,
) util.JSONResponse {
	snapshot, err := syncDB.NewDatabaseSnapshot(req.Context())
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
		var errMsg string
		var invalidUnmarshalErr *json.InvalidUnmarshalError
		var numErr *strconv.NumError
		switch {
		case errors.As(err, &invalidUnmarshalErr):
			errMsg = "unable to parse filter"
		case errors.As(err, &numErr):
			errMsg = "unable to parse limit"
		default:
			errMsg = err.Error()
		}
		return util.JSONResponse{
			Code:    http.StatusBadRequest,
			JSON:    spec.InvalidParam(errMsg),
			Headers: nil,
		}
	}
	if filter.Rooms != nil {
		*filter.Rooms = append(*filter.Rooms, roomID)
	}

	userID, err := spec.NewUserID(device.UserID, true)
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("Device UserID is invalid"),
		}
	}
	ctx := req.Context()
	membershipRes := roomserver.QueryMembershipForUserResponse{}
	membershipReq := roomserver.QueryMembershipForUserRequest{UserID: *userID, RoomID: roomID}
	if err = rsAPI.QueryMembershipForUser(ctx, &membershipReq, &membershipRes); err != nil {
		logrus.WithError(err).Error("unable to query membership")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if !membershipRes.RoomExists {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("room does not exist"),
		}
	}

	stateFilter := synctypes.StateFilter{
		Limit:                   filter.Limit,
		NotSenders:              filter.NotSenders,
		NotTypes:                filter.NotTypes,
		Senders:                 filter.Senders,
		Types:                   filter.Types,
		LazyLoadMembers:         filter.LazyLoadMembers,
		IncludeRedundantMembers: filter.IncludeRedundantMembers,
		NotRooms:                filter.NotRooms,
		Rooms:                   filter.Rooms,
		ContainsURL:             filter.ContainsURL,
	}

	id, requestedEvent, err := snapshot.SelectContextEvent(ctx, roomID, eventID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return util.JSONResponse{
				Code: http.StatusNotFound,
				JSON: spec.NotFound(fmt.Sprintf("Event %s not found", eventID)),
			}
		}
		logrus.WithError(err).WithField("eventID", eventID).Error("unable to find requested event")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// verify the user is allowed to see the context for this room/event
	startTime := time.Now()
	filteredEvents, err := internal.ApplyHistoryVisibilityFilter(ctx, snapshot, rsAPI, []*rstypes.HeaderedEvent{&requestedEvent}, nil, *userID, "context")
	if err != nil {
		logrus.WithError(err).Error("unable to apply history visibility filter")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	logrus.WithFields(logrus.Fields{
		"duration": time.Since(startTime),
		"room_id":  roomID,
	}).Debug("applied history visibility (context)")
	if len(filteredEvents) == 0 {
		return util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("User is not allowed to query context"),
		}
	}

	// Limit is split up for before/after events
	if filter.Limit > 1 {
		filter.Limit /= 2
	}

	eventsBefore, err := snapshot.SelectContextBeforeEvent(ctx, id, roomID, filter)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		logrus.WithError(err).Error("unable to fetch before events")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	_, eventsAfter, err := snapshot.SelectContextAfterEvent(ctx, id, roomID, filter)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		logrus.WithError(err).Error("unable to fetch after events")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	startTime = time.Now()
	eventsBeforeFiltered, eventsAfterFiltered, err := applyHistoryVisibilityOnContextEvents(ctx, snapshot, rsAPI, eventsBefore, eventsAfter, *userID)
	if err != nil {
		logrus.WithError(err).Error("unable to apply history visibility filter")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	logrus.WithFields(logrus.Fields{
		"duration": time.Since(startTime),
		"room_id":  roomID,
	}).Debug("applied history visibility (context eventsBefore/eventsAfter)")

	// TODO: Get the actual state at the last event returned by SelectContextAfterEvent
	state, err := snapshot.CurrentState(ctx, roomID, &stateFilter, nil)
	if err != nil {
		logrus.WithError(err).Error("unable to fetch current room state")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	eventsBeforeClient := synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(eventsBeforeFiltered), synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})
	eventsAfterClient := synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(eventsAfterFiltered), synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})

	newState := state
	if filter.LazyLoadMembers {
		allEvents := make([]*rstypes.HeaderedEvent, 0, len(eventsBeforeFiltered)+len(eventsAfterFiltered)+1)
		allEvents = append(allEvents, eventsBeforeFiltered...)
		allEvents = append(allEvents, eventsAfterFiltered...)
		allEvents = append(allEvents, &requestedEvent)
		evs := synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(allEvents), synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})
		newState, err = applyLazyLoadMembers(ctx, device, snapshot, roomID, evs, lazyLoadCache)
		if err != nil {
			logrus.WithError(err).Error("unable to load membership events")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
	}

	ev := synctypes.ToClientEventDefault(func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	}, requestedEvent)
	response := ContextRespsonse{
		Event:        &ev,
		EventsAfter:  eventsAfterClient,
		EventsBefore: eventsBeforeClient,
		State: synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(newState), synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		}),
	}

	if len(response.State) > filter.Limit {
		response.State = response.State[len(response.State)-filter.Limit:]
	}
	start, end, err := getStartEnd(ctx, snapshot, eventsBefore, eventsAfter)
	if err == nil {
		response.End = end.String()
		response.Start = start.String()
	}
	succeeded = true
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: response,
	}
}

// applyHistoryVisibilityOnContextEvents is a helper function to avoid roundtrips to the roomserver
// by combining the events before and after the context event. Returns the filtered events,
// and an error, if any.
func applyHistoryVisibilityOnContextEvents(
	ctx context.Context, snapshot storage.DatabaseTransaction, rsAPI roomserver.SyncRoomserverAPI,
	eventsBefore, eventsAfter []*rstypes.HeaderedEvent,
	userID spec.UserID,
) (filteredBefore, filteredAfter []*rstypes.HeaderedEvent, err error) {
	eventIDsBefore := make(map[string]struct{}, len(eventsBefore))
	eventIDsAfter := make(map[string]struct{}, len(eventsAfter))

	// Remember before/after eventIDs, so we can restore them
	// after applying history visibility checks
	for _, ev := range eventsBefore {
		eventIDsBefore[ev.EventID()] = struct{}{}
	}
	for _, ev := range eventsAfter {
		eventIDsAfter[ev.EventID()] = struct{}{}
	}

	allEvents := make([]*rstypes.HeaderedEvent, 0, len(eventsBefore)+len(eventsAfter))
	allEvents = append(allEvents, eventsBefore...)
	allEvents = append(allEvents, eventsAfter...)
	filteredEvents, err := internal.ApplyHistoryVisibilityFilter(ctx, snapshot, rsAPI, allEvents, nil, userID, "context")
	if err != nil {
		return nil, nil, err
	}

	// "Restore" events in the correct context
	for _, ev := range filteredEvents {
		if _, ok := eventIDsBefore[ev.EventID()]; ok {
			filteredBefore = append(filteredBefore, ev)
		}
		if _, ok := eventIDsAfter[ev.EventID()]; ok {
			filteredAfter = append(filteredAfter, ev)
		}
	}
	return filteredBefore, filteredAfter, nil
}

func getStartEnd(ctx context.Context, snapshot storage.DatabaseTransaction, startEvents, endEvents []*rstypes.HeaderedEvent) (start, end types.TopologyToken, err error) {
	if len(startEvents) > 0 {
		start, err = snapshot.EventPositionInTopology(ctx, startEvents[0].EventID())
		if err != nil {
			return
		}
	}
	if len(endEvents) > 0 {
		end, err = snapshot.EventPositionInTopology(ctx, endEvents[0].EventID())
	}
	return
}

func applyLazyLoadMembers(
	ctx context.Context,
	device *userapi.Device,
	snapshot storage.DatabaseTransaction,
	roomID string,
	events []synctypes.ClientEvent,
	lazyLoadCache caching.LazyLoadCache,
) ([]*rstypes.HeaderedEvent, error) {
	eventSenders := make(map[string]struct{})
	// get members who actually send an event
	for _, e := range events {
		// Don't add membership events the client should already know about
		if _, cached := lazyLoadCache.IsLazyLoadedUserCached(device, e.RoomID, e.Sender); cached {
			continue
		}
		eventSenders[e.Sender] = struct{}{}
	}

	wantUsers := make([]string, 0, len(eventSenders))
	for userID := range eventSenders {
		wantUsers = append(wantUsers, userID)
	}

	// Query missing membership events
	filter := synctypes.DefaultStateFilter()
	filter.Senders = &wantUsers
	filter.Types = &[]string{spec.MRoomMember}
	memberships, err := snapshot.GetStateEventsForRoom(ctx, roomID, &filter)
	if err != nil {
		return nil, err
	}

	// cache the membership events
	for _, membership := range memberships {
		lazyLoadCache.StoreLazyLoadedUser(device, roomID, *membership.StateKey(), membership.EventID())
	}

	return memberships, nil
}

func parseRoomEventFilter(req *http.Request) (*synctypes.RoomEventFilter, error) {
	// Default room filter
	filter := &synctypes.RoomEventFilter{Limit: 10} //nolint:mnd

	l := req.URL.Query().Get("limit")
	f := req.URL.Query().Get("filter")
	if l != "" {
		limit, err := strconv.Atoi(l)
		if err != nil {
			return nil, err
		}
		// NOTSPEC: feels like a good idea to have an upper bound limit
		if limit > 100 { //nolint:mnd
			limit = 100
		}
		filter.Limit = limit
	}
	if f != "" {
		if err := json.Unmarshal([]byte(f), &filter); err != nil {
			return nil, err
		}
	}

	return filter, nil
}
