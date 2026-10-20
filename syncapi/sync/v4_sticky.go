// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"fmt"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"

	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// defaultStickyEventsLimit is how many sticky events the sticky events
// extension returns when the request does not say.
const defaultStickyEventsLimit = 100

// processStickyEventsExtension handles the MSC4354 sticky events extension
// (MSC4480), matching Synapse. It returns the unexpired sticky events after
// the extension's since token in every joined room the lists or room
// subscriptions match, beyond the lists' windows, and nil when there are none.
// History visibility does not apply to them, sticky events of ignored users
// are left out, and so are those in the timeline of their room in this
// response, whose sticky events gain how long they remain sticky.
func (rp *RequestPool) processStickyEventsExtension(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	userID string,
	req *types.StickyEventsRequest,
	toPos types.StreamingToken,
	responseLists map[string]types.SlidingList,
	roomSubscriptions map[string]bool,
	responseRooms map[string]types.SlidingRoomData,
) (*types.StickyEventsResponse, error) {
	now := time.Now()
	if err := rp.addStickyTimelineTTLs(ctx, snapshot, responseRooms, now); err != nil {
		logrus.WithError(err).Warn("Failed to add sticky TTLs to timeline events")
	}

	limit := defaultStickyEventsLimit
	if req.Limit != nil {
		limit = *req.Limit
	}
	if limit <= 0 {
		return nil, nil
	}
	limit = min(limit, types.MaxStickyEventsInSync)

	var from types.StreamPosition
	if req.Since != "" {
		since, err := types.NewStreamPositionFromString(req.Since)
		if err != nil {
			logrus.WithError(err).Warn("Invalid sticky events since token, starting from the beginning")
		} else {
			from = since
		}
	}
	to := toPos.StickyPosition
	if to <= from {
		return nil, nil
	}

	interested := make(map[string]bool, len(roomSubscriptions))
	for roomID := range roomSubscriptions {
		interested[roomID] = true
	}
	for _, list := range responseLists {
		for _, roomID := range list.MatchedRoomIDs {
			interested[roomID] = true
		}
	}
	joinedRooms, err := snapshot.RoomIDsWithMembership(ctx, userID, spec.Join)
	if err != nil {
		return nil, fmt.Errorf("load joined rooms: %w", err)
	}
	rooms := make([]string, 0, len(joinedRooms))
	for _, roomID := range joinedRooms {
		if interested[roomID] {
			rooms = append(rooms, roomID)
		}
	}

	stickyEvents, err := snapshot.StickyEventsInRooms(ctx, rooms, from, to, spec.AsTimestamp(now), limit)
	if err != nil {
		return nil, fmt.Errorf("load sticky events: %w", err)
	}
	if len(stickyEvents) == 0 {
		return nil, nil
	}
	nextBatch := to
	if len(stickyEvents) == limit {
		nextBatch = stickyEvents[len(stickyEvents)-1].Position
	}

	ignored := map[string]any{}
	if ignores, ignoreErr := snapshot.IgnoresForUser(ctx, userID); ignoreErr == nil && ignores != nil && ignores.List != nil {
		ignored = ignores.List
	}
	eventIDs := make([]string, 0, len(stickyEvents))
	for _, sticky := range stickyEvents {
		if _, ok := ignored[sticky.Sender]; !ok {
			eventIDs = append(eventIDs, sticky.EventID)
		}
	}
	events, err := snapshot.Events(ctx, eventIDs)
	if err != nil {
		return nil, fmt.Errorf("load events: %w", err)
	}
	eventsByID := make(map[string]*rstypes.HeaderedEvent, len(events))
	for _, ev := range events {
		eventsByID[ev.EventID()] = ev
	}

	resp := &types.StickyEventsResponse{
		Rooms:     make(map[string]types.StickyEventsRoom),
		NextBatch: fmt.Sprintf("%d", nextBatch),
	}
	for _, sticky := range stickyEvents {
		ev, ok := eventsByID[sticky.EventID]
		if !ok || timelineHas(responseRooms[sticky.RoomID].Timeline, sticky.EventID) {
			continue
		}
		clientEvent, convErr := synctypes.ToClientEvent(ev, synctypes.FormatSync, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rp.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})
		if convErr != nil {
			logrus.WithError(convErr).WithField("event_id", sticky.EventID).Warn("Failed to convert sticky event")
			continue
		}
		if clientEvent.MSC4354Sticky == nil {
			// The event was redacted after this snapshot was taken.
			continue
		}
		if err = synctypes.SetStickyTTL(clientEvent, sticky.TTL(now)); err != nil {
			return nil, err
		}
		room := resp.Rooms[sticky.RoomID]
		room.Events = append(room.Events, *clientEvent)
		resp.Rooms[sticky.RoomID] = room
	}
	return resp, nil
}

// addStickyTimelineTTLs tells the client how long the sticky events in the
// room timelines of the response remain sticky.
func (rp *RequestPool) addStickyTimelineTTLs(
	ctx context.Context, snapshot storage.DatabaseTransaction, responseRooms map[string]types.SlidingRoomData, now time.Time,
) error {
	var eventIDs []string
	for _, room := range responseRooms {
		for _, ev := range room.Timeline {
			if ev.MSC4354Sticky != nil {
				eventIDs = append(eventIDs, ev.EventID)
			}
		}
	}
	if len(eventIDs) == 0 {
		return nil
	}
	stickyEvents, err := snapshot.StickyEventsByID(ctx, eventIDs, spec.AsTimestamp(now))
	if err != nil {
		return err
	}
	ttls := make(map[string]time.Duration, len(stickyEvents))
	for _, sticky := range stickyEvents {
		ttls[sticky.EventID] = sticky.TTL(now)
	}
	for _, room := range responseRooms {
		// The timeline shares its events with the map entry.
		for i := range room.Timeline {
			if ttl, ok := ttls[room.Timeline[i].EventID]; ok {
				if err = synctypes.SetStickyTTL(&room.Timeline[i], ttl); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func timelineHas(timeline []synctypes.ClientEvent, eventID string) bool {
	for _, ev := range timeline {
		if ev.EventID == eventID {
			return true
		}
	}
	return false
}
