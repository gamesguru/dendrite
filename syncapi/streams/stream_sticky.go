// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package streams

import (
	"context"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/caching"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// StickyEventsStreamProvider delivers MSC4354 sticky events in the
// msc4354_sticky section of joined rooms. Each unexpired sticky event is
// delivered once as the client's position in this stream advances, unless
// the timeline of the same response already carries it. Joining a room
// delivers every unexpired sticky event in it.
//
// It must run after the PDU stream, whose timelines, ignored users and newly
// joined rooms it reads from the request.
type StickyEventsStreamProvider struct {
	DefaultStreamProvider
	rsAPI         roomserverAPI.SyncRoomserverAPI
	lazyLoadCache caching.LazyLoadCache
}

func (p *StickyEventsStreamProvider) Setup(
	ctx context.Context, snapshot storage.DatabaseTransaction,
) {
	p.DefaultStreamProvider.Setup(ctx, snapshot)

	p.latestMutex.Lock()
	defer p.latestMutex.Unlock()

	id, err := snapshot.MaxStreamPositionForStickyEvents(ctx)
	if err != nil {
		panic(err)
	}
	p.latest = id
}

func (p *StickyEventsStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.IncrementalSync(ctx, snapshot, req, 0, p.LatestPosition(ctx))
}

func (p *StickyEventsStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	if to <= from && len(req.NewlyJoinedRooms) == 0 {
		return from
	}
	now := time.Now()
	joinedRooms, err := snapshot.RoomIDsWithMembership(ctx, req.Device.UserID, spec.Join)
	if err != nil {
		req.Log.WithError(err).Error("p.DB.RoomIDsWithMembership failed")
		return from
	}
	// Like the PDU stream, leave out rooms with partial state unless members
	// are lazily loaded.
	partialState := make(map[string]bool)
	if !req.Filter.Room.State.LazyLoadMembers {
		partialStateRoomIDs, partialErr := p.rsAPI.GetPartialStateRoomIDs(ctx)
		if partialErr != nil {
			req.Log.WithError(partialErr).Warn("Failed to get partial state rooms")
		}
		for _, roomID := range partialStateRoomIDs {
			partialState[roomID] = true
		}
	}
	var rooms, newlyJoinedRooms []string
	for _, roomID := range joinedRooms {
		if partialState[roomID] {
			continue
		}
		if _, ok := req.NewlyJoinedRooms[roomID]; ok {
			newlyJoinedRooms = append(newlyJoinedRooms, roomID)
		} else {
			rooms = append(rooms, roomID)
		}
	}

	// Deliver at most a page of sticky events, and continue from the last
	// one delivered in the next response.
	events, err := snapshot.StickyEventsInRooms(ctx, rooms, from, to, spec.AsTimestamp(now), types.MaxStickyEventsInSync)
	if err != nil {
		req.Log.WithError(err).Error("p.DB.StickyEventsInRooms failed")
		return from
	}
	newPos := max(from, to)
	if len(events) == types.MaxStickyEventsInSync {
		newPos = events[len(events)-1].Position
	}
	// A newly joined room gets every unexpired sticky event at once,
	// including those from before the client's position.
	if len(newlyJoinedRooms) > 0 {
		joinedEvents, joinErr := snapshot.StickyEventsInRooms(ctx, newlyJoinedRooms, 0, to, spec.AsTimestamp(now), 0)
		if joinErr != nil {
			req.Log.WithError(joinErr).Error("p.DB.StickyEventsInRooms failed")
			return from
		}
		events = append(events, joinedEvents...)
	}

	if err = p.addStickyEvents(ctx, snapshot, req, events, now); err != nil {
		req.Log.WithError(err).Error("Failed to add sticky events to the sync response")
		return from
	}
	if err = p.addTimelineTTLs(ctx, snapshot, req, now); err != nil {
		req.Log.WithError(err).Warn("Failed to add sticky TTLs to timeline events")
	}
	return newPos
}

// addStickyEvents adds the sticky events to the msc4354_sticky sections of
// their rooms, leaving out those of ignored users and those that the timeline
// of the room already carries.
func (p *StickyEventsStreamProvider) addStickyEvents(
	ctx context.Context, snapshot storage.DatabaseTransaction, req *types.SyncRequest,
	stickyEvents []types.StickyEvent, now time.Time,
) error {
	if len(stickyEvents) == 0 {
		return nil
	}
	ignores, err := snapshot.IgnoresForUser(ctx, req.Device.UserID)
	if err != nil {
		ignores = &req.IgnoredUsers
	}

	eventIDs := make([]string, 0, len(stickyEvents))
	for _, sticky := range stickyEvents {
		if _, ignored := ignores.List[sticky.Sender]; ignored {
			continue
		}
		if jr, ok := req.Response.Rooms.Join[sticky.RoomID]; ok && timelineHasEvent(jr, sticky.EventID) {
			continue
		}
		eventIDs = append(eventIDs, sticky.EventID)
	}
	if len(eventIDs) == 0 {
		return nil
	}
	events, err := snapshot.Events(ctx, eventIDs)
	if err != nil {
		return err
	}
	eventsByID := make(map[string]*rstypes.HeaderedEvent, len(events))
	for _, ev := range events {
		eventsByID[ev.EventID()] = ev
	}

	eventFormat := synctypes.FormatSync
	if req.Filter.EventFormat == synctypes.EventFormatFederation {
		eventFormat = synctypes.FormatSyncFederation
	}
	userIDForSender := func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return p.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	}
	sendersByRoom := make(map[string][]*rstypes.HeaderedEvent)
	// History visibility does not apply to unexpired sticky events, which
	// every joined user may see.
	for _, sticky := range stickyEvents {
		ev, ok := eventsByID[sticky.EventID]
		if !ok {
			continue
		}
		clientEvent, convErr := synctypes.ToClientEvent(ev, eventFormat, userIDForSender)
		if convErr != nil {
			req.Log.WithError(convErr).WithField("event_id", sticky.EventID).Warn("Failed to convert sticky event")
			continue
		}
		if clientEvent.MSC4354Sticky == nil {
			// The event was redacted after this snapshot was taken.
			continue
		}
		if err = synctypes.SetStickyTTL(clientEvent, sticky.TTL(now)); err != nil {
			return err
		}
		jr, ok := req.Response.Rooms.Join[sticky.RoomID]
		if !ok {
			jr = types.NewJoinResponse()
			req.Response.Rooms.Join[sticky.RoomID] = jr
		}
		if jr.MSC4354Sticky == nil {
			jr.MSC4354Sticky = &types.ClientEvents{}
		}
		jr.MSC4354Sticky.Events = append(jr.MSC4354Sticky.Events, *clientEvent)
		sendersByRoom[sticky.RoomID] = append(sendersByRoom[sticky.RoomID], ev)
	}

	if req.Filter.Room.State.LazyLoadMembers {
		for roomID, senders := range sendersByRoom {
			if err = p.lazyLoadSenders(ctx, snapshot, req, roomID, senders, eventFormat); err != nil {
				return err
			}
		}
	}
	return nil
}

// lazyLoadSenders adds the memberships of the senders of sticky events to the
// state section, as for the senders of timeline events, unless the client
// already has them.
func (p *StickyEventsStreamProvider) lazyLoadSenders(
	ctx context.Context, snapshot storage.DatabaseTransaction, req *types.SyncRequest,
	roomID string, events []*rstypes.HeaderedEvent, eventFormat synctypes.ClientEventFormat,
) error {
	jr := req.Response.Rooms.Join[roomID]
	present := make(map[string]struct{})
	for _, sections := range [][]synctypes.ClientEvent{jr.State.Events, jr.Timeline.Events} {
		for _, ev := range sections {
			if ev.Type == spec.MRoomMember && ev.StateKey != nil {
				present[*ev.StateKey] = struct{}{}
			}
		}
	}
	var keys []gomatrixserverlib.StateKeyTuple
	for _, ev := range events {
		senderID := string(ev.SenderID())
		if _, ok := present[senderID]; ok {
			continue
		}
		// The client has this membership from a response it acknowledged.
		if member, ok := p.lazyLoadCache.IsLazyLoadedUserCached(req.Device, roomID, senderID); ok && types.StreamPosition(member.SentAt) <= req.Since.PDUPosition {
			continue
		}
		present[senderID] = struct{}{}
		keys = append(keys, gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomMember, StateKey: senderID})
	}
	if len(keys) == 0 {
		return nil
	}
	memberships, err := snapshot.GetStateEvents(ctx, roomID, keys)
	if err != nil {
		return err
	}
	pdus := make([]gomatrixserverlib.PDU, 0, len(memberships))
	for _, membership := range memberships {
		pdus = append(pdus, membership)
		// Only a response with a later PDU position acknowledges it.
		p.lazyLoadCache.StoreLazyLoadedUser(req.Device, roomID, *membership.StateKey(), caching.LazyLoadedMember{
			EventID: membership.EventID(),
			SentAt:  int64(req.Since.PDUPosition) + 1,
		})
	}
	stateEvents := synctypes.ToClientEvents(pdus, eventFormat, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return p.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})
	jr.State.Events = append(jr.State.Events, stateEvents...)
	return nil
}

// addTimelineTTLs tells the client how long the sticky events in the
// timelines of the response remain sticky, as MSC4354 requires for every
// sticky event sent to clients.
func (p *StickyEventsStreamProvider) addTimelineTTLs(
	ctx context.Context, snapshot storage.DatabaseTransaction, req *types.SyncRequest, now time.Time,
) error {
	var eventIDs []string
	for _, jr := range req.Response.Rooms.Join {
		if jr.Timeline == nil {
			continue
		}
		for _, ev := range jr.Timeline.Events {
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
	for _, jr := range req.Response.Rooms.Join {
		if jr.Timeline == nil {
			continue
		}
		for i := range jr.Timeline.Events {
			if ttl, ok := ttls[jr.Timeline.Events[i].EventID]; ok {
				if err = synctypes.SetStickyTTL(&jr.Timeline.Events[i], ttl); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func timelineHasEvent(jr *types.JoinResponse, eventID string) bool {
	if jr.Timeline == nil {
		return false
	}
	for _, ev := range jr.Timeline.Events {
		if ev.EventID == eventID {
			return true
		}
	}
	return false
}
