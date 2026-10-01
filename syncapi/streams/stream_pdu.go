package streams

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/notifier"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// The max number of per-room goroutines to have running.
// Too high and this will consume lots of CPU, too low and complete
// sync responses will take longer to process.
const PDU_STREAM_WORKERS = 256

// The maximum number of tasks that can be queued in total before
// backpressure will build up and the rests will start to block.
const PDU_STREAM_QUEUESIZE = PDU_STREAM_WORKERS * 8

type PDUStreamProvider struct {
	DefaultStreamProvider

	// userID+deviceID -> lazy loading cache
	lazyLoadCache caching.LazyLoadCache
	rsAPI         roomserverAPI.SyncRoomserverAPI
	notifier      *notifier.Notifier
}

func (p *PDUStreamProvider) Setup(
	ctx context.Context, snapshot storage.DatabaseTransaction,
) {
	p.DefaultStreamProvider.Setup(ctx, snapshot)

	p.latestMutex.Lock()
	defer p.latestMutex.Unlock()

	id, err := snapshot.MaxStreamPositionForPDUs(ctx)
	if err != nil {
		panic(err)
	}
	p.latest = id
}

//nolint:gocyclo
func (p *PDUStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	from := types.StreamPosition(0)
	to := p.LatestPosition(ctx)

	// Get the current sync position which we will base the sync response on.
	// For complete syncs, we want to start at the most recent events and work
	// backwards, so that we show the most recent events in the room.
	r := types.Range{
		From:      to,
		To:        0,
		Backwards: true,
	}

	// Extract room state and recent events for all rooms the user is joined to.
	joinedRoomIDs, err := snapshot.RoomIDsWithMembership(ctx, req.Device.UserID, spec.Join)
	if err != nil {
		req.Log.WithError(err).Error("p.DB.RoomIDsWithMembership failed")
		return from
	}

	stateFilter := req.Filter.Room.State
	eventFilter := req.Filter.Room.Timeline

	// MSC3706: Filter out partial state rooms for non-lazy syncs
	// Partial state rooms may have incomplete state which can cause issues for clients
	// expecting full room state. Lazy-load syncs can include partial state rooms.
	if !stateFilter.LazyLoadMembers {
		partialStateRoomIDs, partialErr := p.rsAPI.GetPartialStateRoomIDs(ctx)
		if partialErr != nil {
			req.Log.WithError(partialErr).Warn("Failed to get partial state rooms")
		} else if len(partialStateRoomIDs) > 0 {
			partialStateRooms := make(map[string]bool, len(partialStateRoomIDs))
			for _, roomID := range partialStateRoomIDs {
				partialStateRooms[roomID] = true
			}
			filteredRoomIDs := make([]string, 0, len(joinedRoomIDs))
			for _, roomID := range joinedRoomIDs {
				if !partialStateRooms[roomID] {
					filteredRoomIDs = append(filteredRoomIDs, roomID)
				} else {
					req.Log.WithField("room_id", roomID).Debug("Excluding partial state room from non-lazy sync")
				}
			}
			joinedRoomIDs = filteredRoomIDs
		}
	}

	if err = p.addIgnoredUsersToFilter(ctx, snapshot, req, &eventFilter); err != nil {
		req.Log.WithError(err).Error("unable to update event filter with ignored users")
	}
	bundler := p.newSyncBundler(snapshot, req)

	eventFormat := synctypes.FormatSync
	if req.Filter.EventFormat == synctypes.EventFormatFederation {
		eventFormat = synctypes.FormatSyncFederation
	}

	recentEvents, err := snapshot.RecentEvents(ctx, joinedRoomIDs, r, &eventFilter, true, true)
	if err != nil {
		return from
	}
	// Build up a /sync response. Add joined rooms.
	for _, roomID := range joinedRoomIDs {
		// There is no point in building responses for the remaining rooms if
		// the client has already gone away.
		if ctxErr := req.Context.Err(); ctxErr != nil {
			req.Log.WithError(ctxErr).Debugln("Abandoning complete sync, the client went away")
			return from
		}
		events := recentEvents[roomID]
		// Invalidate the lazyLoadCache, otherwise we end up with missing displaynames/avatars
		// TODO: This might be inefficient, when joined to many and/or large rooms.
		joinedUsers := p.notifier.JoinedUsers(roomID)
		for _, sharedUser := range joinedUsers {
			p.lazyLoadCache.InvalidateLazyLoadedUser(req.Device, roomID, sharedUser)
		}

		// get the join response for each room
		jr, jerr := p.getJoinResponseForCompleteSync(
			ctx, snapshot, roomID, &stateFilter, req.WantFullState, req.Device, false,
			events.Events, events.Limited, eventFormat, to, bundler,
		)
		if jerr != nil {
			if req.Context.Err() != nil || abandonsSync(jerr) {
				logAbandoned(req, jerr, roomID, "complete")
				return from
			}
			req.Log.WithError(jerr).Error("p.getJoinResponseForCompleteSync failed")
			continue
		}
		req.Response.Rooms.Join[roomID] = jr
		req.Rooms[roomID] = spec.Join
	}

	// Add peeked rooms.
	peeks, err := snapshot.PeeksInRange(ctx, req.Device.UserID, req.Device.ID, r)
	if err != nil {
		req.Log.WithError(err).Error("p.DB.PeeksInRange failed")
		return from
	}
	if len(peeks) > 0 {
		peekRooms := make([]string, 0, len(peeks))
		for _, peek := range peeks {
			if !peek.Deleted {
				peekRooms = append(peekRooms, peek.RoomID)
			}
		}

		recentEvents, err = snapshot.RecentEvents(ctx, peekRooms, r, &eventFilter, true, true)
		if err != nil {
			return from
		}

		for _, roomID := range peekRooms {
			var jr *types.JoinResponse
			events := recentEvents[roomID]
			jr, err = p.getJoinResponseForCompleteSync(
				ctx, snapshot, roomID, &stateFilter, req.WantFullState, req.Device, true,
				events.Events, events.Limited, eventFormat, to, bundler,
			)
			if err != nil {
				if req.Context.Err() != nil || abandonsSync(err) {
					logAbandoned(req, err, roomID, "complete")
					return from
				}
				req.Log.WithError(err).Error("p.getJoinResponseForCompleteSync failed")
				continue
			}
			req.Response.Rooms.Peek[roomID] = jr
		}
	}

	return to
}

//nolint:gocyclo
func (p *PDUStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) (newPos types.StreamPosition) {
	r := types.Range{
		From:      from,
		To:        to,
		Backwards: from > to,
	}

	var err error
	var stateDeltas []types.StateDelta
	var syncJoinedRooms []string

	stateFilter := req.Filter.Room.State
	eventFilter := req.Filter.Room.Timeline

	if req.WantFullState {
		if stateDeltas, syncJoinedRooms, err = snapshot.GetStateDeltasForFullStateSync(ctx, req.Device, r, req.Device.UserID, &stateFilter, p.rsAPI); err != nil {
			req.Log.WithError(err).Error("p.DB.GetStateDeltasForFullStateSync failed")
			return from
		}
	} else {
		if stateDeltas, syncJoinedRooms, err = snapshot.GetStateDeltas(ctx, req.Device, r, req.Device.UserID, &stateFilter, p.rsAPI); err != nil {
			req.Log.WithError(err).Error("p.DB.GetStateDeltas failed")
			return from
		}
	}

	// MSC3706: Filter out partial state rooms for non-lazy syncs
	if !stateFilter.LazyLoadMembers && len(stateDeltas) > 0 {
		partialStateRoomIDs, partialErr := p.rsAPI.GetPartialStateRoomIDs(ctx)
		if partialErr != nil {
			req.Log.WithError(partialErr).Warn("Failed to get partial state rooms")
		} else if len(partialStateRoomIDs) > 0 {
			partialStateRooms := make(map[string]bool, len(partialStateRoomIDs))
			for _, roomID := range partialStateRoomIDs {
				partialStateRooms[roomID] = true
			}
			filteredDeltas := make([]types.StateDelta, 0, len(stateDeltas))
			filteredJoinedRooms := make([]string, 0, len(syncJoinedRooms))
			for _, delta := range stateDeltas {
				if !partialStateRooms[delta.RoomID] {
					filteredDeltas = append(filteredDeltas, delta)
				} else {
					req.Log.WithField("room_id", delta.RoomID).Debug("Excluding partial state room from non-lazy incremental sync")
				}
			}
			for _, roomID := range syncJoinedRooms {
				if !partialStateRooms[roomID] {
					filteredJoinedRooms = append(filteredJoinedRooms, roomID)
				}
			}
			stateDeltas = filteredDeltas
			syncJoinedRooms = filteredJoinedRooms
		}
	}

	for _, roomID := range syncJoinedRooms {
		req.Rooms[roomID] = spec.Join
	}

	if len(stateDeltas) == 0 {
		return to
	}

	if err = p.addIgnoredUsersToFilter(ctx, snapshot, req, &eventFilter); err != nil {
		req.Log.WithError(err).Error("unable to update event filter with ignored users")
	}
	bundler := p.newSyncBundler(snapshot, req)

	dbEvents, err := p.getRecentEvents(ctx, stateDeltas, r, eventFilter, snapshot)
	if err != nil {
		req.Log.WithError(err).Error("unable to get recent events")
		return r.From
	}

	newPos = from
	for _, delta := range stateDeltas {
		// There is no point in building deltas for the remaining rooms if the
		// client has already gone away.
		if ctxErr := req.Context.Err(); ctxErr != nil {
			req.Log.WithError(ctxErr).Debugln("Abandoning incremental sync, the client went away")
			return newPos
		}
		newRange := r
		// If this room was joined in this sync, try to fetch
		// as much timeline events as allowed by the filter.
		if delta.NewlyJoined {
			// Reverse the range, so we get the most recent first.
			// This will be limited by the eventFilter.
			newRange = types.Range{
				From:      r.To,
				To:        0,
				Backwards: true,
			}
		}
		var pos types.StreamPosition
		if pos, err = p.addRoomDeltaToResponse(ctx, snapshot, req.Device, newRange, delta, &eventFilter, &stateFilter, req, dbEvents, bundler); err != nil {
			if req.Context.Err() != nil || abandonsSync(err) {
				// Stay at from: a position past this room's events would
				// tell the client it has seen events it was never sent.
				logAbandoned(req, err, delta.RoomID, "incremental")
				return from
			}
			req.Log.WithError(err).WithField("room_id", delta.RoomID).Error("d.addRoomDeltaToResponse failed")
			continue
		}
		// Reset the position, as it is only for the special case of newly joined rooms
		if delta.NewlyJoined {
			pos = newRange.From
		}
		switch {
		case r.Backwards && pos < newPos:
			fallthrough
		case !r.Backwards && pos > newPos:
			newPos = pos
		}
	}

	return newPos
}

func (p *PDUStreamProvider) getRecentEvents(ctx context.Context, stateDeltas []types.StateDelta, r types.Range, eventFilter synctypes.RoomEventFilter, snapshot storage.DatabaseTransaction) (map[string]types.RecentEvents, error) {
	var roomIDs []string
	var newlyJoinedRoomIDs []string
	for _, delta := range stateDeltas {
		if delta.NewlyJoined {
			newlyJoinedRoomIDs = append(newlyJoinedRoomIDs, delta.RoomID)
		} else {
			roomIDs = append(roomIDs, delta.RoomID)
		}
	}
	dbEvents := make(map[string]types.RecentEvents)
	if len(roomIDs) > 0 {
		events, err := snapshot.RecentEvents(
			ctx, roomIDs, r,
			&eventFilter, true, true,
		)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
		}
		for k, v := range events {
			dbEvents[k] = v
		}
	}
	if len(newlyJoinedRoomIDs) > 0 {
		// For rooms that were joined in this sync, try to fetch
		// as much timeline events as allowed by the filter.

		filter := eventFilter
		// If we're going backwards, grep at least X events, this is mostly to satisfy Sytest
		if eventFilter.Limit < recentEventBackwardsLimit {
			filter.Limit = recentEventBackwardsLimit // TODO: Figure out a better way
			diff := r.From - r.To
			if diff > 0 && diff < recentEventBackwardsLimit {
				filter.Limit = int(diff)
			}
		}

		events, err := snapshot.RecentEvents(
			ctx, newlyJoinedRoomIDs, types.Range{
				From:      r.To,
				To:        0,
				Backwards: true,
			},
			&filter, true, true,
		)
		if err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return nil, err
			}
		}
		for k, v := range events {
			dbEvents[k] = v
		}
	}

	return dbEvents, nil
}

// Limit the recent events to X when going backwards.
const recentEventBackwardsLimit = 100

//nolint:gocyclo
func (p *PDUStreamProvider) addRoomDeltaToResponse(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	device *userapi.Device,
	r types.Range,
	delta types.StateDelta,
	eventFilter *synctypes.RoomEventFilter,
	stateFilter *synctypes.StateFilter,
	req *types.SyncRequest,
	dbEvents map[string]types.RecentEvents,
	bundler *syncBundler,
) (types.StreamPosition, error) {
	var err error
	recentStreamEvents := dbEvents[delta.RoomID].Events
	limited := dbEvents[delta.RoomID].Limited

	recEvents := gomatrixserverlib.ReverseTopologicalOrdering(
		gomatrixserverlib.ToPDUs(snapshot.StreamEventsToEvents(ctx, device, recentStreamEvents, p.rsAPI)),
		gomatrixserverlib.TopologicalOrderByPrevEvents,
	)
	recentEvents := make([]*rstypes.HeaderedEvent, len(recEvents))
	for i := range recEvents {
		he, ok := recEvents[i].(*rstypes.HeaderedEvent)
		if !ok {
			continue
		}
		recentEvents[i] = he
	}

	// If we didn't return any events at all then don't bother doing anything else.
	if len(recentEvents) == 0 && len(delta.StateEvents) == 0 {
		return r.To, nil
	}

	// A room's state can hold tens of thousands of events, so building its
	// delta takes a while. Stop between the expensive steps once the client
	// has gone away rather than finishing a response nobody will read.
	if err = ctx.Err(); err != nil {
		return r.From, err
	}

	// Work out what the highest stream position is for all of the events in this
	// room that were returned.
	latestPosition := r.To
	if r.Backwards {
		latestPosition = r.From
	}
	updateLatestPosition := func(mostRecentEventID string) {
		var pos types.StreamPosition
		if _, pos, err = snapshot.PositionInTopology(ctx, mostRecentEventID); err == nil {
			switch {
			case r.Backwards && pos < latestPosition:
				fallthrough
			case !r.Backwards && pos > latestPosition:
				latestPosition = pos
			}
		}
	}

	if stateFilter.LazyLoadMembers {
		delta.StateEvents, err = p.lazyLoadMembers(
			ctx, snapshot, delta.RoomID, true, limited, stateFilter,
			device, recentEvents, delta.StateEvents,
			req.Since.PDUPosition, latestPosition,
		)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return r.From, fmt.Errorf("p.lazyLoadMembers: %w", err)
		}
	}

	hasMembershipChange := false
	for _, recentEvent := range recentStreamEvents {
		if recentEvent.Type() == spec.MRoomMember && recentEvent.StateKey() != nil {
			if membership, _ := recentEvent.Membership(); membership == spec.Join {
				req.MembershipChanges[*recentEvent.StateKey()] = struct{}{}
			}
			hasMembershipChange = true
		}
	}

	// MSC3706: Check if this room became un-partial-stated (completed partial state resync)
	// in the sync range. If so, we need to send the room summary with updated member counts.
	// This is similar to Synapse's forced_newly_joined_room_ids mechanism.
	if !hasMembershipChange {
		unPartialStatedRooms, unPartialErr := snapshot.UnPartialStatedRoomsInRange(ctx, device.UserID, r)
		if unPartialErr != nil && abandonsSync(unPartialErr) {
			return r.From, fmt.Errorf("snapshot.UnPartialStatedRoomsInRange: %w", unPartialErr)
		}
		if unPartialErr != nil {
			logrus.WithError(unPartialErr).WithField("room_id", delta.RoomID).Warn("failed to get un-partial-stated rooms")
		} else {
			for _, roomID := range unPartialStatedRooms {
				if roomID == delta.RoomID {
					hasMembershipChange = true
					logrus.WithFields(logrus.Fields{
						"room_id": delta.RoomID,
						"user_id": device.UserID,
					}).Debug("Room became un-partial-stated, forcing summary update")
					break
				}
			}
		}
	}

	if err = ctx.Err(); err != nil {
		return r.From, err
	}

	// Applies the history visibility rules
	events, err := applyHistoryVisibilityFilter(ctx, snapshot, p.rsAPI, delta.RoomID, device.UserID, recentEvents)
	if err != nil {
		if abandonsSync(err) {
			return r.From, fmt.Errorf("applyHistoryVisibilityFilter: %w", err)
		}
		logrus.WithError(err).WithField("room_id", delta.RoomID).Error("unable to apply history visibility filter")
	}

	if r.Backwards && len(events) > eventFilter.Limit {
		// We're going backwards and the events are ordered chronologically, so take the last `limit` events
		events = events[len(events)-eventFilter.Limit:]
		limited = true
	}

	prevBatch, err := snapshot.GetBackwardTopologyPos(ctx, events)
	if err != nil {
		return r.From, fmt.Errorf("p.DB.GetBackwardTopologyPos: %w", err)
	}
	if err = ctx.Err(); err != nil {
		return r.From, err
	}

	eventFormat := synctypes.FormatSync
	if req.Filter.EventFormat == synctypes.EventFormatFederation {
		eventFormat = synctypes.FormatSyncFederation
	}

	// Now that we've filtered the timeline, work out which state events are still
	// left. Anything that appears in the filtered timeline will be removed from the
	// "state" section and kept in "timeline".
	sEvents := gomatrixserverlib.HeaderedReverseTopologicalOrdering(
		gomatrixserverlib.ToPDUs(removeDuplicates(delta.StateEvents, events)),
		// sorting by auth events is not stable unless we know the create and historical PL events,
		// which we don't for deltas like this.
		gomatrixserverlib.TopologicalOrderByPrevEvents,
	)
	delta.StateEvents = make([]*rstypes.HeaderedEvent, len(sEvents))
	var skipped int
	for i := range sEvents {
		ev := sEvents[i]
		he, ok := ev.(*rstypes.HeaderedEvent)
		if !ok {
			skipped++
			continue
		}
		delta.StateEvents[i-skipped] = he
	}
	delta.StateEvents = delta.StateEvents[:len(sEvents)-skipped]

	if len(delta.StateEvents) > 0 {
		if last := delta.StateEvents[len(delta.StateEvents)-1]; last != nil {
			updateLatestPosition(last.EventID())
		}
	}
	if len(events) > 0 {
		if last := events[len(events)-1]; last != nil {
			updateLatestPosition(last.EventID())
		}
	}

	switch delta.Membership {
	case spec.Join:
		jr := types.NewJoinResponse()
		if hasMembershipChange {
			jr.Summary, err = snapshot.GetRoomSummary(ctx, delta.RoomID, device.UserID)
			if err != nil {
				if abandonsSync(err) {
					return r.From, fmt.Errorf("snapshot.GetRoomSummary: %w", err)
				}
				logrus.WithError(err).WithField("room_id", delta.RoomID).Warn("failed to get room summary")
			}
		}
		jr.Timeline.PrevBatch = &prevBatch
		jr.Timeline.Events = synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(events), eventFormat, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return p.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})
		// If we are limited by the filter AND the history visibility filter
		// didn't "remove" events, return that the response is limited.
		jr.Timeline.Limited = (limited && len(events) == len(recentEvents)) || delta.NewlyJoined
		if err = bundler.bundle(ctx, jr.Timeline, events, eventFormat, r.High()); err != nil {
			return r.From, err
		}
		jr.State.Events = synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(delta.StateEvents), eventFormat, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return p.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})

		// MSC4115: Add membership metadata to events (stable feature, enabled by default)
		if err := synctypes.AnnotateEventsWithMembership(jr.Timeline.Events, "join", true); err != nil {
			logrus.WithError(err).Warn("Failed to annotate incremental timeline events with membership")
		}
		if err := synctypes.AnnotateEventsWithMembership(jr.State.Events, "join", true); err != nil {
			logrus.WithError(err).Warn("Failed to annotate incremental state events with membership")
		}

		req.Response.Rooms.Join[delta.RoomID] = jr

	case spec.Peek:
		jr := types.NewJoinResponse()
		jr.Timeline.PrevBatch = &prevBatch
		// TODO: Apply history visibility on peeked rooms
		jr.Timeline.Events = synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(recentEvents), eventFormat, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return p.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})
		jr.Timeline.Limited = limited
		if err = bundler.bundle(ctx, jr.Timeline, recentEvents, eventFormat, r.High()); err != nil {
			return r.From, err
		}
		jr.State.Events = synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(delta.StateEvents), eventFormat, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return p.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})

		// MSC4115: For peeked rooms, user is not joined (membership is "leave")
		if err := synctypes.AnnotateEventsWithMembership(jr.Timeline.Events, "leave", true); err != nil {
			logrus.WithError(err).Warn("Failed to annotate peek timeline events with membership")
		}
		if err := synctypes.AnnotateEventsWithMembership(jr.State.Events, "leave", true); err != nil {
			logrus.WithError(err).Warn("Failed to annotate peek state events with membership")
		}

		req.Response.Rooms.Peek[delta.RoomID] = jr

	case spec.Leave:
		fallthrough // transitions to leave are the same as ban

	case spec.Ban:
		lr := types.NewLeaveResponse()
		lr.Timeline.PrevBatch = &prevBatch
		lr.Timeline.Events = synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(events), eventFormat, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return p.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})
		// If we are limited by the filter AND the history visibility filter
		// didn't "remove" events, return that the response is limited.
		lr.Timeline.Limited = limited && len(events) == len(recentEvents)
		if err = bundler.bundle(ctx, lr.Timeline, events, eventFormat, r.High()); err != nil {
			return r.From, err
		}
		lr.State.Events = synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(delta.StateEvents), eventFormat, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return p.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})

		// MSC4115: Annotate with appropriate membership ("leave" or "ban")
		membership := "leave"
		if delta.Membership == spec.Ban {
			membership = "ban"
		}
		if err := synctypes.AnnotateEventsWithMembership(lr.Timeline.Events, membership, true); err != nil {
			logrus.WithError(err).Warn("Failed to annotate leave/ban timeline events with membership")
		}
		if err := synctypes.AnnotateEventsWithMembership(lr.State.Events, membership, true); err != nil {
			logrus.WithError(err).Warn("Failed to annotate leave/ban state events with membership")
		}

		req.Response.Rooms.Leave[delta.RoomID] = lr
	}

	return latestPosition, nil
}

// syncBundler adds bundled aggregations to the limited timelines of one sync
// response. Its bundler is built on first use and then shared by all rooms.
type syncBundler struct {
	snapshot storage.DatabaseTransaction
	rsAPI    roomserverAPI.SyncRoomserverAPI
	req      *types.SyncRequest
	bundler  *internal.RelationBundler
}

func (p *PDUStreamProvider) newSyncBundler(snapshot storage.DatabaseTransaction, req *types.SyncRequest) *syncBundler {
	return &syncBundler{snapshot: snapshot, rsAPI: p.rsAPI, req: req}
}

// bundle adds bundled aggregations to a limited timeline, as the spec
// requires. An unlimited timeline already carries every child the client has
// not seen, so the client aggregates those itself, and the timeline is only
// stripped of aggregations that arrived with the events. Children after upTo
// are left for the client to aggregate from a later response.
//
// Bundles are an optimisation for the client, so any failure other than one
// that abandons the sync serves the timeline without them rather than
// dropping the room from the response.
func (s *syncBundler) bundle(
	ctx context.Context, timeline *types.Timeline, events []*rstypes.HeaderedEvent, format synctypes.ClientEventFormat, upTo types.StreamPosition,
) error {
	if !timeline.Limited {
		return internal.StripRelations(timeline.Events)
	}
	err := s.init(ctx)
	if err == nil {
		err = s.bundler.UpTo(upTo).Bundle(ctx, events, timeline.Events, format)
	}
	if err == nil {
		return nil
	}
	if ctx.Err() != nil || abandonsSync(err) {
		return fmt.Errorf("bundling relations: %w", err)
	}
	logrus.WithError(err).WithField("user_id", s.req.Device.UserID).Warn("Failed to bundle relations, serving the timeline without them")
	return internal.StripRelations(timeline.Events)
}

func (s *syncBundler) init(ctx context.Context) error {
	if s.bundler != nil {
		return nil
	}
	userID, err := spec.NewUserID(s.req.Device.UserID, true)
	if err != nil {
		return err
	}
	watermark, err := s.snapshot.MaxStreamPositionForRelations(ctx)
	if err != nil {
		return err
	}
	// The ignored users were loaded into the request along with the timeline filter.
	s.bundler = internal.NewRelationBundlerAt(s.snapshot, s.rsAPI, *userID, &s.req.IgnoredUsers, watermark)
	return nil
}

// applyHistoryVisibilityFilter gets the current room state and supplies it to ApplyHistoryVisibilityFilter, to make
// sure we always return the required events in the timeline.
func applyHistoryVisibilityFilter(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	rsAPI roomserverAPI.SyncRoomserverAPI,
	roomID, userID string,
	recentEvents []*rstypes.HeaderedEvent,
) ([]*rstypes.HeaderedEvent, error) {
	// We need to make sure we always include the latest state events, if they are in the timeline.
	alwaysIncludeIDs := make(map[string]struct{})
	var stateTypes []string
	var senders []string
	for _, ev := range recentEvents {
		if ev.StateKey() != nil {
			stateTypes = append(stateTypes, ev.Type())
			senders = append(senders, string(ev.SenderID()))
		}
	}

	// Only get the state again if there are state events in the timeline
	if len(stateTypes) > 0 {
		filter := synctypes.DefaultStateFilter()
		filter.Types = &stateTypes
		filter.Senders = &senders
		stateEvents, err := snapshot.CurrentState(ctx, roomID, &filter, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to get current room state for history visibility calculation: %w", err)
		}

		for _, ev := range stateEvents {
			alwaysIncludeIDs[ev.EventID()] = struct{}{}
		}
	}

	parsedUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return nil, err
	}

	startTime := time.Now()
	events, err := internal.ApplyHistoryVisibilityFilter(ctx, snapshot, rsAPI, recentEvents, alwaysIncludeIDs, *parsedUserID, "sync")
	if err != nil {
		return nil, err
	}
	logrus.WithFields(logrus.Fields{
		"duration": time.Since(startTime),
		"room_id":  roomID,
		"before":   len(recentEvents),
		"after":    len(events),
	}).Debugf("Applied history visibility (sync)")
	return events, nil
}

//nolint:gocyclo
func (p *PDUStreamProvider) getJoinResponseForCompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	stateFilter *synctypes.StateFilter,
	wantFullState bool,
	device *userapi.Device,
	isPeek bool,
	recentStreamEvents []types.StreamEvent,
	limited bool,
	eventFormat synctypes.ClientEventFormat,
	sentAt types.StreamPosition,
	bundler *syncBundler,
) (jr *types.JoinResponse, err error) {
	jr = types.NewJoinResponse()
	// TODO: When filters are added, we may need to call this multiple times to get enough events.
	//       See: https://github.com/matrix-org/synapse/blob/v0.19.3/synapse/handlers/sync.py#L316

	// Work our way through the timeline events and pick out the event IDs
	// of any state events that appear in the timeline. We'll specifically
	// exclude them at the next step, so that we don't get duplicate state
	// events in both `recentStreamEvents` and `stateEvents`.
	var excludingEventIDs []string
	if !wantFullState {
		excludingEventIDs = make([]string, 0, len(recentStreamEvents))
		for _, event := range recentStreamEvents {
			if event.StateKey() != nil {
				excludingEventIDs = append(excludingEventIDs, event.EventID())
			}
		}
	}

	stateEvents, err := snapshot.CurrentState(ctx, roomID, stateFilter, excludingEventIDs)
	if err != nil {
		return jr, err
	}
	// As for incremental syncs, stop between the expensive steps of a large
	// room once the client has gone away.
	if err = ctx.Err(); err != nil {
		return nil, err
	}

	jr.Summary, err = snapshot.GetRoomSummary(ctx, roomID, device.UserID)
	if err != nil {
		logrus.WithError(err).Warn("failed to get room summary")
	}

	// We don't include a device here as we don't need to send down
	// transaction IDs for complete syncs, but we do it anyway because Sytest demands it for:
	// "Can sync a room with a message with a transaction id" - which does a complete sync to check.
	recentEvents := snapshot.StreamEventsToEvents(ctx, device, recentStreamEvents, p.rsAPI)

	events := recentEvents
	// Only apply history visibility checks if the response is for joined rooms
	if !isPeek {
		events, err = applyHistoryVisibilityFilter(ctx, snapshot, p.rsAPI, roomID, device.UserID, recentEvents)
		if err != nil {
			if abandonsSync(err) {
				return nil, fmt.Errorf("applyHistoryVisibilityFilter: %w", err)
			}
			logrus.WithError(err).WithField("room_id", roomID).Error("unable to apply history visibility filter")
		}
	}

	// If we are limited by the filter AND the history visibility filter
	// didn't "remove" events, return that the response is limited.
	limited = limited && len(events) == len(recentEvents)
	stateEvents = removeDuplicates(stateEvents, events)
	if stateFilter.LazyLoadMembers {
		if err != nil {
			return nil, err
		}
		stateEvents, err = p.lazyLoadMembers(
			ctx, snapshot, roomID,
			false, limited, stateFilter,
			device, recentEvents, stateEvents,
			0, sentAt,
		)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}
	}

	if err = ctx.Err(); err != nil {
		return nil, err
	}

	// Retrieve the backward topology position, i.e. the position of the
	// oldest event in the room's topology.
	var prevBatch *types.TopologyToken
	if len(events) > 0 {
		var backwardTopologyPos, backwardStreamPos types.StreamPosition
		event := events[0]
		// If this is the beginning of the room, we can't go back further. We're going to return
		// the TopologyToken from the last event instead. (Synapse returns the /sync next_Batch)
		if event.Type() == spec.MRoomCreate && event.StateKeyEquals("") {
			event = events[len(events)-1]
		}
		backwardTopologyPos, backwardStreamPos, err = snapshot.PositionInTopology(ctx, event.EventID())
		if err != nil {
			return jr, err
		}
		prevBatch = &types.TopologyToken{
			Depth:       backwardTopologyPos,
			PDUPosition: backwardStreamPos,
		}
		prevBatch.Decrement()
	}

	jr.Timeline.PrevBatch = prevBatch
	jr.Timeline.Events = synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(events), eventFormat, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return p.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})
	// If we are limited by the filter AND the history visibility filter
	// didn't "remove" events, return that the response is limited.
	jr.Timeline.Limited = limited && len(events) == len(recentEvents)
	if err = bundler.bundle(ctx, jr.Timeline, events, eventFormat, sentAt); err != nil {
		return jr, err
	}
	jr.State.Events = synctypes.ToClientEvents(gomatrixserverlib.ToPDUs(stateEvents), eventFormat, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return p.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})

	// MSC4115: Add membership metadata to events
	// TODO: Add config check for MSC enablement (currently enabled by default as it's stable)
	// For joined rooms, annotate all events with "join" membership
	// This is a simplified implementation - a full implementation would look up
	// historical membership for each event, but this covers the common case
	if err := synctypes.AnnotateEventsWithMembership(jr.Timeline.Events, "join", true); err != nil {
		logrus.WithError(err).Warn("Failed to annotate timeline events with membership")
	}
	if err := synctypes.AnnotateEventsWithMembership(jr.State.Events, "join", true); err != nil {
		logrus.WithError(err).Warn("Failed to annotate state events with membership")
	}

	return jr, nil
}

// lazyLoadMembers keeps only the memberships the client needs for the given
// timeline. The since argument is the PDU position the client synced from,
// and sentAt the position this response covers. A membership counts as
// already sent only once the client has synced from the response that
// carried it, because a client that never received a response retries from
// its previous token.
func (p *PDUStreamProvider) lazyLoadMembers(
	ctx context.Context, snapshot storage.DatabaseTransaction, roomID string,
	incremental, limited bool, stateFilter *synctypes.StateFilter,
	device *userapi.Device,
	timelineEvents, stateEvents []*rstypes.HeaderedEvent,
	since, sentAt types.StreamPosition,
) ([]*rstypes.HeaderedEvent, error) {
	if len(timelineEvents) == 0 {
		return stateEvents, nil
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return nil, err
	}
	// Work out which memberships to include
	timelineUsers := make(map[string]struct{})
	if !incremental {
		timelineUsers[device.UserID] = struct{}{}
	}
	// Add all users the client doesn't know about yet to a list
	for _, event := range timelineEvents {
		// Membership was not sent yet, or not in a response the client received
		if member, ok := p.lazyLoadCache.IsLazyLoadedUserCached(device, roomID, string(event.SenderID())); !ok || types.StreamPosition(member.SentAt) > since {
			timelineUsers[string(event.SenderID())] = struct{}{}
		}
	}
	// Preallocate with the same amount, even if it will end up with fewer values
	newStateEvents := make([]*rstypes.HeaderedEvent, 0, len(stateEvents))
	// Remove existing membership events we don't care about, e.g. users not in the timeline.events
	for _, event := range stateEvents {
		if event.Type() == spec.MRoomMember && event.StateKey() != nil {
			// If this is a gapped incremental sync, we still want this membership
			isGappedIncremental := limited && incremental
			// We want this users membership event, keep it in the list
			userID := ""
			stateKeyUserID, queryErr := p.rsAPI.QueryUserIDForSender(ctx, *validRoomID, spec.SenderID(*event.StateKey()))
			if queryErr == nil && stateKeyUserID != nil {
				userID = stateKeyUserID.String()
			}
			if _, ok := timelineUsers[userID]; ok || isGappedIncremental || userID == device.UserID {
				newStateEvents = append(newStateEvents, event)
				if !stateFilter.IncludeRedundantMembers {
					p.lazyLoadCache.StoreLazyLoadedUser(device, roomID, userID, caching.LazyLoadedMember{EventID: event.EventID(), SentAt: int64(sentAt)})
				}
				delete(timelineUsers, userID)
			}
		} else {
			newStateEvents = append(newStateEvents, event)
		}
	}
	wantUsers := make([]string, 0, len(timelineUsers))
	for userID := range timelineUsers {
		wantUsers = append(wantUsers, userID)
	}
	// Query missing membership events
	filter := synctypes.DefaultStateFilter()
	filter.Senders = &wantUsers
	filter.Types = &[]string{spec.MRoomMember}
	memberships, err := snapshot.GetStateEventsForRoom(ctx, roomID, &filter)
	if err != nil {
		return stateEvents, err
	}
	// cache the membership events
	for _, membership := range memberships {
		p.lazyLoadCache.StoreLazyLoadedUser(device, roomID, *membership.StateKey(), caching.LazyLoadedMember{EventID: membership.EventID(), SentAt: int64(sentAt)})
	}
	stateEvents = make([]*rstypes.HeaderedEvent, 0, len(newStateEvents)+len(memberships))
	stateEvents = append(stateEvents, newStateEvents...)
	stateEvents = append(stateEvents, memberships...)
	return stateEvents, nil
}

// addIgnoredUsersToFilter adds ignored users to the eventfilter and
// the syncreq itself for further use in streams.
func (p *PDUStreamProvider) addIgnoredUsersToFilter(ctx context.Context, snapshot storage.DatabaseTransaction, req *types.SyncRequest, eventFilter *synctypes.RoomEventFilter) error {
	ignores, err := snapshot.IgnoresForUser(ctx, req.Device.UserID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		return err
	}
	req.IgnoredUsers = *ignores
	userList := make([]string, 0, len(ignores.List))
	for userID := range ignores.List {
		userList = append(userList, userID)
	}
	if len(userList) > 0 {
		eventFilter.NotSenders = &userList
	}
	return nil
}

func removeDuplicates[T gomatrixserverlib.PDU](stateEvents, recentEvents []T) []T {
	for _, recentEv := range recentEvents {
		if recentEv.StateKey() == nil {
			continue // not a state event
		}
		// TODO: This is a linear scan over all the current state events in this room. This will
		//       be slow for big rooms. We should instead sort the state events by event ID  (ORDER BY)
		//       then do a binary search to find matching events, similar to what roomserver does.
		for j := 0; j < len(stateEvents); j++ {
			if stateEvents[j].EventID() == recentEv.EventID() {
				// overwrite the element to remove with the last element then pop the last element.
				// This is orders of magnitude faster than re-slicing, but doesn't preserve ordering
				// (we don't care about the order of stateEvents)
				stateEvents[j] = stateEvents[len(stateEvents)-1]
				stateEvents = stateEvents[:len(stateEvents)-1]
				break // there shouldn't be multiple events with the same event ID
			}
		}
	}
	return stateEvents
}

// abandonsSync reports whether err must abandon the sync without advancing
// the client's position: the client went away, or the database dropped its
// connection, as it does during a PostgreSQL failover. Carrying on would
// leave the room's events out of a response whose position covers them, and
// the client would never receive them.
func abandonsSync(err error) bool {
	return sqlutil.ErrIsCanceled(err) || sqlutil.IsTransient(err)
}

// logAbandoned records why a sync was abandoned: routinely because the client
// went away, or, worth a warning, because the database was unavailable.
func logAbandoned(req *types.SyncRequest, err error, roomID, kind string) {
	logger := req.Log.WithError(err).WithField("room_id", roomID)
	if req.Context.Err() != nil || sqlutil.ErrIsCanceled(err) {
		logger.Debugf("Abandoning %s sync, the client went away", kind)
		return
	}
	logger.Warnf("Abandoning %s sync, the database is unavailable", kind)
}
