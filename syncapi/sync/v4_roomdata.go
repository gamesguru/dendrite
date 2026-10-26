// Copyright 2026 The Zendrite Authors
// Copyright 2025 Jackmaninov
// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"fmt"
	"math"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	syncinternal "codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// roomMembershipResult holds the result of membership detection.
type roomMembershipResult struct {
	membership  string
	inviteEvent *rstypes.HeaderedEvent
}

// detectRoomMembership determines the user's membership in a room
// Checks both PDU-based membership and active invites.
func (rp *RequestPool) detectRoomMembership(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	userID string,
	currentPos types.StreamingToken,
) roomMembershipResult {
	// First check PDU-based membership (join/leave/ban)
	pduMembership, _, err := snapshot.SelectMembershipForUser(ctx, roomID, userID, math.MaxInt64)
	if err != nil {
		logrus.WithError(err).WithField("room_id", roomID).Warn("Failed to get PDU membership")
		pduMembership = "leave"
	}

	result := roomMembershipResult{membership: pduMembership}

	// Check for active invite that might override PDU membership
	if currentPos.InvitePosition > 0 {
		inviteRange := types.Range{From: 0, To: currentPos.InvitePosition, Backwards: false}
		invites, retired, _, inviteErr := snapshot.InviteEventsInRange(ctx, userID, inviteRange)
		if inviteErr != nil {
			logrus.WithError(inviteErr).WithField("room_id", roomID).Warn("Failed to check for invites")
		} else if invite, hasInvite := invites[roomID]; hasInvite {
			if _, isRetired := retired[roomID]; !isRetired {
				// The invites table also holds knocks.
				result.membership = spec.Invite
				if eventMembership, _ := invite.Membership(); eventMembership == spec.Knock {
					result.membership = spec.Knock
				}
				result.inviteEvent = invite
			}
		}
	}

	logrus.WithFields(logrus.Fields{
		"room_id":        roomID,
		"user_id":        userID,
		"membership":     result.membership,
		"pdu_membership": pduMembership,
		"pdu_pos":        currentPos.PDUPosition,
		"invite_pos":     currentPos.InvitePosition,
	}).Debug("[V4_SYNC] Room membership detected")

	return result
}

// shouldFetchRequiredState determines if required state should be fetched.
// Incremental syncs used to only attempt this for "$LAZY" patterns; now that
// getRequiredState computes a real delta (see getRequiredStateDelta) and
// correctly returns nothing when unchanged, it's safe to always attempt it.
func shouldFetchRequiredState(requiredStateConfig *types.RequiredStateConfig) bool {
	return requiredStateConfig != nil && (len(requiredStateConfig.Include) > 0 || requiredStateConfig.LazyMembers)
}

// populateMemberCounts adds joined and invited member counts to room data.
func (rp *RequestPool) populateMemberCounts(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	roomData *types.SlidingRoomData,
	currentPos types.StreamingToken,
) {
	countPos := currentPos.PDUPosition
	if currentPos.InvitePosition > countPos {
		countPos = currentPos.InvitePosition
	}

	joinedCount, err := snapshot.MembershipCount(ctx, roomID, "join", countPos)
	if err != nil {
		logrus.WithError(err).WithField("room_id", roomID).Error("Failed to get joined member count")
	} else {
		roomData.JoinedCount = joinedCount
	}

	invitedCount, err := snapshot.MembershipCount(ctx, roomID, "invite", countPos)
	if err != nil {
		logrus.WithError(err).WithField("room_id", roomID).Error("Failed to get invited member count")
	} else {
		roomData.InvitedCount = invitedCount
	}
}

// populatePrevBatch generates prev_batch token if timeline is limited.
func (rp *RequestPool) populatePrevBatch(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomData *types.SlidingRoomData,
	timelineLimited bool,
) {
	if !timelineLimited || len(roomData.Timeline) == 0 {
		return
	}

	earliestEvent := roomData.Timeline[0]
	if earliestEvent.EventID == "" {
		return
	}

	topologyToken, err := snapshot.EventPositionInTopology(ctx, earliestEvent.EventID)
	if err != nil {
		logrus.WithError(err).WithField("event_id", earliestEvent.EventID).Error("Failed to get topology position for prev_batch")
		return
	}

	topologyToken.Decrement()
	roomData.PrevBatch = topologyToken.String()
}

// BuildRoomData constructs SlidingRoomData for a single room
// Phase 3: Basic implementation with timeline and metadata
// Phase 4: Required state filtering
// Phase 11: Invite state support for Element X
// roomState determines if this is initial/live/previously for proper incremental sync
// fromToken is the position from the sync request (nil for initial sync) - used for num_live calculation
// ignoreTimelineBound: if true, fetch timeline from scratch (for timeline_limit expansion)
//
//	This is separate from initial sync - we still set initial=false but fetch historical events
func (rp *RequestPool) BuildRoomData(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	userID string,
	timelineLimit int,
	roomState types.RoomStreamState,
	currentPos types.StreamingToken,
	fromToken *types.StreamingToken,
	requiredStateConfig *types.RequiredStateConfig,
	ignoreTimelineBound bool,
	forceRequiredState bool,
) (*types.SlidingRoomData, error) {
	// CRITICAL: initial indicates if this is the FIRST TIME this room is being sent on this CONNECTION
	isInitialForRoom := fromToken == nil || roomState.Status == types.HaveSentRoomNever
	roomData := &types.SlidingRoomData{
		Initial: isInitialForRoom,
	}

	// Detect membership (PDU + invite check)
	membershipResult := rp.detectRoomMembership(ctx, snapshot, roomID, userID, currentPos)
	roomData.Membership = membershipResult.membership

	// If user is invited, return stripped state instead of timeline/required_state
	if membershipResult.membership == spec.Invite {
		logrus.WithField("room_id", roomID).Info("[V4_SYNC] Building invite room data (stripped state)")
		return rp.buildInviteRoomData(ctx, snapshot, roomID, userID, isInitialForRoom, membershipResult.inviteEvent)
	}
	// Knocked rooms likewise only get stripped state.
	if membershipResult.membership == spec.Knock {
		return rp.buildKnockRoomData(ctx, snapshot, roomID, userID, isInitialForRoom, membershipResult.inviteEvent)
	}

	// Get timeline events
	var timelineLimited bool
	var numLive int
	if timelineLimit > 0 {
		timelineFromToken := fromToken
		if ignoreTimelineBound || isInitialForRoom {
			timelineFromToken = nil
		}
		timeline, limited, numLiveCount, timelineErr := rp.getTimelineEvents(ctx, snapshot, roomID, userID, timelineLimit, roomState, currentPos, timelineFromToken)
		if timelineErr != nil {
			logrus.WithError(timelineErr).WithField("room_id", roomID).Error("Failed to get timeline events")
		} else {
			roomData.Timeline = timeline
			timelineLimited = limited
			numLive = numLiveCount
		}
	}

	// Get room metadata
	roomData.Name = rp.getRoomNameFromDB(ctx, snapshot, roomID)
	roomData.AvatarURL = rp.getRoomAvatar(ctx, snapshot, roomID)
	roomData.Topic = rp.getRoomTopic(ctx, snapshot, roomID)

	// Get required state events
	if shouldFetchRequiredState(requiredStateConfig) {
		logrus.WithFields(logrus.Fields{
			"room_id":         roomID,
			"user_id":         userID,
			"initial":         isInitialForRoom,
			"timeline_events": len(roomData.Timeline),
		}).Debug("[REQUIRED_STATE] Getting required state")

		requiredState, stateErr := rp.getRequiredState(ctx, snapshot, roomID, userID, requiredStateConfig, roomData.Timeline, isInitialForRoom || forceRequiredState, roomState, currentPos)
		if stateErr != nil {
			// Fatal for this room rather than logged and dropped. On an
			// incremental sync the state is a one-shot delta, so emitting the
			// room anyway would advance its LastToken past changes that were
			// never sent (see v4.go, which records currentPos for every room
			// in the response). Skipping the room leaves its position alone.
			return nil, fmt.Errorf("get required state for %s: %w", roomID, stateErr)
		}
		roomData.RequiredState = requiredState
		logrus.WithFields(logrus.Fields{
			"room_id":     roomID,
			"user_id":     userID,
			"state_count": len(requiredState),
		}).Debug("[REQUIRED_STATE] Returned required state events")
	}

	// Notification counts (hardcoded to 0 per Synapse behavior for encrypted rooms)
	roomData.NotificationCount = 0
	roomData.HighlightCount = 0

	// Member counts
	rp.populateMemberCounts(ctx, snapshot, roomID, roomData, currentPos)

	// Limited flag and prev_batch token
	roomData.Limited = timelineLimited
	rp.populatePrevBatch(ctx, snapshot, roomData, timelineLimited)

	// num_live for notification triggering
	roomData.NumLive = numLive

	// Additional room fields
	roomData.IsDM = rp.isDirectMessage(ctx, roomID, userID)
	roomData.BumpStamp = rp.calculateBumpStamp(ctx, snapshot, roomID, roomData.Timeline)
	roomData.Heroes = rp.getHeroes(ctx, snapshot, roomID, userID)

	return roomData, nil
}

// getTimelineEvents retrieves recent timeline events for a room
// For initial syncs (NEVER), gets historical events
// For incremental syncs (LIVE/PREVIOUSLY), gets only new events since last sync
// Returns the events, whether the timeline was limited (truncated due to hitting the limit), and num_live count
// fromToken is used to calculate num_live (how many events arrived after the sync request's since token).
func (rp *RequestPool) getTimelineEvents(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	userID string, // filters the timeline by history visibility and bundles relations
	limit int,
	roomState types.RoomStreamState,
	currentPos types.StreamingToken,
	fromToken *types.StreamingToken,
) (timeline []synctypes.ClientEvent, limited bool, numLive int, err error) {
	// Create a trace region for timeline event retrieval
	timelineRegion, _ := internal.StartRegion(ctx, "SlidingSync.getTimelineEvents")
	defer timelineRegion.EndRegion()
	timelineRegion.SetTag("room_id", roomID)
	timelineRegion.SetTag("limit", limit)
	timelineRegion.SetTag("current_pdu_pos", currentPos.PDUPosition)
	if fromToken != nil {
		timelineRegion.SetTag("from_pdu_pos", fromToken.PDUPosition)
		timelineRegion.SetTag("is_incremental", true)
	} else {
		timelineRegion.SetTag("is_incremental", false)
	}

	// Create a filter with the limit
	filter := synctypes.RoomEventFilter{
		Limit: limit,
	}

	// CRITICAL: the lower bound is the OLDER of since and roomState.LastToken -
	// using since alone drops events from a room that was excluded from prior
	// responses while the connection advanced elsewhere. Brand-new rooms
	// (Status == HaveSentRoomNever) take the fromToken == nil branch instead.
	var fromPos, toPos types.StreamPosition
	fromPos = currentPos.PDUPosition // Always go backwards from current

	if fromToken == nil {
		// Initial sync (no since token) - get recent historical events
		toPos = 0
		logrus.WithFields(logrus.Fields{
			"room_id": roomID,
			"from":    fromPos,
			"to":      toPos,
			"mode":    "historical (no since token)",
		}).Debug("[TIMELINE] Fetching historical events for initial sync")
	} else {
		// Incremental sync (has since token) - get events since whichever is
		// older: the sync-level since token, or this room's own last-sent
		// token (if it lagged behind because the room wasn't included in
		// every response).
		toPos = fromToken.PDUPosition
		mode := "incremental (since token)"
		if roomState.LastToken != nil && roomState.LastToken.PDUPosition < toPos {
			toPos = roomState.LastToken.PDUPosition
			mode = "incremental (room last-sent token, lagged behind since token)"
		}
		logrus.WithFields(logrus.Fields{
			"room_id": roomID,
			"from":    fromPos,
			"to":      toPos,
			"mode":    mode,
		}).Debug("[TIMELINE] Fetching incremental events since sync token")
	}

	// Get events in the determined range
	recentEvents, err := snapshot.RecentEvents(
		ctx,
		[]string{roomID},
		types.Range{
			From:      fromPos, // High value (current position)
			To:        toPos,   // Low value (0 for initial, lastSentPos for incremental)
			Backwards: true,    // Get most recent first, limit will apply
		},
		&filter,
		true, // Chronological order (oldest first)
		true, // Only sync events
	)
	if err != nil {
		return nil, false, 0, err
	}

	events, ok := recentEvents[roomID]
	if !ok || len(events.Events) == 0 {
		return []synctypes.ClientEvent{}, false, 0, nil
	}

	// Drop the events the user may not see under the room's history
	// visibility, as sync v2 does. A failure fails closed: no timeline.
	events.Events, err = rp.visibleStreamEvents(ctx, snapshot, roomID, userID, events.Events)
	if err != nil {
		return nil, false, 0, fmt.Errorf("apply history visibility: %w", err)
	}
	if len(events.Events) == 0 {
		return []synctypes.ClientEvent{}, events.Limited, 0, nil
	}

	// Calculate num_live BEFORE converting to ClientEvents (while we have StreamPosition)
	// Uses Synapse's algorithm: Count how many events arrived after the request's from_token
	// This is connection-level logic (based on request token), not room-level logic
	numLive = 0
	if fromToken != nil {
		// Iterate in reverse chronological order and break early when hitting historical events
		for i := len(events.Events) - 1; i >= 0; i-- {
			eventPos := events.Events[i].StreamPosition
			// Compare event position to the sync request's from_token PDU position
			if eventPos > fromToken.PDUPosition {
				numLive++
			} else {
				// Optimization from Synapse: break once we hit an event that's not live
				break
			}
		}
	}

	// Convert to ClientEvents
	clientEvents := make([]synctypes.ClientEvent, 0, len(events.Events))
	for _, event := range events.Events {
		clientEvent, err := synctypes.ToClientEvent(event, synctypes.FormatSync, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rp.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})
		if err != nil {
			logrus.WithError(err).WithField("event_id", event.EventID()).Warn("Failed to convert event to client format")
			continue
		}

		clientEvents = append(clientEvents, *clientEvent)
	}
	rp.bundleTimeline(ctx, snapshot, userID, events.Events, clientEvents, events.Limited, fromPos)

	logrus.WithFields(logrus.Fields{
		"room_id":        roomID,
		"num_live":       numLive,
		"total":          len(clientEvents),
		"has_from_token": fromToken != nil,
		"limited":        events.Limited,
	}).Debug("[NUM_LIVE] Calculated num_live in getTimelineEvents")

	// Add output tags to trace
	timelineRegion.SetTag("events_returned", len(clientEvents))
	timelineRegion.SetTag("num_live", numLive)
	timelineRegion.SetTag("limited", events.Limited)

	// Return the events, limited flag, and num_live count
	return clientEvents, events.Limited, numLive, nil
}

// visibleStreamEvents returns the events the user may see under the room's
// history visibility, in their original order.
func (rp *RequestPool) visibleStreamEvents(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID, userID string,
	events []types.StreamEvent,
) ([]types.StreamEvent, error) {
	headered := make([]*rstypes.HeaderedEvent, len(events))
	for i := range events {
		headered[i] = events[i].HeaderedEvent
	}
	visible, err := syncinternal.ApplyTimelineHistoryVisibilityFilter(ctx, snapshot, rp.rsAPI, roomID, userID, headered, syncinternal.SlidingSyncEndpoint)
	if err != nil {
		return nil, err
	}
	if len(visible) == len(events) {
		return events, nil
	}
	visibleIDs := make(map[string]struct{}, len(visible))
	for _, ev := range visible {
		visibleIDs[ev.EventID()] = struct{}{}
	}
	filtered := make([]types.StreamEvent, 0, len(visible))
	for _, ev := range events {
		if _, ok := visibleIDs[ev.EventID()]; ok {
			filtered = append(filtered, ev)
		}
	}
	return filtered, nil
}

// bundleTimeline adds bundled aggregations to a limited timeline, the same
// way as /sync does. Children after upTo are left for the client to
// aggregate from a later response. A failure serves the timeline without
// bundles, since they are only an optimisation for the client.
func (rp *RequestPool) bundleTimeline(
	ctx context.Context, snapshot storage.DatabaseTransaction, userID string,
	events []types.StreamEvent, clients []synctypes.ClientEvent, limited bool, upTo types.StreamPosition,
) {
	err := syncinternal.StripRelations(clients)
	if err != nil || !limited {
		return
	}
	user, err := spec.NewUserID(userID, true)
	if err != nil {
		return
	}
	headered := make([]*rstypes.HeaderedEvent, 0, len(events))
	for _, event := range events {
		headered = append(headered, event.HeaderedEvent)
	}
	bundler, err := syncinternal.NewRelationBundler(ctx, snapshot, rp.rsAPI, *user)
	if err == nil {
		err = bundler.UpTo(upTo).Bundle(ctx, headered, clients, synctypes.FormatSync)
	}
	if err != nil {
		logrus.WithError(err).WithField("user_id", userID).Warn("Failed to bundle relations, serving the timeline without them")
		_ = syncinternal.StripRelations(clients)
	}
}

// getRoomNameFromDB retrieves m.room.name state event from database.
func (rp *RequestPool) getRoomNameFromDB(ctx context.Context, snapshot storage.DatabaseTransaction, roomID string) string {
	event, err := snapshot.GetStateEvent(ctx, roomID, "m.room.name", "")
	if err == nil && event != nil {
		return gjson.GetBytes(event.Content(), "name").Str
	}
	// The sync consumer can briefly lag behind createRoom. Roomserver state is
	// authoritative, so use it as a fallback for initial room metadata.
	currentStateQuerier, ok := rp.rsAPI.(interface {
		QueryCurrentState(context.Context, *roomserverAPI.QueryCurrentStateRequest, *roomserverAPI.QueryCurrentStateResponse) error
	})
	if !ok {
		return ""
	}
	var response roomserverAPI.QueryCurrentStateResponse
	if queryErr := currentStateQuerier.QueryCurrentState(ctx, &roomserverAPI.QueryCurrentStateRequest{
		RoomID: roomID,
		StateTuples: []gomatrixserverlib.StateKeyTuple{{
			EventType: spec.MRoomName,
			StateKey:  "",
		}},
	}, &response); queryErr == nil {
		if event := response.StateEvents[gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomName, StateKey: ""}]; event != nil {
			return gjson.GetBytes(event.Content(), "name").Str
		}
	}
	return ""
}

// getRoomAvatar retrieves m.room.avatar state event.
func (rp *RequestPool) getRoomAvatar(ctx context.Context, snapshot storage.DatabaseTransaction, roomID string) string {
	event, err := snapshot.GetStateEvent(ctx, roomID, "m.room.avatar", "")
	if err != nil || event == nil {
		return ""
	}

	return gjson.GetBytes(event.Content(), "url").Str
}

// getRoomTopic retrieves m.room.topic state event.
func (rp *RequestPool) getRoomTopic(ctx context.Context, snapshot storage.DatabaseTransaction, roomID string) string {
	event, err := snapshot.GetStateEvent(ctx, roomID, "m.room.topic", "")
	if err != nil || event == nil {
		return ""
	}

	return gjson.GetBytes(event.Content(), "topic").Str
}

// getRequiredState retrieves and filters state events based on required_state configuration
// Phase 4: Supports include/exclude patterns with wildcard matching
// Phase 5: Supports $LAZY pattern for lazy member loading.
//
// Incremental syncs also merge in getRequiredStateDelta's result; the
// current-state fetch still runs when lazySenders is non-empty, for $LAZY's
// point-in-time snapshot.
func (rp *RequestPool) getRequiredState(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	userID string,
	config *types.RequiredStateConfig,
	timeline []synctypes.ClientEvent,
	isInitialForRoom bool,
	roomState types.RoomStreamState,
	currentPos types.StreamingToken,
) ([]synctypes.ClientEvent, error) {
	reqStateRegion, _ := internal.StartRegion(ctx, "SlidingSync.getRequiredState")
	defer reqStateRegion.EndRegion()
	reqStateRegion.SetTag("room_id", roomID)

	lazySenders := rp.extractLazySenders(config, timeline)

	// Analyze the include patterns to determine if we can use targeted queries
	// instead of loading all state and filtering in Go.
	var filtered []*rstypes.HeaderedEvent
	if isInitialForRoom || len(lazySenders) > 0 {
		var err error
		filtered, err = rp.fetchCurrentRequiredState(ctx, snapshot, roomID, userID, config, lazySenders, reqStateRegion)
		if err != nil {
			return nil, err
		}
	}

	if !isInitialForRoom {
		deltaEvents, truncated, err := rp.getRequiredStateDelta(ctx, snapshot, roomID, userID, config, lazySenders, roomState, currentPos)
		if err != nil {
			return nil, err
		}
		if truncated {
			// More matching events changed in the gap than one scan returns,
			// and the scan keeps the newest, so the delta is missing the oldest
			// of them. Current state is complete by construction, so fall back
			// to it rather than ship a delta with holes in it.
			logrus.WithFields(logrus.Fields{
				"room_id": roomID,
				"user_id": userID,
				"limit":   requiredStateDeltaLimit,
			}).Warn("[REQUIRED_STATE] Delta hit the scan limit; falling back to current state")
			filtered, err = rp.fetchCurrentRequiredState(ctx, snapshot, roomID, userID, config, lazySenders, reqStateRegion)
			if err != nil {
				return nil, err
			}
		} else {
			filtered = mergeUniqueStateEvents(filtered, deltaEvents)
		}
		reqStateRegion.SetTag("delta_state_events", len(deltaEvents))
	}

	// Apply exclude patterns
	if len(config.Exclude) > 0 {
		var kept []*rstypes.HeaderedEvent
		for _, event := range filtered {
			if !rp.matchesExcludePatterns(event, userID, config, lazySenders) {
				kept = append(kept, event)
			}
		}
		filtered = kept
	}

	reqStateRegion.SetTag("filtered_state_events", len(filtered))

	clientEvents := make([]synctypes.ClientEvent, 0, len(filtered))
	for _, event := range filtered {
		clientEvent, err := synctypes.ToClientEvent(event, synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return rp.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})
		if err != nil {
			logrus.WithError(err).WithField("event_id", event.EventID()).Warn("Failed to convert state event to client format")
			continue
		}
		clientEvents = append(clientEvents, *clientEvent)
	}

	return clientEvents, nil
}

// fetchCurrentRequiredState returns the room's current state events matching
// config, by targeted query where the patterns allow it and by loading all
// state otherwise.
func (rp *RequestPool) fetchCurrentRequiredState(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	userID string,
	config *types.RequiredStateConfig,
	lazySenders map[string]bool,
	reqStateRegion internal.Trace,
) ([]*rstypes.HeaderedEvent, error) {
	if rp.canUseTargetedStateQueries(config) {
		reqStateRegion.SetTag("query_mode", "targeted")
		return rp.fetchTargetedState(ctx, snapshot, roomID, userID, config, lazySenders)
	}

	// Fallback: wildcard patterns require loading all state
	emptyFilter := synctypes.StateFilter{}
	allState, err := snapshot.GetStateEventsForRoom(ctx, roomID, &emptyFilter)
	if err != nil {
		return nil, err
	}
	reqStateRegion.SetTag("query_mode", "full")
	reqStateRegion.SetTag("total_state_events", len(allState))

	var filtered []*rstypes.HeaderedEvent
	for _, event := range allState {
		if rp.matchesRequiredState(event, userID, config, lazySenders) {
			filtered = append(filtered, event)
		}
	}
	return filtered, nil
}

// getRequiredStateDelta fetches state events matching config's patterns that
// changed within (roomState.LastToken, currentPos]. Mirrors Synapse's
// get_current_state_deltas, reusing the already gap-aware RecentEvents.
func (rp *RequestPool) getRequiredStateDelta(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	userID string,
	config *types.RequiredStateConfig,
	lazySenders map[string]bool,
	roomState types.RoomStreamState,
	currentPos types.StreamingToken,
) (delta []*rstypes.HeaderedEvent, truncated bool, err error) {
	if roomState.LastToken == nil {
		// No prior per-room position to delta from. Shouldn't normally happen
		// for a non-initial room, but be safe: skip rather than risk scanning
		// an unbounded range.
		return nil, false, nil
	}

	deltaFilter := &synctypes.RoomEventFilter{
		Limit: requiredStateDeltaLimit,
		// Scanning every event in the gap to keep a handful of state changes is
		// wasteful, and the limit below is spent on timeline messages that can
		// never match. Ask only for the types the config can actually match.
		Types: requiredStateTypes(config),
	}
	recentEvents, err := snapshot.RecentEvents(
		ctx,
		[]string{roomID},
		types.Range{
			From:      currentPos.PDUPosition,
			To:        roomState.LastToken.PDUPosition,
			Backwards: true,
		},
		deltaFilter,
		true, // chronological order
		true, // only sync events
	)
	if err != nil {
		return nil, false, err
	}

	events, ok := recentEvents[roomID]
	if !ok {
		return nil, false, nil
	}

	var result []*rstypes.HeaderedEvent
	for _, ev := range events.Events {
		if ev.StateKey() == nil {
			continue // not a state event
		}
		if rp.matchesRequiredState(ev.HeaderedEvent, userID, config, lazySenders) {
			result = append(result, ev.HeaderedEvent)
		}
	}
	return result, events.Limited, nil
}

// requiredStateDeltaLimit caps one delta scan. The query orders by position
// descending, so hitting the cap drops the OLDEST changes in the gap - exactly
// the ones a delta exists to recover - which is why callers must react to the
// truncation rather than ignore it.
const requiredStateDeltaLimit = 1000

// requiredStateTypes lists the event types config can match, or nil when a
// wildcard type means anything can match and no filter is possible.
func requiredStateTypes(config *types.RequiredStateConfig) *[]string {
	eventTypes := make([]string, 0, len(config.Include))
	for _, pattern := range config.Include {
		if len(pattern) != 2 { //nolint:mnd // a pattern is [type, state_key]
			continue
		}
		if pattern[0] == "*" {
			return nil
		}
		eventTypes = append(eventTypes, pattern[0])
	}
	if len(eventTypes) == 0 {
		return nil
	}
	return &eventTypes
}

// stateEventKey identifies the state slot an event occupies, matching the
// (type, state_key) pair the room's current state is keyed on.
func stateEventKey(ev *rstypes.HeaderedEvent) string {
	stateKey := ""
	if ev.StateKey() != nil {
		stateKey = *ev.StateKey()
	}
	return ev.Type() + "|" + stateKey
}

// mergeUniqueStateEvents appends additional to existing, keeping one event per
// (type, state_key).
//
// The existing slice holds current state, so it wins any slot it already
// covers - the current-state and delta fetches legitimately overlap (e.g. a
// $LAZY member who also changed). Within additional the LAST event wins: it arrives
// in chronological order and may hold several events for one slot that changed
// more than once inside the gap, and keeping the first would deliver a stale
// value that never corrects itself, since the next sync resumes above it.
func mergeUniqueStateEvents(existing, additional []*rstypes.HeaderedEvent) []*rstypes.HeaderedEvent {
	if len(additional) == 0 {
		return existing
	}

	covered := make(map[string]bool, len(existing))
	for _, ev := range existing {
		covered[stateEventKey(ev)] = true
	}

	at := make(map[string]int, len(additional))
	result := existing
	for _, ev := range additional {
		key := stateEventKey(ev)
		if covered[key] {
			continue
		}
		if i, ok := at[key]; ok {
			result[i] = ev // a later change to the same slot supersedes it
			continue
		}
		at[key] = len(result)
		result = append(result, ev)
	}
	return result
}

// canUseTargetedStateQueries checks whether the required_state patterns can be
// satisfied with targeted DB queries instead of loading all state. Returns false
// if any pattern uses a wildcard type (e.g. ["*", "*"] or ["*", ""]), which
// requires loading all state to evaluate.
func (rp *RequestPool) canUseTargetedStateQueries(config *types.RequiredStateConfig) bool {
	for _, pattern := range config.Include {
		if len(pattern) != 2 { //nolint:mnd
			continue
		}
		if pattern[0] == "*" {
			return false // Wildcard type — must load all state
		}
	}
	return true
}

// fetchTargetedState fetches only the specific state events matching the
// required_state patterns using targeted DB queries. For non-member types,
// it uses a single filtered query. For member events, it uses point queries
// for each specific state_key ($ME, $LAZY senders, or literal keys).
// This avoids loading all 27k member events for large rooms when the client
// only needs a handful.
func (rp *RequestPool) fetchTargetedState(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	userID string,
	config *types.RequiredStateConfig,
	lazySenders map[string]bool,
) ([]*rstypes.HeaderedEvent, error) {
	// Collect non-member types and member state_keys separately.
	nonMemberTypes := map[string]bool{}
	// memberKeys tracks specific state_keys for m.room.member point queries.
	// nil means "not requested", empty means "requested but no keys resolved yet".
	var memberKeys map[string]bool
	needAllMembers := false

	// lazy_members is shorthand for the $LAZY include pattern. Add the
	// senders from the timeline even when include is otherwise empty.
	if config.LazyMembers && len(lazySenders) > 0 {
		memberKeys = make(map[string]bool, len(lazySenders))
		for sender := range lazySenders {
			memberKeys[sender] = true
		}
	}

	for _, pattern := range config.Include {
		if len(pattern) != 2 { //nolint:mnd
			continue
		}
		evType, stateKeyPattern := pattern[0], pattern[1]

		if evType == "m.room.member" {
			if memberKeys == nil {
				memberKeys = map[string]bool{}
			}
			switch stateKeyPattern {
			case "*":
				needAllMembers = true
			case "$ME":
				memberKeys[userID] = true
			case "$LAZY":
				for sender := range lazySenders {
					memberKeys[sender] = true
				}
			default:
				// Literal state_key
				memberKeys[stateKeyPattern] = true
			}
		} else {
			// For non-member types with a specific state_key, we still collect
			// the type and fetch via the type filter. The state_key filtering
			// happens in Go since the SQL doesn't support state_key filtering,
			// but non-member types are few so this is fine.
			nonMemberTypes[evType] = true
		}
	}

	// Keep the tuples for a roomserver fallback. The sync database is updated
	// asynchronously from the roomserver output stream, so a just-joined room
	// can have authoritative current state before the sync state table catches
	// up. This fallback only fills missing state slots; the sync snapshot remains
	// the source for any slots it already has.
	stateTuples := make([]gomatrixserverlib.StateKeyTuple, 0, len(config.Include)+len(memberKeys))
	seenTuples := make(map[gomatrixserverlib.StateKeyTuple]struct{})
	addTuple := func(eventType, stateKey string) {
		tuple := gomatrixserverlib.StateKeyTuple{EventType: eventType, StateKey: stateKey}
		if _, exists := seenTuples[tuple]; !exists {
			seenTuples[tuple] = struct{}{}
			stateTuples = append(stateTuples, tuple)
		}
	}
	for _, pattern := range config.Include {
		if len(pattern) != 2 || pattern[0] == "*" {
			continue
		}
		eventType, stateKey := pattern[0], pattern[1]
		if eventType == "m.room.member" {
			switch stateKey {
			case "*":
				continue // The roomserver wildcard could return an unbounded member list.
			case "$ME":
				stateKey = userID
			case "$LAZY":
				for sender := range lazySenders {
					addTuple(eventType, sender)
				}
				continue
			}
		}
		addTuple(eventType, stateKey)
	}
	// lazy_members is shorthand for requesting the timeline senders' membership
	// events. Include those keys in the roomserver fallback too, even when the
	// caller did not provide an explicit $LAZY include pattern.
	for stateKey := range memberKeys {
		addTuple("m.room.member", stateKey)
	}

	var result []*rstypes.HeaderedEvent

	// Fetch non-member state events using a type filter.
	if len(nonMemberTypes) > 0 {
		types := make([]string, 0, len(nonMemberTypes))
		for t := range nonMemberTypes {
			types = append(types, t)
		}
		filter := synctypes.StateFilter{Types: &types}
		events, err := snapshot.GetStateEventsForRoom(ctx, roomID, &filter)
		if err != nil {
			return nil, err
		}
		// Apply state_key matching for non-member events (since SQL only filters by type).
		for _, ev := range events {
			stateKey := ""
			if ev.StateKey() != nil {
				stateKey = *ev.StateKey()
			}
			if rp.stateKeyMatchesInclude(ev.Type(), stateKey, userID, config, lazySenders) {
				result = append(result, ev)
			}
		}
	}

	// Fetch member events.
	if needAllMembers {
		// Client wants all members — use type filter for m.room.member.
		memberType := []string{"m.room.member"}
		filter := synctypes.StateFilter{Types: &memberType}
		events, err := snapshot.GetStateEventsForRoom(ctx, roomID, &filter)
		if err != nil {
			return nil, err
		}
		result = append(result, events...)
	} else if len(memberKeys) > 0 {
		// Point queries for specific members — O(timeline_limit) queries, each
		// an indexed lookup on (room_id, type, state_key).
		for stateKey := range memberKeys {
			ev, err := snapshot.GetStateEvent(ctx, roomID, "m.room.member", stateKey)
			if err != nil {
				return nil, err
			}
			if ev != nil {
				result = append(result, ev)
			}
		}
	}

	if len(stateTuples) > 0 {
		currentStateQuerier, ok := rp.rsAPI.(interface {
			QueryCurrentState(context.Context, *roomserverAPI.QueryCurrentStateRequest, *roomserverAPI.QueryCurrentStateResponse) error
		})
		if ok {
			var response roomserverAPI.QueryCurrentStateResponse
			if err := currentStateQuerier.QueryCurrentState(ctx, &roomserverAPI.QueryCurrentStateRequest{
				RoomID:         roomID,
				AllowWildcards: true,
				StateTuples:    stateTuples,
			}, &response); err == nil {
				existing := make(map[string]bool, len(result))
				for _, event := range result {
					existing[stateEventKey(event)] = true
				}
				for _, event := range response.StateEvents {
					if event == nil || event.StateKey() == nil || existing[stateEventKey(event)] {
						continue
					}
					if rp.matchesRequiredState(event, userID, config, lazySenders) {
						result = append(result, event)
						existing[stateEventKey(event)] = true
					}
				}
			}
		}
	}

	return result, nil
}

// stateKeyMatchesInclude checks if an event's type+state_key matches any
// include pattern. Used for non-member events after type-filtered DB query.
func (rp *RequestPool) stateKeyMatchesInclude(
	evType, stateKey, userID string,
	config *types.RequiredStateConfig,
	lazySenders map[string]bool,
) bool {
	for _, pattern := range config.Include {
		if len(pattern) != 2 { //nolint:mnd
			continue
		}
		if matchesPattern(evType, pattern[0]) && matchesStateKeyPattern(stateKey, pattern[1], userID, lazySenders) {
			return true
		}
	}
	return false
}

// matchesExcludePatterns checks if an event matches any exclude pattern.
func (rp *RequestPool) matchesExcludePatterns(
	event *rstypes.HeaderedEvent,
	userID string,
	config *types.RequiredStateConfig,
	lazySenders map[string]bool,
) bool {
	eventType := event.Type()
	stateKey := ""
	if event.StateKey() != nil {
		stateKey = *event.StateKey()
	}
	for _, pattern := range config.Exclude {
		if len(pattern) == 2 { //nolint:mnd
			if matchesPattern(eventType, pattern[0]) && matchesStateKeyPattern(stateKey, pattern[1], userID, lazySenders) {
				return true
			}
		}
	}
	return false
}

// matchesRequiredState checks if an event matches the required_state configuration
// Phase 5: Added lazySenders parameter for $LAZY pattern matching.
func (rp *RequestPool) matchesRequiredState(
	event *rstypes.HeaderedEvent,
	userID string,
	config *types.RequiredStateConfig,
	lazySenders map[string]bool,
) bool {
	eventType := event.Type()
	stateKey := ""
	if event.StateKey() != nil {
		stateKey = *event.StateKey()
	}

	// Check if excluded
	for _, pattern := range config.Exclude {
		if len(pattern) == 2 { //nolint:mnd
			if matchesPattern(eventType, pattern[0]) && matchesStateKeyPattern(stateKey, pattern[1], userID, lazySenders) {
				return false // Explicitly excluded
			}
		}
	}

	// lazy_members is equivalent to including $LAZY members.
	if config.LazyMembers && eventType == "m.room.member" && lazySenders[stateKey] {
		return true
	}

	// Check if included
	for _, pattern := range config.Include {
		if len(pattern) == 2 { //nolint:mnd
			if matchesPattern(eventType, pattern[0]) && matchesStateKeyPattern(stateKey, pattern[1], userID, lazySenders) {
				// Debug: Log $ME membership matches
				if pattern[0] == "m.room.member" && pattern[1] == "$ME" {
					logrus.WithFields(logrus.Fields{
						"event_type": eventType,
						"state_key":  stateKey,
						"user_id":    userID,
						"event_id":   event.EventID(),
						"matched":    true,
					}).Debug("[REQUIRED_STATE] $ME membership pattern matched")
				}
				return true // Matches include pattern
			}
		}
	}

	return false // Not included
}

// matchesPattern checks if a value matches a pattern (supports "*" wildcard).
func matchesPattern(value, pattern string) bool {
	if pattern == "*" {
		return true
	}
	// TODO: Support prefix wildcards like "m.room.*"
	return value == pattern
}

// matchesStateKeyPattern checks if a state key matches a pattern
// Supports "*" wildcard, "$ME" (current user), and "$LAZY" (timeline senders).
func matchesStateKeyPattern(stateKey, pattern, userID string, lazySenders map[string]bool) bool {
	if pattern == "*" {
		return true
	}
	if pattern == "$ME" {
		return stateKey == userID
	}
	// Phase 5: $LAZY pattern - only include if in timeline senders
	if pattern == "$LAZY" {
		if lazySenders == nil {
			return false // No timeline, no lazy members
		}
		return lazySenders[stateKey]
	}
	return stateKey == pattern
}

// extractLazySenders extracts sender IDs from timeline events if $LAZY is specified
// Phase 5: Returns a map of sender IDs for lazy member loading.
func (rp *RequestPool) extractLazySenders(config *types.RequiredStateConfig, timeline []synctypes.ClientEvent) map[string]bool {
	// lazy_members is the MSC4186 spelling of the legacy $LAZY include
	// pattern.
	hasLazy := config.LazyMembers
	for _, pattern := range config.Include {
		if len(pattern) == 2 && pattern[1] == "$LAZY" {
			hasLazy = true
			break
		}
	}

	if !hasLazy {
		return nil
	}

	// Extract unique sender IDs from timeline
	senders := make(map[string]bool)
	for _, event := range timeline {
		if event.Sender != "" {
			senders[event.Sender] = true
		}
	}

	return senders
}

// BumpEventTypes defines the event types that count as "activity" for bump_stamp calculation
// Per MSC4186/Synapse, only these events should bump a room to the top of the list.
var BumpEventTypes = map[string]bool{
	"m.room.create":    true,
	"m.room.message":   true,
	"m.room.encrypted": true,
	"m.sticker":        true,
	"m.call.invite":    true,
	"m.poll.start":     true,
	"m.beacon_info":    true,
}

// calculateBumpStamp calculates the stream position of the most recent "bumping" event
// Returns an opaque integer (stream position) for use in client-side room sorting
// Per MSC4186: Only specific event types count as "bump" events.
func (rp *RequestPool) calculateBumpStamp(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	timeline []synctypes.ClientEvent,
) int64 {
	// Strategy 1: Check timeline for the most recent bump event
	// Timeline is in chronological order (oldest first), so iterate backwards
	for i := len(timeline) - 1; i >= 0; i-- {
		event := timeline[i]
		if BumpEventTypes[event.Type] {
			// Found a bump event - use its stream position if available
			// Note: ClientEvent doesn't have stream position, so we'll use timestamp as fallback
			// The timestamp is still useful for sorting since newer events have higher timestamps
			return int64(event.OriginServerTS)
		}
	}

	// Strategy 2: No bump events in timeline - query database
	// Use the efficient MaxStreamPositionsForRooms query which filters by bump event types
	bumpStamps, err := snapshot.MaxStreamPositionsForRooms(ctx, []string{roomID})
	if err != nil {
		logrus.WithError(err).WithField("room_id", roomID).Warn("Failed to get bump_stamp from database")
		return 0
	}

	if pos, ok := bumpStamps[roomID]; ok {
		return int64(pos)
	}

	// No bump events found - use 0 (room will sort to bottom)
	return 0
}

// buildInviteRoomData constructs room data for an invited room
// Returns stripped state events for room preview (MSC4186 Section 4.3)
// Phase 11: Element X compatibility
// inviteEvent contains the invite_room_state in its unsigned field for federated invites.
func (rp *RequestPool) buildInviteRoomData(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	userID string,
	isInitial bool,
	inviteEvent *rstypes.HeaderedEvent,
) (*types.SlidingRoomData, error) {
	roomData := rp.buildStrippedRoomData(ctx, snapshot, roomID, userID, isInitial, inviteEvent, "invite_room_state")
	// MSC4186 spec uses "stripped_state", Synapse/Element X use "invite_state"
	roomData.InviteState = roomData.StrippedState
	// buildStrippedRoomData starts a new room object, so carry the membership
	// that caused this path over to the response.
	roomData.Membership = spec.Invite
	return roomData, nil
}

// buildKnockRoomData constructs room data for a room the user knocked on,
// from the knock_room_state recorded with the knock. Only stripped_state is
// set, since invite_state would make clients treat the room as an invite.
func (rp *RequestPool) buildKnockRoomData(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	userID string,
	isInitial bool,
	knockEvent *rstypes.HeaderedEvent,
) (*types.SlidingRoomData, error) {
	roomData := rp.buildStrippedRoomData(ctx, snapshot, roomID, userID, isInitial, knockEvent, "knock_room_state")
	roomData.Membership = spec.Knock
	return roomData, nil
}

// buildStrippedRoomData builds the stripped state of an invited or knocked
// room from the stripped state stored under unsignedKey on the membership
// event, falling back to local state for rooms this server is in.
func (rp *RequestPool) buildStrippedRoomData(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	userID string,
	isInitial bool,
	inviteEvent *rstypes.HeaderedEvent,
	unsignedKey string,
) *types.SlidingRoomData {
	roomData := &types.SlidingRoomData{
		Initial: isInitial,
	}

	var strippedEvents []synctypes.ClientEvent

	// For federated invites, the room state is embedded in the invite event's unsigned field
	// as "invite_room_state". This is the same approach V3 sync uses (see NewInviteResponse).
	if inviteEvent != nil {
		if inviteRoomState := gjson.GetBytes(inviteEvent.Unsigned(), unsignedKey); inviteRoomState.IsArray() {
			// Parse the invite_room_state array
			for _, stateEvent := range inviteRoomState.Array() {
				// Convert raw JSON to ClientEvent
				clientEvent := synctypes.ClientEvent{
					Type:     stateEvent.Get("type").String(),
					StateKey: func() *string { s := stateEvent.Get("state_key").String(); return &s }(),
					Sender:   stateEvent.Get("sender").String(),
					Content:  spec.RawJSON(stateEvent.Get("content").Raw),
				}
				strippedEvents = append(strippedEvents, clientEvent)

				// Extract room metadata from stripped state
				switch clientEvent.Type {
				case "m.room.name":
					roomData.Name = gjson.GetBytes(clientEvent.Content, "name").String()
				case "m.room.avatar":
					roomData.AvatarURL = gjson.GetBytes(clientEvent.Content, "url").String()
				case "m.room.topic":
					roomData.Topic = gjson.GetBytes(clientEvent.Content, "topic").String()
				}
			}

			// Add the invite event itself (the m.room.member event for this user)
			inviteClientEvent, err := synctypes.ToClientEvent(inviteEvent, synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return rp.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
			})
			if err == nil {
				// Clear unsigned to not expose internal data
				inviteClientEvent.Unsigned = nil
				strippedEvents = append(strippedEvents, *inviteClientEvent)
			}
		}
	}

	// Fallback: If no invite_room_state (local invites), try to get state from local DB
	if len(strippedEvents) == 0 {
		strippedStateTypes := []struct {
			eventType string
			stateKey  string
		}{
			{"m.room.create", ""},
			{"m.room.name", ""},
			{"m.room.avatar", ""},
			{"m.room.topic", ""},
			{"m.room.join_rules", ""},
			{"m.room.encryption", ""},
			{"m.room.member", userID},
		}

		for _, stateType := range strippedStateTypes {
			event, err := snapshot.GetStateEvent(ctx, roomID, stateType.eventType, stateType.stateKey)
			if err != nil || event == nil {
				continue
			}

			clientEvent, err := synctypes.ToClientEvent(event, synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return rp.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
			})
			if err != nil {
				continue
			}

			strippedEvents = append(strippedEvents, *clientEvent)
		}

		// Get metadata from local DB for local invites
		if roomData.Name == "" {
			roomData.Name = rp.getRoomNameFromDB(ctx, snapshot, roomID)
		}
		if roomData.AvatarURL == "" {
			roomData.AvatarURL = rp.getRoomAvatar(ctx, snapshot, roomID)
		}
		if roomData.Topic == "" {
			roomData.Topic = rp.getRoomTopic(ctx, snapshot, roomID)
		}
	}

	roomData.StrippedState = strippedEvents

	logrus.WithFields(logrus.Fields{
		"room_id":          roomID,
		"stripped_count":   len(strippedEvents),
		"name":             roomData.Name,
		"has_invite_event": inviteEvent != nil,
		"unsigned_key":     unsignedKey,
	}).Debug("[V4_SYNC] Built stripped room data")

	return roomData
}

// getHeroes fetches room heroes with displayname and avatar_url in MSC4186 format.
// Heroes are used for rooms without explicit names to show "User A, User B" style names.
// Per MSC4186: heroes should include up to 5 members (excluding the current user).
func (rp *RequestPool) getHeroes(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	roomID string,
	userID string,
) []types.MSC4186Hero {
	// Get room summary which includes hero user IDs
	summary, err := snapshot.GetRoomSummary(ctx, roomID, userID)
	if err != nil {
		logrus.WithError(err).WithField("room_id", roomID).Warn("[V4_SYNC] Failed to get room summary for heroes")
		return nil
	}

	if len(summary.Heroes) == 0 {
		return nil
	}

	heroes := make([]types.MSC4186Hero, 0, len(summary.Heroes))

	// For each hero user ID, fetch their member event to get displayname and avatar_url
	for _, heroUserID := range summary.Heroes {
		hero := types.MSC4186Hero{
			UserID: heroUserID,
		}

		// Get the member event for this user
		memberEvent, err := snapshot.GetStateEvent(ctx, roomID, spec.MRoomMember, heroUserID)
		if err != nil || memberEvent == nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"room_id": roomID,
				"user_id": heroUserID,
			}).Debug("[V4_SYNC] Could not get member event for hero")
			// Still include the hero with just the user ID
			heroes = append(heroes, hero)
			continue
		}

		// Parse displayname and avatar_url from member event content
		content := memberEvent.Content()
		if displayname := gjson.GetBytes(content, "displayname").String(); displayname != "" {
			hero.Displayname = displayname
		}
		if avatarURL := gjson.GetBytes(content, "avatar_url").String(); avatarURL != "" {
			hero.AvatarURL = avatarURL
		}

		heroes = append(heroes, hero)
	}

	return heroes
}
