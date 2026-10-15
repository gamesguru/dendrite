// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"fmt"
	"math"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
)

//nolint:gochecknoinits
func init() {
	prometheus.MustRegister(calculateHistoryVisibilityDuration)
}

// calculateHistoryVisibilityDuration stores the time it takes to
// calculate the history visibility.
var calculateHistoryVisibilityDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "zendrite",
		Subsystem: "syncapi",
		Name:      "calculateHistoryVisibility_duration_millis",
		Help:      "How long it takes to calculate the history visibility",
		Buckets: []float64{ // milliseconds
			5, 10, 25, 50, 75, 100, 250, 500,
			1000, 2000, 3000, 4000, 5000, 6000,
			7000, 8000, 9000, 10000, 15000, 20000,
		},
	},
	[]string{"api"},
)

var historyVisibilityPriority = map[gomatrixserverlib.HistoryVisibility]uint8{
	spec.WorldReadable:                         0,
	gomatrixserverlib.HistoryVisibilityShared:  1,
	gomatrixserverlib.HistoryVisibilityInvited: 2, //nolint:mnd
	gomatrixserverlib.HistoryVisibilityJoined:  3, //nolint:mnd
}

// eventVisibility contains the history visibility and membership state at a given event.
type eventVisibility struct {
	visibility        gomatrixserverlib.HistoryVisibility
	membershipAtEvent string
	membershipCurrent string
}

// member reports whether the user saw the event as a member of the room:
// joined, or invited under invited visibility. Only other viewers are served
// redacted copies of events sent by erased users (MSC4025).
func (ev eventVisibility) member() bool {
	return ev.membershipAtEvent == spec.Join ||
		(ev.membershipAtEvent == spec.Invite && ev.visibility == gomatrixserverlib.HistoryVisibilityInvited)
}

// allowed checks the eventVisibility if the user is allowed to see the event.
// Rules as defined by https://spec.matrix.org/v1.3/client-server-api/#server-behaviour-5
func (ev eventVisibility) allowed() (allowed bool) {
	switch ev.visibility {
	case gomatrixserverlib.HistoryVisibilityWorldReadable:
		// If the history_visibility was set to world_readable, allow.
		return true
	case gomatrixserverlib.HistoryVisibilityJoined:
		// If the user’s membership was join, allow.
		if ev.membershipAtEvent == spec.Join {
			return true
		}
		return false
	case gomatrixserverlib.HistoryVisibilityShared:
		// If the user’s membership was join, allow.
		// If history_visibility was set to shared, and the user joined the room at any point after the event was sent, allow.
		if ev.membershipAtEvent == spec.Join || ev.membershipCurrent == spec.Join {
			return true
		}
		return false
	case gomatrixserverlib.HistoryVisibilityInvited:
		// If the user’s membership was join, allow.
		if ev.membershipAtEvent == spec.Join {
			return true
		}
		if ev.membershipAtEvent == spec.Invite {
			return true
		}
		return false
	default:
		return false
	}
}

// The endpoint names of sync v2 and sliding sync timelines for
// ApplyHistoryVisibilityFilter.
const (
	SyncEndpoint        = "sync"
	SlidingSyncEndpoint = "sliding_sync"
)

// ApplyHistoryVisibilityFilter applies the room history visibility filter on types.HeaderedEvents.
// Returns the filtered events and an error, if any.
//
// This function assumes that all provided events are from the same room.
func ApplyHistoryVisibilityFilter(
	ctx context.Context,
	syncDB storage.DatabaseTransaction,
	rsAPI api.SyncRoomserverAPI,
	events []*types.HeaderedEvent,
	alwaysIncludeEventIDs map[string]struct{},
	userID spec.UserID, endpoint string,
) ([]*types.HeaderedEvent, error) {
	if len(events) == 0 {
		return events, nil
	}
	start := time.Now()

	// try to get the current membership of the user
	membershipCurrent, _, err := syncDB.SelectMembershipForUser(ctx, events[0].RoomID().String(), userID.String(), math.MaxInt64)
	if err != nil {
		return nil, err
	}

	// Get the mapping from eventID -> eventVisibility
	eventsFiltered := make([]*types.HeaderedEvent, 0, len(events))
	firstEvRoomID := events[0].RoomID()
	senderID, err := rsAPI.QuerySenderIDForUser(ctx, firstEvRoomID, userID)
	if err != nil {
		return nil, err
	}
	visibilities, membershipKnown := visibilityForEvents(ctx, rsAPI, events, senderID, firstEvRoomID)

	// MSC4354: joined users see unexpired sticky events whatever the history
	// visibility. Sync v2 and sliding sync timelines keep the usual rules, as
	// in Synapse, since both deliver sticky events in a section of their own.
	now := time.Now()
	showSticky := membershipCurrent == spec.Join && endpoint != SyncEndpoint && endpoint != SlidingSyncEndpoint
	// Indexes into eventsFiltered of events visible without membership.
	var nonMemberIdx []int

	for _, ev := range events {
		// Validate same room assumption
		if ev.RoomID().String() != firstEvRoomID.String() {
			return nil, fmt.Errorf("events from different rooms supplied to ApplyHistoryVisibilityFilter")
		}

		evVis := visibilities[ev.EventID()]
		evVis.membershipCurrent = membershipCurrent
		// Always include specific state events for /sync responses
		if alwaysIncludeEventIDs != nil {
			if _, ok := alwaysIncludeEventIDs[ev.EventID()]; ok {
				eventsFiltered = append(eventsFiltered, ev)
				continue
			}
		}

		// The time of receipt is not known here; origin_server_ts stands in
		// for it unless it lies in the future.
		if showSticky && ev.IsSticky(now, now) {
			eventsFiltered = append(eventsFiltered, ev)
			continue
		}

		// NOTSPEC: Always allow user to see their own membership events (spec contains more "rules")
		if senderID != nil {
			if ev.Type() == spec.MRoomMember && ev.StateKeyEquals(string(*senderID)) {
				eventsFiltered = append(eventsFiltered, ev)
				continue
			}
		}

		// Always allow history evVis events on boundaries. This is done
		// by setting the effective evVis to the least restrictive
		// of the old vs new.
		// https://spec.matrix.org/v1.3/client-server-api/#server-behaviour-5
		if ev.Type() == spec.MRoomHistoryVisibility {
			hisVis, err := ev.HistoryVisibility()

			if err == nil && hisVis != "" {
				prevHisVis := gjson.GetBytes(ev.Unsigned(), "prev_content.history_visibility").String()
				oldPrio, ok := historyVisibilityPriority[gomatrixserverlib.HistoryVisibility(prevHisVis)]
				// if we can't get the previous history visibility, default to shared.
				if !ok {
					oldPrio = historyVisibilityPriority[gomatrixserverlib.HistoryVisibilityShared]
				}
				// no OK check, since this should have been validated when setting the value
				newPrio := historyVisibilityPriority[hisVis]
				if oldPrio < newPrio {
					evVis.visibility = gomatrixserverlib.HistoryVisibility(prevHisVis)
				} else {
					evVis.visibility = hisVis
				}
			}
		}
		// do the actual check
		allowed := evVis.allowed()
		if allowed {
			if !evVis.member() {
				nonMemberIdx = append(nonMemberIdx, len(eventsFiltered))
			}
			eventsFiltered = append(eventsFiltered, ev)
		}
	}
	if err = redactErasedSenders(ctx, rsAPI, eventsFiltered, nonMemberIdx); err != nil {
		return nil, err
	}
	if membershipKnown {
		annotateRequesterMembership(eventsFiltered, visibilities, senderID)
	}
	calculateHistoryVisibilityDuration.With(prometheus.Labels{"api": endpoint}).Observe(float64(time.Since(start).Milliseconds()))
	return eventsFiltered, nil
}

// redactErasedSenders replaces the events at the given indexes, which the
// user sees without having been a member of the room when they were sent,
// with redacted copies if their sender is erased (MSC4025).
func redactErasedSenders(
	ctx context.Context, rsAPI api.SyncRoomserverAPI,
	events []*types.HeaderedEvent, indexes []int,
) error {
	if len(indexes) == 0 {
		return nil
	}
	senders := make(map[string][]int, len(indexes)) // user ID -> indexes
	for _, i := range indexes {
		ev := events[i]
		userID, err := rsAPI.QueryUserIDForSender(ctx, ev.RoomID(), ev.SenderID())
		if err != nil || userID == nil {
			continue
		}
		senders[userID.String()] = append(senders[userID.String()], i)
	}
	if len(senders) == 0 {
		return nil
	}
	userIDs := make([]string, 0, len(senders))
	for userID := range senders {
		userIDs = append(userIDs, userID)
	}
	erased, err := rsAPI.QueryErasedUsers(ctx, userIDs)
	if err != nil {
		return fmt.Errorf("rsAPI.QueryErasedUsers: %w", err)
	}
	for userID := range erased {
		for _, i := range senders[userID] {
			redacted, err := types.RedactedCopy(events[i])
			if err != nil {
				return fmt.Errorf("types.RedactedCopy: %w", err)
			}
			copied := *events[i]
			copied.PDU = redacted
			events[i] = &copied
		}
	}
	return nil
}

// annotateRequesterMembership replaces the events with copies that carry the
// requesting user's membership at each event, which clients receive as
// unsigned.membership (MSC4115). The membership includes the change made by
// the event itself, so the user's own membership events carry their own
// membership. Copies keep events shared through caches unchanged.
func annotateRequesterMembership(events []*types.HeaderedEvent, visibilities map[string]eventVisibility, senderID *spec.SenderID) {
	for i, ev := range events {
		membership := spec.Leave
		if vis, ok := visibilities[ev.EventID()]; ok {
			membership = vis.membershipAtEvent
		}
		if senderID != nil && ev.Type() == spec.MRoomMember && ev.StateKeyEquals(string(*senderID)) {
			if own, err := ev.Membership(); err == nil {
				membership = own
			}
		}
		copied := *ev
		copied.MembershipForRequester = membership
		events[i] = &copied
	}
}

// visibilityForEvents returns a map from eventID to eventVisibility containing the visibility and the membership
// of `senderID` at the given event. If provided sender ID is nil, assume that membership is Leave.
// MembershipKnown is false if the roomserver could not calculate the memberships, which then
// default to leave.
func visibilityForEvents(
	ctx context.Context,
	rsAPI api.SyncRoomserverAPI,
	events []*types.HeaderedEvent,
	senderID *spec.SenderID, roomID spec.RoomID,
) (result map[string]eventVisibility, membershipKnown bool) {
	eventIDs := make([]string, len(events))
	for i := range events {
		eventIDs[i] = events[i].EventID()
	}

	result = make(map[string]eventVisibility, len(eventIDs))
	membershipKnown = true

	// get the membership events for all eventIDs
	var err error
	membershipEvents := make(map[string]*types.HeaderedEvent)
	if senderID != nil {
		membershipEvents, err = rsAPI.QueryMembershipAtEvent(ctx, roomID, eventIDs, *senderID)
		if err != nil {
			membershipKnown = false
			logrus.WithError(err).Error("visibilityForEvents: failed to fetch membership at event, defaulting to 'leave'")
		}
	}

	// Create a map from eventID -> eventVisibility
	for _, event := range events {
		eventID := event.EventID()
		vis := eventVisibility{
			membershipAtEvent: spec.Leave, // default to leave, to not expose events by accident
			visibility:        event.Visibility,
		}
		ev, ok := membershipEvents[eventID]
		if !ok || ev == nil {
			result[eventID] = vis
			continue
		}

		membership, err := ev.Membership()
		if err != nil {
			result[eventID] = vis
			continue
		}
		vis.membershipAtEvent = membership

		result[eventID] = vis
	}
	return result, membershipKnown
}

// ApplyTimelineHistoryVisibilityFilter applies the history visibility filter
// to the timeline of a sync response. It always keeps the timeline's state
// events that are part of the current room state, so the client learns the
// latest state even if it cannot see the event that set it.
func ApplyTimelineHistoryVisibilityFilter(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	rsAPI api.SyncRoomserverAPI,
	roomID, userID string,
	recentEvents []*types.HeaderedEvent,
	endpoint string,
) ([]*types.HeaderedEvent, error) {
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
	events, err := ApplyHistoryVisibilityFilter(ctx, snapshot, rsAPI, recentEvents, alwaysIncludeIDs, *parsedUserID, endpoint)
	if err != nil {
		return nil, err
	}
	logrus.WithFields(logrus.Fields{
		"duration": time.Since(startTime),
		"room_id":  roomID,
		"before":   len(recentEvents),
		"after":    len(events),
	}).Debugf("Applied history visibility (%s)", endpoint)
	return events, nil
}
