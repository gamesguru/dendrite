// Copyright 2025 Shane Jaroch
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/roomserver/state"
	"codefloe.com/pat-s/zendrite/roomserver/storage/shared"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// maxResyncWindow bounds how many events a resync recomputes the state of.
const maxResyncWindow = 100000

// windowEvent is an event received since the join that started a partial state.
type windowEvent struct {
	nid     types.EventNID
	id      string
	depth   int64
	prevIDs []string
}

// recomputeWindowState gives every event received since the partial state join
// the state it would have had with the room's full state, as Synapse does when it
// de-partial-states an event. The events were stored with state that lacked the
// members the join left out, and /state_ids at them would otherwise keep showing
// that. The state before the join is the anchor state, and each later event gets
// the state after its prev events, in order, so the join's state flows to the
// latest events.
//
// An event with a prev event that has no stored state is left as it is. The
// window is the events between the latest events and the join. It returns false
// if the join is not among their ancestors, in which case nothing changes.
func (r *Inputer) recomputeWindowState(
	ctx context.Context,
	updater *shared.RoomUpdater,
	roomState *state.StateResolution,
	roomInfo *types.RoomInfo,
	joinEventID string,
	anchorEntries []types.StateEntry,
	latestIDs []string,
) (done bool, newlyRejected []string, err error) {
	roomNID := roomInfo.RoomNID
	joinEvents, err := updater.EventsFromIDs(ctx, roomInfo, []string{joinEventID})
	if err != nil {
		return false, nil, fmt.Errorf("updater.EventsFromIDs (join): %w", err)
	}
	if len(joinEvents) != 1 {
		return false, nil, nil
	}
	join := joinEvents[0]

	window, reachedJoin, err := r.collectWindow(ctx, updater, roomInfo, join, latestIDs)
	if err != nil {
		return false, nil, err
	}
	if !reachedJoin {
		return false, nil, nil
	}
	order := windowOrder(window)
	if order == nil {
		return false, nil, nil
	}

	anchorNID, err := updater.AddState(ctx, roomNID, nil, anchorEntries)
	if err != nil {
		return false, nil, fmt.Errorf("updater.AddState (anchor): %w", err)
	}
	if err = updater.SetState(ctx, join.EventNID, anchorNID); err != nil {
		return false, nil, fmt.Errorf("updater.SetState (join): %w", err)
	}

	for _, ev := range order {
		prevStates, stateErr := updater.StateAtEventIDs(ctx, ev.prevIDs)
		if stateErr != nil {
			var missingEvent types.MissingEventError
			var missingState types.MissingStateError
			if errors.As(stateErr, &missingEvent) || errors.As(stateErr, &missingState) {
				logrus.WithError(stateErr).WithField("event_id", ev.id).Debug("Keeping the stored state of an event with a prev event of unknown state")
				continue
			}
			return false, nil, fmt.Errorf("updater.StateAtEventIDs: %w", stateErr)
		}
		if len(prevStates) != len(ev.prevIDs) || hasUnknownState(prevStates) {
			logrus.WithField("event_id", ev.id).Debug("Keeping the stored state of an event with a prev event of unknown state")
			continue
		}
		stateNID, calcErr := roomState.CalculateAndStoreStateAfterEvents(ctx, prevStates)
		if calcErr != nil {
			return false, nil, fmt.Errorf("roomState.CalculateAndStoreStateAfterEvents: %w", calcErr)
		}
		becameRejected, recheckErr := r.recheckWindowEvent(ctx, updater, roomInfo, roomState, ev, stateNID)
		if recheckErr != nil {
			return false, nil, fmt.Errorf("r.recheckWindowEvent(%s): %w", ev.id, recheckErr)
		}
		if becameRejected {
			newlyRejected = append(newlyRejected, ev.id)
		}
		if err = updater.SetState(ctx, ev.nid, stateNID); err != nil {
			return false, nil, fmt.Errorf("updater.SetState: %w", err)
		}
	}
	logrus.WithFields(logrus.Fields{
		"room_nid": roomNID,
		"events":   len(order),
	}).Info("Recomputed the state of the events received during the partial state")
	return true, newlyRejected, nil
}

// recheckWindowEvent checks the event against the complete state before it. An
// event that was provisionally accepted with partial state can therefore become
// rejected, while an event rejected only because its state was incomplete can
// become accepted.
func (r *Inputer) recheckWindowEvent(
	ctx context.Context,
	updater *shared.RoomUpdater,
	roomInfo *types.RoomInfo,
	roomState *state.StateResolution,
	ev windowEvent,
	stateBefore types.StateSnapshotNID,
) (becameRejected bool, err error) {
	entries, err := roomState.LoadStateAtSnapshot(ctx, stateBefore)
	if err != nil {
		return false, fmt.Errorf("roomState.LoadStateAtSnapshot: %w", err)
	}
	nids := make([]types.EventNID, 0, len(entries))
	for _, entry := range entries {
		nids = append(nids, entry.EventNID)
	}
	stateEvents, err := updater.Events(ctx, roomInfo.RoomVersion, nids)
	if err != nil {
		return false, fmt.Errorf("updater.Events (state before event): %w", err)
	}
	authEvents, err := gomatrixserverlib.NewAuthEvents(gomatrixserverlib.ToPDUs(stateEvents))
	if err != nil {
		return false, fmt.Errorf("gomatrixserverlib.NewAuthEvents: %w", err)
	}
	stored, err := updater.EventsFromIDs(ctx, roomInfo, []string{ev.id})
	if err != nil {
		return false, fmt.Errorf("updater.EventsFromIDs (window event): %w", err)
	}
	if len(stored) != 1 {
		return false, fmt.Errorf("window event %s is missing", ev.id)
	}
	wasRejected, err := updater.IsEventRejected(ctx, roomInfo.RoomNID, ev.id)
	if err != nil {
		return false, fmt.Errorf("updater.IsEventRejected: %w", err)
	}
	allowedErr := gomatrixserverlib.Allowed(stored[0].PDU, authEvents, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return r.Queryer.QueryUserIDForSender(ctx, roomID, senderID)
	})
	rejected := allowedErr != nil
	if wasRejected && !rejected {
		if err = gomatrixserverlib.VerifyEventSignatures(ctx, stored[0].PDU, r.FSAPI.KeyRing(), func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.Queryer.QueryUserIDForSender(ctx, roomID, senderID)
		}); err != nil {
			// Re-evaluation can undo an auth rejection, but must not undo a
			// rejection caused by an unverifiable event signature.
			logrus.WithError(err).WithField("event_id", ev.id).Warn("Keeping rejected event after signature verification failed")
			rejected = true
		}
	}
	if wasRejected != rejected {
		logrus.WithFields(logrus.Fields{
			"event_id": ev.id,
			"rejected": rejected,
		}).WithError(allowedErr).Info("Updated event auth result after partial state resync")
		if err = updater.SetEventRejected(ev.nid, rejected); err != nil {
			return false, fmt.Errorf("updater.SetEventRejected: %w", err)
		}
	}
	return !wasRejected && rejected, nil
}

func hasUnknownState(states []types.StateAtEvent) bool {
	for _, s := range states {
		if s.BeforeStateSnapshotNID == 0 {
			return true
		}
	}
	return false
}

// collectWindow walks back from the latest events to the join, collecting the
// events deeper than the join. Events at or below its depth, other than the
// join, are not part of the window.
func (r *Inputer) collectWindow(
	ctx context.Context,
	updater *shared.RoomUpdater,
	roomInfo *types.RoomInfo,
	join types.Event,
	latestIDs []string,
) (window map[string]windowEvent, reachedJoin bool, err error) {
	window = make(map[string]windowEvent)
	seen := map[string]bool{}
	queue := append([]string(nil), latestIDs...)
	for len(queue) > 0 {
		var batch []string
		for _, id := range queue {
			switch {
			case id == join.EventID():
				reachedJoin = true
			case !seen[id]:
				seen[id] = true
				batch = append(batch, id)
			}
		}
		queue = nil
		if len(batch) == 0 {
			break
		}
		events, evErr := updater.EventsFromIDs(ctx, roomInfo, batch)
		if evErr != nil {
			return nil, false, fmt.Errorf("updater.EventsFromIDs: %w", evErr)
		}
		for _, ev := range events {
			if ev.Depth() <= join.Depth() {
				continue
			}
			if len(window) >= maxResyncWindow {
				return nil, false, fmt.Errorf("more than %d events since the partial state join", maxResyncWindow)
			}
			window[ev.EventID()] = windowEvent{
				nid: ev.EventNID, id: ev.EventID(), depth: ev.Depth(), prevIDs: ev.PrevEventIDs(),
			}
			queue = append(queue, ev.PrevEventIDs()...)
		}
	}
	return window, reachedJoin, nil
}

// windowOrder returns the window's events with each after its prev events that
// are in the window. Depth only picks between events that are both ready. It
// returns nil if the prev events form a cycle.
func windowOrder(window map[string]windowEvent) []windowEvent {
	waiting := make(map[string]int, len(window))
	children := make(map[string][]string, len(window))
	for id, ev := range window {
		for _, prev := range ev.prevIDs {
			if _, ok := window[prev]; ok {
				waiting[id]++
				children[prev] = append(children[prev], id)
			}
		}
		if _, ok := waiting[id]; !ok {
			waiting[id] = 0
		}
	}
	var ready []windowEvent
	for id, n := range waiting {
		if n == 0 {
			ready = append(ready, window[id])
		}
	}
	less := func(a, b windowEvent) bool {
		if a.depth != b.depth {
			return a.depth < b.depth
		}
		return a.id < b.id
	}
	order := make([]windowEvent, 0, len(window))
	for len(ready) > 0 {
		sort.Slice(ready, func(i, j int) bool { return less(ready[i], ready[j]) })
		ev := ready[0]
		ready = ready[1:]
		order = append(order, ev)
		for _, child := range children[ev.id] {
			waiting[child]--
			if waiting[child] == 0 {
				ready = append(ready, window[child])
			}
		}
	}
	if len(order) != len(window) {
		return nil
	}
	return order
}

// recomputeStateSinceJoin recomputes the state of the events received since the
// join that started the partial state, if there is one on record and the events
// lead back to it. A failure to do so for lack of such a join is not an error:
// the events keep the state they had.
func (r *Inputer) recomputeStateSinceJoin(
	ctx context.Context,
	updater *shared.RoomUpdater,
	roomState *state.StateResolution,
	roomInfo *types.RoomInfo,
	anchorStateIDs []string,
) (done bool, newlyRejected []string, err error) {
	logger := logrus.WithField("room_nid", roomInfo.RoomNID)
	joinEventID, err := r.DB.GetPartialStateJoinEventID(ctx, roomInfo.RoomNID)
	if err != nil {
		return false, nil, fmt.Errorf("r.DB.GetPartialStateJoinEventID: %w", err)
	}
	if joinEventID == "" {
		logger.Debug("No partial state join on record, keeping the state of the events received since")
		return false, nil, nil
	}
	anchorEntries, err := r.DB.StateEntriesForEventIDs(ctx, anchorStateIDs, true)
	if err != nil {
		return false, nil, fmt.Errorf("r.DB.StateEntriesForEventIDs (anchor): %w", err)
	}
	anchorEntries = types.DeduplicateStateEntries(anchorEntries)
	latest := make([]string, 0, len(updater.LatestEvents()))
	for _, ev := range updater.LatestEvents() {
		latest = append(latest, ev.EventID)
	}
	done, rejected, err := r.recomputeWindowState(ctx, updater, roomState, roomInfo, joinEventID, anchorEntries, latest)
	if err != nil {
		return false, nil, fmt.Errorf("r.recomputeWindowState: %w", err)
	}
	if !done {
		logger.Warn("The latest events do not lead back to the partial state join, keeping their stored state")
	}
	return done, rejected, nil
}
