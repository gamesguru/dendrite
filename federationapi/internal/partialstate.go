// Copyright 2025 Jackmaninov
// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/process"
)

const (
	partialStateWorkerCount = 4
	// Initial backoff delay after first failure.
	partialStateMinBackoff = time.Minute * 1
	// Maximum backoff delay (cap).
	partialStateMaxBackoff = time.Hour * 1
	// Maximum number of retries before giving up on a room.
	partialStateMaxRetries = 16
	// Jitter bounds for backoff calculation.
	maxJitterMultiplier = 1.4
	minJitterMultiplier = 0.8
)

// roomRetryInfo tracks retry state for a single room.
type roomRetryInfo struct {
	retryAt    time.Time
	retryCount uint32
}

// PartialStateWorker handles background resync of rooms with partial state from MSC3706 faster joins.
// After a partial state join, this worker fetches the full room state in the background.
type PartialStateWorker struct {
	process  *process.ProcessContext
	rsAPI    roomserverAPI.FederationRoomserverAPI
	fedAPI   *FederationInternalAPI
	workerCh chan types.RoomNID
	retryMu  sync.Mutex
	retryMap map[types.RoomNID]*roomRetryInfo
}

// NewPartialStateWorker creates a new partial state worker.
func NewPartialStateWorker(
	processCtx *process.ProcessContext,
	rsAPI roomserverAPI.FederationRoomserverAPI,
	fedAPI *FederationInternalAPI,
) *PartialStateWorker {
	return &PartialStateWorker{
		process:  processCtx,
		rsAPI:    rsAPI,
		fedAPI:   fedAPI,
		workerCh: make(chan types.RoomNID, 100),
		retryMap: make(map[types.RoomNID]*roomRetryInfo),
	}
}

// backoffDuration calculates the backoff duration for a given retry count using
// exponential backoff with jitter, similar to the federation queue statistics.
func (w *PartialStateWorker) backoffDuration(retryCount uint32) time.Duration {
	// Add jitter to minimize thundering herd effects
	jitter := rand.Float64()*(maxJitterMultiplier-minJitterMultiplier) + minJitterMultiplier

	// Exponential backoff: minBackoff * 2^retryCount, capped at maxBackoff
	backoff := float64(partialStateMinBackoff) * math.Pow(2, float64(retryCount)) * jitter //nolint:mnd

	duration := time.Duration(backoff)
	if duration > partialStateMaxBackoff {
		duration = partialStateMaxBackoff
	}
	return duration
}

// Start begins the partial state worker, queuing all rooms with partial state for processing.
func (w *PartialStateWorker) Start() error {
	// Skip if rsAPI is not available (e.g., in tests)
	if w.rsAPI == nil {
		return nil
	}

	// Start worker goroutines
	for i := 0; i < partialStateWorkerCount; i++ {
		w.process.Go(func() { w.worker(i) })
	}

	// Start retry goroutine
	w.process.Go(w.retryLoop)

	// Queue all rooms with partial state for processing
	roomNIDs, err := w.rsAPI.GetAllPartialStateRooms(w.process.Context())
	if err != nil {
		logrus.WithError(err).Error("Failed to load partial state rooms on startup")
		return err
	}

	if len(roomNIDs) > 0 {
		logrus.WithField("count", len(roomNIDs)).Info("Queuing partial state rooms for background resync")

		// Stagger the initial queue to avoid thundering herd
		offset := time.Second * 5 //nolint:mnd
		step := time.Second
		if max := len(roomNIDs); max > 60 { //nolint:mnd
			step = (time.Second * 60) / time.Duration(max) //nolint:mnd
		}

		process.Stagger(w.process.Context(), roomNIDs, offset, step, w.QueueRoom)
	}

	return nil
}

// QueueRoom adds a room to the queue for partial state processing.
func (w *PartialStateWorker) QueueRoom(roomNID types.RoomNID) {
	select {
	case w.workerCh <- roomNID:
	default:
		// Channel full, add to retry map with no retry count increment
		w.retryMu.Lock()
		if _, exists := w.retryMap[roomNID]; !exists {
			w.retryMap[roomNID] = &roomRetryInfo{
				retryAt:    time.Now().Add(time.Second * 30), //nolint:mnd
				retryCount: 0,
			}
		}
		w.retryMu.Unlock()
	}
}

// worker processes rooms from the channel.
func (w *PartialStateWorker) worker(workerID int) {
	for {
		var roomNID types.RoomNID
		select {
		case <-w.process.Context().Done():
			return
		case room, ok := <-w.workerCh:
			if !ok {
				return
			}
			roomNID = room
		}
		if w.process.Context().Err() != nil {
			return
		}

		if err := w.processRoom(roomNID); err != nil {
			// Get current retry count
			w.retryMu.Lock()
			info, exists := w.retryMap[roomNID]
			if !exists {
				info = &roomRetryInfo{retryCount: 0}
			}
			info.retryCount++

			logger := logrus.WithFields(logrus.Fields{
				"room_nid":    roomNID,
				"worker_id":   workerID,
				"retry_count": info.retryCount,
			})

			// Check if we've exceeded max retries
			if info.retryCount >= partialStateMaxRetries {
				logger.WithError(err).Error("Giving up on partial state resync after max retries")
				// Remove from retry map - we're giving up
				delete(w.retryMap, roomNID)
				w.retryMu.Unlock()
				continue
			}

			// Schedule retry with exponential backoff
			backoff := w.backoffDuration(info.retryCount)
			info.retryAt = time.Now().Add(backoff)
			w.retryMap[roomNID] = info
			w.retryMu.Unlock()

			logger.WithError(err).WithField("retry_in", backoff).Warn("Failed to resync partial state room, will retry with backoff")
		} else {
			// Success - clear retry info
			w.retryMu.Lock()
			delete(w.retryMap, roomNID)
			w.retryMu.Unlock()
		}
	}
}

// retryLoop periodically retries failed rooms.
func (w *PartialStateWorker) retryLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-w.process.Context().Done():
			return
		case <-ticker.C:
			w.retryMu.Lock()
			now := time.Now()
			var toRetry []types.RoomNID
			for roomNID, info := range w.retryMap {
				if now.After(info.retryAt) {
					toRetry = append(toRetry, roomNID)
				}
			}
			// Don't delete from retryMap here - the worker will update it on failure
			// or delete it on success. We only need to re-queue the room.
			w.retryMu.Unlock()

			for _, roomNID := range toRetry {
				// Send directly to channel instead of QueueRoom to preserve retry state
				select {
				case w.workerCh <- roomNID:
				default:
					// Channel full, will be picked up on next tick
				}
			}
		}
	}
}

// processRoom fetches and applies full room state, clears the partial-state flag,
// and notifies waiters. It skips rooms that are no longer partial. If the room ID
// lookup fails or room info is absent, it only attempts to clear the flag.
// Other roomserver errors propagate; missing latest events or usable servers
// cause an error. If all servers fail, it returns the last fetch or apply error.
func (w *PartialStateWorker) processRoom(roomNID types.RoomNID) error {
	trace, ctx := internal.StartTask(w.process.Context(), "PartialStateWorker.processRoom")
	defer trace.EndTask()
	trace.SetTag("room_nid", roomNID)

	resyncStartTime := time.Now()
	logger := logrus.WithFields(logrus.Fields{
		"room_nid": roomNID,
		"trace":    "join_timing",
	})

	hasPartialState, err := w.rsAPI.IsRoomPartialState(ctx, roomNID)
	if err != nil {
		return err
	}
	if !hasPartialState {
		logger.Debug("Room no longer has partial state, skipping")
		return nil
	}

	servers, err := w.rsAPI.GetPartialStateServers(ctx, roomNID)
	if err != nil {
		return err
	}

	joinServer, err := w.rsAPI.GetPartialStateJoinServer(ctx, roomNID)
	if err != nil {
		return err
	}

	roomID, err := w.rsAPI.RoomIDFromNID(ctx, roomNID)
	if err != nil {
		logger.WithError(err).Warn("Room not found for partial state room")
		_, err = w.rsAPI.ClearRoomPartialState(ctx, roomNID)
		return err
	}

	roomInfo, err := w.rsAPI.RoomInfoByNID(ctx, roomNID)
	if err != nil {
		return err
	}
	if roomInfo == nil {
		logger.Warn("Room info not found for partial state room")
		_, err = w.rsAPI.ClearRoomPartialState(ctx, roomNID)
		return err
	}

	logger = logger.WithField("room_id", roomID)
	trace.SetTag("room_id", roomID)
	logger.Info("Starting partial state resync")

	latestEventIDs, _, _, err := w.rsAPI.LatestEventIDs(ctx, roomNID)
	if err != nil {
		return fmt.Errorf("failed to get latest events: %w", err)
	}
	if len(latestEventIDs) == 0 {
		return fmt.Errorf("no latest events found for room")
	}

	// Build an ordered server list: join server first (always tried, even if
	// backing off), then other available servers.
	origin := w.fedAPI.cfg.Matrix.ServerName
	orderedServers := w.buildServerList(logger, joinServer, servers)
	if len(orderedServers) == 0 {
		return fmt.Errorf("no servers available for partial state resync (all blacklisted or backing off, no join server)")
	}

	var lastErr error
	for _, entry := range orderedServers {
		serverName := entry.name
		logger := logger.WithField("server", serverName)
		frontier := latestEventIDs[0]
		for attempt := 0; attempt < 4; attempt++ {
			anchor := w.findStateAnchor(ctx, roomID, frontier, origin)
			stateEventIDs, fetchErr := w.fetchStateViaAnchor(
				ctx, logger, origin, serverName, roomID, anchor,
				roomInfo.RoomVersion, entry.bypassBackoff, orderedServers,
			)
			if fetchErr != nil {
				logger.WithError(fetchErr).Warn("Failed to fetch state from server")
				lastErr = fetchErr
				break
			}
			current, _, _, latestErr := w.rsAPI.LatestEventIDs(ctx, roomNID)
			if latestErr != nil {
				lastErr = latestErr
				break
			}
			if len(current) == 0 {
				lastErr = fmt.Errorf("no latest events found for room")
				break
			}
			if current[0] != frontier {
				frontier = current[0]
				continue
			}
			if err = w.applyState(ctx, logger, roomID, roomNID, roomInfo, stateEventIDs, resyncStartTime); err != nil {
				lastErr = err
				break
			}
			return nil
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("room state kept changing during partial state resync")
		}
	}

	return lastErr
}

// getStoredEvent returns a locally stored event, including rejected events.
// It propagates roomserver query errors and returns an error if no event is found.
func (w *PartialStateWorker) getStoredEvent(ctx context.Context, roomID, eventID string) (gomatrixserverlib.PDU, error) {
	var res roomserverAPI.QueryEventsByIDResponse
	if err := w.rsAPI.QueryEventsByID(ctx, &roomserverAPI.QueryEventsByIDRequest{
		RoomID: roomID, EventIDs: []string{eventID}, ExcludeRejected: true,
	}, &res); err != nil {
		return nil, err
	}
	if len(res.Events) == 0 {
		return nil, fmt.Errorf("event %s not found locally", eventID)
	}
	return res.Events[0].PDU, nil
}

// stateAnchor says which event to ask /state_ids about and which events to
// apply on top of the answer to get the state at the latest event.
type stateAnchor struct {
	// askID is the event passed to /state_ids, which returns the state before it.
	askID string
	// applyAsk is set when askID's own state change is not covered by chain
	// and must be applied too (askID is not a locally stored event).
	applyAsk bool
	// chain holds locally stored events, oldest first, whose state changes are
	// applied on top of the response.
	chain []gomatrixserverlib.PDU
}

// findStateAnchor follows single-parent links back from latestID until it reaches
// our own join, an event that is not stored locally, or an event with zero or
// multiple parents. The returned chain is ordered oldest first. Our own join is
// the join point: its parent is the newest event remote servers are known to
// have, whatever the resync has stored since, so stopping there keeps the anchor
// the same on a retry. A missing event or lookup error anchors on the queried ID
// and must be applied on top of its state; an event with zero or multiple
// parents anchors on itself so the remote resolves its state.
func (w *PartialStateWorker) findStateAnchor(ctx context.Context, roomID, latestID string, local spec.ServerName) stateAnchor {
	var chain []gomatrixserverlib.PDU // newest first
	curID := latestID
	for {
		cur, err := w.getStoredEvent(ctx, roomID, curID)
		if err != nil {
			// Not stored: nothing local to inspect. With no chain, ask about the
			// latest event as is.
			return stateAnchor{askID: curID, applyAsk: true, chain: reversed(chain)}
		}
		chain = append(chain, cur)
		prevs := cur.PrevEventIDs()
		if len(prevs) != 1 {
			return stateAnchor{askID: cur.EventID(), chain: reversed(chain)}
		}
		if isLocalJoin(cur, local) && !w.isJoinOfSameUser(ctx, roomID, prevs[0], cur) {
			return stateAnchor{askID: prevs[0], applyAsk: true, chain: reversed(chain)}
		}
		curID = prevs[0]
	}
}

// isLocalJoin reports whether ev is a join of a user on this server.
func isLocalJoin(ev gomatrixserverlib.PDU, local spec.ServerName) bool {
	if ev.Type() != spec.MRoomMember || ev.StateKey() == nil {
		return false
	}
	if membership, err := ev.Membership(); err != nil || membership != spec.Join {
		return false
	}
	user, err := spec.NewUserID(*ev.StateKey(), true)
	return err == nil && user.Domain() == local
}

// isJoinOfSameUser reports whether the stored event prevID is a join of the same
// user as ev, which makes ev a profile change rather than the join itself.
func (w *PartialStateWorker) isJoinOfSameUser(ctx context.Context, roomID, prevID string, ev gomatrixserverlib.PDU) bool {
	prev, err := w.getStoredEvent(ctx, roomID, prevID)
	if err != nil || prev.Type() != spec.MRoomMember || prev.StateKey() == nil || ev.StateKey() == nil {
		return false
	}
	if *prev.StateKey() != *ev.StateKey() {
		return false
	}
	membership, err := prev.Membership()
	return err == nil && membership == spec.Join
}

// reversed returns a new slice with the events in reverse order, leaving the input unchanged.
func reversed(in []gomatrixserverlib.PDU) []gomatrixserverlib.PDU {
	out := make([]gomatrixserverlib.PDU, len(in))
	for i, ev := range in {
		out[len(in)-1-i] = ev
	}
	return out
}

// fetchStateViaAnchor returns the state IDs at the latest event: /state_ids at
// the anchor, plus the state changes of the anchor and the events after it.
// It may fetch and store missing events as outliers. BypassBackoff applies to
// serverName; allServers supplies fallback event sources. A failed local anchor
// lookup falls back to federation. State lookup, room-version, batch-fetch, and
// stored-state query errors are returned, as are errors for an unavailable anchor
// or an anchor state event still missing after fetching.
func (w *PartialStateWorker) fetchStateViaAnchor(
	ctx context.Context,
	logger *logrus.Entry,
	origin, serverName spec.ServerName,
	roomID string,
	anchor stateAnchor,
	roomVersion gomatrixserverlib.RoomVersion,
	bypassBackoff bool,
	allServers []serverEntry,
) ([]string, error) {
	stateIDs, err := w.fetchStateViaStateIDs(
		ctx, logger, origin, serverName, roomID, anchor.askID,
		roomVersion, bypassBackoff, allServers,
	)
	if err != nil {
		return nil, err
	}

	overlays := anchor.chain
	if anchor.applyAsk {
		ev, err := w.getStoredEvent(ctx, roomID, anchor.askID)
		if err != nil {
			verImpl, verErr := gomatrixserverlib.GetRoomVersion(roomVersion)
			if verErr != nil {
				return nil, verErr
			}
			ev = w.fetchSingleEvent(ctx, origin, roomID, anchor.askID, verImpl, bypassBackoff, serverName, allServers)
			if ev == nil {
				return nil, fmt.Errorf("could not fetch anchor event %s", anchor.askID)
			}
			if ev.EventID() != anchor.askID {
				return nil, fmt.Errorf("server %s returned event %s for anchor %s", serverName, ev.EventID(), anchor.askID)
			}
		}
		if ev.StateKey() != nil {
			want := map[string]bool{ev.EventID(): true}
			if missing := w.findMissingEvents(ctx, logger, roomID, roomVersion, want, false); len(missing) > 0 {
				if err = w.fetchMissingEventsParallel(ctx, logger, origin, roomID, roomVersion, missing, bypassBackoff, serverName, allServers); err != nil {
					return nil, err
				}
				if still := w.findMissingEvents(ctx, logger, roomID, roomVersion, want, false); len(still) > 0 {
					return nil, fmt.Errorf("anchor event %s still missing after fetch", ev.EventID())
				}
			}
		}
		overlays = append([]gomatrixserverlib.PDU{ev}, overlays...)
	}

	// A locally rejected state event does not change the state, so it must not
	// replace the accepted event for its key; getStoredEvent leaves it out.
	var stateOverlays []gomatrixserverlib.PDU
	for _, ev := range overlays {
		if ev.StateKey() != nil {
			if _, err := w.getStoredEvent(ctx, roomID, ev.EventID()); err != nil {
				continue
			}
			stateOverlays = append(stateOverlays, ev)
		}
	}
	if len(stateOverlays) == 0 {
		return stateIDs, nil
	}
	var stored roomserverAPI.QueryEventsByIDResponse
	if err = w.rsAPI.QueryEventsByID(ctx, &roomserverAPI.QueryEventsByIDRequest{
		RoomID: roomID, EventIDs: stateIDs,
	}, &stored); err != nil {
		return nil, fmt.Errorf("query state events: %w", err)
	}
	logger.WithFields(logrus.Fields{
		"anchor":   anchor.askID,
		"overlays": len(stateOverlays),
	}).Info("Resynced partial state from an anchor event")
	stateIDs, err = overlayStateEvents(stateIDs, stored.Events, stateOverlays)
	if err != nil {
		return nil, fmt.Errorf("overlay state changes on anchor %s: %w", anchor.askID, err)
	}
	return stateIDs, nil
}

type serverEntry struct {
	name          spec.ServerName
	bypassBackoff bool
}

// buildServerList constructs an ordered list of servers to try for state
// fetching. The join server is always first and bypasses backoff checks
// (it processed our join recently, so it's known alive). Other servers
// are filtered by backoff/blacklist status.
func (w *PartialStateWorker) buildServerList(logger *logrus.Entry, joinServer string, servers []string) []serverEntry {
	var result []serverEntry

	// Join server always goes first and bypasses backoff.
	if joinServer != "" {
		result = append(result, serverEntry{
			name:          spec.ServerName(joinServer),
			bypassBackoff: true,
		})
	}

	for _, serverStr := range servers {
		if serverStr == joinServer {
			continue // Already added as first entry.
		}
		serverName := spec.ServerName(serverStr)
		if _, err := w.fedAPI.IsBlacklistedOrBackingOff(serverName); err != nil {
			logger.WithField("server", serverName).Debug("Skipping server for partial state resync (blacklisted or backing off)")
			continue
		}
		result = append(result, serverEntry{
			name:          serverName,
			bypassBackoff: false,
		})
	}

	return result
}

// fetchStateViaStateIDs fetches the room state using /state_ids to get event
// IDs, then fetches missing events in parallel via /event. This is more
// resilient than /state for large rooms because individual event fetches can
// come from different servers and failures are granular.
func (w *PartialStateWorker) fetchStateViaStateIDs(
	ctx context.Context,
	logger *logrus.Entry,
	origin, serverName spec.ServerName,
	roomID, eventID string,
	roomVersion gomatrixserverlib.RoomVersion,
	bypassBackoff bool,
	allServers []serverEntry,
) ([]string, error) {
	trace, ctx := internal.StartRegion(ctx, "fetchStateViaStateIDs")
	defer trace.EndRegion()

	// Step 1: Get state event IDs from /state_ids.
	var stateIDs gomatrixserverlib.StateIDResponse
	var err error
	if bypassBackoff {
		stateIDs, err = w.fedAPI.LookupStateIDsBypassBackoff(ctx, origin, serverName, roomID, eventID)
	} else {
		stateIDs, err = w.fedAPI.LookupStateIDs(ctx, origin, serverName, roomID, eventID)
	}
	if err != nil {
		return nil, fmt.Errorf("LookupStateIDs from %s: %w", serverName, err)
	}

	wantIDs := stateIDs.GetStateEventIDs()
	authIDs := stateIDs.GetAuthEventIDs()
	allIDs := make(map[string]bool, len(wantIDs)+len(authIDs))
	for _, id := range wantIDs {
		allIDs[id] = true
	}
	for _, id := range authIDs {
		allIDs[id] = true
	}

	logger.WithFields(logrus.Fields{
		"state_events": len(wantIDs),
		"auth_events":  len(authIDs),
		"server":       serverName,
	}).Info("Fetched state IDs for partial state resync")

	// Step 2: Figure out which events we already have locally. Treat rejected
	// events as missing so we re-fetch them and give the input path another
	// chance to verify and un-reject them (their signing key may be fetchable
	// now even if it wasn't when the event first arrived).
	missing := w.findMissingEvents(ctx, logger, roomID, roomVersion, allIDs, true)
	logger.WithFields(logrus.Fields{
		"missing": len(missing),
		"total":   len(allIDs),
	}).Info("Determined missing events for partial state resync")

	if len(missing) == 0 {
		return wantIDs, nil
	}

	// Step 3: Fetch missing events in parallel from multiple servers.
	if err := w.fetchMissingEventsParallel(ctx, logger, origin, roomID, roomVersion, missing, bypassBackoff, serverName, allServers); err != nil {
		return nil, fmt.Errorf("fetchMissingEventsParallel: %w", err)
	}

	// Step 4: Verify every wanted state event is now stored locally before we
	// declare the resync complete. Individual fetches can fail silently:
	// fetchSingleEvent drops events it cannot retrieve from any server, and
	// InputRoomEvents batch-store errors are only logged. Without this gate a
	// resync could "complete" with only a fraction of the room's state (e.g. 35
	// of ~1.2k members), after which applyState clears the partial-state flag and
	// the low member count is never retried or corrected (issue #247). Failing
	// here lets processRoom try another server and retry with backoff instead.
	//
	// Completeness is measured by presence, not verification: an event that is
	// stored but rejected (a signature we still cannot verify because its key is
	// unavailable) is as complete as it will ever get, so counting it as missing
	// here would loop forever. Step 2 already re-fetched rejected events to give
	// verification another attempt; anything still rejected is tolerated by the
	// snapshot builder in UpdateStateAfterResync.
	wantSet := make(map[string]bool, len(wantIDs))
	for _, id := range wantIDs {
		wantSet[id] = true
	}
	if stillMissing := w.findMissingEvents(ctx, logger, roomID, roomVersion, wantSet, false); len(stillMissing) > 0 {
		return nil, fmt.Errorf(
			"resync incomplete: %d of %d state events still missing after fetch",
			len(stillMissing), len(wantIDs),
		)
	}

	return wantIDs, nil
}

// This helper returns stateIDs with the entry for ev's type and state key
// replaced by ev, or ev added if that key is not present.
func overlayStateEvent(stateIDs []string, events []*types.HeaderedEvent, ev gomatrixserverlib.PDU) ([]string, error) {
	return overlayStateEvents(stateIDs, events, []gomatrixserverlib.PDU{ev})
}

// This helper applies state events in order, each replacing the entry with the
// same type and state key. The events argument must hold the stored events
// behind stateIDs so their keys can be found.
func overlayStateEvents(stateIDs []string, events []*types.HeaderedEvent, overlays []gomatrixserverlib.PDU) ([]string, error) {
	type key struct{ eventType, stateKey string }
	keyOf := make(map[string]key, len(events)+len(overlays))
	for _, e := range events {
		if e.StateKey() != nil {
			keyOf[e.EventID()] = key{e.Type(), *e.StateKey()}
		}
	}
	for _, id := range stateIDs {
		if _, ok := keyOf[id]; !ok {
			return nil, fmt.Errorf("state event %s is not available for state-key resolution", id)
		}
	}
	out := append(make([]string, 0, len(stateIDs)+len(overlays)), stateIDs...)
	for _, ev := range overlays {
		if ev.StateKey() == nil {
			return nil, fmt.Errorf("cannot overlay non-state event %s", ev.EventID())
		}
		want := key{ev.Type(), *ev.StateKey()}
		kept := out[:0]
		for _, id := range out {
			if k, ok := keyOf[id]; ok && k == want {
				continue
			}
			kept = append(kept, id)
		}
		out = kept
		out = append(out, ev.EventID())
		keyOf[ev.EventID()] = want
	}
	return out, nil
}

// findMissingEvents checks which of the wanted event IDs are not yet stored
// locally and returns the missing ones. When excludeRejected is true, events
// that are stored but rejected count as missing, so the resync re-fetches them
// and the input path gets another chance to verify and un-reject them (e.g.
// after a signing key that was previously unavailable can be fetched). When
// false, any stored copy (rejected or not) counts as present, which is what the
// final completeness gate wants: an event we can never verify must not keep the
// resync retrying forever.
func (w *PartialStateWorker) findMissingEvents(
	ctx context.Context,
	logger *logrus.Entry,
	roomID string,
	_ gomatrixserverlib.RoomVersion,
	wantIDs map[string]bool,
	excludeRejected bool,
) []string {
	idList := make([]string, 0, len(wantIDs))
	for id := range wantIDs {
		idList = append(idList, id)
	}

	var res roomserverAPI.QueryEventsByIDResponse
	if err := w.rsAPI.QueryEventsByID(ctx, &roomserverAPI.QueryEventsByIDRequest{
		RoomID:          roomID,
		EventIDs:        idList,
		ExcludeRejected: excludeRejected,
	}, &res); err != nil {
		logger.WithError(err).Warn("Failed to query existing events, treating all as missing")
		return idList
	}

	known := make(map[string]bool, len(res.Events))
	for _, ev := range res.Events {
		known[ev.EventID()] = true
	}

	var missing []string
	for id := range wantIDs {
		if !known[id] {
			missing = append(missing, id)
		}
	}
	return missing
}

// fetchMissingEventsParallel fetches missing events using parallel workers.
// Events are fetched in chunks and stored as outliers to keep memory bounded.
func (w *PartialStateWorker) fetchMissingEventsParallel(
	ctx context.Context,
	logger *logrus.Entry,
	origin spec.ServerName,
	roomID string,
	roomVersion gomatrixserverlib.RoomVersion,
	missing []string,
	primaryBypassBackoff bool,
	primaryServer spec.ServerName,
	allServers []serverEntry,
) error {
	const (
		batchSize          = 1000
		concurrentRequests = 8
	)

	logger.WithFields(logrus.Fields{
		"missing":    len(missing),
		"batch_size": batchSize,
	}).Info("Fetching missing state events in parallel")

	verImpl, err := gomatrixserverlib.GetRoomVersion(roomVersion)
	if err != nil {
		return fmt.Errorf("GetRoomVersion: %w", err)
	}

	for start := 0; start < len(missing); start += batchSize {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context canceled during chunked state fetch: %w", err)
		}
		end := start + batchSize
		if end > len(missing) {
			end = len(missing)
		}
		batch := missing[start:end]

		logger.Infof("Fetching state chunk %d-%d of %d", start+1, end, len(missing))

		workers := concurrentRequests
		if len(batch) < workers {
			workers = len(batch)
		}
		pending := make(chan string, len(batch))
		for _, id := range batch {
			pending <- id
		}
		close(pending)

		var batchEvents []gomatrixserverlib.PDU
		var mu sync.Mutex
		var wg sync.WaitGroup
		wg.Add(workers)
		for range workers {
			go func() {
				defer wg.Done()
				for eventID := range pending {
					ev := w.fetchSingleEvent(ctx, origin, roomID, eventID, verImpl, primaryBypassBackoff, primaryServer, allServers)
					if ev != nil {
						mu.Lock()
						batchEvents = append(batchEvents, ev)
						mu.Unlock()
					}
				}
			}()
		}
		wg.Wait()

		// Store this batch as outliers in a single InputRoomEvents call.
		outlierInputs := make([]roomserverAPI.InputRoomEvent, 0, len(batchEvents))
		for _, ev := range batchEvents {
			outlierInputs = append(outlierInputs, roomserverAPI.InputRoomEvent{
				Kind:   roomserverAPI.KindOutlier,
				Event:  &types.HeaderedEvent{PDU: ev},
				Origin: origin,
			})
		}
		var ires roomserverAPI.InputRoomEventsResponse
		w.rsAPI.InputRoomEvents(ctx, &roomserverAPI.InputRoomEventsRequest{
			InputRoomEvents: outlierInputs,
		}, &ires)
		if ires.ErrMsg != "" {
			logger.WithField("error", ires.ErrMsg).Warn("Failed to store outlier events batch")
		}
		batchEvents = nil // Release memory before next batch.
	}

	return nil
}

// fetchSingleEvent tries to fetch a single event from the primary server first,
// then falls back to other servers. This spreads the load and handles individual
// server failures gracefully.
func (w *PartialStateWorker) fetchSingleEvent(
	ctx context.Context,
	origin spec.ServerName,
	_ /* roomID */, eventID string,
	verImpl gomatrixserverlib.IRoomVersion,
	primaryBypassBackoff bool,
	primaryServer spec.ServerName,
	allServers []serverEntry,
) gomatrixserverlib.PDU {
	// Try the primary server first.
	ev, err := w.getEventFromServer(ctx, origin, primaryServer, eventID, verImpl, primaryBypassBackoff)
	if err == nil {
		return ev
	}

	// Fall back to other servers (up to 4 additional attempts).
	const maxFallbacks = 4
	tried := 0
	for _, entry := range allServers {
		if entry.name == primaryServer {
			continue
		}
		if tried >= maxFallbacks {
			break
		}
		tried++
		ev, err = w.getEventFromServer(ctx, origin, entry.name, eventID, verImpl, entry.bypassBackoff)
		if err == nil {
			return ev
		}
	}

	return nil
}

const eventFetchTimeout = 30 * time.Second

func (w *PartialStateWorker) getEventFromServer(
	ctx context.Context,
	origin, serverName spec.ServerName,
	eventID string,
	verImpl gomatrixserverlib.IRoomVersion,
	bypassBackoff bool,
) (gomatrixserverlib.PDU, error) {
	reqctx, cancel := context.WithTimeout(ctx, eventFetchTimeout)
	defer cancel()

	var txn gomatrixserverlib.Transaction
	var err error
	if bypassBackoff {
		txn, err = w.fedAPI.GetEventBypassBackoff(reqctx, origin, serverName, eventID)
	} else {
		txn, err = w.fedAPI.GetEvent(reqctx, origin, serverName, eventID)
	}
	if err != nil || len(txn.PDUs) == 0 {
		return nil, fmt.Errorf("GetEvent %s from %s: %w", eventID, serverName, err)
	}

	ev, err := verImpl.NewEventFromUntrustedJSON(txn.PDUs[0])
	if err != nil {
		return nil, fmt.Errorf("parse event %s from %s: %w", eventID, serverName, err)
	}
	if ev.EventID() != eventID {
		return nil, fmt.Errorf("server %s returned event %s when %s was requested", serverName, ev.EventID(), eventID)
	}
	return ev, nil
}

// applyState stores the fetched state and updates the room's current state.
func (w *PartialStateWorker) applyState(
	ctx context.Context,
	logger *logrus.Entry,
	roomID string,
	roomNID types.RoomNID,
	_ *types.RoomInfo,
	stateEventIDs []string,
	resyncStartTime time.Time,
) error {
	updateStateRegion, _ := internal.StartRegion(ctx, "UpdateCurrentStateAfterResync")
	updateStateStartTime := time.Now()
	updateStateRegion.SetTag("state_event_count", len(stateEventIDs))
	if err := w.rsAPI.UpdateCurrentStateAfterResync(ctx, roomID, stateEventIDs); err != nil {
		updateStateRegion.EndRegion()
		logger.WithError(err).Warn("Failed to update current state after resync")
		return err
	}
	updateStateRegion.EndRegion()
	logger.WithFields(logrus.Fields{
		"update_state_ms":   time.Since(updateStateStartTime).Milliseconds(),
		"state_event_count": len(stateEventIDs),
	}).Debug("UpdateCurrentStateAfterResync completed")

	clearStateRegion, _ := internal.StartRegion(ctx, "ClearRoomPartialState")
	clearStateStartTime := time.Now()
	deviceListStreamID, err := w.rsAPI.ClearRoomPartialState(ctx, roomNID)
	clearStateRegion.EndRegion()
	if err != nil {
		logger.WithError(err).Error("Failed to clear partial state flag")
		return err
	}

	logger.WithFields(logrus.Fields{
		"device_list_stream_id": deviceListStreamID,
		"clear_state_ms":        time.Since(clearStateStartTime).Milliseconds(),
		"total_resync_ms":       time.Since(resyncStartTime).Milliseconds(),
	}).Debug("Successfully completed partial state resync")

	w.rsAPI.NotifyUnPartialStated(roomID)

	// TODO(MSC3902): Implement full device list replay
	if deviceListStreamID > 0 {
		logger.Debug("Device list replay would use changes since stream position (not yet implemented)")
	}

	return nil
}
