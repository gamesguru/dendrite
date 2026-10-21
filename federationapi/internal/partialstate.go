// Copyright 2025 Jackmaninov
// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"

	federationTypes "codefloe.com/pat-s/zendrite/federationapi/types"
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
	// How often rooms waiting to be retried are looked at.
	retryTickInterval = time.Second
	// Jitter bounds for backoff calculation.
	maxJitterMultiplier = 1.4
	minJitterMultiplier = 0.8
)

// roomRetryInfo tracks retry state for a single room.
type roomRetryInfo struct {
	retryAt    time.Time
	retryCount uint32
	// failingSince is when servers began to fail to give usable state; zero when the
	// last failure was on our side.
	failingSince time.Time
}

// noteFailure starts the give-up timer on a failure to get state from the servers, and
// resets it on any other failure, which says nothing about them.
func (i *roomRetryInfo) noteFailure(err error) {
	// Only the error as classified by processRoom counts, not a cause inside it.
	_, remote := err.(*remoteStateError) //nolint:errorlint
	switch {
	case !remote:
		i.failingSince = time.Time{}
	case i.failingSince.IsZero():
		i.failingSince = time.Now()
	}
}

// remoteStateError is a resync failure because no server gave usable state, as opposed to
// one on our side. Only these count towards giving up on a room.
type remoteStateError struct{ err error }

func (e *remoteStateError) Error() string { return e.err.Error() }
func (e *remoteStateError) Unwrap() error { return e.err }

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

		// Stagger the initial queue to avoid thundering herd. A user is waiting on each
		// of these joins, so start soon and keep the gaps short: a backlog of 240 rooms
		// or fewer is queued within a minute.
		offset := time.Second
		step := 250 * time.Millisecond       //nolint:mnd
		if max := len(roomNIDs); max > 240 { //nolint:mnd
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
			info.noteFailure(err)

			logger := logrus.WithFields(logrus.Fields{
				"room_nid":    roomNID,
				"worker_id":   workerID,
				"retry_count": info.retryCount,
			})

			// The join is abandoned once the resync has been failing for too long.
			if w.shouldGiveUp(info) {
				delete(w.retryMap, roomNID)
				w.retryMu.Unlock()
				logger.WithError(err).Error("Giving up on partial state resync, leaving the room")
				if abandonErr := w.abandonJoin(roomNID); abandonErr != nil {
					logger.WithError(abandonErr).Error("Failed to abandon the partial state join, will retry")
					w.retryMu.Lock()
					w.retryMap[roomNID] = &roomRetryInfo{retryCount: info.retryCount, retryAt: time.Now().Add(w.backoffDuration(0)), failingSince: info.failingSince}
					w.retryMu.Unlock()
				}
				continue
			}

			// Schedule retry with exponential backoff
			backoff := w.backoffDuration(info.retryCount)
			// Try again when the limit is reached, to give up then rather than a backoff later.
			if w.fedAPI != nil && w.fedAPI.cfg != nil && w.fedAPI.cfg.PartialStateResyncGiveUpAfter > 0 && !info.failingSince.IsZero() {
				untilGiveUp := time.Until(info.failingSince.Add(w.fedAPI.cfg.PartialStateResyncGiveUpAfter))
				if untilGiveUp < backoff {
					backoff = max(untilGiveUp, time.Second)
				}
			}
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

// shouldGiveUp reports whether servers have been failing to give usable state for a room
// for longer than the configured limit.
func (w *PartialStateWorker) shouldGiveUp(info *roomRetryInfo) bool {
	if w.fedAPI == nil || w.fedAPI.cfg == nil || info.failingSince.IsZero() {
		return false
	}
	giveUpAfter := w.fedAPI.cfg.PartialStateResyncGiveUpAfter
	return giveUpAfter > 0 && time.Since(info.failingSince) >= giveUpAfter
}

// abandonJoin makes the local users leave a room whose partial state could not be
// resynced, through the usual leave, and ends its partial state. It may be repeated: only
// those still joined leave, and a room that is no longer partial is left alone.
func (w *PartialStateWorker) abandonJoin(roomNID types.RoomNID) error {
	ctx := w.process.Context()
	partial, err := w.rsAPI.IsRoomPartialState(ctx, roomNID)
	if err != nil || !partial {
		return err
	}
	roomID, err := w.rsAPI.RoomIDFromNID(ctx, roomNID)
	if err != nil {
		return err
	}

	var members roomserverAPI.QueryMembershipsForRoomResponse
	if err = w.rsAPI.QueryMembershipsForRoom(ctx, &roomserverAPI.QueryMembershipsForRoomRequest{
		RoomID: roomID, JoinedOnly: true, LocalOnly: true,
	}, &members); err != nil {
		return fmt.Errorf("failed to query the local members of %s: %w", roomID, err)
	}
	for _, ev := range members.JoinEvents {
		if ev.StateKey == nil {
			continue
		}
		userID, idErr := spec.NewUserID(*ev.StateKey, true)
		if idErr != nil {
			continue
		}
		if err = w.rsAPI.PerformLeave(ctx, &roomserverAPI.PerformLeaveRequest{
			RoomID: roomID,
			Leaver: *userID,
			Reason: "The room's state could not be fetched, so the join was abandoned",
		}, &roomserverAPI.PerformLeaveResponse{}); err != nil {
			return fmt.Errorf("%s failed to leave %s: %w", userID.String(), roomID, err)
		}
	}

	if _, err = w.rsAPI.ClearRoomPartialState(ctx, roomNID); err != nil {
		return err
	}
	w.rsAPI.NotifyUnPartialStated(roomID)
	return nil
}

// retryLoop periodically retries failed rooms.
func (w *PartialStateWorker) retryLoop() {
	ticker := time.NewTicker(retryTickInterval)
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

	// The join that started the partial state is the anchor's reference point.
	// Rows from before it was recorded return "", and the walk falls back.
	joinEventID, err := w.rsAPI.GetPartialStateJoinEventID(ctx, roomNID)
	if err != nil {
		logger.WithError(err).Warn("Failed to look up the partial state join event")
		joinEventID = ""
	}

	// Only failures to get usable state from the servers count towards giving the join up,
	// not ones in applying it or in the room changing under us.
	var lastErr error
	remoteOnly := true
	for _, entry := range orderedServers {
		serverName := entry.name
		logger := logger.WithField("server", serverName)
		frontier := latestEventIDs[0]
		// The state at the anchor does not change while the events after it do, so a
		// retry because the room moved on keeps what was fetched for it.
		anchorStates := map[string][]string{}
		for attempt := 0; attempt < 4; attempt++ {
			anchor, found := stateAnchor{}, false
			if joinEventID != "" {
				anchor, found = w.findWindowAnchor(ctx, roomID, frontier, joinEventID)
			}
			if !found {
				anchor = w.findStateAnchor(ctx, roomID, frontier, joinEventID, origin)
			}
			stateEventIDs, anchorStateIDs, fetchErr := w.fetchStateViaAnchor(
				ctx, logger, origin, serverName, roomID, anchor, joinEventID,
				roomInfo.RoomVersion, entry.bypassBackoff, orderedServers, anchorStates,
			)
			if fetchErr != nil {
				logger.WithError(fetchErr).Warn("Failed to fetch state from server")
				lastErr = fetchErr
				// Only a server failing to answer counts; the helper can also fail on our side,
				// for instance loading what it stored.
				var remoteErr *remoteStateError
				if !errors.As(fetchErr, &remoteErr) {
					remoteOnly = false
				}
				break
			}
			remoteOnly = false // from here on, failures are on our side
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
			if err = w.applyState(ctx, logger, roomID, roomNID, roomInfo, stateEventIDs, anchorStateIDs, current, resyncStartTime); errors.Is(err, roomserverAPI.ErrResyncFrontierChanged) {
				current, _, _, latestErr = w.rsAPI.LatestEventIDs(ctx, roomNID)
				if latestErr != nil {
					lastErr = latestErr
					break
				}
				if len(current) == 0 {
					lastErr = fmt.Errorf("no latest events found for room")
					break
				}
				frontier = current[0]
				continue
			} else if err != nil {
				lastErr = err
				break
			}
			return nil
		}
		if lastErr == nil {
			remoteOnly = false
			lastErr = fmt.Errorf("room state kept changing during partial state resync")
		}
	}

	if remoteOnly {
		return &remoteStateError{lastErr}
	}
	// Keep the cause, without the claim that every server failed.
	return fmt.Errorf("partial state resync failed: %w", lastErr)
}

// getStoredEvent returns an accepted event stored locally.
// A rejected event is reported as not found, so its state change is never applied.
func (w *PartialStateWorker) getStoredEvent(ctx context.Context, roomID, eventID string) (gomatrixserverlib.PDU, error) {
	return w.queryStoredEvent(ctx, roomID, eventID, true)
}

// queryStoredEvent returns an event stored locally, optionally leaving out rejected ones.
func (w *PartialStateWorker) queryStoredEvent(ctx context.Context, roomID, eventID string, excludeRejected bool) (gomatrixserverlib.PDU, error) {
	var res roomserverAPI.QueryEventsByIDResponse
	if err := w.rsAPI.QueryEventsByID(ctx, &roomserverAPI.QueryEventsByIDRequest{
		RoomID: roomID, EventIDs: []string{eventID}, ExcludeRejected: excludeRejected,
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
// the join that started the partial state, an event that is not stored locally,
// or an event with zero or multiple parents. The returned chain is ordered oldest
// first. The join is the join point: its parent is the newest event remote
// servers are known to have, whatever the resync has stored since, so stopping
// there keeps the anchor the same on a retry. Rooms without a recorded join
// event fall back to the first join of a user on this server. A missing event or
// lookup error anchors on the queried ID and must be applied on top of its state.
// An event with zero or multiple parents anchors on itself so the remote
// resolves its state.
func (w *PartialStateWorker) findStateAnchor(ctx context.Context, roomID, latestID, joinEventID string, local spec.ServerName) stateAnchor {
	var chain []gomatrixserverlib.PDU // newest first
	curID := latestID
	for {
		// A rejected event is walked through: it changes no state, and its parent
		// is as good a way back as any. The overlay step leaves its change out.
		cur, err := w.queryStoredEvent(ctx, roomID, curID, false)
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
		isJoinPoint := cur.EventID() == joinEventID
		if joinEventID == "" {
			isJoinPoint = isLocalJoin(cur, local) && !w.isJoinOfSameUser(ctx, roomID, prevs[0], cur)
		}
		if isJoinPoint {
			return stateAnchor{askID: prevs[0], applyAsk: true, chain: reversed(chain)}
		}
		curID = prevs[0]
	}
}

// findWindowAnchor anchors on the parent of the recorded join, whatever shape the
// events received since then make: a branch, or an event that merges two, leaves
// no single chain back to the join. The chain is the join and every stored event
// leading from it to latestID, each after its prev events. It reports false if
// the events do not lead back to a join that has a single parent, in which case
// findStateAnchor is the way to go.
func (w *PartialStateWorker) findWindowAnchor(ctx context.Context, roomID, latestID, joinEventID string) (stateAnchor, bool) {
	join, err := w.queryStoredEvent(ctx, roomID, joinEventID, false)
	if err != nil || len(join.PrevEventIDs()) != 1 {
		return stateAnchor{}, false
	}
	window := map[string]gomatrixserverlib.PDU{}
	reachedJoin := false
	queue := []string{latestID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if id == joinEventID {
			reachedJoin = true
			continue
		}
		if _, seen := window[id]; seen {
			continue
		}
		ev, queryErr := w.queryStoredEvent(ctx, roomID, id, false)
		if queryErr != nil || ev.Depth() < join.Depth() {
			continue
		}
		window[id] = ev
		queue = append(queue, ev.PrevEventIDs()...)
	}
	if !reachedJoin {
		return stateAnchor{}, false
	}
	return stateAnchor{
		askID:    join.PrevEventIDs()[0],
		applyAsk: true,
		chain:    append([]gomatrixserverlib.PDU{join}, orderByPrevEvents(window)...),
	}, true
}

// orderByPrevEvents returns the events with each after those of its prev events
// that are among them. Depth, then event ID, picks between events that are both
// ready; it is advisory otherwise. Events in a cycle are left out.
func orderByPrevEvents(events map[string]gomatrixserverlib.PDU) []gomatrixserverlib.PDU {
	waiting := make(map[string]int, len(events))
	children := make(map[string][]string, len(events))
	for id, ev := range events {
		waiting[id] += 0
		for _, prev := range ev.PrevEventIDs() {
			if _, ok := events[prev]; ok {
				waiting[id]++
				children[prev] = append(children[prev], id)
			}
		}
	}
	var ready []gomatrixserverlib.PDU
	for id, n := range waiting {
		if n == 0 {
			ready = append(ready, events[id])
		}
	}
	order := make([]gomatrixserverlib.PDU, 0, len(events))
	for len(ready) > 0 {
		sort.Slice(ready, func(i, j int) bool {
			if ready[i].Depth() != ready[j].Depth() {
				return ready[i].Depth() < ready[j].Depth()
			}
			return ready[i].EventID() < ready[j].EventID()
		})
		ev := ready[0]
		ready = ready[1:]
		order = append(order, ev)
		for _, child := range children[ev.EventID()] {
			waiting[child]--
			if waiting[child] == 0 {
				ready = append(ready, events[child])
			}
		}
	}
	return order
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
	joinEventID string,
	roomVersion gomatrixserverlib.RoomVersion,
	bypassBackoff bool,
	allServers []serverEntry,
	anchorStates map[string][]string,
) (stateIDs, anchorStateIDs []string, err error) {
	stateIDs, err = cachedAnchorStateIDs(anchorStates, anchor.askID, func() ([]string, error) {
		return w.fetchStateViaStateIDs(
			ctx, logger, origin, serverName, roomID, anchor.askID,
			roomVersion, bypassBackoff, allServers,
		)
	})
	if err != nil {
		return nil, nil, err
	}

	overlays := anchor.chain
	if anchor.applyAsk {
		ev, err := w.getStoredEvent(ctx, roomID, anchor.askID)
		if err != nil {
			verImpl, verErr := gomatrixserverlib.GetRoomVersion(roomVersion)
			if verErr != nil {
				return nil, nil, verErr
			}
			ev = w.fetchSingleEvent(ctx, origin, roomID, anchor.askID, verImpl, bypassBackoff, serverName, allServers)
			if ev == nil {
				return nil, nil, fmt.Errorf("could not fetch anchor event %s", anchor.askID)
			}
			if ev.EventID() != anchor.askID {
				return nil, nil, fmt.Errorf("server %s returned event %s for anchor %s", serverName, ev.EventID(), anchor.askID)
			}
		}
		if ev.StateKey() != nil {
			want := map[string]bool{ev.EventID(): true}
			if missing := w.findMissingEvents(ctx, logger, roomID, roomVersion, want, false); len(missing) > 0 {
				if err = w.fetchMissingEventsParallel(ctx, logger, origin, roomID, roomVersion, missing, bypassBackoff, serverName, allServers); err != nil {
					return nil, nil, err
				}
				if still := w.findMissingEvents(ctx, logger, roomID, roomVersion, want, false); len(still) > 0 {
					return nil, nil, fmt.Errorf("anchor event %s still missing after fetch", ev.EventID())
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
		return stateIDs, stateIDsBeforeJoin(stateIDs, anchor, joinEventID), nil
	}

	var stored roomserverAPI.QueryEventsByIDResponse
	if err = w.rsAPI.QueryEventsByID(ctx, &roomserverAPI.QueryEventsByIDRequest{
		RoomID: roomID, EventIDs: stateIDs,
	}, &stored); err != nil {
		return nil, nil, fmt.Errorf("query state events: %w", err)
	}

	// The events received while the state was partial were accepted without the
	// room's members to check them against. Apply each one to the state only if the
	// auth rules allow it there, as the full state can now show that it is not: a
	// kick by someone who had left, say, must not remove its target.
	state := make(map[gomatrixserverlib.StateKeyTuple]gomatrixserverlib.PDU, len(stored.Events))
	for _, he := range stored.Events {
		if he.StateKey() != nil {
			state[gomatrixserverlib.StateKeyTuple{EventType: he.Type(), StateKey: *he.StateKey()}] = he.PDU
		}
	}
	userIDQuerier := func(room spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return w.rsAPI.QueryUserIDForSender(ctx, room, senderID)
	}
	// This server's own events are trusted, as is the anchor: it predates our join,
	// and the server that gave it to us had accepted it.
	trusted := func(ev gomatrixserverlib.PDU) bool {
		if ev.EventID() == anchor.askID {
			return true
		}
		user, queryErr := w.rsAPI.QueryUserIDForSender(ctx, ev.RoomID(), ev.SenderID())
		return queryErr == nil && user != nil && w.fedAPI.cfg.Matrix.IsLocalServerName(user.Domain())
	}
	allowed, skipped := allowedOverlays(state, stateOverlays, trusted, userIDQuerier)
	for _, ev := range skipped {
		logger.WithField("event_id", ev.EventID()).Warn("Not applying an event received during the partial join: the room's full state does not allow it")
	}
	stateOverlays = allowed

	logger.WithFields(logrus.Fields{
		"anchor":   anchor.askID,
		"overlays": len(stateOverlays),
		"skipped":  len(skipped),
	}).Info("Resynced partial state from an anchor event")
	// The state before the join is the state at the anchor, which the anchor
	// event's own change completes.
	var beforeJoin []string
	if canRecomputeSinceJoin(anchor, joinEventID) {
		beforeJoin = stateIDs
		if len(stateOverlays) > 0 && stateOverlays[0].EventID() == anchor.askID {
			beforeJoin, err = overlayStateEvents(stateIDs, stored.Events, stateOverlays[:1])
			if err != nil {
				return nil, nil, fmt.Errorf("overlay anchor %s: %w", anchor.askID, err)
			}
		}
	}
	stateIDs, err = overlayStateEvents(stateIDs, stored.Events, stateOverlays)
	if err != nil {
		return nil, nil, fmt.Errorf("overlay state changes on anchor %s: %w", anchor.askID, err)
	}
	return stateIDs, beforeJoin, nil
}

// cachedAnchorStateIDs reuses a successful state lookup for an anchor across
// retries. Events may advance the room while federation is in flight, but the
// state before the same anchor remains valid.
func cachedAnchorStateIDs(
	cache map[string][]string,
	anchorID string,
	fetch func() ([]string, error),
) ([]string, error) {
	if stateIDs, ok := cache[anchorID]; ok {
		return slices.Clone(stateIDs), nil
	}
	stateIDs, err := fetch()
	if err != nil {
		return nil, err
	}
	cache[anchorID] = slices.Clone(stateIDs)
	return stateIDs, nil
}

// canRecomputeSinceJoin reports whether the anchor is the parent of the join that
// started the partial state, so that its state is the one before that join.
func canRecomputeSinceJoin(anchor stateAnchor, joinEventID string) bool {
	return joinEventID != "" && anchor.applyAsk && len(anchor.chain) > 0 && anchor.chain[0].EventID() == joinEventID
}

// stateIDsBeforeJoin returns stateIDs if they are the state before the join that
// started the partial state, and nil if the anchor is not that join's parent.
func stateIDsBeforeJoin(stateIDs []string, anchor stateAnchor, joinEventID string) []string {
	if canRecomputeSinceJoin(anchor, joinEventID) {
		return stateIDs
	}
	return nil
}

// allowedOverlays returns, in order, the state events that the auth rules allow on top
// of state, and those they do not. Each event allowed is added to the state the
// next is checked against. Trusted events, such as this server's own, are applied
// without a check. An event that cannot be checked is applied, so that a
// failure here cannot leave the room without state it should have.
func allowedOverlays(
	state map[gomatrixserverlib.StateKeyTuple]gomatrixserverlib.PDU,
	overlays []gomatrixserverlib.PDU,
	trusted func(gomatrixserverlib.PDU) bool,
	userIDQuerier spec.UserIDForSender,
) (allowed, skipped []gomatrixserverlib.PDU) {
	current := make(map[gomatrixserverlib.StateKeyTuple]gomatrixserverlib.PDU, len(state))
	for tuple, ev := range state {
		current[tuple] = ev
	}
	for _, ev := range overlays {
		if !trusted(ev) && !allowedByState(current, ev, userIDQuerier) {
			skipped = append(skipped, ev)
			continue
		}
		allowed = append(allowed, ev)
		current[gomatrixserverlib.StateKeyTuple{EventType: ev.Type(), StateKey: *ev.StateKey()}] = ev
	}
	return allowed, skipped
}

// allowedByState reports whether the auth rules allow ev given the state it follows.
func allowedByState(
	state map[gomatrixserverlib.StateKeyTuple]gomatrixserverlib.PDU, ev gomatrixserverlib.PDU,
	userIDQuerier spec.UserIDForSender,
) bool {
	var authEvents []gomatrixserverlib.PDU
	for _, tuple := range gomatrixserverlib.StateNeededForAuth([]gomatrixserverlib.PDU{ev}).Tuples() {
		if authEvent, ok := state[tuple]; ok {
			authEvents = append(authEvents, authEvent)
		}
	}
	provider, err := gomatrixserverlib.NewAuthEvents(authEvents)
	if err != nil {
		return true
	}
	return gomatrixserverlib.Allowed(ev, provider, userIDQuerier) == nil
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
		return nil, &remoteStateError{fmt.Errorf("LookupStateIDs from %s: %w", serverName, err)}
	}

	wantIDs := stateIDs.GetStateEventIDs()
	authIDs := stateIDs.GetAuthEventIDs()
	if len(wantIDs) == 0 {
		// A room's state is never empty, it has at least the create event. Try
		// another server instead of treating every event as missing.
		return nil, &remoteStateError{fmt.Errorf("LookupStateIDs from %s: the response has no state events", serverName)}
	}
	allIDs := make(map[string]bool, len(wantIDs)+len(authIDs)+1)
	for _, id := range wantIDs {
		allIDs[id] = true
	}
	for _, id := range authIDs {
		allIDs[id] = true
	}
	// The response is the state before the event, which leaves out the event's
	// own change. It is needed to build the state after it, so fetch it too, as
	// Synapse does.
	allIDs[eventID] = true

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

	// Step 3: Fetch the missing events. When a tenth or more of them are missing,
	// one /state request for the whole state is cheaper than a request per event,
	// and it is what servers expect to be asked for: it needs no /event route.
	// This is Synapse's heuristic. Events it leaves out are fetched one by one.
	if len(missing)*10 >= len(allIDs) {
		if err := w.fetchMissingViaState(ctx, logger, origin, serverName, roomID, eventID, roomVersion, allIDs, missing); err != nil {
			logger.WithError(err).Warn("Fetching missing state events via /state failed, fetching them individually")
		} else {
			missing = w.findMissingEvents(ctx, logger, roomID, roomVersion, idSet(missing), true)
		}
	}
	if len(missing) == 0 {
		return wantIDs, nil
	}

	// Fetch the rest in parallel from multiple servers.
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

// idSet returns the IDs as a set.
func idSet(ids []string) map[string]bool {
	set := make(map[string]bool, len(ids))
	for _, id := range ids {
		set[id] = true
	}
	return set
}

// fetchMissingViaState asks the server for the whole state at eventID with a
// single /state request, and stores the missing events from the state and auth
// chain it returns as outliers. Events the response was not asked about are
// ignored, so a server cannot slip extra events in. Events it leaves out remain
// missing and are for the caller to find by another route.
func (w *PartialStateWorker) fetchMissingViaState(
	ctx context.Context,
	logger *logrus.Entry,
	origin, serverName spec.ServerName,
	roomID, eventID string,
	roomVersion gomatrixserverlib.RoomVersion,
	wanted map[string]bool,
	missing []string,
) error {
	verImpl, err := gomatrixserverlib.GetRoomVersion(roomVersion)
	if err != nil {
		return fmt.Errorf("GetRoomVersion: %w", err)
	}
	logger.WithField("missing", len(missing)).Info("Fetching missing state events via /state")
	state, err := w.fedAPI.LookupState(ctx, origin, serverName, roomID, eventID, roomVersion)
	if err != nil {
		return &remoteStateError{fmt.Errorf("LookupState from %s: %w", serverName, err)}
	}

	missingSet := idSet(missing)
	var inputs []roomserverAPI.InputRoomEvent
	for _, raw := range append(append(gomatrixserverlib.EventJSONs{}, state.GetStateEvents()...), state.GetAuthEvents()...) {
		ev, evErr := verImpl.NewEventFromUntrustedJSON(raw)
		if evErr != nil {
			logger.WithError(evErr).Warn("Ignoring an unparseable event in /state")
			continue
		}
		if !missingSet[ev.EventID()] || !wanted[ev.EventID()] || ev.RoomID().String() != roomID {
			continue
		}
		delete(missingSet, ev.EventID())
		inputs = append(inputs, roomserverAPI.InputRoomEvent{
			Kind:   roomserverAPI.KindOutlier,
			Event:  &types.HeaderedEvent{PDU: ev},
			Origin: origin,
		})
	}
	if len(inputs) == 0 {
		return nil
	}
	var ires roomserverAPI.InputRoomEventsResponse
	w.rsAPI.InputRoomEvents(ctx, &roomserverAPI.InputRoomEventsRequest{InputRoomEvents: inputs}, &ires)
	if ires.ErrMsg != "" {
		return fmt.Errorf("store events from /state: %s", ires.ErrMsg)
	}
	return nil
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
	stateEventIDs, anchorStateIDs []string,
	expectedLatestEventIDs []string,
	resyncStartTime time.Time,
) error {
	updateStateRegion, _ := internal.StartRegion(ctx, "UpdateCurrentStateAfterResync")
	updateStateStartTime := time.Now()
	updateStateRegion.SetTag("state_event_count", len(stateEventIDs))
	if err := w.rsAPI.UpdateCurrentStateAfterResync(ctx, roomID, stateEventIDs, anchorStateIDs, expectedLatestEventIDs); err != nil {
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

	// The joined hosts only held the servers of the partial /send_join response, so
	// rebuild them from the complete state before anything is sent to the room again,
	// such as the device list updates replayed by the hooks NotifyUnPartialStated runs.
	if err = w.refreshJoinedHosts(ctx, roomID); err != nil {
		logger.WithError(err).Warn("Failed to refresh the joined hosts after resync")
	}

	w.rsAPI.NotifyUnPartialStated(roomID)

	// TODO(MSC3902): Implement full device list replay
	if deviceListStreamID > 0 {
		logger.Debug("Device list replay would use changes since stream position (not yet implemented)")
	}

	return nil
}

// refreshJoinedHosts replaces the joined hosts of a room with the servers of its
// joined members, as known from the room's current state.
func (w *PartialStateWorker) refreshJoinedHosts(ctx context.Context, roomID string) error {
	var res roomserverAPI.QueryMembershipsForRoomResponse
	if err := w.rsAPI.QueryMembershipsForRoom(ctx, &roomserverAPI.QueryMembershipsForRoomRequest{
		RoomID: roomID, JoinedOnly: true,
	}, &res); err != nil {
		return fmt.Errorf("QueryMembershipsForRoom: %w", err)
	}
	hosts := make([]federationTypes.JoinedHost, 0, len(res.JoinEvents))
	for _, ev := range res.JoinEvents {
		if ev.StateKey == nil {
			continue
		}
		_, domain, err := gomatrixserverlib.SplitID('@', *ev.StateKey)
		if err != nil {
			continue
		}
		hosts = append(hosts, federationTypes.JoinedHost{MemberEventID: ev.EventID, ServerName: domain})
	}
	if _, err := w.fedAPI.db.UpdateRoom(ctx, roomID, hosts, nil, true); err != nil {
		return fmt.Errorf("UpdateRoom: %w", err)
	}
	return nil
}
