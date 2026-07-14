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

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
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
		go w.worker(i)
	}

	// Start retry goroutine
	go w.retryLoop()

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

		for _, roomNID := range roomNIDs {
			roomNID := roomNID
			time.AfterFunc(offset, func() {
				w.QueueRoom(roomNID)
			})
			offset += step
		}
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
	for roomNID := range w.workerCh {
		select {
		case <-w.process.Context().Done():
			return
		default:
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

// processRoom fetches the full state for a room with partial state.
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
	for i, entry := range orderedServers {
		serverName := entry.name
		logger := logger.WithField("server", serverName)

		stateEventIDs, err := w.fetchStateViaStateIDs(
			ctx, logger, origin, serverName, roomID, latestEventIDs[0],
			roomInfo.RoomVersion, entry.bypassBackoff, orderedServers,
		)
		if err != nil {
			logger.WithError(err).Warn("Failed to fetch state from server")
			lastErr = err
			// If this was the join server and it failed, don't give up — try others.
			if i == 0 && len(orderedServers) > 1 {
				continue
			}
			continue
		}

		if err = w.applyState(ctx, logger, roomID, roomNID, roomInfo, stateEventIDs, resyncStartTime); err != nil {
			lastErr = err
			continue
		}
		return nil
	}

	return lastErr
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

	// Step 2: Figure out which events we already have locally.
	missing := w.findMissingEvents(ctx, logger, roomID, roomVersion, allIDs)
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
	wantSet := make(map[string]bool, len(wantIDs))
	for _, id := range wantIDs {
		wantSet[id] = true
	}
	if stillMissing := w.findMissingEvents(ctx, logger, roomID, roomVersion, wantSet); len(stillMissing) > 0 {
		return nil, fmt.Errorf(
			"resync incomplete: %d of %d state events still missing after fetch",
			len(stillMissing), len(wantIDs),
		)
	}

	return wantIDs, nil
}

// findMissingEvents checks which of the wanted event IDs are not yet stored
// locally and returns the missing ones.
func (w *PartialStateWorker) findMissingEvents(
	ctx context.Context,
	logger *logrus.Entry,
	roomID string,
	_ gomatrixserverlib.RoomVersion,
	wantIDs map[string]bool,
) []string {
	idList := make([]string, 0, len(wantIDs))
	for id := range wantIDs {
		idList = append(idList, id)
	}

	var res roomserverAPI.QueryEventsByIDResponse
	if err := w.rsAPI.QueryEventsByID(ctx, &roomserverAPI.QueryEventsByIDRequest{
		RoomID:   roomID,
		EventIDs: idList,
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

func (w *PartialStateWorker) getEventFromServer(
	ctx context.Context,
	origin, serverName spec.ServerName,
	eventID string,
	verImpl gomatrixserverlib.IRoomVersion,
	bypassBackoff bool,
) (gomatrixserverlib.PDU, error) {
	reqctx, cancel := context.WithTimeout(ctx, 30*time.Second) //nolint:mnd
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
