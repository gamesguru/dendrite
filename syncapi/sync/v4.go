// Copyright 2025 Jackmaninov
// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal"
	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
	syncinternal "codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

const maxSlidingSyncRequestEntries = 100

// mustAwaitFullState checks if a required_state configuration requires full room state.
// Returns true if the subscription should be skipped for partial state rooms.
// Per MSC3706/Synapse: partial state rooms can only be subscribed to if the requested
// state can be satisfied from the local server's perspective.
func mustAwaitFullState(requiredState types.RequiredStateConfig, cfg *config.Global) bool {
	for _, tuple := range requiredState.Include {
		if len(tuple) < 2 { //nolint:mnd
			continue
		}
		stateType, stateKey := tuple[0], tuple[1]

		// Wildcard state type requests require full state
		if stateType == "*" {
			return true
		}

		// For m.room.member events, check if we need remote user memberships
		if stateType == "m.room.member" {
			// Wildcard member requests require full state
			if stateKey == "*" {
				return true
			}
			// $LAZY and $ME are special - can be satisfied locally
			if stateKey == "$LAZY" || stateKey == "$ME" {
				continue
			}
			// Check if this is a remote user
			if strings.HasPrefix(stateKey, "@") && strings.Contains(stateKey, ":") {
				// Extract server name from user ID
				parts := strings.SplitN(stateKey, ":", 2)
				if len(parts) == 2 { //nolint:mnd
					serverName := spec.ServerName(parts[1])
					if !cfg.IsLocalServerName(serverName) {
						// Remote user membership request requires full state
						return true
					}
				}
			}
		}
	}
	return false
}

// V4ConnectionState tracks per-connection state for sliding sync
// Phase 10: Stream-based delta tracking.
type V4ConnectionState struct {
	// Database connection key (stable identifier)
	ConnectionKey int64
	// Connection position for THIS response (created at start of request)
	ConnectionPosition int64
	// Stream states from previous syncs (for delta computation)
	// map[roomID]map[stream]*StreamState
	PreviousStreamStates map[string]map[string]*types.SlidingSyncStreamState
	// Room configs from previous syncs (for timeline expansion tracking)
	// map[roomID]*RoomConfig
	PreviousRoomConfigs map[string]*types.SlidingSyncRoomConfig
	// Rooms whose account_data position this response has already written, so
	// copyForwardAccountDataState knows which ones still need carrying over.
	AccountDataWritten map[string]bool
}

// laggingRoomSet lists the rooms whose own last-sent token sits behind the
// connection's since token, along with the oldest of those tokens.
type laggingRoomSet struct {
	roomIDs     []string
	oldestToken types.StreamPosition
}

// laggingRooms finds rooms that were excluded from earlier responses while the
// connection advanced elsewhere. Events sent into such a room during the gap
// predate since, so the since-keyed change filter cannot see them; they are
// recovered by the widened lower bound in getTimelineEvents, but only if the
// room survives filtering in the first place.
func laggingRooms(roomStates map[string]types.RoomStreamState, since types.StreamPosition) laggingRoomSet {
	var lagging laggingRoomSet
	for roomID, roomState := range roomStates {
		if roomState.LastToken == nil || roomState.LastToken.PDUPosition >= since {
			continue
		}
		if len(lagging.roomIDs) == 0 || roomState.LastToken.PDUPosition < lagging.oldestToken {
			lagging.oldestToken = roomState.LastToken.PDUPosition
		}
		lagging.roomIDs = append(lagging.roomIDs, roomID)
	}
	return lagging
}

// determineRoomStreamState determines the RoomStreamState for a room based on connection state
// This is used to drive incremental sync behavior (initial vs live vs previously).
// CRITICAL: Detects membership transitions (like v3 sync's NewlyJoined) to properly handle
// rejoin scenarios where a user left/was kicked and then rejoined.
func determineRoomStreamState(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	connState *V4ConnectionState,
	roomID string,
	userID string,
) types.RoomStreamState {
	if connState == nil || connState.PreviousStreamStates == nil {
		logrus.WithField("room_id", roomID).Debug("[V4_STATE_DEBUG] connState is nil or no PreviousStreamStates")
		return types.RoomStreamState{
			Status:    types.HaveSentRoomNever,
			LastToken: nil,
		}
	}

	var previousState *types.SlidingSyncStreamState
	if connState.PreviousStreamStates[roomID] != nil {
		previousState = connState.PreviousStreamStates[roomID]["events"]
	}

	if previousState == nil {
		// Room has never been sent on this connection
		logrus.WithField("room_id", roomID).Debug("[V4_STATE_DEBUG] No previous state found for room - status=NEVER")
		return types.RoomStreamState{
			Status:    types.HaveSentRoomNever,
			LastToken: nil,
		}
	}

	// Room was sent before - parse the last token
	var lastToken *types.StreamingToken
	if previousState.LastToken != "" {
		parsedToken, err := types.NewStreamTokenFromString(previousState.LastToken)
		if err != nil {
			logrus.WithError(err).WithField("room_id", roomID).Warn("[V4_STATE_DEBUG] Failed to parse LastToken, treating as initial")
			return types.RoomStreamState{
				Status:    types.HaveSentRoomNever,
				LastToken: nil,
			}
		}
		lastToken = &parsedToken
	}

	if lastToken == nil {
		logrus.WithFields(logrus.Fields{
			"room_id":     roomID,
			"room_status": previousState.RoomStatus,
			"last_token":  previousState.LastToken,
		}).Debug("[V4_STATE_DEBUG] LastToken is nil after parsing - status=NEVER")
		return types.RoomStreamState{
			Status:    types.HaveSentRoomNever,
			LastToken: nil,
		}
	}

	// CRITICAL FIX: Check for membership transitions (like v3 sync's NewlyJoined detection)
	// If the user has transitioned TO join from a non-join state, treat as newly joined
	// This handles kick→rejoin, leave→rejoin, ban→unban+join, invite→join scenarios

	// First query the current membership (at the latest position)
	currentMembership, _, err := snapshot.SelectMembershipForUser(
		ctx, roomID, userID, math.MaxInt64,
	)
	if err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"room_id": roomID,
			"user_id": userID,
		}).Warn("[V4_STATE_DEBUG] Failed to query current membership, treating as incremental")
		// On error, fall through to normal LIVE/PREVIOUSLY logic
	} else if currentMembership == spec.Join {
		// User is currently joined - check if this is a transition from non-join
		// Query their membership at the last sync position to detect transitions
		// Use lastToken.PDUPosition as the topological position cutoff
		// SelectMembershipForUser returns the membership at or before that position
		prevMembership, _, err := snapshot.SelectMembershipForUser(
			ctx, roomID, userID, int64(lastToken.PDUPosition),
		)
		if err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"room_id":  roomID,
				"user_id":  userID,
				"position": lastToken.PDUPosition,
			}).Warn("[V4_STATE_DEBUG] Failed to query previous membership, treating as incremental")
			// On error, fall through to normal LIVE/PREVIOUSLY logic
		} else if prevMembership != spec.Join {
			// Membership transition detected: non-join → join
			// This is a "newly joined" room - treat as initial regardless of previous connection state
			logrus.WithFields(logrus.Fields{
				"room_id":            roomID,
				"prev_membership":    prevMembership,
				"current_membership": currentMembership,
				"last_position":      lastToken.PDUPosition,
			}).Info("[V4_STATE_DEBUG] Membership transition detected (rejoin) - status=NEVER")
			return types.RoomStreamState{
				Status:    types.HaveSentRoomNever,
				LastToken: nil, // Nil token to trigger full state/timeline fetch
			}
		}
		// else: prevMembership == join, so this is a continuing join (not a transition)
	}

	// No membership transition detected - determine if LIVE or PREVIOUSLY based on room_status
	if previousState.RoomStatus == types.HaveSentRoomLive.String() {
		logrus.WithFields(logrus.Fields{
			"room_id":     roomID,
			"room_status": previousState.RoomStatus,
			"last_token":  previousState.LastToken,
		}).Debug("[V4_STATE_DEBUG] Room status=LIVE from database")
		return types.RoomStreamState{
			Status:    types.HaveSentRoomLive,
			LastToken: lastToken,
		}
	}

	logrus.WithFields(logrus.Fields{
		"room_id":     roomID,
		"room_status": previousState.RoomStatus,
		"last_token":  previousState.LastToken,
	}).Debug("[V4_STATE_DEBUG] Room status=PREVIOUSLY from database")
	return types.RoomStreamState{
		Status:    types.HaveSentRoomPreviously,
		LastToken: lastToken,
	}
}

// logV4Response logs full response body at trace level (sensitive data)
// Enable with logging.*.level: trace in config.
func logV4Response(response any, userID, deviceID string, statusCode int) {
	responseBodyJSON, err := json.Marshal(response)
	if err == nil {
		logrus.WithFields(logrus.Fields{
			"timestamp":     time.Now().Format(time.RFC3339),
			"user_id":       userID,
			"device_id":     deviceID,
			"status":        statusCode,
			"response_body": string(responseBodyJSON),
		}).Trace("[V4_SYNC_DEBUG] Full response")
	}
}

// copyForwardStreamStates persists the "events" stream state of every room the
// client has been sent on this connection but which is absent from the response
// being built, so that cascade-deleting old positions does not lose it and make
// the room look like it was never sent.
//
// Each such room is recorded as PREVIOUSLY rather than keeping its stored
// status, mirroring Synapse's record_unsent_rooms: the room was sent before but
// is not part of this response. LastToken is carried over untouched, so it keeps
// pointing at the position the room was last actually sent at, which is where a
// future gap recovery has to resume from.
//
// Nothing distinguishes PREVIOUSLY from LIVE when reading the state back yet
// (see determineRoomStreamState's callers), so this only aligns what is stored
// with the state machine documented on types.HaveSentRoomFlag.
//
// It returns the number of rooms whose state was copied forward. Once the
// request context is done it stops and returns the context's error instead of
// failing, and logging, one write per remaining room: the client is gone, so
// the new position is never acknowledged and is deleted when the client
// retries from the position it still holds.
func (rp *RequestPool) copyForwardStreamStates(
	ctx context.Context,
	connState *V4ConnectionState,
	roomsToPopulate map[string]types.RoomSubscriptionConfig,
) (int, error) {
	copiedCount := 0
	for roomID, streams := range connState.PreviousStreamStates {
		if err := ctx.Err(); err != nil {
			return copiedCount, err
		}
		// Skip rooms that were processed in this response (they already have updated state)
		if _, processed := roomsToPopulate[roomID]; processed {
			continue
		}
		eventsStream, ok := streams["events"]
		if !ok {
			continue
		}
		if updateErr := rp.db.UpdateConnectionStream(ctx, connState.ConnectionPosition, roomID, "events", types.HaveSentRoomPreviously.String(), eventsStream.LastToken); updateErr != nil {
			if err := ctx.Err(); err != nil {
				return copiedCount, err
			}
			logrus.WithError(updateErr).WithFields(logrus.Fields{
				"room_id":             roomID,
				"connection_position": connState.ConnectionPosition,
			}).Error("[V4_STATE_DEBUG] Failed to copy forward stream state")
			continue
		}
		copiedCount++
	}
	if copiedCount > 0 {
		logrus.WithFields(logrus.Fields{
			"copied_count":        copiedCount,
			"connection_position": connState.ConnectionPosition,
		}).Debug("[V4_STATE_DEBUG] Copied forward stream states for unchanged rooms")
	}
	return copiedCount, nil
}

// copyForwardRoomConfigs persists the room config of every room the client has
// been sent on this connection but which is absent from the response being
// built. Without it, cascade-deleting the old position loses the config and
// the room's next appearance is mistaken for a timeline expansion.
//
// Like copyForwardStreamStates, it stops at the first sign of cancellation and
// returns the context's error.
func (rp *RequestPool) copyForwardRoomConfigs(
	ctx context.Context,
	connState *V4ConnectionState,
	roomsToPopulate map[string]types.RoomSubscriptionConfig,
) error {
	copiedCount := 0
	for roomID, prevConfig := range connState.PreviousRoomConfigs {
		if err := ctx.Err(); err != nil {
			return err
		}
		// Skip rooms that were processed in this response (they already have updated config)
		if _, processed := roomsToPopulate[roomID]; processed {
			continue
		}
		if updateErr := rp.db.UpdateRoomConfig(ctx, connState.ConnectionPosition, roomID, prevConfig.TimelineLimit, prevConfig.RequiredStateID); updateErr != nil {
			if err := ctx.Err(); err != nil {
				return err
			}
			logrus.WithError(updateErr).WithFields(logrus.Fields{
				"room_id":             roomID,
				"connection_position": connState.ConnectionPosition,
			}).Error("[V4_STATE_DEBUG] Failed to copy forward room config")
			continue
		}
		copiedCount++
	}
	if copiedCount > 0 {
		logrus.WithFields(logrus.Fields{
			"copied_count":        copiedCount,
			"connection_position": connState.ConnectionPosition,
		}).Debug("[V4_STATE_DEBUG] Copied forward room configs for unchanged rooms")
	}
	return nil
}

// abandonV4Sync answers a sliding sync request whose context ended while its
// response was being built. The client cannot receive the answer, so it is
// reported once here instead of once per room or extension that failed after
// the fact.
//
// The new connection position, possibly only partly written, is never handed
// out: the response carries the position the client already holds (or none
// for an initial sync), so a retry resumes from state that was fully
// persisted, and DeleteOtherConnectionPositions removes the partial one.
func abandonV4Sync(ctx context.Context, since *types.SlidingSyncStreamToken, connState *V4ConnectionState) util.JSONResponse {
	logrus.WithError(context.Cause(ctx)).WithFields(logrus.Fields{
		"connection_key":      connState.ConnectionKey,
		"connection_position": connState.ConnectionPosition,
	}).Debug("[V4_SYNC] Abandoning sliding sync request, the client went away")
	resp := types.SlidingSyncResponse{
		Lists:      make(map[string]types.SlidingList),
		Rooms:      make(map[string]types.SlidingRoomData),
		Extensions: &types.ExtensionResponse{},
	}
	if since != nil {
		resp.Pos = since.String()
	}
	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: resp,
	}
}

// recordDeliveredReceipts marks receipts as delivered on this connection, in a
// write transaction separate from the response's read-only snapshot.
//
// The connection receipt table is keyed by connection, not by position, so a
// receipt recorded here is filtered out of every later response, including a
// retry from an older position. It therefore must not be recorded for a
// response the client will never see: it returns false, having written
// nothing, once the request context is done, and the caller abandons the
// response. Other write failures are logged and leave the receipts to be
// delivered again.
func (rp *RequestPool) recordDeliveredReceipts(ctx context.Context, connectionKey int64, receipts []types.OutputReceiptEvent) bool {
	if len(receipts) == 0 {
		return true
	}
	if ctx.Err() != nil {
		return false
	}
	logrus.WithField("count", len(receipts)).Debug("[RECEIPTS] Updating connection state for delivered receipts")
	txn, err := rp.db.NewDatabaseTransaction(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return false
		}
		logrus.WithError(err).Error("[RECEIPTS] Failed to create write transaction")
		return true
	}
	defer func() { _ = txn.Rollback() }()
	for _, receipt := range receipts {
		if err = txn.UpsertConnectionReceipt(
			ctx, connectionKey,
			receipt.RoomID, receipt.Type, receipt.UserID,
			receipt.EventID, receipt.ThreadID, receipt.Timestamp,
		); err != nil {
			if ctx.Err() != nil {
				return false
			}
			logrus.WithError(err).WithFields(logrus.Fields{
				"room_id": receipt.RoomID,
				"type":    receipt.Type,
			}).Error("[RECEIPTS] Failed to update connection receipt")
			// Keep the receipts already recorded.
			break
		}
	}
	// Checked last so that a cancellation during the upserts commits nothing.
	if ctx.Err() != nil {
		return false
	}
	if err := txn.Commit(); err != nil {
		if ctx.Err() != nil {
			return false
		}
		logrus.WithError(err).Error("[RECEIPTS] Failed to commit connection receipt updates")
	}
	return true
}

// OnIncomingSyncRequestV4 handles POST /v4/sync requests (MSC4186 Simplified Sliding Sync)
//
//nolint:gocyclo
func (rp *RequestPool) OnIncomingSyncRequestV4(req *http.Request, device *userapi.Device) util.JSONResponse {
	// Create a root span for tracing the entire sync request
	trace, ctx := internal.StartTask(req.Context(), "SlidingSync.V4")
	defer trace.EndTask()
	trace.SetTag("user_id", device.UserID)
	trace.SetTag("device_id", device.ID)

	// Replace request context with traced context
	req = req.WithContext(ctx)

	// Parse request body
	var v4Req types.SlidingSyncRequest
	if err := json.NewDecoder(req.Body).Decode(&v4Req); err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.BadJSON(fmt.Sprintf("Failed to parse request body: %s", err.Error())),
		}
	}
	if len(v4Req.Lists) > maxSlidingSyncRequestEntries || len(v4Req.RoomSubscriptions) > maxSlidingSyncRequestEntries {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("At most 100 lists and 100 room subscriptions are allowed")}
	}

	// Read from query parameters if present (takes precedence over JSON body for compatibility)
	// Element Web and other clients using MSC3575 may send pos/timeout as URL params
	if posQuery := req.URL.Query().Get("pos"); posQuery != "" {
		v4Req.Pos = posQuery
	}
	if timeoutQuery := req.URL.Query().Get("timeout"); timeoutQuery != "" {
		logrus.WithField("timeout_query", timeoutQuery).Debug("[V4_SYNC] Got timeout from query param")
		if timeout, err := time.ParseDuration(timeoutQuery + "ms"); err == nil {
			v4Req.Timeout = int(timeout.Milliseconds())
			logrus.WithField("timeout_ms", v4Req.Timeout).Debug("[V4_SYNC] Parsed timeout successfully")
		} else {
			logrus.WithError(err).WithField("timeout_query", timeoutQuery).Error("[V4_SYNC] Failed to parse timeout from query param")
		}
	}

	// Default connection ID if not provided
	connID := v4Req.ConnID
	if connID == "" {
		connID = "default"
	}
	trace.SetTag("conn_id", connID)
	trace.SetTag("is_initial", v4Req.Pos == "")
	trace.SetTag("num_lists", len(v4Req.Lists))

	// DEBUG: Log incoming request details
	logrus.WithFields(logrus.Fields{
		"user_id":        device.UserID,
		"device_id":      device.ID,
		"conn_id":        connID,
		"pos":            v4Req.Pos,
		"timeout":        v4Req.Timeout,
		"num_lists":      len(v4Req.Lists),
		"num_room_subs":  len(v4Req.RoomSubscriptions),
		"has_extensions": v4Req.Extensions != nil,
	}).Info("[V4_SYNC] Incoming sync request")

	// DEBUG: Full request body logging at trace level (sensitive data)
	// Enable with logging.*.level: trace in config
	requestBodyJSON, err := json.Marshal(v4Req)
	if err == nil {
		logrus.WithFields(logrus.Fields{
			"timestamp":    time.Now().Format(time.RFC3339),
			"user_id":      device.UserID,
			"device_id":    device.ID,
			"method":       req.Method,
			"path":         req.URL.Path,
			"query":        req.URL.RawQuery,
			"request_body": string(requestBodyJSON),
		}).Trace("[V4_SYNC_DEBUG] Full request")
	}

	// Parse position token if provided
	var since *types.SlidingSyncStreamToken
	if v4Req.Pos != "" {
		since, err = types.ParseSlidingSyncStreamToken(v4Req.Pos)
		if err != nil {
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.MatrixError{
					ErrCode: spec.ErrorUnknownPos,
					Err:     fmt.Sprintf("Invalid position token: %s", err.Error()),
				},
			}
		}
	}

	// Phase 10: Get or create connection (returns connection_key)
	connectionKey, err := rp.db.GetOrCreateConnection(ctx, device.UserID, device.ID, connID)
	if err != nil {
		logrus.WithError(err).Error("Failed to get or create sliding sync connection")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	// Validate position token if provided
	if since != nil {
		// Validate that the position exists and belongs to this connection
		err = rp.db.ValidateConnectionPosition(ctx, since.ConnectionPosition, connectionKey)
		if err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"provided_position": since.ConnectionPosition,
				"connection_key":    connectionKey,
			}).Warn("Invalid position token - client should start fresh")
			// Return M_UNKNOWN_POS to signal the client to start a fresh connection
			// This matches Synapse behavior and tells the client the position is stale
			return util.JSONResponse{
				Code: http.StatusBadRequest,
				JSON: spec.MatrixError{
					ErrCode: spec.ErrorUnknownPos,
					Err:     "Connection position not found or expired. Please start a new sync connection.",
				},
			}
		}

		// Clean up old positions (like Synapse does)
		// Now that the client has used this position, we can delete all other positions
		// This prevents old state from accumulating and bleeding into new sessions
		if cleanupErr := rp.db.DeleteOtherConnectionPositions(ctx, connectionKey, since.ConnectionPosition); cleanupErr != nil {
			logrus.WithError(cleanupErr).WithFields(logrus.Fields{
				"connection_key": connectionKey,
				"keep_position":  since.ConnectionPosition,
			}).Warn("Failed to clean up old connection positions")
			// Non-fatal - continue with the sync
		}
	}

	// Phase 10: Load previous stream states and room configs for delta computation
	// IMPORTANT: Only load previous states for incremental syncs (pos is non-empty)
	// For initial syncs (pos=""), start fresh with no previous state
	// Use position-specific query to avoid old state from previous sessions bleeding in
	var previousStreamStates map[string]map[string]*types.SlidingSyncStreamState
	var previousRoomConfigs map[string]*types.SlidingSyncRoomConfig
	if since != nil {
		// Load streams for the SPECIFIC position the client is syncing from
		// This is critical: we want the state AS IT WAS at that position, not "latest across all positions"
		previousStreamStates, err = rp.db.GetConnectionStreamsByPosition(ctx, since.ConnectionPosition)
		if err != nil {
			logrus.WithError(err).Error("Failed to load connection stream states")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		logrus.WithFields(logrus.Fields{
			"connection_key":      connectionKey,
			"connection_position": since.ConnectionPosition,
			"num_rooms_loaded":    len(previousStreamStates),
		}).Debug("[V4_STATE_DEBUG] Loaded previous stream states for specific position")
		// Log specific room states for debugging
		for roomID, streams := range previousStreamStates {
			if eventsStream, ok := streams["events"]; ok {
				logrus.WithFields(logrus.Fields{
					"room_id":             roomID,
					"room_status":         eventsStream.RoomStatus,
					"last_token":          eventsStream.LastToken,
					"connection_position": eventsStream.ConnectionPosition,
				}).Debug("[V4_STATE_DEBUG] Loaded room state from database")
			}
		}

		// Also load room configs for timeline expansion tracking
		previousRoomConfigs, err = rp.db.GetRoomConfigsByPosition(ctx, since.ConnectionPosition)
		if err != nil {
			logrus.WithError(err).Error("Failed to load connection room configs")
			// Non-fatal - continue with empty configs (will trigger timeline expansion)
			previousRoomConfigs = make(map[string]*types.SlidingSyncRoomConfig)
		} else {
			logrus.WithFields(logrus.Fields{
				"connection_key":      connectionKey,
				"connection_position": since.ConnectionPosition,
				"num_configs_loaded":  len(previousRoomConfigs),
			}).Debug("[V4_STATE_DEBUG] Loaded previous room configs for specific position")
		}
	} else {
		// Initial sync - no previous states
		previousStreamStates = make(map[string]map[string]*types.SlidingSyncStreamState)
		previousRoomConfigs = make(map[string]*types.SlidingSyncRoomConfig)
		logrus.Debug("[V4_STATE_DEBUG] Initial sync - no previous stream states")

		// Clear stale receipt delivery state for this connection
		// This ensures receipts are re-delivered on fresh sync (e.g., after logout/login, token expiry)
		if deleteErr := rp.db.DeleteConnectionReceipts(ctx, connectionKey); deleteErr != nil {
			logrus.WithError(deleteErr).WithField("connection_key", connectionKey).Warn("Failed to clear connection receipts on fresh sync")
			// Non-fatal - continue with the sync
		}
	}

	// Create connection state for this request
	connState := &V4ConnectionState{
		ConnectionKey:        connectionKey,
		ConnectionPosition:   0, // Will be set after creating position
		PreviousStreamStates: previousStreamStates,
		PreviousRoomConfigs:  previousRoomConfigs,
	}

	// DEBUG: Log connection state
	numRoomsSent := len(previousStreamStates)
	logrus.WithFields(logrus.Fields{
		"conn_id":        connID,
		"connection_key": connectionKey,
		"num_rooms_sent": numRoomsSent,
	}).Debug("[V4_SYNC] Connection state loaded")

	// Update presence and last seen (reuse existing v3 logic)
	rp.updateLastSeen(req, device)
	rp.updatePresenceExplicit(rp.db, v4Req.SetPresence, device.UserID)
	rp.waitForMembershipChanges(ctx, device.UserID)

	// Main sync loop - similar to v3 sync
	// Loop until we have updates to return or timeout expires
	for {
		startTime := time.Now()

		// Get current position from notifier
		currentPos := rp.Notifier.CurrentPosition()

		// For incremental syncs with timeout, wait for changes
		// This implements long-polling (per MSC4186 spec line 236)
		// Synapse behavior: always wait if timeout > 0 AND since != nil, regardless of global position
		logrus.WithFields(logrus.Fields{
			"since_nil": since == nil,
			"timeout":   v4Req.Timeout,
			"will_wait": since != nil && v4Req.Timeout > 0,
		}).Info("[V4_SYNC] Long polling check")

		if since != nil && v4Req.Timeout > 0 {
			logrus.WithField("timeout_ms", v4Req.Timeout).Info("[V4_SYNC] Entering long poll wait")
			// Incremental sync with timeout - wait for new events
			timeout := time.Duration(v4Req.Timeout) * time.Millisecond

			// Wait for changes using the notifier
			timer := time.NewTimer(timeout)
			defer timer.Stop()

			// Create a minimal sync request for the notifier
			syncReq := &types.SyncRequest{
				Context:       ctx,
				Device:        device,
				Since:         since.StreamToken,
				Timeout:       timeout,
				WantFullState: false,
			}

			userStream := rp.Notifier.GetListener(*syncReq)
			defer userStream.Close()

			select {
			case <-userStream.GetNotifyChannel(since.StreamToken):
				// New events arrived, continue processing
				logrus.Info("[V4_SYNC] User stream notified - events may be available")
				currentPos = rp.Notifier.CurrentPosition()
				currentPos.ApplyUpdates(userStream.GetSyncPosition())
				logrus.WithFields(logrus.Fields{
					"old_pos": since.StreamToken.String(),
					"new_pos": currentPos.String(),
				}).Info("[V4_SYNC] Position updated after notification")
			case <-timer.C:
				// Timeout - return current position without changes
				logrus.Info("[V4_SYNC] Timeout expired with no changes")
				timeoutResp := rp.buildInterruptResponse(ctx, &v4Req, device, connState, since, currentPos, connectionKey, "timeout")
				logV4Response(*timeoutResp, device.UserID, device.ID, http.StatusOK)
				return util.JSONResponse{
					Code: http.StatusOK,
					JSON: timeoutResp,
				}
			case <-process.Quiescing(ctx):
				// A planned handover answers as if the timeout had been
				// reached, so the next request reaches the successor.
				logrus.Info("[V4_SYNC] Answering early for a planned handover")
				return util.JSONResponse{
					Code: http.StatusOK,
					JSON: rp.buildInterruptResponse(ctx, &v4Req, device, connState, since, currentPos, connectionKey, "timeout"),
				}
			case <-ctx.Done():
				// Client disconnected
				logrus.Info("[V4_SYNC] Client disconnected during wait")
				disconnectResp := rp.buildInterruptResponse(ctx, &v4Req, device, connState, since, currentPos, connectionKey, "disconnect")
				logV4Response(*disconnectResp, device.UserID, device.ID, http.StatusOK)
				return util.JSONResponse{
					Code: http.StatusOK,
					JSON: disconnectResp,
				}
			}
		}

		// The notifier advances after the consumer commits, so it can lag events
		// that are already stored, such as a leave that the client's own request
		// has just completed. Every snapshot this response opens from here on is
		// newer, so it can read up to the committed high-water mark.
		currentPos.PDUPosition = rp.committedPDUPosition(ctx, currentPos.PDUPosition)

		// Phase 10: Create new connection position for this sync response
		// This is what goes into the pos token
		connState.ConnectionPosition, err = rp.db.CreateConnectionPosition(ctx, connState.ConnectionKey)
		if err != nil {
			if ctx.Err() != nil {
				return abandonV4Sync(ctx, since, connState)
			}
			logrus.WithError(err).Error("Failed to create connection position")
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}

		// Create new position token for response
		newToken := types.NewSlidingSyncStreamToken(connState.ConnectionPosition, currentPos)

		// Build response
		response := types.SlidingSyncResponse{
			Pos:        newToken.String(),
			Lists:      make(map[string]types.SlidingList),
			Rooms:      make(map[string]types.SlidingRoomData),
			Extensions: &types.ExtensionResponse{},
		}

		// Phase 2: Process room lists
		// Track which rooms appear in lists and their timeline_limit
		roomsInLists := make(map[string]types.RoomSubscriptionConfig) // map[roomID]config with timeline_limit and required_state
		roomsRemovedFromLists := make(map[string]bool)
		// When pos is empty, force initial sync for all lists
		forceInitialSync := (since == nil)
		for listName, listConfig := range v4Req.Lists {
			logrus.WithFields(logrus.Fields{
				"list_name":      listName,
				"range":          listConfig.Range,
				"timeline_limit": listConfig.TimelineLimit,
				"force_initial":  forceInitialSync,
			}).Debug("[V4_SYNC] Processing list")

			if ctx.Err() != nil {
				return abandonV4Sync(ctx, since, connState)
			}
			list, err := rp.processRoomList(ctx, device.UserID, listName, listConfig, connState, forceInitialSync)
			if err != nil {
				if ctx.Err() != nil {
					return abandonV4Sync(ctx, since, connState)
				}
				// Log error but continue processing other lists
				logrus.WithError(err).WithField("list_name", listName).Error("[V4_SYNC] Failed to process list")
				continue
			}
			// Always include the list in response (Synapse returns lists even with no changes)
			response.Lists[listName] = list
			logrus.WithFields(logrus.Fields{
				"list_name": listName,
				"count":     list.Count,
				"num_ops":   len(list.Ops),
			}).Info("[V4_SYNC] List processed")

			for i, op := range list.Ops {
				logrus.WithFields(logrus.Fields{
					"list_name":    listName,
					"op_index":     i,
					"op_type":      op.Op,
					"range":        op.Range,
					"num_room_ids": len(op.RoomIDs),
				}).Debug("[V4_SYNC] List operation")
			}

			// Track every room in the current window, not only rooms that
			// appeared in an operation. Incremental responses omit list
			// operations when the window did not change, but those rooms may
			// still have new events or an expanded timeline.
			for _, roomID := range list.WindowRoomIDs {
				config := types.RoomSubscriptionConfig{
					TimelineLimit: listConfig.TimelineLimit,
					RequiredState: listConfig.RequiredState,
				}
				if existing, exists := roomsInLists[roomID]; exists {
					roomsInLists[roomID] = mergeRoomSubscriptionConfig(existing, config)
				} else {
					roomsInLists[roomID] = config
				}
			}
			for _, roomID := range list.RemovedRoomIDs {
				roomsRemovedFromLists[roomID] = true
				config := types.RoomSubscriptionConfig{
					TimelineLimit: listConfig.TimelineLimit,
					RequiredState: listConfig.RequiredState,
				}
				if existing, exists := roomsInLists[roomID]; exists {
					roomsInLists[roomID] = mergeRoomSubscriptionConfig(existing, config)
				} else {
					roomsInLists[roomID] = config
				}
			}
		}

		// Phase 3: Process room subscriptions
		// Build set of all rooms we need to return data for (from lists + subscriptions)
		roomsToPopulate := make(map[string]types.RoomSubscriptionConfig)

		// Get partial state room IDs for filtering (MSC3706 faster joins)
		// This is used for both list rooms and explicit subscriptions
		partialStateRooms := make(map[string]bool)
		partialStateRoomIDs, err := rp.rsAPI.GetPartialStateRoomIDs(ctx)
		if err != nil {
			logrus.WithError(err).Warn("[V4_SYNC] Failed to get partial state rooms")
		} else {
			for _, roomID := range partialStateRoomIDs {
				partialStateRooms[roomID] = true
			}
			if len(partialStateRooms) > 0 {
				logrus.WithField("count", len(partialStateRooms)).Debug("[V4_SYNC] Found partial state rooms for filtering")
			}
		}

		// Add rooms from lists with their config (timeline_limit and required_state) from the list config
		// Also apply partial state filtering (MSC3706)
		for roomID, config := range roomsInLists {
			// Filter partial state rooms if required_state needs full state
			if partialStateRooms[roomID] && mustAwaitFullState(config.RequiredState, rp.cfg.Matrix) {
				logrus.WithFields(logrus.Fields{
					"room_id": roomID,
				}).Debug("[V4_SYNC] Filtering out partial state room from list - requires full state")
				continue
			}
			roomsToPopulate[roomID] = config
		}

		// Add/merge rooms from explicit subscriptions
		// Explicit subscriptions override list config for that room
		// Per MSC4186 and Synapse behavior, we must filter subscriptions to only include
		// rooms where the user has appropriate membership (not self-left)
		// Filters applied (matching Synapse):
		// 1. Membership filtering: exclude self-left rooms, allow kicked rooms
		// 2. Ignored user invite filtering: exclude invites from ignored users
		// 3. Partial state filtering (MSC3706): exclude partial state rooms if required_state needs full state
		roomsLeftOnConnection := make(map[string]bool)
		if len(v4Req.RoomSubscriptions) > 0 {
			subSnapshot, subSnapshotErr := rp.db.NewDatabaseSnapshot(ctx)
			if subSnapshotErr != nil {
				logrus.WithError(subSnapshotErr).Error("[V4_SYNC] Failed to create snapshot for subscription filtering")
			} else {
				defer func() { _ = subSnapshot.Rollback() }()

				// Get list of kicked rooms (leave where sender != user) to allow those subscriptions
				kickedRoomIDs, kickedErr := subSnapshot.KickedRoomIDs(ctx, device.UserID)
				kickedRooms := make(map[string]bool)
				if kickedErr != nil {
					logrus.WithError(kickedErr).Warn("[V4_SYNC] Failed to get kicked rooms, will filter all left rooms")
				} else {
					for _, roomID := range kickedRoomIDs {
						kickedRooms[roomID] = true
					}
				}

				// Get ignored users for invite filtering
				ignoredUsers := make(map[string]bool)
				ignoresData, ignoresErr := subSnapshot.IgnoresForUser(ctx, device.UserID)
				if ignoresErr != nil {
					logrus.WithError(ignoresErr).Warn("[V4_SYNC] Failed to get ignored users")
				} else if ignoresData != nil && ignoresData.List != nil {
					for userID := range ignoresData.List {
						ignoredUsers[userID] = true
					}
				}

				// Get current invites to build room -> sender map for ignored user filtering
				inviteSenders := make(map[string]string)
				if len(ignoredUsers) > 0 {
					maxInviteID, maxInviteErr := subSnapshot.MaxStreamPositionForInvites(ctx)
					if maxInviteErr == nil && maxInviteID > 0 {
						inviteRange := types.Range{From: 0, To: maxInviteID, Backwards: false}
						invites, _, _, invitesErr := subSnapshot.InviteEventsInRange(ctx, device.UserID, inviteRange)
						if invitesErr != nil {
							logrus.WithError(invitesErr).Warn("[V4_SYNC] Failed to get invites for ignored user filtering")
						} else {
							for roomID, inviteEvent := range invites {
								inviteSenders[roomID] = string(inviteEvent.SenderID())
							}
						}
					}
				}

				for roomID, subConfig := range v4Req.RoomSubscriptions {
					// Check if user is allowed to subscribe to this room
					// Per Synapse's filter_membership_for_sync:
					// - Include joined, invited, banned rooms
					// - Include kicked rooms (leave where sender != user)
					// - Exclude self-left rooms (unless newly_left, which we don't track yet)
					membership, _, membershipErr := subSnapshot.SelectMembershipForUser(ctx, roomID, device.UserID, math.MaxInt64)
					if membershipErr != nil {
						logrus.WithError(membershipErr).WithFields(logrus.Fields{
							"room_id": roomID,
							"user_id": device.UserID,
						}).Debug("[V4_SYNC] Failed to get membership for subscription, skipping room")
						continue
					}

					// Filter based on membership
					if membership == spec.Leave {
						// Check if this is a kick (in kickedRooms) or self-leave
						if !kickedRooms[roomID] {
							// Include a self-leave once when this connection already
							// saw the room, so its client receives the leave event.
							if determineRoomStreamState(ctx, subSnapshot, connState, roomID, device.UserID).Status == types.HaveSentRoomNever {
								logrus.WithFields(logrus.Fields{
									"room_id": roomID,
									"user_id": device.UserID,
								}).Debug("[V4_SYNC] Filtering out unseen self-left room from subscription")
								continue
							}
							roomsLeftOnConnection[roomID] = true
						}
						// Kicked - allow subscription
						logrus.WithFields(logrus.Fields{
							"room_id":    roomID,
							"user_id":    device.UserID,
							"membership": membership,
						}).Debug("[V4_SYNC] Allowing kicked room in subscription")
					}

					// Filter invites from ignored users
					if membership == spec.Invite {
						if sender, ok := inviteSenders[roomID]; ok && ignoredUsers[sender] {
							logrus.WithFields(logrus.Fields{
								"room_id":       roomID,
								"user_id":       device.UserID,
								"invite_sender": sender,
							}).Debug("[V4_SYNC] Filtering out invite from ignored user")
							continue
						}
					}

					// Filter partial state rooms if required_state needs full state (MSC3706)
					if partialStateRooms[roomID] && mustAwaitFullState(subConfig.RequiredState, rp.cfg.Matrix) {
						logrus.WithFields(logrus.Fields{
							"room_id": roomID,
							"user_id": device.UserID,
						}).Debug("[V4_SYNC] Filtering out partial state room - subscription requires full state")
						continue
					}

					roomsToPopulate[roomID] = subConfig
				}
			}
		}

		// Phase 3.5: Filter to only changed rooms for incremental sync
		// For initial sync (since == nil), include all rooms
		// For incremental sync, only include rooms with changes since last sync
		if since != nil && len(roomsToPopulate) > 0 {
			// Create temporary snapshot for filtering query
			filterSnapshot, filterSnapshotErr := rp.db.NewDatabaseSnapshot(ctx)
			if filterSnapshotErr != nil {
				if ctx.Err() != nil {
					return abandonV4Sync(ctx, since, connState)
				}
				return util.JSONResponse{
					Code: http.StatusInternalServerError,
					JSON: spec.InternalServerError{},
				}
			}
			defer func() { _ = filterSnapshot.Rollback() }()

			// Get list of all candidate room IDs
			candidateRoomIDs := make([]string, 0, len(roomsToPopulate))
			for roomID := range roomsToPopulate {
				candidateRoomIDs = append(candidateRoomIDs, roomID)
			}

			// Query database for rooms that have PDU events since the last sync position
			roomsWithPDUChanges, pduChangesErr := filterSnapshot.RoomsWithEventsSince(ctx, candidateRoomIDs, since.StreamToken.PDUPosition)
			if pduChangesErr != nil {
				logrus.WithError(pduChangesErr).Error("[V4_SYNC] Failed to filter PDU changed rooms")
				// Continue without filtering on error - return all rooms
			} else {
				// Also query for rooms with invite changes
				// Invites are tracked separately in InvitePosition stream
				roomsWithInviteChanges, inviteChangesErr := filterSnapshot.RoomsWithInvitesSince(ctx, device.UserID, candidateRoomIDs, since.StreamToken.InvitePosition)
				if inviteChangesErr != nil {
					logrus.WithError(inviteChangesErr).Error("[V4_SYNC] Failed to filter invite changed rooms")
					// Continue with just PDU filtering
					roomsWithInviteChanges = nil
				}

				// Build set of rooms to keep (rooms with PDU or invite changes + rooms never sent before)
				roomsToKeep := make(map[string]bool)
				roomKeepReasons := make(map[string]string) // Track why each room is kept for debugging
				for _, roomID := range roomsWithPDUChanges {
					roomsToKeep[roomID] = true
					roomKeepReasons[roomID] = "has_pdu_changes"
				}
				for _, roomID := range roomsWithInviteChanges {
					roomsToKeep[roomID] = true
					if roomKeepReasons[roomID] != "" {
						roomKeepReasons[roomID] += ",has_invite_changes"
					} else {
						roomKeepReasons[roomID] = "has_invite_changes"
					}
				}

				// Also include rooms that have never been sent on this connection
				// These should always be included as they're "new" to the client,
				// and rooms whose own last-sent token lags behind the connection's
				// since token: their missed events can all predate since, in which
				// case RoomsWithEventsSince above does not see them at all and the
				// widened lower bound in getTimelineEvents never gets to run.
				//
				// determineRoomStreamState issues membership queries, so each
				// candidate's state is resolved once and reused for both checks.
				roomStates := make(map[string]types.RoomStreamState, len(roomsToPopulate))
				for roomID := range roomsToPopulate {
					roomStates[roomID] = determineRoomStreamState(ctx, filterSnapshot, connState, roomID, device.UserID)
				}

				for roomID, roomState := range roomStates {
					if roomState.Status == types.HaveSentRoomNever {
						roomsToKeep[roomID] = true
						if roomKeepReasons[roomID] != "" {
							roomKeepReasons[roomID] += ",status_never"
						} else {
							roomKeepReasons[roomID] = "status_never"
						}
					}
				}
				for roomID := range roomsRemovedFromLists {
					if roomStates[roomID].Status == types.HaveSentRoomNever {
						continue
					}
					// A room which leaves a list window still needs room data when
					// the client explicitly subscribes to it. The empty `lists`
					// field tells the client that it is no longer in that window.
					if _, subscribed := v4Req.RoomSubscriptions[roomID]; subscribed {
						roomsToKeep[roomID] = true
						continue
					}
					membership, _, membershipErr := filterSnapshot.SelectMembershipForUser(ctx, roomID, device.UserID, math.MaxInt64)
					if membershipErr == nil && (membership == spec.Leave || membership == spec.Ban) {
						roomsToKeep[roomID] = true
					}
				}
				// A previously sent room must be returned once when the user leaves,
				// even if its leave event is absent from the stream-change query.
				for roomID := range roomsLeftOnConnection {
					roomsToKeep[roomID] = true
				}

				lagging := laggingRooms(roomStates, since.StreamToken.PDUPosition)
				if len(lagging.roomIDs) > 0 {
					// One batched query from the oldest lagging token. This can
					// over-report a room whose events all fall between that
					// oldest token and its own, which costs an extra room with
					// an empty timeline rather than a wrong response.
					laggingWithEvents, laggingErr := filterSnapshot.RoomsWithEventsSince(ctx, lagging.roomIDs, lagging.oldestToken)
					if laggingErr != nil {
						logrus.WithError(laggingErr).Error("[V4_SYNC] Failed to filter lagging rooms; including all of them")
						laggingWithEvents = lagging.roomIDs
					}
					for _, roomID := range laggingWithEvents {
						roomsToKeep[roomID] = true
						if roomKeepReasons[roomID] != "" {
							roomKeepReasons[roomID] += ",lagging_room_token"
						} else {
							roomKeepReasons[roomID] = "lagging_room_token"
						}
					}
				}

				// Include rooms whose timeline limit grew, whether they came from a
				// list or an explicit subscription. Expansion itself requires a
				// response even when the room has had no new events.
				for roomID, roomConfig := range roomsToPopulate {
					previous := connState.PreviousRoomConfigs[roomID]
					if previous == nil {
						continue
					}
					if roomConfig.TimelineLimit > previous.TimelineLimit || rp.requiredStateChanged(ctx, roomConfig.RequiredState, previous.RequiredStateID) {
						roomsToKeep[roomID] = true
					}
				}

				// Preserve subscription handling for rooms whose previous config
				// was not loaded into the connection snapshot.
				if len(v4Req.RoomSubscriptions) > 0 {
					subscriptionRoomIDs := make([]string, 0, len(v4Req.RoomSubscriptions))
					for roomID := range v4Req.RoomSubscriptions {
						subscriptionRoomIDs = append(subscriptionRoomIDs, roomID)
					}

					prevConfigs, batchErr := rp.db.GetLatestRoomConfigsBatch(ctx, connState.ConnectionKey, subscriptionRoomIDs)
					if batchErr != nil {
						logrus.WithError(batchErr).Debug("[V4_SYNC] Failed to batch get previous room configs")
					} else {
						for roomID, subConfig := range v4Req.RoomSubscriptions {
							prevConfig := prevConfigs[roomID]
							if prevConfig != nil {
								// Room was sent before - check if timeline_limit expanded
								if subConfig.TimelineLimit > prevConfig.TimelineLimit || rp.requiredStateChanged(ctx, subConfig.RequiredState, prevConfig.RequiredStateID) {
									roomsToKeep[roomID] = true
									reason := fmt.Sprintf("timeline_expanded:%d->%d", prevConfig.TimelineLimit, subConfig.TimelineLimit)
									if roomKeepReasons[roomID] != "" {
										roomKeepReasons[roomID] += "," + reason
									} else {
										roomKeepReasons[roomID] = reason
									}
									logrus.WithFields(logrus.Fields{
										"room_id":    roomID,
										"prev_limit": prevConfig.TimelineLimit,
										"new_limit":  subConfig.TimelineLimit,
									}).Info("[V4_SYNC] Timeline limit expanded - resending room data")
								}
							} else {
								// Room was never sent before via subscription - include it
								// (This handles new room subscriptions)
								if !roomsToKeep[roomID] {
									roomsToKeep[roomID] = true
									if roomKeepReasons[roomID] != "" {
										roomKeepReasons[roomID] += ",new_subscription"
									} else {
										roomKeepReasons[roomID] = "new_subscription"
									}
									logrus.WithFields(logrus.Fields{
										"room_id":        roomID,
										"timeline_limit": subConfig.TimelineLimit,
									}).Info("[V4_SYNC] New room subscription - including room data")
								}
							}
						}
					}
				}

				// Log filtering decisions for debugging
				logrus.WithFields(logrus.Fields{
					"total_pdu_changes":    len(roomsWithPDUChanges),
					"total_invite_changes": len(roomsWithInviteChanges),
					"rooms_to_keep":        len(roomsToKeep),
				}).Debug("[V4_STATE_DEBUG] Room filtering results")
				for roomID, reason := range roomKeepReasons {
					logrus.WithFields(logrus.Fields{
						"room_id": roomID,
						"reason":  reason,
					}).Debug("[V4_STATE_DEBUG] Room kept in sync")
				}

				// Filter roomsToPopulate to only rooms we want to keep
				filteredRooms := make(map[string]types.RoomSubscriptionConfig)
				for roomID, config := range roomsToPopulate {
					if roomsToKeep[roomID] {
						filteredRooms[roomID] = config
					}
				}

				logrus.WithFields(logrus.Fields{
					"before_filter": len(roomsToPopulate),
					"after_filter":  len(filteredRooms),
					"filtered_out":  len(roomsToPopulate) - len(filteredRooms),
				}).Debug("[V4_SYNC] Filtered rooms to only changed rooms")

				roomsToPopulate = filteredRooms
			}
		}

		// Phase 3: Populate room data for all rooms
		// Create a single database snapshot for all room queries
		snapshot, err := rp.db.NewDatabaseSnapshot(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return abandonV4Sync(ctx, since, connState)
			}
			return util.JSONResponse{
				Code: http.StatusInternalServerError,
				JSON: spec.InternalServerError{},
			}
		}
		var succeeded bool
		defer func() {
			if succeeded {
				_ = snapshot.Commit() // Best effort
			}
			_ = snapshot.Rollback() // No-op if already committed
		}()

		logrus.WithFields(logrus.Fields{
			"num_rooms_to_populate": len(roomsToPopulate),
			"from_lists":            len(roomsInLists),
			"from_subscriptions":    len(v4Req.RoomSubscriptions),
		}).Debug("[V4_SYNC] Populating room data")

	populate:
		for roomID, config := range roomsToPopulate {
			if ctx.Err() != nil {
				break
			}
			// Phase 10: Determine room state from previous stream states
			// This drives incremental sync behavior (initial vs live vs previously)
			roomState := determineRoomStreamState(ctx, snapshot, connState, roomID, device.UserID)

			// Check for timeline expansion per MSC4186
			// Per MSC4186: "if the timeline_limit has increased (to say N) the server SHOULD
			// ignore this and send down the latest N events, even if some of those events
			// have previously been sent. [...] The server should return rooms that have
			// expanded timelines immediately, rather than waiting for the next update"
			//
			// This check must happen early because we need to know if the timeline is
			// expanding before we decide to skip the room due to "extension only" updates.
			// Two cases to handle:
			// 1. Timeline limit increased from previous value (subscription or list)
			// 2. NEW subscription added for a room that was previously only in lists
			timelineExpanded := false
			if since != nil {
				// Use room configs from the position we loaded (copy-forwarded to new position)
				// This avoids the cascade deletion issue where old configs were lost
				prevConfig := connState.PreviousRoomConfigs[roomID]
				if prevConfig != nil {
					// Room was sent before - check if timeline_limit expanded
					if config.TimelineLimit > prevConfig.TimelineLimit {
						timelineExpanded = true
						logrus.WithFields(logrus.Fields{
							"room_id":    roomID,
							"prev_limit": prevConfig.TimelineLimit,
							"new_limit":  config.TimelineLimit,
						}).Info("[V4_SYNC] Timeline expanded - fetching historical events")
					}
				} else {
					// No previous config found but room might have been sent via lists
					// Check if this is a subscription for a room that was already sent
					if _, isSubscription := v4Req.RoomSubscriptions[roomID]; isSubscription {
						if roomState.Status == types.HaveSentRoomLive || roomState.Status == types.HaveSentRoomPreviously {
							// Room was sent before (tracked in stream state) but no room config stored
							// This can happen for rooms sent via lists before we started tracking configs
							// Treat as expansion to send historical events
							timelineExpanded = true
							logrus.WithFields(logrus.Fields{
								"room_id":     roomID,
								"new_limit":   config.TimelineLimit,
								"room_status": roomState.Status,
								"reason":      "new_subscription_for_previously_sent_room",
							}).Info("[V4_SYNC] New subscription for previously sent room - fetching historical events")
						}
					}
				}
			}

			logrus.WithFields(logrus.Fields{
				"room_id":           roomID,
				"room_status":       roomState.Status,
				"is_initial":        roomState.Status.IsInitial(),
				"timeline_limit":    config.TimelineLimit,
				"timeline_expanded": timelineExpanded,
			}).Debug("[V4_SYNC] Building room data")

			// Pass required_state config (Phase 4)
			var requiredStateConfig *types.RequiredStateConfig
			if len(config.RequiredState.Include) > 0 || len(config.RequiredState.Exclude) > 0 || config.RequiredState.LazyMembers {
				requiredStateConfig = &config.RequiredState
			}

			// Prepare fromToken for num_live calculation and initial flag
			var fromPosPtr *types.StreamingToken
			if since != nil {
				fromPosPtr = &since.StreamToken
			}

			requiredStateExpanded := false
			if previous := connState.PreviousRoomConfigs[roomID]; previous != nil {
				requiredStateExpanded = rp.requiredStateChanged(ctx, config.RequiredState, previous.RequiredStateID)
			}
			roomData, buildErr := rp.BuildRoomData(ctx, snapshot, roomID, device.UserID, config.TimelineLimit, roomState, currentPos, fromPosPtr, requiredStateConfig, timelineExpanded, requiredStateExpanded)
			if buildErr != nil {
				if ctx.Err() != nil {
					break populate
				}
				// Log error but continue with other rooms. This room was not
				// sent, so let copyForwardStreamStates carry its previous
				// state into the new connection position.
				logrus.WithError(buildErr).WithField("room_id", roomID).Error("[V4_SYNC] Failed to build room data")
				delete(roomsToPopulate, roomID)
				continue
			}
			roomData.Avatar = roomData.AvatarURL
			// Always emit lists, including an empty array when the room has
			// left every list window but is still present as a subscription.
			roomData.Lists = []string{}
			for listName, list := range response.Lists {
				if slices.Contains(list.WindowRoomIDs, roomID) && !slices.Contains(roomData.Lists, listName) {
					roomData.Lists = append(roomData.Lists, listName)
				}
			}
			if !roomData.Initial && fromPosPtr != nil {
				changed := map[string]bool{}
				markChanged := func(event synctypes.ClientEvent) {
					switch event.Type {
					case spec.MRoomName:
						changed[spec.MRoomName] = true
					case spec.MRoomAvatar:
						changed[spec.MRoomAvatar] = true
					case spec.MRoomTopic:
						changed[spec.MRoomTopic] = true
					}
				}
				for _, event := range roomData.Timeline {
					markChanged(event)
				}
				for _, event := range roomData.RequiredState {
					markChanged(event)
				}
				if !changed[spec.MRoomName] {
					roomData.Name = ""
				}
				if !changed[spec.MRoomAvatar] {
					roomData.AvatarURL = ""
					roomData.Avatar = ""
				}
				if !changed[spec.MRoomTopic] {
					roomData.Topic = ""
				}
			}

			// A room can have a stored PDU position newer than the connection's
			// position even though the client has no visible room data to apply
			// (for example, events outside its history visibility). Do not send
			// an empty room object for that case. Keep initial rooms, expansions,
			// state deltas, and membership leaves, which all carry an update even
			// when their timeline is empty.
			_, subscribed := v4Req.RoomSubscriptions[roomID]
			if _, allSubscribed := v4Req.RoomSubscriptions["*"]; allSubscribed {
				subscribed = true
			}
			if since != nil && !subscribed && !roomData.Initial && !timelineExpanded && !requiredStateExpanded &&
				len(roomData.Timeline) == 0 && len(roomData.RequiredState) == 0 &&
				!roomsLeftOnConnection[roomID] && roomData.Membership != string(spec.Leave) && roomData.Membership != string(spec.Ban) {
				// The room was not actually sent (it had no visible update).
				// Do not mark it processed, otherwise its stream state is
				// dropped when the previous connection position is deleted and
				// the next sync treats it as never sent.
				delete(roomsToPopulate, roomID)
				continue
			}

			// Set expanded_timeline flag if timeline_limit increased
			// This signals to clients that we're sending historical events due to expansion
			if timelineExpanded {
				roomData.ExpandedTimeline = true
				// Clear receipt delivery state for this room so receipts are re-delivered
				// This is necessary because the client is resetting its view of the room (initial:true)
				// and needs to receive current receipt positions to avoid stuck unread badges
				if deleteErr := rp.db.DeleteConnectionReceiptsForRoom(ctx, connectionKey, roomID); deleteErr != nil {
					logrus.WithError(deleteErr).WithFields(logrus.Fields{
						"room_id":        roomID,
						"connection_key": connectionKey,
					}).Warn("[V4_SYNC] Failed to clear room receipts on timeline expansion")
					// Non-fatal - continue with the sync
				}
			}

			response.Rooms[roomID] = *roomData

			// Phase 10: Track stream state for "events" stream
			// Store current position so we can compute deltas next time
			// Room is sent in this response, so mark as "live" for next sync
			roomStatus := types.HaveSentRoomLive.String()
			lastToken := currentPos.String()
			logrus.WithFields(logrus.Fields{
				"room_id":             roomID,
				"connection_position": connState.ConnectionPosition,
				"room_status":         roomStatus,
				"last_token":          lastToken,
			}).Debug("[V4_STATE_DEBUG] Persisting stream state to database")
			if streamErr := rp.db.UpdateConnectionStream(ctx, connState.ConnectionPosition, roomID, "events", roomStatus, lastToken); streamErr != nil {
				if ctx.Err() != nil {
					break populate
				}
				logrus.WithError(streamErr).WithFields(logrus.Fields{
					"room_id":             roomID,
					"connection_position": connState.ConnectionPosition,
				}).Error("[V4_STATE_DEBUG] Failed to persist stream state")
				// Continue anyway - this is not fatal
			} else {
				logrus.WithFields(logrus.Fields{
					"room_id":             roomID,
					"connection_position": connState.ConnectionPosition,
				}).Debug("[V4_STATE_DEBUG] Successfully persisted stream state")
			}

			// Track room config (timeline_limit) for detecting expanded subscriptions
			// This allows us to detect when a client increases timeline_limit and needs more events
			// We need a valid required_state_id due to foreign key constraint
			var requiredStateID int64
			if requiredStateConfig != nil {
				// Serialize required_state to JSON for deduplication
				requiredStateJSON, marshalErr := json.Marshal(requiredStateConfig)
				if marshalErr != nil {
					logrus.WithError(marshalErr).WithField("room_id", roomID).Error("[V4_STATE_DEBUG] Failed to serialize required_state")
				} else {
					var getErr error
					requiredStateID, getErr = rp.db.GetOrCreateRequiredStateID(ctx, connState.ConnectionKey, string(requiredStateJSON))
					if getErr != nil {
						if ctx.Err() != nil {
							break populate
						}
						logrus.WithError(getErr).WithField("room_id", roomID).Error("[V4_STATE_DEBUG] Failed to get/create required_state_id")
					}
				}
			}
			// If no required_state or error, use a default empty config
			if requiredStateID == 0 {
				emptyJSON := "[]"
				var defaultErr error
				requiredStateID, defaultErr = rp.db.GetOrCreateRequiredStateID(ctx, connState.ConnectionKey, emptyJSON)
				if defaultErr != nil {
					if ctx.Err() != nil {
						break populate
					}
					logrus.WithError(defaultErr).WithField("room_id", roomID).Error("[V4_STATE_DEBUG] Failed to get/create default required_state_id")
					// Continue without storing room config - this will cause timeline expansion to repeat
					// but at least it won't cause the sync to fail
				}
			}
			if requiredStateID != 0 {
				if configErr := rp.db.UpdateRoomConfig(ctx, connState.ConnectionPosition, roomID, config.TimelineLimit, requiredStateID); configErr != nil {
					if ctx.Err() != nil {
						break populate
					}
					logrus.WithError(configErr).WithFields(logrus.Fields{
						"room_id":           roomID,
						"timeline_limit":    config.TimelineLimit,
						"required_state_id": requiredStateID,
					}).Error("[V4_STATE_DEBUG] Failed to persist room config")
					// Continue anyway - this is not fatal
				} else {
					logrus.WithFields(logrus.Fields{
						"room_id":           roomID,
						"timeline_limit":    config.TimelineLimit,
						"required_state_id": requiredStateID,
					}).Debug("[V4_STATE_DEBUG] Persisted room config for timeline expansion tracking")
				}
			}
		}

		// A pass the client gave up on is never acknowledged, so stop here
		// rather than writing, and logging the failure of, every remaining row.
		if ctx.Err() != nil {
			return abandonV4Sync(ctx, since, connState)
		}

		// CRITICAL FIX: Copy forward stream states for rooms that were previously sent
		// but not processed in this response. Without this, when we delete old positions
		// (via cascade delete), we lose the stream state for rooms that had no changes.
		// This causes those rooms to incorrectly appear as "never sent" on the next request,
		// even though they were sent before.
		if since != nil && connState.PreviousStreamStates != nil {
			if _, copyErr := rp.copyForwardStreamStates(ctx, connState, roomsToPopulate); copyErr != nil {
				return abandonV4Sync(ctx, since, connState)
			}
		}

		// CRITICAL FIX: Copy forward room configs for rooms that were previously sent
		// but not processed in this response. Without this, when we delete old positions
		// (via cascade delete), we lose the room config for rooms that had no changes.
		// This causes those rooms to incorrectly trigger timeline expansion on the next request.
		if since != nil && connState.PreviousRoomConfigs != nil {
			if copyErr := rp.copyForwardRoomConfigs(ctx, connState, roomsToPopulate); copyErr != nil {
				return abandonV4Sync(ctx, since, connState)
			}
		}

		// Phase 9: Process extensions
		var fromPosPtr *types.StreamingToken
		if since != nil {
			fromPosPtr = &since.StreamToken
		}
		// Build room subscriptions map from request
		roomSubscriptions := make(map[string]bool, len(v4Req.RoomSubscriptions))
		for roomID := range v4Req.RoomSubscriptions {
			roomSubscriptions[roomID] = true
		}
		extensionResp, updatedPos, deliveredReceipts, err := rp.ProcessExtensions(ctx, snapshot, connState, &v4Req, device.UserID, device.ID, connectionKey, fromPosPtr, currentPos, response.Lists, roomSubscriptions, response.Rooms)
		if ctx.Err() != nil {
			return abandonV4Sync(ctx, since, connState)
		}
		// Runs whether or not the account_data extension was processed: rooms it
		// did not write still need their stored position carried onto this
		// connection position before the old one is cascade-deleted.
		if copyErr := rp.copyForwardAccountDataState(ctx, connState); copyErr != nil {
			return abandonV4Sync(ctx, since, connState)
		}
		succeeded = true
		if err != nil {
			logrus.WithError(err).Error("Failed to process extensions")
			// Continue anyway - extensions are optional, return empty extension response
			response.Extensions = &types.ExtensionResponse{}
		} else {
			response.Extensions = extensionResp
			// Use the updated position from extensions (fixes receipt position tracking)
			oldPos := currentPos
			currentPos = updatedPos
			// CRITICAL: Update response.Pos with the new position that includes updated extension positions
			// This fixes the receipt repetition bug where receipt position wasn't advancing
			newToken = types.NewSlidingSyncStreamToken(connState.ConnectionPosition, currentPos)
			response.Pos = newToken.String()

			// DEBUG: Log position update if it changed
			if oldPos.ReceiptPosition != currentPos.ReceiptPosition {
				logrus.WithFields(logrus.Fields{
					"old_receipt_pos": oldPos.ReceiptPosition,
					"new_receipt_pos": currentPos.ReceiptPosition,
					"updated_token":   response.Pos,
				}).Info("[V4_SYNC] Receipt position advanced in token")
			}

			// Update connection state for delivered receipts in a write transaction
			// IMPORTANT: This must be done in a separate write transaction, NOT in the read-only snapshot
			if !rp.recordDeliveredReceipts(ctx, connectionKey, deliveredReceipts) {
				return abandonV4Sync(ctx, since, connState)
			}
		}

		// DEBUG: Log final response summary
		logrus.WithFields(logrus.Fields{
			"new_pos":   response.Pos,
			"num_lists": len(response.Lists),
			"num_rooms": len(response.Rooms),
			"has_ext":   response.Extensions != nil,
		}).Info("[V4_SYNC] Returning response")

		// Log room IDs in response
		if len(response.Rooms) > 0 {
			roomIDs := make([]string, 0, len(response.Rooms))
			for roomID := range response.Rooms {
				roomIDs = append(roomIDs, roomID)
			}
			logrus.WithField("room_ids", roomIDs).Debug("[V4_SYNC] Response rooms")
		}

		// DEBUG: Log response structure to verify JSON format
		listOpsCount := make(map[string]int)
		for listName, list := range response.Lists {
			listOpsCount[listName] = len(list.Ops)
		}
		logrus.WithFields(logrus.Fields{
			"pos":            response.Pos,
			"list_ops":       listOpsCount,
			"has_extensions": response.Extensions != nil,
		}).Debug("[V4_SYNC] Response structure")

		// Check if response has meaningful updates
		// Similar to v3 sync's HasUpdates() check
		hasUpdates := v4ResponseHasUpdates(response)
		logrus.WithField("has_updates", hasUpdates).Info("[V4_SYNC] Checked for updates")

		// If no updates and timeout remaining, loop again with bumped position
		// This handles the case where global position advanced but there are no user-specific changes
		if !hasUpdates && v4Req.Timeout > 0 && since != nil {
			// Bump since to current position
			since = types.NewSlidingSyncStreamToken(connState.ConnectionPosition, currentPos)
			// Reduce timeout by elapsed time
			elapsed := time.Since(startTime)
			v4Req.Timeout = int(time.Duration(v4Req.Timeout)*time.Millisecond-elapsed) / int(time.Millisecond)
			if v4Req.Timeout < 0 {
				v4Req.Timeout = 0
			}
			logrus.WithFields(logrus.Fields{
				"elapsed_ms":  elapsed.Milliseconds(),
				"new_timeout": v4Req.Timeout,
			}).Info("[V4_SYNC] No updates, looping again with reduced timeout")
			continue
		}

		// Return response (either has updates or timeout expired or first sync)
		logV4Response(response, device.UserID, device.ID, http.StatusOK)
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: response,
		}
	}
}

// v4ResponseHasUpdates checks if a sliding sync response has meaningful updates
// Similar to v3 sync's HasUpdates() method.
func v4ResponseHasUpdates(response types.SlidingSyncResponse) bool {
	// Check if any list has operations
	for _, list := range response.Lists {
		if len(list.Ops) > 0 {
			return true
		}
	}

	// Check if there are room updates
	if len(response.Rooms) > 0 {
		return true
	}

	// Check if extensions have meaningful data
	if response.Extensions != nil {
		// to_device events
		if response.Extensions.ToDevice != nil && len(response.Extensions.ToDevice.Events) > 0 {
			return true
		}

		// E2EE updates (device lists changed)
		if response.Extensions.E2EE != nil && response.Extensions.E2EE.DeviceLists != nil {
			if len(response.Extensions.E2EE.DeviceLists.Changed) > 0 || len(response.Extensions.E2EE.DeviceLists.Left) > 0 {
				return true
			}
		}

		// Account data updates
		if response.Extensions.AccountData != nil {
			if len(response.Extensions.AccountData.Global) > 0 || len(response.Extensions.AccountData.Rooms) > 0 {
				return true
			}
		}

		// Receipt updates
		if response.Extensions.Receipts != nil && len(response.Extensions.Receipts.Rooms) > 0 {
			return true
		}

		// Typing updates
		if response.Extensions.Typing != nil && len(response.Extensions.Typing.Rooms) > 0 {
			return true
		}

		// Sticky events (MSC4354)
		if response.Extensions.StickyEvents != nil && len(response.Extensions.StickyEvents.Rooms) > 0 {
			return true
		}
	}

	return false
}

// getRoomsForList fetches rooms based on invite filter configuration.
func (rp *RequestPool) getRoomsForList(
	ctx context.Context,
	userID string,
	filters *types.SlidingRoomFilter,
) ([]RoomWithBumpStamp, error) {
	// Check if filter explicitly requests only invites
	isInvite := func() *bool {
		if filters == nil {
			return nil
		}
		if filters.IsInvited != nil {
			return filters.IsInvited
		}
		return filters.IsInvite
	}()
	if isInvite != nil && *isInvite {
		return rp.GetRoomsForUser(ctx, userID, spec.Invite)
	}
	if isInvite != nil && !*isInvite {
		return rp.GetRoomsForUser(ctx, userID, "join")
	}

	// No invite filter - get rooms with active memberships (joined + invited + banned + kicked)
	return rp.getAllActiveMembershipRooms(ctx, userID)
}

// getAllActiveMembershipRooms fetches all rooms where user has active membership
// Per MSC4186 and Synapse behavior: joined, invited, knocked, banned, kicked rooms are included
// Self-left rooms are excluded from default lists.
func (rp *RequestPool) getAllActiveMembershipRooms(ctx context.Context, userID string) ([]RoomWithBumpStamp, error) {
	joinedRooms, err := rp.GetRoomsForUser(ctx, userID, "join")
	if err != nil {
		return nil, err
	}
	invitedRooms, err := rp.GetRoomsForUser(ctx, userID, spec.Invite)
	if err != nil {
		return nil, err
	}
	knockedRooms, err := rp.GetRoomsForUser(ctx, userID, spec.Knock)
	if err != nil {
		return nil, err
	}
	bannedRooms, err := rp.GetRoomsForUser(ctx, userID, spec.Ban)
	if err != nil {
		return nil, err
	}
	kickedRooms, err := rp.GetKickedRooms(ctx, userID)
	if err != nil {
		return nil, err
	}

	// Combine all membership types
	rooms := make([]RoomWithBumpStamp, 0, len(joinedRooms)+len(invitedRooms)+len(knockedRooms)+len(bannedRooms)+len(kickedRooms))
	rooms = append(rooms, joinedRooms...)
	rooms = append(rooms, invitedRooms...)
	rooms = append(rooms, knockedRooms...)
	rooms = append(rooms, bannedRooms...)
	rooms = append(rooms, kickedRooms...)

	// Deduplicate rooms - keep first occurrence (highest priority: join > invite > knock > ban > kicked)
	return deduplicateRooms(rooms), nil
}

// deduplicateRooms removes duplicate rooms, keeping the first occurrence.
func deduplicateRooms(rooms []RoomWithBumpStamp) []RoomWithBumpStamp {
	seen := make(map[string]bool, len(rooms))
	deduped := make([]RoomWithBumpStamp, 0, len(rooms))
	for _, room := range rooms {
		if !seen[room.RoomID] {
			seen[room.RoomID] = true
			deduped = append(deduped, room)
		}
	}
	return deduped
}

// mergeRoomSubscriptionConfig combines the room configuration from multiple
// lists. The timeline limit is the maximum requested, while required state is
// the union so a room present in a narrow list and a wide list receives both
// sets of state events.
func mergeRoomSubscriptionConfig(existing, incoming types.RoomSubscriptionConfig) types.RoomSubscriptionConfig {
	if incoming.TimelineLimit > existing.TimelineLimit {
		existing.TimelineLimit = incoming.TimelineLimit
	}
	existing.RequiredState.Include = appendUniqueStatePatterns(existing.RequiredState.Include, incoming.RequiredState.Include)
	// Exclude only what every merged list excludes: one list's exclusion must
	// not remove state another list asked for.
	existing.RequiredState.Exclude = intersectStatePatterns(existing.RequiredState.Exclude, incoming.RequiredState.Exclude)
	existing.RequiredState.LazyMembers = existing.RequiredState.LazyMembers || incoming.RequiredState.LazyMembers
	return existing
}

func intersectStatePatterns(a, b [][]string) [][]string {
	var out [][]string
	for _, pattern := range a {
		if slices.ContainsFunc(b, func(p []string) bool { return slices.Equal(p, pattern) }) {
			out = append(out, pattern)
		}
	}
	return out
}

func appendUniqueStatePatterns(dst, src [][]string) [][]string {
	for _, pattern := range src {
		if !slices.ContainsFunc(dst, func(existing []string) bool {
			return slices.Equal(existing, pattern)
		}) {
			dst = append(dst, pattern)
		}
	}
	return dst
}

// requiredStateChanged reports whether a subscription asks for state that was
// not requested the last time this connection received the room.
func (rp *RequestPool) requiredStateChanged(
	ctx context.Context, current types.RequiredStateConfig, previousID int64,
) bool {
	if len(current.Include) == 0 && len(current.Exclude) == 0 && !current.LazyMembers {
		return false
	}
	currentJSON, err := json.Marshal(current)
	if err != nil {
		return true
	}
	previousJSON, err := rp.db.GetRequiredState(ctx, previousID)
	if err != nil {
		logrus.WithError(err).Warn("Failed to load previous required_state config")
		return true
	}
	return string(currentJSON) != previousJSON
}

// processRoomList handles a single room list configuration.
func (rp *RequestPool) processRoomList(
	ctx context.Context,
	userID string,
	listName string,
	config types.SlidingListConfig,
	connState *V4ConnectionState,
	forceInitialSync bool,
) (types.SlidingList, error) {
	// Get all rooms for the user based on filter configuration
	rooms, err := rp.getRoomsForList(ctx, userID, config.Filters)
	if err != nil {
		return types.SlidingList{}, err
	}

	// Apply filters if specified
	if config.Filters != nil {
		rooms, err = rp.ApplyRoomFilters(ctx, rooms, config.Filters, userID)
		if err != nil {
			return types.SlidingList{}, err
		}
	}

	// Sort by activity (most recent first)
	SortRoomsByActivity(rooms)

	// Total count before windowing
	totalCount := len(rooms)

	// Apply sliding window if range is specified
	var windowedRooms []RoomWithBumpStamp
	var rangeSpec []int
	if len(config.Range) == 2 { //nolint:mnd
		rangeSpec = config.Range
		windowedRooms = ApplySlidingWindow(rooms, rangeSpec)
	} else {
		// No range specified, return all rooms
		windowedRooms = rooms
		rangeSpec = []int{0, len(rooms) - 1}
	}

	// Extract the room IDs in the current window. This is needed even when the
	// list has no operations: clients still need room data for unchanged
	// windows when a room has new events or an expanded timeline.
	roomIDs := make([]string, len(windowedRooms))
	for i, room := range windowedRooms {
		roomIDs[i] = room.RoomID
	}

	// Load the previously sent window. On an initial sync the stored list is
	// deliberately ignored so that the response is a SYNC, not a delta against
	// a window the client never received.
	var previousRoomIDs []string
	if !forceInitialSync {
		previousRoomIDsJSON, exists, err := rp.db.GetConnectionList(ctx, connState.ConnectionKey, listName)
		if err != nil {
			logrus.WithError(err).WithField("list", listName).Error("Failed to load connection list")
		} else if exists {
			if err := json.Unmarshal([]byte(previousRoomIDsJSON), &previousRoomIDs); err != nil {
				logrus.WithError(err).WithField("list", listName).Error("Failed to decode connection list JSON")
			}
		}
	}

	listChanged := forceInitialSync || !equalStringSlices(previousRoomIDs, roomIDs)
	var removedRoomIDs []string
	if !forceInitialSync {
		for _, previousRoomID := range previousRoomIDs {
			if !slices.Contains(roomIDs, previousRoomID) {
				removedRoomIDs = append(removedRoomIDs, previousRoomID)
			}
		}
	}
	ops := []types.SlidingOperation{}

	logrus.WithFields(logrus.Fields{
		"list_name":       listName,
		"list_changed":    listChanged,
		"prev_room_count": len(previousRoomIDs),
		"curr_room_count": len(roomIDs),
		"force_initial":   forceInitialSync,
	}).Debug("[V4_SYNC] List change detection")

	if listChanged {
		// maxOps=5 means we fall back to SYNC if more than 5 INSERT/DELETE
		// operations would be needed to describe the change.
		const maxOps = 5
		opsForList := GenerateListOperations(previousRoomIDs, roomIDs, rangeSpec, maxOps)
		ops = append(ops, opsForList...)

		for _, listOp := range opsForList {
			logrus.WithFields(logrus.Fields{
				"list_name":    listName,
				"op_type":      listOp.Op,
				"num_room_ids": len(listOp.RoomIDs),
				"index":        listOp.Index,
			}).Debug("[V4_SYNC] Generated list operation")
		}

		// Store the current window even when it is empty, so a later sync can
		// emit the DELETEs for rooms that have just left the list.
		roomIDsJSON, err := json.Marshal(roomIDs)
		if err != nil {
			logrus.WithError(err).WithField("list", listName).Error("Failed to encode room IDs to JSON")
		} else if err := rp.db.UpdateConnectionList(ctx, connState.ConnectionKey, listName, string(roomIDsJSON)); err != nil {
			logrus.WithError(err).WithField("list", listName).Error("Failed to persist connection list")
		}
	} else {
		logrus.WithField("list_name", listName).Debug("[V4_SYNC] List unchanged, no operations needed")
	}

	matchedRoomIDs := make([]string, len(rooms))
	for i, room := range rooms {
		matchedRoomIDs[i] = room.RoomID
	}
	return types.SlidingList{
		Count:          totalCount,
		Ops:            ops,
		MatchedRoomIDs: matchedRoomIDs,
		WindowRoomIDs:  roomIDs,
		RemovedRoomIDs: removedRoomIDs,
	}, nil
}

// equalStringSlices checks if two string slices have the same elements in order.
func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// buildInterruptResponse builds a sync response for timeout/disconnect cases.
// It processes requested lists, tracks rooms from operations, and populates room data.
func (rp *RequestPool) buildInterruptResponse(
	ctx context.Context,
	v4Req *types.SlidingSyncRequest,
	device *userapi.Device,
	connState *V4ConnectionState,
	since *types.SlidingSyncStreamToken,
	currentPos types.StreamingToken,
	connectionKey int64,
	responseType string, // "timeout" or "disconnect"
) *types.SlidingSyncResponse {
	resp := &types.SlidingSyncResponse{
		Pos:        since.String(),
		Lists:      make(map[string]types.SlidingList),
		Rooms:      make(map[string]types.SlidingRoomData),
		Extensions: &types.ExtensionResponse{},
	}

	// A disconnected client cannot receive this response. The timeout and
	// disconnect cases can also become ready together, so check before any I/O.
	if ctx.Err() != nil {
		return resp
	}

	// Process requested lists to include their current state
	roomsInLists := make(map[string]types.RoomSubscriptionConfig)
	for listName, listConfig := range v4Req.Lists {
		if ctx.Err() != nil {
			return resp
		}
		list, listErr := rp.processRoomList(ctx, device.UserID, listName, listConfig, connState, false)
		if listErr != nil {
			if errors.Is(listErr, context.Canceled) || errors.Is(listErr, context.DeadlineExceeded) {
				return resp
			}
			logrus.WithError(listErr).WithFields(logrus.Fields{
				"list_name":     listName,
				"response_type": responseType,
			}).Error("[V4_SYNC] Failed to process list")
			continue
		}
		resp.Lists[listName] = list

		// Track every room in the current window, not only rooms in list
		// operations, so unchanged windows still get room data.
		for _, roomID := range list.WindowRoomIDs {
			config := types.RoomSubscriptionConfig{
				TimelineLimit: listConfig.TimelineLimit,
				RequiredState: listConfig.RequiredState,
			}
			if existing, exists := roomsInLists[roomID]; exists {
				roomsInLists[roomID] = mergeRoomSubscriptionConfig(existing, config)
			} else {
				roomsInLists[roomID] = config
			}
		}
		for _, roomID := range list.RemovedRoomIDs {
			config := types.RoomSubscriptionConfig{
				TimelineLimit: listConfig.TimelineLimit,
				RequiredState: listConfig.RequiredState,
			}
			if existing, exists := roomsInLists[roomID]; exists {
				roomsInLists[roomID] = mergeRoomSubscriptionConfig(existing, config)
			} else {
				roomsInLists[roomID] = config
			}
		}
	}

	// Populate room data for rooms that appear in list operations
	if ctx.Err() != nil {
		return resp
	}
	if len(roomsInLists) > 0 {
		snapshot, snapshotErr := rp.db.NewDatabaseSnapshot(ctx)
		if snapshotErr != nil {
			if errors.Is(snapshotErr, context.Canceled) || errors.Is(snapshotErr, context.DeadlineExceeded) {
				return resp
			}
			logrus.WithError(snapshotErr).WithField("response_type", responseType).Error("[V4_SYNC] Failed to create snapshot for room data")
		} else {
			var succeeded bool
			defer func() {
				if succeeded {
					_ = snapshot.Commit()
				}
				_ = snapshot.Rollback()
			}()

			logrus.WithFields(logrus.Fields{
				"num_rooms":     len(roomsInLists),
				"response_type": responseType,
			}).Debug("[V4_SYNC] Populating room data for interrupt response")

			for roomID, config := range roomsInLists {
				if ctx.Err() != nil {
					return resp
				}
				var requiredStateConfig *types.RequiredStateConfig
				if len(config.RequiredState.Include) > 0 || len(config.RequiredState.Exclude) > 0 || config.RequiredState.LazyMembers {
					requiredStateConfig = &config.RequiredState
				}

				roomState := determineRoomStreamState(ctx, snapshot, connState, roomID, device.UserID)

				// An interrupt response persists no stream state and returns
				// pos unchanged, so anything it emits is emitted again on the
				// next timeout and in the following real response. Pin
				// LastToken to since so getTimelineEvents does not widen the
				// range down to a lagging room token here: the gap is caught
				// up by the next real response, which does persist state.
				if roomState.LastToken != nil && since != nil {
					pinned := since.StreamToken
					roomState.LastToken = &pinned
				}

				var fromPosPtr *types.StreamingToken
				if since != nil {
					fromPosPtr = &since.StreamToken
				}

				roomData, buildErr := rp.BuildRoomData(ctx, snapshot, roomID, device.UserID, config.TimelineLimit, roomState, since.StreamToken, fromPosPtr, requiredStateConfig, false, false)
				if buildErr != nil {
					if errors.Is(buildErr, context.Canceled) || errors.Is(buildErr, context.DeadlineExceeded) {
						return resp
					}
					logrus.WithError(buildErr).WithFields(logrus.Fields{
						"room_id":       roomID,
						"response_type": responseType,
					}).Error("[V4_SYNC] Failed to build room data")
					continue
				}

				roomData.Avatar = roomData.AvatarURL
				roomData.Lists = []string{}
				for listName, list := range resp.Lists {
					if slices.Contains(list.WindowRoomIDs, roomID) {
						roomData.Lists = append(roomData.Lists, listName)
					}
				}
				resp.Rooms[roomID] = *roomData
			}
			succeeded = true
		}
	}

	if ctx.Err() != nil {
		return resp
	}

	// Process extensions (provides e2ee data like OTK counts)
	extSnapshot, extSnapshotErr := rp.db.NewDatabaseSnapshot(ctx)
	if extSnapshotErr != nil {
		if errors.Is(extSnapshotErr, context.Canceled) || errors.Is(extSnapshotErr, context.DeadlineExceeded) {
			return resp
		}
		logrus.WithError(extSnapshotErr).WithField("response_type", responseType).Error("[V4_SYNC] Failed to create snapshot for extensions")
	} else {
		var succeeded bool
		defer func() {
			if succeeded {
				_ = extSnapshot.Commit()
			}
			_ = extSnapshot.Rollback()
		}()

		var fromPosPtr *types.StreamingToken
		if since != nil {
			fromPosPtr = &since.StreamToken
		}
		roomSubscriptions := make(map[string]bool, len(v4Req.RoomSubscriptions))
		for roomID := range v4Req.RoomSubscriptions {
			roomSubscriptions[roomID] = true
		}
		extensionResp, _, _, extErr := rp.ProcessExtensions(ctx, extSnapshot, connState, v4Req, device.UserID, device.ID, connectionKey, fromPosPtr, currentPos, resp.Lists, roomSubscriptions, resp.Rooms)
		if extErr != nil {
			if errors.Is(extErr, context.Canceled) || errors.Is(extErr, context.DeadlineExceeded) {
				return resp
			}
			logrus.WithError(extErr).WithField("response_type", responseType).Error("[V4_SYNC] Failed to process extensions")
		} else {
			resp.Extensions = extensionResp
		}
		succeeded = true
	}

	return resp
}

// committedPDUPosition returns the later of pos and the highest PDU stream
// position committed to the database.
func (rp *RequestPool) committedPDUPosition(ctx context.Context, pos types.StreamPosition) types.StreamPosition {
	snapshot, err := rp.db.NewDatabaseSnapshot(ctx)
	if err != nil {
		return pos
	}
	defer snapshot.Rollback() //nolint:errcheck
	committed, err := snapshot.MaxStreamPositionForPDUs(ctx)
	if err != nil || committed <= pos {
		return pos
	}
	return committed
}

// waitForMembershipChanges waits briefly for the user's membership changes that
// the roomserver has already accepted, such as a leave or a kick that a client
// request has just completed, to reach the sync API. The roomserver and the sync
// API disagree on whether the user is joined to those rooms until then.
func (rp *RequestPool) waitForMembershipChanges(ctx context.Context, userID string) {
	if rp.rsAPI == nil {
		return
	}
	user, err := spec.NewUserID(userID, true)
	if err != nil {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()

	rsRooms, err := rp.rsAPI.QueryRoomsForUser(ctx, *user, spec.Join)
	if err != nil {
		return
	}
	snapshot, err := rp.db.NewDatabaseSnapshot(ctx)
	if err != nil {
		return
	}
	syncRooms, err := snapshot.RoomIDsWithMembership(ctx, userID, spec.Join)
	snapshot.Rollback() //nolint:errcheck
	if err != nil {
		return
	}

	joined := make(map[string]bool, len(rsRooms))
	for _, roomID := range rsRooms {
		joined[roomID.String()] = true
	}
	var differing []string
	for _, roomID := range syncRooms {
		if !joined[roomID] {
			differing = append(differing, roomID)
		}
		delete(joined, roomID)
	}
	for roomID := range joined {
		differing = append(differing, roomID)
	}

	for _, roomID := range differing {
		if ctx.Err() != nil {
			return
		}
		var res roomserverAPI.QueryMembershipForUserResponse
		if err := rp.rsAPI.QueryMembershipForUser(ctx, &roomserverAPI.QueryMembershipForUserRequest{
			RoomID: roomID,
			UserID: *user,
		}, &res); err != nil || res.EventID == "" {
			continue
		}
		syncinternal.WaitForMembershipInTopology(ctx, rp.db, res.EventID)
	}
}
