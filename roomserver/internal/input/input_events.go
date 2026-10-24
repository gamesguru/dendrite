// Copyright 2024 New Vector Ltd.
// Copyright 2019, 2020 The Matrix.org Foundation C.I.C.
// Copyright 2018 New Vector Ltd
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package input

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	fedapi "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/acls"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/internal/helpers"
	"codefloe.com/pat-s/zendrite/roomserver/state"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	userAPI "codefloe.com/pat-s/zendrite/userapi/api"
)

// MaximumMissingProcessingTime is the maximum time we allow "processRoomEvent" to fetch
// e.g. missing auth/prev events. This duration is used for AckWait, and if it is exceeded
// NATS queues the event for redelivery.
const MaximumMissingProcessingTime = time.Minute * 5

var processRoomEventDuration = prometheus.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "zendrite",
		Subsystem: "roomserver",
		Name:      "processroomevent_duration_millis",
		Help:      "How long it takes the roomserver to process an event",
		Buckets: []float64{ // milliseconds
			5, 10, 25, 50, 75, 100, 250, 500,
			1000, 2000, 3000, 4000, 5000, 6000,
			7000, 8000, 9000, 10000, 15000, 20000,
		},
	},
	[]string{"room_id"},
)

// retryClientTransaction answers a retried client transaction whose first
// attempt produced the event originalID. It reports whether it handled the
// retry; if not, the retry is processed as a new event.
//
// The transaction is recorded just before the first attempt publishes its
// output events, and the event is marked as sent in the same database
// transaction that updates the room, which commits after publishing. An event
// that is not marked as sent therefore failed on one side of publishing or the
// other: its output was never published (the broker failed), or it was
// published and the room update then failed to commit (the database failed).
// The two cannot be told apart, and answering the retry with the event would
// lose it in the first case, while storing a new event would duplicate it in
// the second. So the event is processed again instead: its output events are
// published, possibly a second time, and the room is updated with it. The
// consumers of the output stream store events by event ID, so clients and
// servers see the event once either way, and the retry is answered with it.
func (r *Inputer) retryClientTransaction(
	ctx context.Context,
	virtualHost spec.ServerName,
	input *api.InputRoomEvent,
	roomInfo *types.RoomInfo,
	originalID string,
	logger *logrus.Entry,
) (bool, error) {
	logger = logger.WithField("original_event_id", originalID)
	events, err := r.DB.EventsFromIDs(ctx, roomInfo, []string{originalID})
	if err != nil {
		return true, fmt.Errorf("r.DB.EventsFromIDs: %w", err)
	}
	if len(events) == 0 || events[0].PDU == nil {
		// The transaction is recorded only after its event is stored, and
		// both are purged with the room, so this is not expected.
		logger.Warn("The event of a retried client transaction is not stored, storing the retry instead")
		return false, nil
	}
	original := events[0]
	sent, err := r.DB.EventSentToOutput(ctx, original.EventNID)
	if err != nil {
		return true, fmt.Errorf("r.DB.EventSentToOutput: %w", err)
	}
	if sent {
		logger.Info("Skipping a retried client transaction")
		return true, types.DuplicateTransactionError{EventID: originalID}
	}
	logger.Info("Processing the unsent event of a retried client transaction again")
	again := *input
	again.Event = &types.HeaderedEvent{PDU: original.PDU}
	if err = r.processRoomEvent(ctx, virtualHost, &again); err != nil {
		return true, fmt.Errorf("processing event %s of a retried client transaction: %w", originalID, err)
	}
	return true, types.DuplicateTransactionError{EventID: originalID}
}

// processRoomEvent can only be called once at a time
//
// TODO(#375): This should be rewritten to allow concurrent calls. The
// difficulty is in ensuring that we correctly annotate events with the correct
// state deltas when sending to JetStream streams
// TODO: Break up function - we should probably do transaction ID checks before calling this.
//
//nolint:gocyclo
func (r *Inputer) processRoomEvent(
	ctx context.Context,
	virtualHost spec.ServerName,
	input *api.InputRoomEvent,
) error {
	select {
	case <-ctx.Done():
		// Before we do anything, make sure the context hasn't expired for this pending task.
		// If it has then we'll give up straight away — it's probably a synchronous input
		// request and the caller has already given up, but the inbox task was still queued.
		return context.DeadlineExceeded
	default:
	}

	trace, ctx := internal.StartRegion(ctx, "processRoomEvent")
	trace.SetTag("room_id", input.Event.RoomID().String())
	trace.SetTag("event_id", input.Event.EventID())
	defer trace.EndRegion()

	// Measure how long it takes to process this event.
	started := time.Now()
	defer func() {
		timetaken := time.Since(started)
		processRoomEventDuration.With(prometheus.Labels{
			"room_id": input.Event.RoomID().String(),
		}).Observe(float64(timetaken.Milliseconds()))
	}()

	// SkipEventAuth is only safe for events that won't be federated, to avoid
	// causing state divergence or breaking federation in a room.
	if input.SkipEventAuth && input.SendAsServer != api.DoNotSendToOtherServers {
		return fmt.Errorf("SkipEventAuth is only allowed for events that are not sent to other servers")
	}

	// Parse and validate the event JSON
	headered := input.Event
	event := headered.PDU
	logger := util.GetLogger(ctx).WithFields(logrus.Fields{
		"event_id": event.EventID(),
		"room_id":  event.RoomID().String(),
		"kind":     input.Kind,
		"origin":   input.Origin,
		"type":     event.Type(),
	})
	if input.HasState {
		logger = logger.WithFields(logrus.Fields{
			"has_state": input.HasState,
			"state_ids": len(input.StateEventIDs),
		})
	}

	// Don't waste time processing the event if the room doesn't exist.
	// A room entry locally will only be created in response to a create
	// event.
	roomInfo, rerr := r.DB.RoomInfo(ctx, event.RoomID().String())
	if rerr != nil {
		return fmt.Errorf("r.DB.RoomInfo: %w", rerr)
	}
	isCreateEvent := event.Type() == spec.MRoomCreate && event.StateKeyEquals("")
	if roomInfo == nil && !isCreateEvent {
		return fmt.Errorf("room %s does not exist for event %s", event.RoomID().String(), event.EventID())
	}
	sender, err := r.Queryer.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
	if err != nil {
		return fmt.Errorf("failed getting userID for sender %q. %w", event.SenderID(), err)
	}
	senderDomain := spec.ServerName("")
	if sender != nil {
		senderDomain = sender.Domain()
	}

	// A retried client transaction must not store a second event. The first
	// attempt may have been stored by a previous owner after an HA takeover,
	// or after its request failed. It is always ahead of the retry on this
	// room's input subject, so its mapping is known by now.
	if input.Kind == api.KindNew && input.TransactionID != nil && roomInfo != nil {
		originalID, terr := r.DB.ClientTransactionEventID(
			ctx, event.RoomID().String(), string(event.SenderID()),
			input.TransactionID.SessionID, input.TransactionID.TransactionID,
		)
		if terr != nil {
			return fmt.Errorf("r.DB.ClientTransactionEventID: %w", terr)
		}
		if originalID != "" && originalID != event.EventID() {
			if handled, rerr := r.retryClientTransaction(ctx, virtualHost, input, roomInfo, originalID, logger); handled {
				return rerr
			}
		}
	}

	// Check if the room has partial state (MSC3706 faster joins)
	// This affects how we handle missing auth events and authorization failures
	var hasPartialState bool
	if roomInfo != nil {
		hasPartialState, _ = r.DB.IsRoomPartialState(ctx, roomInfo.RoomNID)
		if hasPartialState {
			logger = logger.WithField("partial_state", true)
		}
	}

	// If we already know about this outlier and it hasn't been rejected
	// then we won't attempt to reprocess it. If it was rejected or has now
	// arrived as a different kind of event, then we can attempt to reprocess,
	// in case we have learned something new or need to weave the event into
	// the DAG now.
	if input.Kind == api.KindOutlier && roomInfo != nil {
		wasRejected, werr := r.DB.IsEventRejected(ctx, roomInfo.RoomNID, event.EventID())
		switch {
		case errors.Is(werr, sql.ErrNoRows):
			// We haven't seen this event before so continue.
		case werr != nil:
			// Something has gone wrong trying to find out if we rejected
			// this event already.
			logger.WithError(werr).Errorf("Failed to check if event %q is already seen", event.EventID())
			return werr
		case !wasRejected:
			// We've seen this event before and it wasn't rejected so we
			// should ignore it.
			logger.Debugf("Already processed event %q, ignoring", event.EventID())
			return nil
		}
	}

	var missingAuth, missingPrev bool
	serverRes := &fedapi.QueryJoinedHostServerNamesInRoomResponse{}
	if !isCreateEvent {
		var missingAuthIDs, missingPrevIDs []string
		missingAuthIDs, missingPrevIDs, err = r.DB.MissingAuthPrevEvents(ctx, event)
		if err != nil {
			return fmt.Errorf("updater.MissingAuthPrevEvents: %w", err)
		}
		missingAuth = len(missingAuthIDs) > 0
		missingPrev = !input.HasState && len(missingPrevIDs) > 0
	}

	// If we have missing events (auth or prev), we build a list of servers to ask
	if missingAuth || missingPrev {
		serverReq := &fedapi.QueryJoinedHostServerNamesInRoomRequest{
			RoomID:             event.RoomID().String(),
			ExcludeSelf:        true,
			ExcludeBlacklisted: true,
		}
		if err = r.FSAPI.QueryJoinedHostServerNamesInRoom(ctx, serverReq, serverRes); err != nil {
			return fmt.Errorf("r.FSAPI.QueryJoinedHostServerNamesInRoom: %w", err)
		}
		// Sort all of the servers into a map so that we can randomize
		// their order. Then make sure that the input origin and the
		// event origin are first on the list.
		servers := map[spec.ServerName]struct{}{}
		for _, server := range serverRes.ServerNames {
			servers[server] = struct{}{}
		}
		// Don't try to talk to ourselves.
		delete(servers, r.Cfg.Matrix.ServerName)
		// Now build up the list of servers.
		serverRes.ServerNames = serverRes.ServerNames[:0]
		if input.Origin != "" && input.Origin != r.Cfg.Matrix.ServerName {
			serverRes.ServerNames = append(serverRes.ServerNames, input.Origin)
			delete(servers, input.Origin)
		}
		// Only perform this check if the sender mxid_mapping can be resolved.
		// Don't fail processing the event if we have no mxid_maping.
		if sender != nil && senderDomain != input.Origin && senderDomain != r.Cfg.Matrix.ServerName {
			serverRes.ServerNames = append(serverRes.ServerNames, senderDomain)
			delete(servers, senderDomain)
		}
		for server := range servers {
			serverRes.ServerNames = append(serverRes.ServerNames, server)
			delete(servers, server)
		}
		// If the room has partial state, add the join server to the list as a
		// privileged entry. It processed our join recently and is known alive.
		if hasPartialState && roomInfo != nil {
			if joinServer, jsErr := r.DB.GetPartialStateJoinServer(ctx, roomInfo.RoomNID); jsErr == nil && joinServer != "" {
				jsName := spec.ServerName(joinServer)
				if jsName != r.Cfg.Matrix.ServerName {
					// Prepend so it's tried first, even if backing off.
					found := false
					for _, s := range serverRes.ServerNames {
						if s == jsName {
							found = true
							break
						}
					}
					if !found {
						serverRes.ServerNames = append([]spec.ServerName{jsName}, serverRes.ServerNames...)
					}
				}
			}
		}
		var joinServerName spec.ServerName
		if hasPartialState && roomInfo != nil {
			if joinServer, jsErr := r.DB.GetPartialStateJoinServer(ctx, roomInfo.RoomNID); jsErr == nil && joinServer != "" {
				joinServerName = spec.ServerName(joinServer)
			}
		}
		serverRes.ServerNames = withoutBackingOffServers(serverRes.ServerNames, input.Origin, joinServerName, r.FSAPI.IsServerBackingOff)
	}

	// Check that the auth events of the event are known.
	// If they aren't then we will ask the federation API for them.
	// During partial state download (MSC3706), skip federation auth fetching for
	// outlier events. The partial state worker fetches auth events as part of the
	// /state_ids response and stores them alongside state events. Fetching auth
	// per-event here causes rate-limiting storms against the origin server, leading
	// to multi-minute stalls between batch processing.
	authEvents, _ := gomatrixserverlib.NewAuthEvents(nil)
	knownEvents := map[string]*types.Event{}
	isRejected := false
	var rejectionErr error

	checkAuthEvents := func(fetchUnknown bool) error {
		if err := r.fetchAuthEvents(
			ctx, logger, roomInfo, virtualHost, headered, authEvents, knownEvents, serverRes.ServerNames,
			fetchUnknown,
		); err != nil {
			if input.Kind != api.KindOutlier {
				return fmt.Errorf("r.fetchAuthEvents: %w", err)
			}
			// An outlier whose auth events cannot be obtained, e.g. because they
			// are badly signed, is rejected rather than failing the whole batch
			// it came with, such as the room state of a join.
			isRejected = true
			rejectionErr = fmt.Errorf("r.fetchAuthEvents: %w", err)
			logger.WithError(err).Warnf("Rejecting outlier %s whose auth events could not be obtained", event.EventID())
		}

		// An event whose auth events include a rejected event is rejected as a
		// side effect, even if the remaining auth events would allow it.
		if !isRejected {
			for _, knownEvent := range knownEvents {
				if knownEvent.Rejected {
					isRejected = true
					rejectionErr = fmt.Errorf("auth event %s was rejected", knownEvent.EventID())
					break
				}
			}
		}

		// Check if the event is allowed by its auth events. If it isn't then
		// we consider the event to be "rejected" — it will still be persisted.
		// Skip this check for admin operations that set SkipEventAuth.
		if !input.SkipEventAuth && !isRejected {
			if err := gomatrixserverlib.Allowed(event, authEvents, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.Queryer.QueryUserIDForSender(ctx, roomID, senderID)
			}); err != nil {
				// During partial state (MSC3706 faster joins), we may be missing member events
				// that would authorize this event. In this case, we accept the event provisionally
				// rather than rejecting it. The full state resync will validate events properly.
				if hasPartialState {
					logger.WithError(err).Debugf("Event %s failed auth during partial state, accepting provisionally", event.EventID())
				} else {
					isRejected = true
					rejectionErr = err
					logger.WithError(rejectionErr).Warnf("Event %s not allowed by auth events", event.EventID())
				}
			}
		}
		return nil
	}

	// If we are missing prev events, the auth events that we cannot find are
	// often part of the state that we are about to fetch. Ask for the missing
	// prev events and state first, and only then for any auth events that are
	// still unknown. Doing it the other way round makes us depend on the
	// event's own /event_auth response even when the server implements the
	// /get_missing_events and /state_ids flow.
	deferAuthUntilMissingState := !hasPartialState && missingPrev && input.Kind == api.KindNew && !input.SkipEventAuth

	if hasPartialState && input.Kind == api.KindOutlier {
		logger.Debugf("Skipping federation auth fetch for outlier %s (partial state active)", event.EventID())
	} else if !deferAuthUntilMissingState {
		if err = checkAuthEvents(!hasPartialState || !missingPrev || input.Kind != api.KindNew); err != nil {
			return err
		}
	}

	// At this point we are checking whether we know all of the prev events, and
	// if we know the state before the prev events. This is necessary before we
	// try to do `calculateAndSetState` on the event later, otherwise it will fail
	// with missing event NIDs. If there's anything missing then we'll go and fetch
	// the prev events and state from the federation. Note that we only do this if
	// we weren't already told what the state before the event should be — if the
	// HasState option was set and a state set was provided (as is the case in a
	// typical federated room join) then we won't bother trying to fetch prev events
	// because we may not be allowed to see them and we have no choice but to trust
	// the state event IDs provided to us in the join instead.
	// didStateResolution tracks whether we performed expensive federation
	// state lookups for this event. Used to set a cooldown if the event is
	// ultimately rejected, preventing the same expensive work on every
	// subsequent transaction.
	didStateResolution := false
	var stateResolutionRoomID string
	// stateFromGap is set when the state before the event was fetched to
	// bridge a gap in the room DAG. That state covers the remote server's
	// branch only, so unlike the state given with a federated join it must
	// not replace the room's current state and forward extremities.
	stateFromGap := false

	if missingPrev && input.Kind == api.KindNew && !input.SkipMissingEvents {
		// Don't do this for KindOld events, otherwise old events that we fetch
		// to satisfy missing prev events/state will end up recursively calling
		// processRoomEvent. Also skip if SkipMissingEvents is set (e.g. for local
		// user leave events where we don't want to block on federation).

		stateResolutionRoomID = event.RoomID().String()

		// For rooms in partial state (MSC3706), skip expensive federation state
		// resolution. The partial state worker is handling full state download
		// in the background. Attempting state resolution here would likely fail
		// (we're missing most state) and could trigger cooldowns that block
		// future events from being processed.
		if hasPartialState {
			logger.Debugf("Skipping state resolution for room %s (partial state active, background worker will handle)", stateResolutionRoomID)
		}

		// Check if state resolution recently failed for this room. If so, skip
		// the expensive federation lookup. This prevents rooms stuck in a
		// failure loop from consuming unbounded CPU and memory by loading
		// thousands of state events on every incoming transaction.
		//
		// The event is deferred, not rejected: nothing about it has been
		// looked at. The sender does not resend it, and a rejected event is
		// left out of the auth events of every later event that cites it, so
		// a rejected membership, for example, gets its sender's following
		// events rejected too. Nothing is stored instead, so the event stays
		// missing, and the first event in the room after the cooldown fetches
		// it with its other missing prev events.
		if !isRejected {
			if lastFail, ok := r.missingStateCooldown.Load(stateResolutionRoomID); ok {
				lastFailTime, _ := lastFail.(time.Time)
				if remaining := missingStateCooldownDuration - time.Since(lastFailTime); remaining > 0 {
					logger.Warnf("Deferring event with missing prev events in room %s (state resolution cooldown active after recent failure)", stateResolutionRoomID)
					return fmt.Errorf("%w for room %s, ending in %s", errMissingStateCooldown, stateResolutionRoomID, remaining.Round(time.Second))
				}
				// Cooldown expired, clear it and retry.
				r.missingStateCooldown.Delete(stateResolutionRoomID)
			}
		}

		if !isRejected && len(serverRes.ServerNames) > 0 {
			didStateResolution = true
			missingState := missingStateReq{
				origin:           input.Origin,
				virtualHost:      virtualHost,
				inputer:          r,
				db:               r.DB,
				roomInfo:         roomInfo,
				federation:       r.FSAPI,
				keys:             r.KeyRing,
				roomsMu:          internal.NewMutexByRoom(),
				servers:          serverRes.ServerNames,
				hadEvents:        map[string]bool{},
				haveEvents:       map[string]gomatrixserverlib.PDU{},
				unfindableEvents: map[string]bool{},
			}
			var stateSnapshot *parsedRespState
			gapFilled := false
			if hasPartialState {
				// Try to close the gap with the missing events first: with their prev events
				// known the state before this event follows from theirs, as it does for any
				// event. If they don't close it, look up the state before the event they lead
				// to, as Synapse does.
				fillErr := missingState.fillGapDuringPartialState(ctx, event, headered.Version())
				switch {
				case fillErr == nil:
					gapFilled = true
					missingPrev = false
				case unavailableHere(ctx, fillErr):
					return fmt.Errorf("missingState.fillGapDuringPartialState: %w", fillErr)
				default:
					// The gap is not closed, or the missing events could not be had: look up
					// the state before the event, which rejects it if that fails too.
				}
			}
			if gapFilled {
				// The gap is closed, so there is nothing more to do.
			} else if stateSnapshot, err = missingState.processEventWithMissingState(ctx, event, headered.Version()); err != nil {
				// A failure of this server's own database, broker or HA
				// ownership, as during a PostgreSQL failover, says nothing
				// about the event. Rejecting it would be permanent, since the
				// sender does not resend it, and would start the cooldown that
				// rejects the room's following events too. Fail the input
				// instead: nothing is stored, and the next event in the room
				// fetches this one again as a missing prev event.
				if unavailableHere(ctx, err) {
					return fmt.Errorf("missingState.processEventWithMissingState: %w", err)
				}
				// Something went wrong with retrieving the missing state, so we can't
				// really do anything with the event other than reject it at this point.
				isRejected = true
				rejectionErr = fmt.Errorf("missingState.processEventWithMissingState: %w", err)
				var e gomatrixserverlib.EventValidationError
				if errors.As(err, &e) {
					if e.Persistable && stateSnapshot != nil {
						// We retrieved some state and we ended up having to call /state_ids for
						// the new event in question (probably because closing the gap by using
						// /get_missing_events didn't do what we hoped) so we'll instead overwrite
						// the state snapshot with the newly resolved state.
						missingPrev = false
						input.HasState = true
						input.StateEventIDs = stateSnapshotEventIDs(stateSnapshot)
						stateFromGap = true
					}
				}
			} else if stateSnapshot != nil {
				// We retrieved some state and we ended up having to call /state_ids for
				// the new event in question (probably because closing the gap by using
				// /get_missing_events didn't do what we hoped) so we'll instead overwrite
				// the state snapshot with the newly resolved state.
				missingPrev = false
				input.HasState = true
				input.StateEventIDs = stateSnapshotEventIDs(stateSnapshot)
				stateFromGap = true
			} else {
				// We retrieved some state and it would appear that rolling forward the
				// state did everything we needed it to do, so we can just resolve the
				// state for the event in the normal way.
				missingPrev = false
			}
		} else if !isRejected {
			// We're missing prev events or state for the event, but for some reason
			// we don't know any servers to ask. In this case we can't do anything but
			// reject the event and hope that it gets unrejected later.
			isRejected = true
			rejectionErr = fmt.Errorf("missing prev events and no other servers to ask")
		}
	}

	// If we deferred the auth fetch until after resolving the missing prev
	// events, do it now. The state we fetched will usually have made the
	// unknown auth events available locally.
	if deferAuthUntilMissingState {
		if err = checkAuthEvents(true); err != nil {
			return err
		}
	}

	// Accumulate the auth event NIDs.
	authEventIDs := event.AuthEventIDs()
	authEventNIDs := make([]types.EventNID, 0, len(authEventIDs))
	for _, authEventID := range authEventIDs {
		if _, ok := knownEvents[authEventID]; !ok {
			// Unknown auth events only really matter if the event actually failed
			// auth. If it passed auth then we can assume that everything that was
			// known was sufficient, even if extraneous auth events were specified
			// but weren't found. A rejected outlier is stored as rejected all the
			// same, so that it does not fail the batch it arrived with.
			if isRejected && input.Kind != api.KindOutlier {
				if event.StateKey() != nil {
					return fmt.Errorf(
						"missing auth event %s for state event %s (type %q, state key %q)",
						authEventID, event.EventID(), event.Type(), *event.StateKey(),
					)
				} else {
					return fmt.Errorf(
						"missing auth event %s for timeline event %s (type %q)",
						authEventID, event.EventID(), event.Type(),
					)
				}
			}
		} else if !knownEvents[authEventID].Rejected {
			authEventNIDs = append(authEventNIDs, knownEvents[authEventID].EventNID)
		}
	}

	var softfail bool
	// Check if the room is in partial state (MSC3706 faster joins).
	// During partial state, we skip soft-fail checks because we may not have
	// accurate membership state for the sender in the current state.
	var isPartialState bool
	if roomInfo != nil {
		isPartialState, _ = r.DB.IsRoomPartialState(ctx, roomInfo.RoomNID)
	}

	if input.Kind == api.KindNew && !isCreateEvent && !isPartialState && !input.SkipEventAuth {
		// Check that the event passes authentication checks based on the
		// current room state. Skip this for partial state rooms per MSC3706.
		softfail, err = helpers.CheckForSoftFail(ctx, r.DB, roomInfo, headered, input.StateEventIDs, r.Queryer)
		if err != nil {
			logger.WithError(err).Warn("Error authing soft-failed event")
		}
	}

	// Get the state before the event so that we can work out if the event was
	// allowed at the time, and also to get the history visibility. We won't
	// bother doing this if the event was already rejected as it just ends up
	// burning CPU time. Also skip for SkipEventAuth events (admin operations).
	historyVisibility := gomatrixserverlib.HistoryVisibilityShared // Default to shared.
	if input.Kind != api.KindOutlier && rejectionErr == nil && !isRejected && !isCreateEvent && !input.SkipEventAuth {
		historyVisibility, rejectionErr, err = r.processStateBefore(ctx, roomInfo, input, missingPrev, isPartialState)
		if err != nil {
			return fmt.Errorf("r.processStateBefore: %w", err)
		}
		if rejectionErr != nil {
			isRejected = true
		}
	}

	if roomInfo == nil {
		roomInfo, err = r.DB.GetOrCreateRoomInfo(ctx, event)
		if err != nil {
			return fmt.Errorf("r.DB.GetOrCreateRoomInfo: %w", err)
		}
	}

	eventTypeNID, err := r.DB.GetOrCreateEventTypeNID(ctx, event.Type())
	if err != nil {
		return fmt.Errorf("r.DB.GetOrCreateEventTypeNID: %w", err)
	}

	eventStateKeyNID, err := r.DB.GetOrCreateEventStateKeyNID(ctx, event.StateKey())
	if err != nil {
		return fmt.Errorf("r.DB.GetOrCreateEventStateKeyNID: %w", err)
	}

	// Store the event.
	eventNID, stateAtEvent, err := r.DB.StoreEvent(ctx, event, roomInfo, eventTypeNID, eventStateKeyNID, authEventNIDs, isRejected)
	if err != nil {
		return fmt.Errorf("updater.StoreEvent: %w", err)
	}

	// If we performed expensive state resolution for this event, update the
	// cooldown based on whether the event was ultimately accepted or rejected.
	// This ensures that rooms stuck in a rejection loop don't keep burning CPU
	// and memory on federation state lookups for every incoming transaction.
	if didStateResolution && stateResolutionRoomID != "" {
		switch {
		case !isRejected:
			r.missingStateCooldown.Delete(stateResolutionRoomID)
		case errors.Is(rejectionErr, errNoValidMissingEvents):
			logger.Debugf("Not entering missing-state cooldown after invalid /get_missing_events response")
		default:
			r.missingStateCooldown.Store(stateResolutionRoomID, time.Now())
			logger.Warnf("State resolution for room %s resulted in rejection, entering %s cooldown", stateResolutionRoomID, missingStateCooldownDuration)
		}
	}

	// For outliers we can stop after we've stored the event itself as it
	// doesn't have any associated state to store and we don't need to
	// notify anyone about it.
	if input.Kind == api.KindOutlier {
		logger.WithField("rejected", isRejected).Debug("Stored outlier")
		return nil
	}

	// Request the room info again — it's possible that the room has been
	// created by now if it didn't exist already.
	roomInfo, err = r.DB.RoomInfo(ctx, event.RoomID().String())
	if err != nil {
		return fmt.Errorf("updater.RoomInfo: %w", err)
	}
	if roomInfo == nil {
		return fmt.Errorf("updater.RoomInfo missing for room %s", event.RoomID().String())
	}

	if input.HasState || (!missingPrev && stateAtEvent.BeforeStateSnapshotNID == 0) {
		// We haven't calculated a state for this event yet.
		// Lets calculate one.
		err = r.calculateAndSetState(ctx, input, roomInfo, &stateAtEvent, event, isRejected)
		if err != nil {
			return fmt.Errorf("r.calculateAndSetState: %w", err)
		}
	}

	// if storing this event results in it being redacted then do so.
	// we do this after calculating state for this event as we may need to get power levels
	var (
		redactedEventID string
		redactionEvent  gomatrixserverlib.PDU
		redactedEvent   gomatrixserverlib.PDU
	)
	if !isRejected && !isCreateEvent {
		resolver := state.NewStateResolution(r.DB, roomInfo, r.Queryer)
		redactionEvent, redactedEvent, err = r.DB.MaybeRedactEvent(ctx, roomInfo, eventNID, event, &resolver, r.Queryer)
		if err != nil {
			return err
		}
		if redactedEvent != nil {
			redactedEventID = redactedEvent.EventID()
		}
	}

	// We stop here if the event is rejected: We've stored it but won't update
	// forward extremities or notify downstream components about it.
	switch {
	case isRejected:
		logger.WithError(rejectionErr).Warn("Stored rejected event")
		if rejectionErr != nil {
			return types.RejectedError(rejectionErr.Error())
		}
		return nil

	case softfail:
		logger.WithError(rejectionErr).Warn("Stored soft-failed event")
		if rejectionErr != nil {
			return types.RejectedError(rejectionErr.Error())
		}
		return nil
	}

	// Index an accepted annotation, so that its sender can be stopped from
	// sending it again (MSC2677). This runs after MaybeRedactEvent, which
	// strips the content of an annotation whose redaction arrived first, so
	// that it is not indexed. The index only serves that check, so failing
	// to update it does not fail the event.
	if err = r.DB.StoreAnnotation(ctx, roomInfo, event); err != nil {
		logger.WithError(err).Warn("Failed to index annotation")
	}

	// TODO: Revist this to ensure we don't replace a current state mxid_mapping with an older one.
	if event.Version() == gomatrixserverlib.RoomVersionPseudoIDs && event.Type() == spec.MRoomMember {
		mapping := gomatrixserverlib.MemberContent{}
		if err = json.Unmarshal(event.Content(), &mapping); err != nil {
			return err
		}
		if mapping.MXIDMapping != nil {
			storeUserID, userErr := spec.NewUserID(mapping.MXIDMapping.UserID, true)
			if userErr != nil {
				return userErr
			}
			err = r.RSAPI.StoreUserRoomPublicKey(ctx, mapping.MXIDMapping.UserRoomKey, *storeUserID, event.RoomID())
			if err != nil {
				return fmt.Errorf("failed storing user room public key: %w", err)
			}
		}
	}

	switch input.Kind {
	case api.KindNew:
		// Remember which event this client transaction produced before its
		// output events are published, so that a retry is answered with it
		// rather than storing another event. Once published, clients and
		// servers may have received the event even if the request then
		// fails, for example because the database connection is lost before
		// the room update commits. A retry finds the event not marked as
		// sent and processes it again (see retryClientTransaction), so it
		// is delivered whichever side of publishing the request failed on.
		if input.TransactionID != nil {
			if err = r.DB.StoreClientTransaction(
				ctx, event.RoomID().String(), string(event.SenderID()),
				input.TransactionID.SessionID, input.TransactionID.TransactionID, event.EventID(),
			); err != nil {
				// Nothing is published yet: the retry stores a new event.
				return fmt.Errorf("r.DB.StoreClientTransaction: %w", err)
			}
		}
		// State fetched to bridge a gap is merged like any other state.
		rewritesState := input.HasState && !stateFromGap
		localLeftJoin, err := r.updateLatestEvents(
			ctx,                 // context
			roomInfo,            // room info for the room being updated
			stateAtEvent,        // state at event (below)
			event,               // event
			input.SendAsServer,  // send as server
			input.TransactionID, // transaction ID
			rewritesState,       // rewrites state?
			historyVisibility,   // the history visibility before the event
		)
		if err != nil {
			return fmt.Errorf("r.updateLatestEvents: %w", err)
		}
		if localLeftJoin {
			r.ScheduleAutoPurgeIfEmpty(ctx, roomInfo)
		}
	case api.KindOld:
		err = r.OutputProducer.ProduceRoomEvents(event.RoomID().String(), []api.OutputEvent{
			{
				Type: api.OutputTypeOldRoomEvent,
				OldRoomEvent: &api.OutputOldRoomEvent{
					Event:             headered,
					HistoryVisibility: historyVisibility,
				},
			},
		})
		if err != nil {
			return fmt.Errorf("r.WriteOutputEvents (old): %w", err)
		}
	}

	// If this is a membership event, it is possible we newly joined a federated room and eventually
	// missed to update our m.room.server_acl - the following ensures we set the ACLs
	// TODO: This probably performs badly in benchmarks
	if event.Type() == spec.MRoomMember {
		membership, _ := event.Membership()
		if membership == spec.Join {
			_, serverName, _ := gomatrixserverlib.SplitID('@', *event.StateKey())
			// only handle local membership events
			if r.Cfg.Matrix.IsLocalServerName(serverName) {
				var aclEvent *types.HeaderedEvent
				aclEvent, err = r.DB.GetStateEvent(ctx, event.RoomID().String(), acls.MRoomServerACL, "")
				if err != nil {
					logrus.WithError(err).Error("failed to get server ACLs")
				}
				if aclEvent != nil {
					strippedEvent := tables.StrippedEvent{
						RoomID:       aclEvent.RoomID().String(),
						EventType:    aclEvent.Type(),
						StateKey:     *aclEvent.StateKey(),
						ContentValue: string(aclEvent.Content()),
					}
					r.ACLs.OnServerACLUpdate(strippedEvent)
				}
			}
		}
	}

	// Handle remote room upgrades, e.g. remove published room
	if event.Type() == "m.room.tombstone" && event.StateKeyEquals("") && !r.Cfg.Matrix.IsLocalServerName(senderDomain) {
		if err = r.handleRemoteRoomUpgrade(ctx, event); err != nil {
			return fmt.Errorf("failed to handle remote room upgrade: %w", err)
		}
	}

	// processing this event resulted in an event (which may not be the one we're processing)
	// being redacted. We are guaranteed to have both sides (the redaction/redacted event),
	// so notify downstream components to redact this event - they should have it if they've
	// been tracking our output log.
	if redactedEventID != "" {
		err = r.OutputProducer.ProduceRoomEvents(event.RoomID().String(), []api.OutputEvent{
			{
				Type: api.OutputTypeRedactedEvent,
				RedactedEvent: &api.OutputRedactedEvent{
					RedactedEventID: redactedEventID,
					RedactedBecause: &types.HeaderedEvent{PDU: redactionEvent},
				},
			},
		})
		if err != nil {
			return fmt.Errorf("r.WriteOutputEvents (redactions): %w", err)
		}
	}

	// If guest_access changed and is not can_join, kick all guest users.
	if event.Type() == spec.MRoomGuestAccess && gjson.GetBytes(event.Content(), "guest_access").Str != "can_join" {
		if err = r.kickGuests(ctx, event, roomInfo); err != nil && !errors.Is(err, sql.ErrNoRows) {
			logrus.WithError(err).Error("failed to kick guest users on m.room.guest_access revocation")
		}
	}

	// Everything was OK — the latest events updater didn't error and
	// we've sent output events.
	return nil
}

// handleRemoteRoomUpgrade updates published rooms and room aliases.
func (r *Inputer) handleRemoteRoomUpgrade(ctx context.Context, event gomatrixserverlib.PDU) error {
	oldRoomID := event.RoomID().String()
	newRoomID := gjson.GetBytes(event.Content(), "replacement_room").Str
	return r.DB.UpgradeRoom(ctx, oldRoomID, newRoomID, string(event.SenderID()))
}

// processStateBefore works out what the state is before the event and
// then checks the event auths against the state at the time. It also
// tries to determine what the history visibility was of the event at
// the time, so that it can be sent in the output event to downstream
// components.
//
// For partial state rooms (MSC3706 faster joins), the auth checking uses
// state resolution between the local partial state and the event's auth
// events. This ensures bans and other restrictions are enforced even when
// we don't have complete room state.
//
//nolint:nakedret
func (r *Inputer) processStateBefore(
	ctx context.Context,
	roomInfo *types.RoomInfo,
	input *api.InputRoomEvent,
	missingPrev bool,
	isPartialState bool,
) (historyVisibility gomatrixserverlib.HistoryVisibility, rejectionErr error, err error) {
	historyVisibility = gomatrixserverlib.HistoryVisibilityShared // Default to shared.
	event := input.Event.PDU
	isCreateEvent := event.Type() == spec.MRoomCreate && event.StateKeyEquals("")
	var stateBeforeEvent []gomatrixserverlib.PDU
	switch {
	case isCreateEvent:
		// There's no state before a create event so there is nothing
		// else to do.
		return
	case input.HasState:
		// If we're overriding the state then we need to go and retrieve
		// them from the database. It's a hard error if they are missing.
		var stateEvents []types.Event
		stateEvents, err = r.DB.EventsFromIDs(ctx, roomInfo, input.StateEventIDs)
		if err != nil {
			return "", nil, fmt.Errorf("r.DB.EventsFromIDs: %w", err)
		}
		stateBeforeEvent = make([]gomatrixserverlib.PDU, 0, len(stateEvents))
		for _, entry := range stateEvents {
			stateBeforeEvent = append(stateBeforeEvent, entry.PDU)
		}
	case missingPrev:
		// We don't know all of the prev events, so we can't work out
		// the state before the event. Reject it in that case.
		rejectionErr = fmt.Errorf("event %q has missing prev events", event.EventID())
		return
	case len(event.PrevEventIDs()) == 0:
		// There should be prev events since it's not a create event.
		// A non-create event that claims to have no prev events is
		// invalid, so reject it.
		rejectionErr = fmt.Errorf("event %q must have prev events", event.EventID())
		return
	default:
		// For all non-create events, there must be prev events, so we'll
		// ask the query API for the relevant tuples needed for auth. We
		// will include the history visibility here even though we don't
		// actually need it for auth, because we want to send it in the
		// output events.
		tuplesNeeded := gomatrixserverlib.StateNeededForAuth([]gomatrixserverlib.PDU{event}).Tuples()
		tuplesNeeded = append(tuplesNeeded, gomatrixserverlib.StateKeyTuple{
			EventType: spec.MRoomHistoryVisibility,
			StateKey:  "",
		})
		stateBeforeReq := &api.QueryStateAfterEventsRequest{
			RoomID:       event.RoomID().String(),
			PrevEventIDs: event.PrevEventIDs(),
			StateToFetch: tuplesNeeded,
		}
		stateBeforeRes := &api.QueryStateAfterEventsResponse{}
		if err = r.Queryer.QueryStateAfterEvents(ctx, stateBeforeReq, stateBeforeRes); err != nil {
			return "", nil, fmt.Errorf("r.Queryer.QueryStateAfterEvents: %w", err)
		}
		switch {
		case !stateBeforeRes.RoomExists:
			rejectionErr = fmt.Errorf("room %q does not exist", event.RoomID().String())
			return
		case !stateBeforeRes.PrevEventsExist:
			rejectionErr = fmt.Errorf("prev events of %q are not known", event.EventID())
			return
		default:
			stateBeforeEvent = make([]gomatrixserverlib.PDU, len(stateBeforeRes.StateEvents))
			for i := range stateBeforeRes.StateEvents {
				stateBeforeEvent[i] = stateBeforeRes.StateEvents[i].PDU
			}
		}
	}
	// At this point, stateBeforeEvent should be populated either by
	// the supplied state in the input request, or from the prev events.
	// Check whether the event is allowed or not.
	//
	// For partial state rooms (MSC3706), the state before the event lacks most
	// members, so complete it with the auth events the event cites, where it has no
	// entry of its own. Its own entries stand, so that a ban or a leave still applies.
	stateForAuth := stateBeforeEvent
	if isPartialState {
		if authEventIDs := event.AuthEventIDs(); len(authEventIDs) > 0 {
			authStateEvents, authErr := r.DB.EventsFromIDs(ctx, roomInfo, authEventIDs)
			if authErr != nil {
				// Without the auth events, use the local state only.
				logrus.WithError(authErr).WithField("event_id", event.EventID()).Debug("Could not load the auth events of an event in a partial state room")
			} else {
				authEventPDUs := make([]gomatrixserverlib.PDU, 0, len(authStateEvents))
				for _, authEvent := range authStateEvents {
					authEventPDUs = append(authEventPDUs, authEvent.PDU)
				}
				stateForAuth = mergePartialStateAuth(stateBeforeEvent, authEventPDUs)
			}
		}
	}

	var stateBeforeAuth *gomatrixserverlib.AuthEvents
	stateBeforeAuth, err = gomatrixserverlib.NewAuthEvents(
		gomatrixserverlib.ToPDUs(stateForAuth),
	)
	if err != nil {
		rejectionErr = fmt.Errorf("NewAuthEvents failed: %w", err)
		return
	}
	if rejectionErr = gomatrixserverlib.Allowed(event, stateBeforeAuth, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return r.Queryer.QueryUserIDForSender(ctx, roomID, senderID)
	}); rejectionErr != nil {
		rejectionErr = fmt.Errorf("Allowed() failed for stateBeforeEvent: %w", rejectionErr)
		return
	}
	// Work out what the history visibility was at the time of the
	// event.
	for _, event := range stateBeforeEvent {
		if event.Type() != spec.MRoomHistoryVisibility || !event.StateKeyEquals("") {
			continue
		}
		if hisVis, err := event.HistoryVisibility(); err == nil {
			historyVisibility = hisVis
			break
		}
	}
	return
}

// mergePartialStateAuth completes the state before an event with the auth events
// the event cites, for MSC3706 faster joins.
//
// During partial state the state before an event lacks most members. The state that
// there is comes from the event's real prev events, so wherever it has an entry it
// is what the room looked like, and the entry stays even if the event cites another
// one: a user the room has banned is banned, whatever membership event they cite.
// The cited auth events only fill in what the partial state does not have, such as
// the membership of a sender whose join was left out.
func mergePartialStateAuth(localState, authEvents []gomatrixserverlib.PDU) []gomatrixserverlib.PDU {
	type stateKey struct {
		eventType string
		stateKey  string
	}
	merged := make(map[stateKey]gomatrixserverlib.PDU, len(localState)+len(authEvents))
	for _, ev := range localState {
		if ev.StateKey() != nil {
			merged[stateKey{ev.Type(), *ev.StateKey()}] = ev
		}
	}
	for _, ev := range authEvents {
		if ev.StateKey() == nil {
			continue
		}
		key := stateKey{ev.Type(), *ev.StateKey()}
		if _, ok := merged[key]; !ok {
			merged[key] = ev
		}
	}
	result := make([]gomatrixserverlib.PDU, 0, len(merged))
	for _, ev := range merged {
		result = append(result, ev)
	}
	return result
}

// fetchAuthEvents will check to see if any of the
// auth events specified by the given event are unknown. If they are
// then we will go off and request them from the federation and then
// store them in the database. By the time this function ends, either
// we've failed to retrieve the auth chain altogether (in which case
// an error is returned) or we've successfully retrieved them all and
// they are now in the database. If fetchUnknown is false the unknown ones
// are left alone and only those already known are loaded.
//
//nolint:gocyclo
func (r *Inputer) fetchAuthEvents(
	ctx context.Context,
	logger *logrus.Entry,
	roomInfo *types.RoomInfo,
	virtualHost spec.ServerName,
	event *types.HeaderedEvent,
	auth *gomatrixserverlib.AuthEvents,
	known map[string]*types.Event,
	servers []spec.ServerName,
	fetchUnknown bool,
) error {
	trace, ctx := internal.StartRegion(ctx, "fetchAuthEvents")
	defer trace.EndRegion()

	unknown := map[string]struct{}{}
	authEventIDs := event.AuthEventIDs()
	if len(authEventIDs) == 0 {
		return nil
	}

	for _, authEventID := range authEventIDs {
		authEvents, err := r.DB.EventsFromIDs(ctx, roomInfo, []string{authEventID})
		if err != nil || len(authEvents) == 0 || authEvents[0].PDU == nil {
			unknown[authEventID] = struct{}{}
			continue
		}
		ev := authEvents[0]

		if roomInfo != nil {
			ev.Rejected, err = r.DB.IsEventRejected(ctx, roomInfo.RoomNID, ev.EventID())
			if err != nil && !errors.Is(err, sql.ErrNoRows) {
				return fmt.Errorf("r.DB.IsEventRejected failed: %w", err)
			}
		}
		known[authEventID] = &ev // don't take the pointer of the iterated event
		if !ev.Rejected {
			if err = auth.AddEvent(ev.PDU); err != nil {
				return fmt.Errorf("auth.AddEvent: %w", err)
			}
		}
	}

	// If there are no missing auth events then there is nothing more
	// to do — we've loaded everything that we need.
	if len(unknown) == 0 || !fetchUnknown {
		return nil
	}

	var err error
	var res fclient.RespEventAuth
	var found bool
	for _, serverName := range servers {
		// Request the entire auth chain for the event in question. This should
		// contain all of the auth events — including ones that we already know —
		// so we'll need to filter through those in the next section.
		res, err = r.FSAPI.GetEventAuth(ctx, virtualHost, serverName, event.Version(), event.RoomID().String(), event.EventID())
		if err != nil {
			logger.WithError(err).Warnf("Failed to get event auth from federation for %q: %s", event.EventID(), err)
			continue
		}
		found = true
		break
	}
	if !found {
		// /event_auth is optional and some servers (including Complement's
		// MSC4297 test fixture) do not implement it. Fall back to fetching
		// the unknown auth events directly via /event; the state fetched by
		// /state_ids may already include their auth chains.
		for authEventID := range unknown {
			for _, serverName := range servers {
				txn, getErr := r.FSAPI.GetEvent(ctx, virtualHost, serverName, authEventID)
				if getErr != nil || len(txn.PDUs) == 0 {
					logger.WithError(getErr).Debugf("Failed to get missing auth event %q from %q", authEventID, serverName)
					continue
				}
				for _, pdu := range txn.PDUs {
					res.AuthEvents = append(res.AuthEvents, spec.RawJSON(pdu))
				}
				found = true
				break
			}
		}
	}
	if !found {
		return fmt.Errorf("no servers provided event auth for event ID %q, tried servers %v", event.EventID(), servers)
	}

	// Start with a clean state and see if we can auth with what the remote
	// server told us. Otherwise earlier topologically sorted events could
	// fail to be authed by more recent referenced ones.
	auth.Clear()

	// Reuse these to reduce allocations.
	_authEventNIDs := [5]types.EventNID{}
	var isRejected bool
nextAuthEvent:
	for _, authEvent := range gomatrixserverlib.ReverseTopologicalOrdering(
		gomatrixserverlib.ToPDUs(res.AuthEvents.UntrustedEvents(event.Version())),
		gomatrixserverlib.TopologicalOrderByAuthEvents,
	) {
		// If we already know about this event from the database then we don't
		// need to store it again or do anything further with it, so just skip
		// over it rather than wasting cycles.
		if ev, ok := known[authEvent.EventID()]; ok && ev != nil {
			// Rejected auth events are known, but must not be added to the
			// authorization set. Reprocessing them here can fail because their
			// own auth chain was deliberately rejected or is no longer available.
			if !ev.Rejected {
				if err := auth.AddEvent(authEvent); err != nil {
					return fmt.Errorf("auth.AddEvent: %w", err)
				}
			}
			continue nextAuthEvent
		}

		// Check the signatures of the event. If this fails then we'll simply
		// skip it, because gomatrixserverlib.Allowed() will notice a problem
		// if a critical event is missing anyway.
		if err := gomatrixserverlib.VerifyEventSignatures(ctx, authEvent, r.FSAPI.KeyRing(), func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.Queryer.QueryUserIDForSender(ctx, roomID, senderID)
		}); err != nil {
			continue nextAuthEvent
		}

		// In order to store the new auth event, we need to know its auth chain
		// as NIDs for the `auth_event_nids` column. Let's see if we can find those.
		authEventNIDs := _authEventNIDs[:0]
		for _, eventID := range authEvent.AuthEventIDs() {
			knownEvent, ok := known[eventID]
			if !ok {
				// The remote auth response may not include the full auth chain
				// of every event it returns. The event may still be known to
				// us, for example a previously rejected event. Load it rather
				// than failing the whole auth fetch.
				events, err := r.DB.EventsFromIDs(ctx, roomInfo, []string{eventID})
				if err != nil || len(events) == 0 || events[0].PDU == nil {
					return fmt.Errorf("auth event ID %s not known but should be", eventID)
				}
				ev := events[0]
				if roomInfo != nil {
					ev.Rejected, err = r.DB.IsEventRejected(ctx, roomInfo.RoomNID, eventID)
					if err != nil && !errors.Is(err, sql.ErrNoRows) {
						return fmt.Errorf("r.DB.IsEventRejected failed: %w", err)
					}
				}
				knownEvent = &ev
				known[eventID] = knownEvent
			}
			authEventNIDs = append(authEventNIDs, knownEvent.EventNID)
		}

		// Check if the auth event should be rejected.
		err := gomatrixserverlib.Allowed(authEvent, auth, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.Queryer.QueryUserIDForSender(ctx, roomID, senderID)
		})
		if isRejected = err != nil; isRejected {
			logger.WithError(err).Warnf("Auth event %s rejected", authEvent.EventID())
		}

		if roomInfo == nil {
			roomInfo, err = r.DB.GetOrCreateRoomInfo(ctx, authEvent)
			if err != nil {
				return fmt.Errorf("r.DB.GetOrCreateRoomInfo: %w", err)
			}
		}

		eventTypeNID, err := r.DB.GetOrCreateEventTypeNID(ctx, authEvent.Type())
		if err != nil {
			return fmt.Errorf("r.DB.GetOrCreateEventTypeNID: %w", err)
		}

		eventStateKeyNID, err := r.DB.GetOrCreateEventStateKeyNID(ctx, authEvent.StateKey())
		if err != nil {
			return fmt.Errorf("r.DB.GetOrCreateEventStateKeyNID: %w", err)
		}

		// Finally, store the event in the database.
		eventNID, _, err := r.DB.StoreEvent(ctx, authEvent, roomInfo, eventTypeNID, eventStateKeyNID, authEventNIDs, isRejected)
		if err != nil {
			return fmt.Errorf("updater.StoreEvent: %w", err)
		}

		// Let's take a note of the fact that we now know about this event for
		// authenticating future events.
		if !isRejected {
			if err := auth.AddEvent(authEvent); err != nil {
				return fmt.Errorf("auth.AddEvent: %w", err)
			}
		}

		// Now we know about this event, it was stored and the signatures were OK.
		known[authEvent.EventID()] = &types.Event{
			EventNID: eventNID,
			Rejected: isRejected,
			PDU:      authEvent,
		}
	}

	for _, ev := range known {
		if ev != nil && !ev.Rejected && ev.PDU != nil {
			_ = auth.AddEvent(ev.PDU)
		}
	}

	return nil
}

func (r *Inputer) calculateAndSetState(
	ctx context.Context,
	input *api.InputRoomEvent,
	roomInfo *types.RoomInfo,
	stateAtEvent *types.StateAtEvent,
	event gomatrixserverlib.PDU,
	isRejected bool,
) error {
	trace, ctx := internal.StartRegion(ctx, "calculateAndSetState")
	defer trace.EndRegion()

	var succeeded bool
	updater, err := r.DB.GetRoomUpdater(ctx, roomInfo)
	if err != nil {
		return fmt.Errorf("r.DB.GetRoomUpdater: %w", err)
	}
	defer sqlutil.EndTransactionWithCheck(updater, &succeeded, &err)
	roomState := state.NewStateResolution(updater, roomInfo, r.Queryer)

	if input.HasState {
		// We've been told what the state at the event is so we don't need to calculate it.
		// Check that those state events are in the database and store the state.
		var entries []types.StateEntry
		if entries, err = r.DB.StateEntriesForEventIDs(ctx, input.StateEventIDs, true); err != nil {
			return fmt.Errorf("updater.StateEntriesForEventIDs: %w", err)
		}
		entries = types.DeduplicateStateEntries(entries)

		if stateAtEvent.BeforeStateSnapshotNID, err = updater.AddState(ctx, roomInfo.RoomNID, nil, entries); err != nil {
			return fmt.Errorf("updater.AddState: %w", err)
		}
	} else {
		// We haven't been told what the state at the event is so we need to calculate it from the prev_events
		if stateAtEvent.BeforeStateSnapshotNID, err = roomState.CalculateAndStoreStateBeforeEvent(ctx, event, isRejected); err != nil {
			return fmt.Errorf("roomState.CalculateAndStoreStateBeforeEvent: %w", err)
		}
	}

	err = updater.SetState(ctx, stateAtEvent.EventNID, stateAtEvent.BeforeStateSnapshotNID)
	if err != nil {
		return fmt.Errorf("r.DB.SetState: %w", err)
	}
	succeeded = true
	return nil
}

// kickGuests kicks guests users from m.room.guest_access rooms, if guest access is now prohibited.
func (r *Inputer) kickGuests(ctx context.Context, event gomatrixserverlib.PDU, roomInfo *types.RoomInfo) error {
	membershipNIDs, err := r.DB.GetMembershipEventNIDsForRoom(ctx, roomInfo.RoomNID, true, true)
	if err != nil {
		return err
	}

	if roomInfo == nil {
		return types.ErrorInvalidRoomInfo
	}
	memberEvents, err := r.DB.Events(ctx, roomInfo.RoomVersion, membershipNIDs)
	if err != nil {
		return err
	}

	inputEvents := make([]api.InputRoomEvent, 0, len(memberEvents))
	latestReq := &api.QueryLatestEventsAndStateRequest{
		RoomID: event.RoomID().String(),
	}
	latestRes := &api.QueryLatestEventsAndStateResponse{}
	if err = r.Queryer.QueryLatestEventsAndState(ctx, latestReq, latestRes); err != nil {
		return err
	}

	prevEvents := latestRes.LatestEvents
	for _, memberEvent := range memberEvents {
		if memberEvent.StateKey() == nil {
			continue
		}

		memberUserID, err := r.Queryer.QueryUserIDForSender(ctx, event.RoomID(), spec.SenderID(*memberEvent.StateKey()))
		if err != nil {
			continue
		}

		accountRes := &userAPI.QueryAccountByLocalpartResponse{}
		if err = r.UserAPI.QueryAccountByLocalpart(ctx, &userAPI.QueryAccountByLocalpartRequest{
			Localpart:  memberUserID.Local(),
			ServerName: memberUserID.Domain(),
		}, accountRes); err != nil {
			return err
		}
		if accountRes.Account == nil {
			continue
		}

		if accountRes.Account.AccountType != userAPI.AccountTypeGuest {
			continue
		}

		var memberContent gomatrixserverlib.MemberContent
		if err = json.Unmarshal(memberEvent.Content(), &memberContent); err != nil {
			return err
		}
		memberContent.Membership = spec.Leave

		stateKey := *memberEvent.StateKey()
		fledglingEvent := &gomatrixserverlib.ProtoEvent{
			RoomID:     event.RoomID().String(),
			Type:       spec.MRoomMember,
			StateKey:   &stateKey,
			SenderID:   stateKey,
			PrevEvents: prevEvents,
		}

		if fledglingEvent.Content, err = json.Marshal(memberContent); err != nil {
			return err
		}

		eventsNeeded, err := gomatrixserverlib.StateNeededForProtoEvent(fledglingEvent)
		if err != nil {
			return err
		}

		signingIdentity, err := r.SigningIdentity(ctx, event.RoomID(), *memberUserID)
		if err != nil {
			return err
		}

		event, err := eventutil.BuildEvent(ctx, fledglingEvent, &signingIdentity, time.Now(), &eventsNeeded, latestRes)
		if err != nil {
			return err
		}

		inputEvents = append(inputEvents, api.InputRoomEvent{
			Kind:         api.KindNew,
			Event:        event,
			Origin:       memberUserID.Domain(),
			SendAsServer: string(memberUserID.Domain()),
		})
		prevEvents = []string{event.EventID()}
	}

	inputReq := &api.InputRoomEventsRequest{
		InputRoomEvents: inputEvents,
		Asynchronous:    true, // Needs to be async, as we otherwise create a deadlock
	}
	inputRes := &api.InputRoomEventsResponse{}
	r.InputRoomEvents(ctx, inputReq, inputRes)
	return nil
}

// stateSnapshotEventIDs extracts state event IDs from a parsedRespState,
// using the pre-computed StateEventIDs when outliers were stored in chunks.
func stateSnapshotEventIDs(s *parsedRespState) []string {
	if s.OutliersStored {
		return s.StateEventIDs
	}
	ids := make([]string, 0, len(s.StateEvents))
	for _, e := range s.StateEvents {
		ids = append(ids, e.EventID())
	}
	return ids
}

// withoutBackingOffServers drops the servers we are backing off from, so that
// missing-state resolution does not iterate hundreds of dead servers. Two
// servers are known to be alive and are kept regardless: the origin, which just
// sent us the event, and the partial state join server, which recently
// processed our join. Our backoff towards the origin only reflects our own
// failed sends, for example while it was restarting, and dropping it would
// reject every event that arrives ahead of its prev events.
// UnavailableHere reports whether processing failed on this server's own
// dependencies rather than on the event or the servers asked about it.
// Generic network errors are not counted: while filling a gap they may as
// well come from a remote server, whose failures the cooldown is for.
func unavailableHere(ctx context.Context, err error) bool {
	return ctx.Err() != nil ||
		sqlutil.IsDatabaseUnavailable(err) ||
		errors.Is(err, jetstream.ErrPublicationRefused) ||
		errors.Is(err, nats.ErrTimeout) ||
		errors.Is(err, nats.ErrNoStreamResponse) ||
		jetstream.IsTransientFetchError(err)
}

func withoutBackingOffServers(
	servers []spec.ServerName, origin, joinServer spec.ServerName,
	isBackingOff func(spec.ServerName) bool,
) []spec.ServerName {
	filtered := servers[:0]
	for _, server := range servers {
		if server == origin || server == joinServer || !isBackingOff(server) {
			filtered = append(filtered, server)
		}
	}
	return filtered
}
