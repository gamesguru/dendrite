// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	federationAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/auth"
	"codefloe.com/pat-s/zendrite/roomserver/internal/helpers"
	"codefloe.com/pat-s/zendrite/roomserver/state"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
)

// the max number of servers to backfill from per request. If this is too low we may fail to backfill when
// we could've from another server. If this is too high we may take far too long to successfully backfill
// as we try dead servers.
const (
	maxBackfillServers    = 5
	maxBackfillEvents     = 100
	minBackfillFromEvents = 2
)

type Backfiller struct {
	IsLocalServerName func(spec.ServerName) bool
	DB                storage.Database
	FSAPI             federationAPI.RoomserverFederationAPI
	KeyRing           gomatrixserverlib.JSONVerifier
	Querier           api.QuerySenderIDAPI

	// The servers which should be preferred above other servers when backfilling
	PreferServers []spec.ServerName
}

// PerformBackfill implements api.RoomServerQueryAPI.
func (r *Backfiller) PerformBackfill(
	ctx context.Context,
	request *api.PerformBackfillRequest,
	response *api.PerformBackfillResponse,
) error {
	// if we are requesting the backfill then we need to do a federation hit
	// TODO: we could be more sensible and fetch as many events we already have then request the rest
	//       which is what the syncapi does already.
	if r.IsLocalServerName(request.ServerName) {
		return r.backfillViaFederation(ctx, request, response)
	}
	// someone else is requesting the backfill, try to service their request.
	var err error
	var front []string

	// The limit defines the maximum number of events to retrieve, so it also
	// defines the highest number of elements in the map below.
	visited := make(map[string]bool, request.Limit)

	// this will include these events which is what we want
	front = request.PrevEventIDs()

	info, err := r.DB.RoomInfo(ctx, request.RoomID)
	if err != nil {
		return err
	}
	if info == nil || info.IsStub() {
		return fmt.Errorf("PerformBackfill: missing room info for room %s", request.RoomID)
	}

	// Scan the event tree for events to send back.
	resultNIDs, redactEventIDs, err := helpers.ScanEventTree(ctx, r.DB, info, front, visited, request.Limit, request.ServerName, r.Querier)
	if err != nil {
		return err
	}

	// Retrieve events from the list that was filled previously. If we fail to get
	// events from the database then attempt once to get them from federation instead.
	var loadedEvents []gomatrixserverlib.PDU
	loadedEvents, err = helpers.LoadEvents(ctx, r.DB, info, resultNIDs)
	if err != nil {
		var missingEventErr types.MissingEventError
		if errors.As(err, &missingEventErr) {
			return r.backfillViaFederation(ctx, request, response)
		}
		return err
	}

	for _, event := range loadedEvents {
		if _, ok := redactEventIDs[event.EventID()]; ok {
			// Loaded events can be shared through the event cache, so
			// redact a copy. A copy that cannot be made is left out.
			redacted, redactErr := types.RedactedCopy(event)
			if redactErr != nil {
				continue
			}
			event = redacted
		}
		response.Events = append(response.Events, &types.HeaderedEvent{PDU: event})
	}

	return err
}

func (r *Backfiller) backfillViaFederation(ctx context.Context, req *api.PerformBackfillRequest, res *api.PerformBackfillResponse) error {
	info, err := r.DB.RoomInfo(ctx, req.RoomID)
	if err != nil {
		return err
	}
	if info == nil || info.IsStub() {
		return fmt.Errorf("backfillViaFederation: missing room info for room %s", req.RoomID)
	}
	requester := newBackfillRequester(r.DB, r.FSAPI, r.Querier, req.VirtualHost, r.IsLocalServerName, req.BackwardsExtremities, r.PreferServers, info.RoomVersion)
	requester.roomNID = info.RoomNID
	// Request 100 items regardless of what the query asks for.
	// We don't want to go much higher than this.
	// We can't honor exactly the limit as some sytests rely on requesting more for tests to pass
	// (so we don't need to hit /state_ids which the test has no listener for)
	// Specifically the test "Outbound federation can backfill events"
	fromEventIDs := prioritiseBackfillFromEventIDs(ctx, req.RoomID, req.PrevEventIDs(), requester.ServersAtEvent)
	events, err := gomatrixserverlib.RequestBackfill(
		ctx, req.VirtualHost, requester,
		r.KeyRing, req.RoomID, info.RoomVersion, fromEventIDs, maxBackfillEvents, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return r.Querier.QueryUserIDForSender(ctx, roomID, senderID)
		},
	)
	returned := make(map[string]struct{}, len(events))
	for _, ev := range events {
		returned[ev.EventID()] = struct{}{}
	}
	for _, id := range fromEventIDs {
		if _, ok := returned[id]; !ok {
			logrus.WithField("room_id", req.RoomID).WithField("frontier", id).
				Warn("backfillViaFederation: response did not contain the requested frontier event")
		}
	}
	// Also report whether the predecessors of every returned event that is
	// part of the frontier made it into the response. This distinguishes
	// "server returned the frontier but not its parent" from a failure that
	// happens after the response is received.
	for _, ev := range events {
		isFrontier := false
		for _, id := range fromEventIDs {
			if id == ev.EventID() {
				isFrontier = true
				break
			}
		}
		if !isFrontier {
			continue
		}
		for _, prev := range ev.PrevEventIDs() {
			_, have := returned[prev]
			logrus.WithField("room_id", req.RoomID).
				WithField("frontier", ev.EventID()).
				WithField("prev_event", prev).
				WithField("prev_in_response", have).
				WithField("response_events", len(events)).
				Debug("backfillViaFederation: frontier predecessor check")
		}
	}
	// Only return an error if we really couldn't get any events.
	if err != nil && len(events) == 0 {
		logrus.WithError(err).Errorf("gomatrixserverlib.RequestBackfill failed")
		requester.backfillErrors = append(requester.backfillErrors, err)
		return &api.BackfillUnavailableError{
			Retryable: backfillFailuresRetryable(requester.backfillErrors),
			Err:       err,
		}
	}
	// If we got an error but still got events, that's fine, because a server might have returned a 404 (or something)
	// but other servers could provide the missing event.
	logrus.WithError(err).WithField("room_id", req.RoomID).Infof("backfilled %d events", len(events))

	// persist these new events - auth checks have already been done
	roomNID, storedEvents := persistEvents(ctx, r.DB, r.Querier, events)

	// The history visibility of a backfilled event comes from the state before
	// that event, not from the backfill anchor: one batch can cross a
	// visibility change, and the sync API's history filter applies each event's
	// own value when the event is later read from /messages.
	historyVisibilities := make(map[string]gomatrixserverlib.HistoryVisibility, len(storedEvents))
	redactedInBatch := make(map[string]gomatrixserverlib.PDU)
	// State events fetched while resolving the state above. They are stored but
	// have no before-state here, so they are returned with the most restrictive
	// visibility rather than left invisible to the sync API.
	var sideEffectEvents []gomatrixserverlib.PDU
	// Redactions whose state assignment or validation did not complete. They
	// are left out of the response, so sync does not store a redaction that
	// never redacted its target; a retry fetches and validates them again.
	unvalidatedRedactions := make(map[string]struct{})
	markUnvalidated := func(ev gomatrixserverlib.PDU) {
		if ev.Type() == spec.MRoomRedaction && ev.StateKey() == nil {
			unvalidatedRedactions[ev.EventID()] = struct{}{}
		}
	}
	for _, ev := range storedEvents {
		// now add state for these events
		stateIDs, ok := requester.eventIDToBeforeStateIDs[ev.EventID()]
		if !ok {
			// this should be impossible as all events returned must have pass Step 5 of the PDU checks
			// which requires a list of state IDs.
			logrus.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to find state IDs for event which passed auth checks")
			markUnvalidated(ev.PDU)
			continue
		}
		var entries []types.StateEntry
		if entries, err = r.DB.StateEntriesForEventIDs(ctx, stateIDs, true); err != nil {
			// attempt to fetch the missing events
			sideEffectEvents = append(sideEffectEvents,
				r.fetchAndStoreMissingEvents(ctx, info.RoomVersion, requester, stateIDs, req.VirtualHost)...)
			// try again
			entries, err = r.DB.StateEntriesForEventIDs(ctx, stateIDs, true)
			if err != nil {
				logrus.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to get state entries for event")
				// Keep processing the batch. State events fetched above are
				// already persisted in the roomserver and must still be returned
				// to sync; a later retry would consider them present and omit
				// them from sideEffectEvents.
				markUnvalidated(ev.PDU)
				continue
			}
		}

		historyVisibilities[ev.EventID()] = historyVisibilityBeforeEvent(ctx, r.DB, info, entries)

		var beforeStateSnapshotNID types.StateSnapshotNID
		if beforeStateSnapshotNID, err = r.DB.AddState(ctx, roomNID, entries); err != nil {
			logrus.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to persist state entries to get snapshot nid")
			return err
		}
		if err = r.DB.SetState(ctx, ev.EventNID, beforeStateSnapshotNID); err != nil {
			logrus.WithError(err).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to persist snapshot nid")
			markUnvalidated(ev.PDU)
			continue
		}
		if ev.Type() == spec.MRoomRedaction && ev.StateKey() == nil {
			// persistEvents validated this redaction before the event had a
			// before-state, which fails (and rolls the redaction back). Now that
			// the snapshot exists, validate it again.
			resolver := state.NewStateResolution(r.DB, info, r.Querier)
			_, redacted, rErr := r.DB.MaybeRedactEvent(ctx, info, ev.EventNID, ev.PDU, &resolver, r.Querier)
			if rErr != nil {
				logrus.WithError(rErr).WithField("event_id", ev.EventID()).Error("backfillViaFederation: failed to apply redaction after state assignment")
				markUnvalidated(ev.PDU)
			} else if redacted != nil {
				// The response still holds the unredacted target. Replace it, so
				// the sync copy written from the response is redacted too.
				redactedInBatch[redacted.EventID()] = redacted
			}
		}
	}

	// TODO: update backwards extremities, as that should be moved from syncapi to roomserver at some point.

	res.Events = make([]*types.HeaderedEvent, 0, len(events))
	for i := range events {
		if _, skip := unvalidatedRedactions[events[i].EventID()]; skip {
			continue
		}
		if redacted, ok := redactedInBatch[events[i].EventID()]; ok {
			events[i] = redacted
		}
		vis := historyVisibilities[events[i].EventID()]
		if vis == "" {
			// The state before this event was not available. Do not fall back to
			// the anchor's visibility, which may be less restrictive.
			vis = gomatrixserverlib.HistoryVisibilityJoined
		}
		res.Events = append(res.Events, &types.HeaderedEvent{PDU: events[i], Visibility: vis})
	}
	inResponse := make(map[string]struct{}, len(events))
	for _, ev := range events {
		inResponse[ev.EventID()] = struct{}{}
	}
	for _, ev := range sideEffectEvents {
		if _, ok := inResponse[ev.EventID()]; ok {
			continue
		}
		inResponse[ev.EventID()] = struct{}{}
		// A side-effect event can be the target of a redaction in this batch
		// too; return the redacted copy so the sync write stores it that way.
		if redacted, ok := redactedInBatch[ev.EventID()]; ok {
			ev = redacted
		}
		res.Events = append(res.Events, &types.HeaderedEvent{PDU: ev, Visibility: gomatrixserverlib.HistoryVisibilityJoined})
	}
	if len(redactedInBatch) > 0 {
		res.RedactedEvents = make(map[string]*types.HeaderedEvent, len(redactedInBatch))
		for eventID, redacted := range redactedInBatch {
			res.RedactedEvents[eventID] = &types.HeaderedEvent{PDU: redacted}
		}
	}
	res.HistoryVisibility = requester.historyVisiblity
	return nil
}

// historyVisibilityBeforeEvent returns the history visibility in force in the
// state before an event. The state at a backfill anchor can differ from the
// state an older event was sent in, so each backfilled event needs its own
// value for the history visibility filter.
func historyVisibilityBeforeEvent(
	ctx context.Context, db storage.RoomDatabase, roomInfo *types.RoomInfo, entries []types.StateEntry,
) gomatrixserverlib.HistoryVisibility {
	var eventNIDs []types.EventNID
	for _, entry := range entries {
		if entry.EventTypeNID == types.MRoomHistoryVisibilityNID && entry.EventStateKeyNID == types.EmptyStateKeyNID {
			eventNIDs = append(eventNIDs, entry.EventNID)
			break
		}
	}
	if len(eventNIDs) == 0 {
		// No history_visibility event in the state: the default is shared.
		return gomatrixserverlib.HistoryVisibilityShared
	}
	stateEvents, err := db.Events(ctx, roomInfo.RoomVersion, eventNIDs)
	if err != nil || len(stateEvents) == 0 {
		// Do not fall back to the anchor's visibility, which may be less
		// restrictive: keep the history restricted instead of exposing it.
		logrus.WithError(err).
			Warn("backfill: failed to get the history_visibility state before an event, defaulting to joined")
		return gomatrixserverlib.HistoryVisibilityJoined
	}
	visEvents := make([]gomatrixserverlib.PDU, len(stateEvents))
	for i := range stateEvents {
		visEvents[i] = stateEvents[i].PDU
	}
	return auth.HistoryVisibilityForRoom(visEvents)
}

// backfillFailuresRetryable reports whether a failed backfill might succeed
// later. Only a 404 definitively means that a server does not have the
// requested events. Other responses and errors may be transient or indicate
// that another server can provide the events.
func backfillFailuresRetryable(errs []error) bool {
	if len(errs) == 0 {
		// No server error was recorded, so the failure came from event loading
		// or another local step. Treat it as transient rather than ending
		// history. A request with no servers to ask produces no error at all,
		// so it never reaches this helper.
		return true
	}
	for _, err := range errs {
		var httpErr spec.HTTPError
		if errors.As(err, &httpErr) && httpErr.Code == http.StatusNotFound {
			continue
		}
		return true
	}
	return false
}

func prioritiseBackfillFromEventIDs(
	ctx context.Context,
	roomID string,
	fromEventIDs []string,
	serversAtEvent func(context.Context, string, string) []spec.ServerName,
) []string {
	if len(fromEventIDs) < minBackfillFromEvents {
		return fromEventIDs
	}

	bestIndex := 0
	bestServerCount := len(serversAtEvent(ctx, roomID, fromEventIDs[0]))
	for i := 1; i < len(fromEventIDs); i++ {
		serverCount := len(serversAtEvent(ctx, roomID, fromEventIDs[i]))
		if serverCount > bestServerCount {
			bestIndex = i
			bestServerCount = serverCount
		}
	}
	if bestIndex == 0 {
		return fromEventIDs
	}

	reordered := make([]string, 0, len(fromEventIDs))
	reordered = append(reordered, fromEventIDs[bestIndex])
	reordered = append(reordered, fromEventIDs[:bestIndex]...)
	reordered = append(reordered, fromEventIDs[bestIndex+1:]...)
	return reordered
}

// fetchAndStoreMissingEvents does a best-effort fetch and store of missing events specified in stateIDs. Returns no error as it is just
// best effort. It returns the events it stored, so the caller can hand them to the
// sync API: stored only in the roomserver, they would stay invisible to /messages
// while still being cited as missing parents by backward extremities.
func (r *Backfiller) fetchAndStoreMissingEvents(ctx context.Context, roomVer gomatrixserverlib.RoomVersion,
	backfillRequester *backfillRequester, stateIDs []string, virtualHost spec.ServerName,
) []gomatrixserverlib.PDU {
	servers := backfillRequester.servers

	// work out which are missing
	nidMap, err := r.DB.EventNIDs(ctx, stateIDs)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Warn("cannot query missing events")
		return nil
	}
	missingMap := make(map[string]*types.HeaderedEvent) // id -> event
	for _, id := range stateIDs {
		if _, ok := nidMap[id]; !ok {
			missingMap[id] = nil
		}
	}
	util.GetLogger(ctx).Infof("Fetching %d missing state events (from %d possible servers)", len(missingMap), len(servers))

	// fetch the events from federation. Loop the servers first so if we find one that works we stick with them
	for _, srv := range servers {
		for id, ev := range missingMap {
			if ev != nil {
				continue // already found
			}
			logger := util.GetLogger(ctx).WithField("server", srv).WithField("event_id", id)
			res, err := r.FSAPI.GetEvent(ctx, virtualHost, srv, id)
			if err != nil {
				logger.WithError(err).Warn("failed to get event from server")
				continue
			}
			loader := gomatrixserverlib.NewEventsLoader(roomVer, r.KeyRing, backfillRequester,
				func(roomVer gomatrixserverlib.RoomVersion, eventIDs []string) ([]gomatrixserverlib.PDU, error) {
					return backfillRequester.ProvideEventsContext(ctx, roomVer, eventIDs)
				}, false)
			result, err := loader.LoadAndVerify(ctx, res.PDUs, gomatrixserverlib.TopologicalOrderByPrevEvents, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.Querier.QueryUserIDForSender(ctx, roomID, senderID)
			})
			if err != nil {
				logger.WithError(err).Warn("failed to load and verify event")
				continue
			}
			logger.Infof("returned %d PDUs which made events %+v", len(res.PDUs), result)
			for _, res := range result {
				var signatureErr gomatrixserverlib.SignatureErr
				var authChainErr gomatrixserverlib.AuthChainErr
				var authRulesErr gomatrixserverlib.AuthRulesErr
				switch {
				case res.Error == nil:
				case errors.As(res.Error, &signatureErr):
					// The signature of the event might not be valid anymore, for example if
					// the key ID was reused with a different signature.
					logger.WithError(res.Error).Errorf("event failed PDU checks, storing anyway")
				case errors.As(res.Error, &authChainErr), errors.As(res.Error, &authRulesErr):
					logger.WithError(res.Error).Warn("event failed PDU checks")
					continue
				default:
					logger.WithError(res.Error).Warn("event failed PDU checks")
					continue
				}
				missingMap[id] = &types.HeaderedEvent{PDU: res.Event}
			}
		}
	}

	var newEvents []gomatrixserverlib.PDU
	for _, ev := range missingMap {
		if ev != nil {
			newEvents = append(newEvents, ev.PDU)
		}
	}
	util.GetLogger(ctx).Infof("Persisting %d new events", len(newEvents))
	_, stored := persistEvents(ctx, r.DB, r.Querier, newEvents)
	out := make([]gomatrixserverlib.PDU, 0, len(stored))
	for _, ev := range stored {
		out = append(out, ev.PDU)
	}
	return out
}

// backfillRequester implements gomatrixserverlib.BackfillRequester.
type backfillRequester struct {
	db                storage.Database
	fsAPI             federationAPI.RoomserverFederationAPI
	querier           api.QuerySenderIDAPI
	virtualHost       spec.ServerName
	isLocalServerName func(spec.ServerName) bool
	preferServer      map[spec.ServerName]bool
	bwExtrems         map[string][]string

	// per-request state
	servers                 []spec.ServerName
	backfillErrors          []error
	eventIDToBeforeStateIDs map[string][]string
	eventIDMap              map[string]gomatrixserverlib.PDU
	historyVisiblity        gomatrixserverlib.HistoryVisibility
	roomVersion             gomatrixserverlib.RoomVersion

	// roomNID scopes the partial-state check behind local state reuse. Zero
	// disables the check, which is only the case for requesters a test builds
	// directly.
	roomNID             types.RoomNID
	partialStateChecked bool
	partialState        bool
}

func newBackfillRequester(
	db storage.Database, fsAPI federationAPI.RoomserverFederationAPI,
	querier api.QuerySenderIDAPI,
	virtualHost spec.ServerName,
	isLocalServerName func(spec.ServerName) bool,
	bwExtrems map[string][]string, preferServers []spec.ServerName,
	roomVersion gomatrixserverlib.RoomVersion,
) *backfillRequester {
	preferServer := make(map[spec.ServerName]bool)
	for _, p := range preferServers {
		preferServer[p] = true
	}
	return &backfillRequester{
		db:                      db,
		fsAPI:                   fsAPI,
		querier:                 querier,
		virtualHost:             virtualHost,
		isLocalServerName:       isLocalServerName,
		eventIDToBeforeStateIDs: make(map[string][]string),
		eventIDMap:              make(map[string]gomatrixserverlib.PDU),
		bwExtrems:               bwExtrems,
		preferServer:            preferServer,
		historyVisiblity:        gomatrixserverlib.HistoryVisibilityShared,
		roomVersion:             roomVersion,
	}
}

func (b *backfillRequester) StateIDsBeforeEvent(ctx context.Context, targetEvent gomatrixserverlib.PDU) ([]string, error) {
	b.eventIDMap[targetEvent.EventID()] = targetEvent
	if ids, ok := b.eventIDToBeforeStateIDs[targetEvent.EventID()]; ok {
		return ids, nil
	}
	// Backfill requests are paginated, and each page gets a new requester. If
	// this event was resolved and stored by an earlier page, its before-state
	// snapshot is already authoritative locally; do not ask federation for it
	// again.
	if ids, ok := b.stateIDsFromStoredEvent(ctx, targetEvent.EventID()); ok {
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = ids
		return ids, nil
	}
	if len(targetEvent.PrevEventIDs()) == 0 && targetEvent.Type() == "m.room.create" && targetEvent.StateKeyEquals("") {
		util.GetLogger(ctx).WithField("room_id", targetEvent.RoomID().String()).Info("Backfilled to the beginning of the room")
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = []string{}
		return nil, nil
	}
	// if we have exactly 1 prev event and we know the state of the room at that prev event, then just roll forward the prev event.
	// Else, we have to hit /state_ids because either we don't know the state at all at this event (new backwards extremity) or
	// we don't know the result of state res to merge forks (2 or more prev_events)
	//
	// reason records why federation is needed, for the log: it shows which of
	// these cases dominate, so that the right ones are computed locally.
	var reason string
	switch len(targetEvent.PrevEventIDs()) {
	case 0:
		reason = "no-prev-events"
	case 1:
		prevEventID := targetEvent.PrevEventIDs()[0]
		// The parent may come from an earlier page or an earlier request rather
		// than this batch. Both its event and its before-state are stored then,
		// and are as trustworthy as the stored state reused for the event itself.
		prevEvent, ok := b.eventIDMap[prevEventID]
		if !ok {
			b.hydrateStateEvents(ctx, []string{prevEventID})
			prevEvent, ok = b.eventIDMap[prevEventID]
		}
		if !ok {
			reason = "unknown-parent"
			goto FederationHit
		}
		prevEventStateIDs, ok := b.eventIDToBeforeStateIDs[prevEventID]
		if !ok {
			if prevEventStateIDs, ok = b.stateIDsFromStoredEvent(ctx, prevEventID); ok {
				b.eventIDToBeforeStateIDs[prevEventID] = prevEventStateIDs
			}
		}
		if !ok && prevEvent.Type() == spec.MRoomCreate && len(prevEvent.PrevEventIDs()) == 0 {
			// The create event is the root of the DAG: the state before it is
			// empty by definition, and it has no stored before-state to read.
			prevEventStateIDs, ok = []string{}, true
		}
		if !ok {
			if b.localStateUsable(ctx) {
				reason = "parent-has-no-state"
			} else {
				reason = "local-state-unusable"
			}
			goto FederationHit
		}
		if prevEvent.StateKey() != nil {
			// Rolling a state event forward has to find the entry it replaces,
			// which needs the type and state key of every ID in the before-state.
			// Most of those events are older than this batch but already stored,
			// so read them locally instead of asking a remote server.
			b.hydrateStateEvents(ctx, prevEventStateIDs)
		}
		newStateIDs := b.calculateNewStateIDs(targetEvent, prevEvent, prevEventStateIDs)
		if newStateIDs != nil {
			b.eventIDToBeforeStateIDs[targetEvent.EventID()] = newStateIDs
			return newStateIDs, nil
		}
		// else we failed to calculate the new state, so fallthrough
		reason = "state-event-missing-locally"
	default:
		reason = "fork"
	}

FederationHit:
	var lastErr error
	logrus.WithFields(logrus.Fields{
		"event_id": targetEvent.EventID(),
		"reason":   reason,
		"prevs":    len(targetEvent.PrevEventIDs()),
	}).Info("Requesting /state_ids at event")
	for _, srv := range b.servers { // hit any valid server
		c := gomatrixserverlib.FederatedStateProvider{
			FedClient:          b.fsAPI,
			RememberAuthEvents: false,
			Server:             srv,
			Origin:             b.virtualHost,
		}
		res, err := c.StateIDsBeforeEvent(ctx, targetEvent)
		if err != nil {
			lastErr = err
			b.backfillErrors = append(b.backfillErrors, err)
			logrus.WithError(err).WithFields(logrus.Fields{
				"event_id": targetEvent.EventID(),
				"server":   srv,
			}).Warn("/state_ids failed during backfill")
			continue
		}
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = res
		return res, nil
	}
	return nil, lastErr
}

// stateIDsFromStoredEvent returns an event's persisted before-state. A false
// result means the event has no usable local snapshot and federation may be
// needed. State snapshots are persisted as HAMTs, with state blocks retained
// as a fallback for databases that predate HAMT persistence.
func (b *backfillRequester) stateIDsFromStoredEvent(ctx context.Context, eventID string) ([]string, bool) {
	if !b.localStateUsable(ctx) {
		return nil, false
	}
	states, err := b.db.StateAtEventIDs(ctx, []string{eventID})
	if err != nil || len(states) != 1 || states[0].IsRejected || states[0].BeforeStateSnapshotNID == 0 {
		return nil, false
	}

	// LoadStateAtSnapshot reads the HAMT, falls back to the legacy state
	// blocks, and keeps only the newest entry for each state key.
	resolver := state.NewStateResolution(b.db, nil, b.querier)
	entries, err := resolver.LoadStateAtSnapshot(ctx, states[0].BeforeStateSnapshotNID)
	if err != nil {
		return nil, false
	}

	eventNIDs := make([]types.EventNID, 0, len(entries))
	for _, entry := range entries {
		eventNIDs = append(eventNIDs, entry.EventNID)
	}
	eventIDs, err := b.db.EventIDs(ctx, eventNIDs)
	if err != nil {
		return nil, false
	}
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		id, ok := eventIDs[entry.EventNID]
		if !ok {
			return nil, false
		}
		ids = append(ids, id)
	}
	return ids, true
}

// localStateUsable reports whether a stored before-state snapshot may be
// reused for this room. A snapshot taken while the room had partial state
// (MSC3706) can be incomplete, so the durable marker keeps backfill from using
// it after the partial flag is cleared, including when a join is abandoned.
// The check is per requester because a requester is always scoped to one room.
func (b *backfillRequester) localStateUsable(ctx context.Context) bool {
	if b.partialStateChecked {
		return !b.partialState
	}
	b.partialStateChecked = true
	if b.roomNID == 0 {
		// Only requesters built directly by tests leave this unset.
		return true
	}
	partial, err := b.db.IsRoomPartialState(ctx, b.roomNID)
	if err != nil {
		// Being unable to prove the snapshot complete is enough to distrust
		// it; federation is the safe answer.
		b.partialState = true
		return false
	}
	if partial {
		b.partialState = true
		return false
	}
	incomplete, err := b.db.StateSnapshotsIncomplete(ctx, b.roomNID)
	if err != nil {
		b.partialState = true
		return false
	}
	b.partialState = incomplete
	return !b.partialState
}

// hydrateStateEvents loads the events for ids that are not already known to
// this requester from the local database, so state can be rolled forward without
// a /state_ids request. IDs that are not stored are left out and the caller
// falls back to federation for them. Failures are not fatal for the same reason.
func (b *backfillRequester) hydrateStateEvents(ctx context.Context, ids []string) {
	var missing []string
	for _, id := range ids {
		if _, ok := b.eventIDMap[id]; !ok {
			missing = append(missing, id)
		}
	}
	if len(missing) == 0 {
		return
	}
	events, err := b.ProvideEventsContext(ctx, b.roomVersion, missing)
	if err != nil {
		return
	}
	for _, ev := range events {
		b.eventIDMap[ev.EventID()] = ev
	}
}

func (b *backfillRequester) calculateNewStateIDs(targetEvent, prevEvent gomatrixserverlib.PDU, prevEventStateIDs []string) []string {
	// Copy: the slice is stored as the previous event's own before-state and must
	// not be changed in place. make keeps an empty state non-nil.
	newStateIDs := make([]string, 0, len(prevEventStateIDs)+1)
	newStateIDs = append(newStateIDs, prevEventStateIDs...)
	if prevEvent.StateKey() == nil {
		// state is the same as the previous event
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = newStateIDs
		return newStateIDs
	}

	missingState := false // true if we are missing the info for a state event ID
	foundEvent := false   // true if we found a (type, state_key) match
	// find which state ID to replace, if any
	for i, id := range newStateIDs {
		ev, ok := b.eventIDMap[id]
		if !ok {
			missingState = true
			continue
		}
		// The state IDs BEFORE the target event are the state IDs BEFORE the prev_event PLUS the prev_event itself
		if ev.Type() == prevEvent.Type() && ev.StateKeyEquals(*prevEvent.StateKey()) {
			newStateIDs[i] = prevEvent.EventID()
			foundEvent = true
			break
		}
	}
	if !foundEvent && !missingState {
		// we can be certain that this is new state
		newStateIDs = append(newStateIDs, prevEvent.EventID())
		foundEvent = true
	}

	if foundEvent {
		b.eventIDToBeforeStateIDs[targetEvent.EventID()] = newStateIDs
		return newStateIDs
	}
	return nil
}

func (b *backfillRequester) StateBeforeEvent(ctx context.Context, roomVer gomatrixserverlib.RoomVersion,
	event gomatrixserverlib.PDU, eventIDs []string,
) (map[string]gomatrixserverlib.PDU, error) {
	// try to fetch the events from the database first
	events, err := b.ProvideEventsContext(ctx, roomVer, eventIDs)
	if err != nil {
		// non-fatal, fallthrough
		logrus.WithError(err).Info("Failed to fetch events")
	} else {
		logrus.Infof("Fetched %d/%d events from the database", len(events), len(eventIDs))
		if len(events) == len(eventIDs) {
			result := make(map[string]gomatrixserverlib.PDU)
			for i := range events {
				result[events[i].EventID()] = events[i]
				b.eventIDMap[events[i].EventID()] = events[i]
			}
			return result, nil
		}
	}

	var lastErr error
	for _, srv := range b.servers {
		c := gomatrixserverlib.FederatedStateProvider{
			FedClient:          b.fsAPI,
			RememberAuthEvents: false,
			Server:             srv,
			Origin:             b.virtualHost,
		}
		result, err := c.StateBeforeEvent(ctx, roomVer, event, eventIDs)
		if err != nil {
			lastErr = err
			b.backfillErrors = append(b.backfillErrors, err)
			continue
		}
		for eventID, ev := range result {
			b.eventIDMap[eventID] = ev
		}
		return result, nil
	}
	return nil, lastErr
}

// ServersAtEvent is called when trying to determine which server to request from.
// It returns a list of servers which can be queried for backfill requests. These servers
// will be servers that are in the room already. The entries at the beginning are preferred servers
// and will be tried first. An empty list will fail the request.
func (b *backfillRequester) ServersAtEvent(ctx context.Context, roomID, eventID string) []spec.ServerName {
	// eventID will be a prev_event ID of a backwards extremity, meaning we will not have a database entry for it. Instead, use
	// its successor, so look it up.
	successor := ""
FindSuccessor:
	for sucID, prevEventIDs := range b.bwExtrems {
		for _, pe := range prevEventIDs {
			if pe == eventID {
				successor = sucID
				break FindSuccessor
			}
		}
	}
	if successor == "" {
		logrus.WithField("event_id", eventID).Error("ServersAtEvent: failed to find successor of this event to determine room state")
		return nil
	}
	eventID = successor

	// getMembershipsBeforeEventNID requires a NID, so retrieving the NID for
	// the event is necessary.
	NIDs, err := b.db.EventNIDs(ctx, []string{eventID})
	if err != nil {
		logrus.WithField("event_id", eventID).WithError(err).Error("ServersAtEvent: failed to get event NID for event")
		return nil
	}

	info, err := b.db.RoomInfo(ctx, roomID)
	if err != nil {
		logrus.WithError(err).WithField("room_id", roomID).Error("ServersAtEvent: failed to get RoomInfo for room")
		return nil
	}
	if info == nil || info.IsStub() {
		logrus.WithField("room_id", roomID).Error("ServersAtEvent: failed to get RoomInfo for room, room is missing")
		return nil
	}

	stateEntries, err := helpers.StateBeforeEvent(ctx, b.db, info, NIDs[eventID].EventNID, b.querier)
	if err != nil {
		logrus.WithField("event_id", eventID).WithError(err).Error("ServersAtEvent: failed to load state before event")
		return nil
	}

	// possibly return all joined servers depending on history visiblity
	memberEventsFromVis, visibility, err := joinEventsFromHistoryVisibility(ctx, b.db, b.querier, info, stateEntries, b.virtualHost)
	b.historyVisiblity = visibility
	if err != nil {
		logrus.WithError(err).Error("ServersAtEvent: failed calculate servers from history visibility rules")
		return nil
	}
	logrus.Infof("ServersAtEvent including %d current events from history visibility", len(memberEventsFromVis))

	// Retrieve all "m.room.member" state events of "join" membership, which
	// contains the list of users in the room before the event, therefore all
	// the servers in it at that moment.
	memberEvents, err := helpers.GetMembershipsAtState(ctx, b.db, info, stateEntries, true)
	if err != nil {
		logrus.WithField("event_id", eventID).WithError(err).Error("ServersAtEvent: failed to get memberships before event")
		return nil
	}
	memberEvents = append(memberEvents, memberEventsFromVis...)

	// Store the server names in a temporary map to avoid duplicates.
	serverSet := make(map[spec.ServerName]bool)
	for _, event := range memberEvents {
		if sender, err := b.querier.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID()); err == nil {
			serverSet[sender.Domain()] = true
		}
	}
	var servers []spec.ServerName
	for server := range serverSet {
		if b.isLocalServerName(server) {
			continue
		}
		if b.preferServer[server] { // insert at the front
			servers = append([]spec.ServerName{server}, servers...)
		} else { // insert at the back
			servers = append(servers, server)
		}
	}
	if len(servers) > maxBackfillServers {
		servers = servers[:maxBackfillServers]
	}

	b.servers = servers
	return servers
}

// Backfill performs a backfill request to the given server.
// https://matrix.org/docs/spec/server_server/latest#get-matrix-federation-v1-backfill-roomid
func (b *backfillRequester) Backfill(ctx context.Context, origin, server spec.ServerName, roomID string,
	limit int, fromEventIDs []string,
) (gomatrixserverlib.Transaction, error) {
	tx, err := b.fsAPI.Backfill(ctx, origin, server, roomID, limit, fromEventIDs)
	if err != nil {
		b.backfillErrors = append(b.backfillErrors, err)
	}
	return tx, err
}

func (b *backfillRequester) ProvideEvents(roomVer gomatrixserverlib.RoomVersion, eventIDs []string) ([]gomatrixserverlib.PDU, error) {
	return b.ProvideEventsContext(context.Background(), roomVer, eventIDs)
}

func (b *backfillRequester) ProvideEventsContext(ctx context.Context, roomVer gomatrixserverlib.RoomVersion, eventIDs []string) ([]gomatrixserverlib.PDU, error) {
	nidMap, err := b.db.EventNIDs(ctx, eventIDs)
	if err != nil {
		logrus.WithError(err).WithField("event_ids", eventIDs).Error("Failed to find events")
		return nil, err
	}
	eventNIDs := make([]types.EventNID, len(nidMap))
	i := 0
	for _, nid := range nidMap {
		eventNIDs[i] = nid.EventNID
		i++
	}
	eventsWithNids, err := b.db.Events(ctx, b.roomVersion, eventNIDs)
	if err != nil {
		logrus.WithError(err).WithField("event_nids", eventNIDs).Error("Failed to load events")
		return nil, err
	}
	events := make([]gomatrixserverlib.PDU, len(eventsWithNids))
	for i := range eventsWithNids {
		events[i] = eventsWithNids[i].PDU
	}
	return events, nil
}

// joinEventsFromHistoryVisibility returns all CURRENTLY joined members if our server can read the room history
//
// TODO: Long term we probably want a history_visibility table which stores eventNID | visibility_enum so we can just
// pull all events and then filter by that table.
func joinEventsFromHistoryVisibility(
	ctx context.Context, db storage.RoomDatabase, querier api.QuerySenderIDAPI, roomInfo *types.RoomInfo, stateEntries []types.StateEntry,
	thisServer spec.ServerName,
) ([]types.Event, gomatrixserverlib.HistoryVisibility, error) {
	var eventNIDs []types.EventNID
	for _, entry := range stateEntries {
		// Filter the events to retrieve to only keep the membership events
		if entry.EventTypeNID == types.MRoomHistoryVisibilityNID && entry.EventStateKeyNID == types.EmptyStateKeyNID {
			eventNIDs = append(eventNIDs, entry.EventNID)
			break
		}
	}

	// Get all of the events in this state
	if roomInfo == nil {
		return nil, gomatrixserverlib.HistoryVisibilityJoined, types.ErrorInvalidRoomInfo
	}
	stateEvents, err := db.Events(ctx, roomInfo.RoomVersion, eventNIDs)
	if err != nil {
		// even though the default should be shared, restricting the visibility to joined
		// feels more secure here.
		return nil, gomatrixserverlib.HistoryVisibilityJoined, err
	}
	events := make([]gomatrixserverlib.PDU, len(stateEvents))
	for i := range stateEvents {
		events[i] = stateEvents[i].PDU
	}

	// Can we see events in the room?
	canSeeEvents := auth.IsServerAllowed(ctx, querier, thisServer, true, events)
	visibility := auth.HistoryVisibilityForRoom(events)
	if !canSeeEvents {
		logrus.Infof("ServersAtEvent history not visible to us: %s", visibility)
		return nil, visibility, nil
	}
	// get joined members
	joinEventNIDs, err := db.GetMembershipEventNIDsForRoom(ctx, roomInfo.RoomNID, true, false)
	if err != nil {
		return nil, visibility, err
	}
	evs, err := db.Events(ctx, roomInfo.RoomVersion, joinEventNIDs)
	return evs, visibility, err
}

func persistEvents(ctx context.Context, db storage.Database, querier api.QuerySenderIDAPI, events []gomatrixserverlib.PDU) (types.RoomNID, []types.Event) {
	var roomNID types.RoomNID
	var eventNID types.EventNID
	storedEvents := make([]types.Event, 0, len(events))
	for j, ev := range events {
		nidMap, err := db.EventNIDs(ctx, ev.AuthEventIDs())
		if err != nil { // this shouldn't happen as RequestBackfill already found them
			logrus.WithError(err).WithField("auth_events", ev.AuthEventIDs()).Error("Failed to find one or more auth events")
			continue
		}
		authNids := make([]types.EventNID, len(nidMap))
		i := 0
		for _, nid := range nidMap {
			authNids[i] = nid.EventNID
			i++
		}

		roomInfo, err := db.GetOrCreateRoomInfo(ctx, ev)
		if err != nil {
			logrus.WithError(err).Error("failed to get or create roomNID")
			continue
		}
		roomNID = roomInfo.RoomNID

		eventTypeNID, err := db.GetOrCreateEventTypeNID(ctx, ev.Type())
		if err != nil {
			logrus.WithError(err).Error("failed to get or create eventType NID")
			continue
		}

		eventStateKeyNID, err := db.GetOrCreateEventStateKeyNID(ctx, ev.StateKey())
		if err != nil {
			logrus.WithError(err).Error("failed to get or create eventStateKey NID")
			continue
		}

		eventNID, _, err = db.StoreEvent(ctx, ev, roomInfo, eventTypeNID, eventStateKeyNID, authNids, false)
		if err != nil {
			logrus.WithError(err).WithField("event_id", ev.EventID()).Error("Failed to persist event")
			continue
		}

		resolver := state.NewStateResolution(db, roomInfo, querier)

		_, redactedEvent, err := db.MaybeRedactEvent(ctx, roomInfo, eventNID, ev, &resolver, querier)
		if err != nil {
			// The event is already stored. Skipping it here would leave it
			// without a before-state snapshot (set by the caller for stored
			// events only), so every later backfill would refetch it. Keep it.
			logrus.WithError(err).WithField("event_id", ev.EventID()).Error("Failed to redact event")
			redactedEvent = nil
		}
		// If storing this event results in it being redacted, then do so.
		// It's also possible for this event to be a redaction which results in another event being
		// redacted, which we don't care about since we aren't returning it in this backfill.
		if redactedEvent != nil {
			// A redaction can target an event earlier in this batch as well as
			// the event currently being stored. Keep both response collections
			// in sync with the persisted redacted copy.
			for i := range events {
				if events[i].EventID() == redactedEvent.EventID() {
					events[i] = redactedEvent
				}
			}
			for i := range storedEvents {
				if storedEvents[i].EventID() == redactedEvent.EventID() {
					storedEvents[i].PDU = redactedEvent
				}
			}
			if redactedEvent.EventID() == ev.EventID() {
				ev = redactedEvent
				events[j] = ev
			}
		}
		storedEvents = append(storedEvents, types.Event{
			EventNID: eventNID,
			PDU:      ev,
		})
	}
	return roomNID, storedEvents
}
