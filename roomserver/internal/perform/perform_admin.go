// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package perform

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/element-hq/dendrite/internal/eventutil"
	"github.com/element-hq/dendrite/roomserver/api"
	"github.com/element-hq/dendrite/roomserver/internal/input"
	"github.com/element-hq/dendrite/roomserver/internal/query"
	"github.com/element-hq/dendrite/roomserver/storage"
	"github.com/element-hq/dendrite/roomserver/types"
	"github.com/element-hq/dendrite/setup/config"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
)

type Admin struct {
	DB      storage.Database
	Cfg     *config.RoomServer
	Queryer *query.Queryer
	Inputer *input.Inputer
	Leaver  *Leaver
}

// PerformAdminEvacuateRoom will remove all local users from the given room.
func (r *Admin) PerformAdminEvacuateRoom(
	ctx context.Context,
	roomID string,
) (affected []string, err error) {
	roomInfo, err := r.DB.RoomInfo(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if roomInfo == nil || roomInfo.IsStub() {
		return nil, eventutil.ErrRoomNoExists{}
	}

	memberNIDs, err := r.DB.GetMembershipEventNIDsForRoom(ctx, roomInfo.RoomNID, true, true)
	if err != nil {
		return nil, err
	}

	memberEvents, err := r.DB.Events(ctx, roomInfo.RoomVersion, memberNIDs)
	if err != nil {
		return nil, err
	}

	inputEvents := make([]api.InputRoomEvent, 0, len(memberEvents))
	affected = make([]string, 0, len(memberEvents))
	latestReq := &api.QueryLatestEventsAndStateRequest{
		RoomID: roomID,
	}
	latestRes := &api.QueryLatestEventsAndStateResponse{}
	if err = r.Queryer.QueryLatestEventsAndState(ctx, latestReq, latestRes); err != nil {
		return nil, err
	}
	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return nil, err
	}

	prevEvents := latestRes.LatestEvents
	var senderDomain spec.ServerName
	var eventsNeeded gomatrixserverlib.StateNeeded
	var identity *fclient.SigningIdentity
	var event *types.HeaderedEvent
	for _, memberEvent := range memberEvents {
		if memberEvent.StateKey() == nil {
			continue
		}

		var memberContent gomatrixserverlib.MemberContent
		if err = json.Unmarshal(memberEvent.Content(), &memberContent); err != nil {
			return nil, err
		}
		memberContent.Membership = spec.Leave

		stateKey := *memberEvent.StateKey()
		fledglingEvent := &gomatrixserverlib.ProtoEvent{
			RoomID:     roomID,
			Type:       spec.MRoomMember,
			StateKey:   &stateKey,
			SenderID:   stateKey,
			PrevEvents: prevEvents,
		}

		userID, err := r.Queryer.QueryUserIDForSender(ctx, *validRoomID, spec.SenderID(fledglingEvent.SenderID))
		if err != nil || userID == nil {
			continue
		}
		senderDomain = userID.Domain()

		if fledglingEvent.Content, err = json.Marshal(memberContent); err != nil {
			return nil, err
		}

		eventsNeeded, err = gomatrixserverlib.StateNeededForProtoEvent(fledglingEvent)
		if err != nil {
			return nil, err
		}

		identity, err = r.Cfg.Matrix.SigningIdentityFor(senderDomain)
		if err != nil {
			continue
		}

		event, err = eventutil.BuildEvent(ctx, fledglingEvent, identity, time.Now(), &eventsNeeded, latestRes)
		if err != nil {
			return nil, err
		}

		inputEvents = append(inputEvents, api.InputRoomEvent{
			Kind:         api.KindNew,
			Event:        event,
			Origin:       senderDomain,
			SendAsServer: string(senderDomain),
		})
		affected = append(affected, stateKey)
		prevEvents = []string{event.EventID()}
	}

	inputReq := &api.InputRoomEventsRequest{
		InputRoomEvents: inputEvents,
		Asynchronous:    false,
	}
	inputRes := &api.InputRoomEventsResponse{}
	r.Inputer.InputRoomEvents(ctx, inputReq, inputRes)
	return affected, nil
}

// PerformAdminEvacuateUser will remove the given user from all rooms.
func (r *Admin) PerformAdminEvacuateUser(
	ctx context.Context,
	userID string,
) (affected []string, err error) {
	fullUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return nil, err
	}
	if !r.Cfg.Matrix.IsLocalServerName(fullUserID.Domain()) {
		return nil, fmt.Errorf("can only evacuate local users using this endpoint")
	}

	roomIDs, err := r.DB.GetRoomsByMembership(ctx, *fullUserID, spec.Join)
	if err != nil {
		return nil, err
	}

	inviteRoomIDs, err := r.DB.GetRoomsByMembership(ctx, *fullUserID, spec.Invite)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	allRooms := append(roomIDs, inviteRoomIDs...)
	affected = make([]string, 0, len(allRooms))
	for _, roomID := range allRooms {
		leaveReq := &api.PerformLeaveRequest{
			RoomID: roomID,
			Leaver: *fullUserID,
		}
		leaveRes := &api.PerformLeaveResponse{}
		outputEvents, err := r.Leaver.PerformLeave(ctx, leaveReq, leaveRes)
		if err != nil {
			return nil, err
		}
		affected = append(affected, roomID)
		if len(outputEvents) == 0 {
			continue
		}
		if err := r.Inputer.OutputProducer.ProduceRoomEvents(roomID, outputEvents); err != nil {
			return nil, err
		}
	}
	return affected, nil
}

// PerformAdminPurgeRoom removes all traces for the given room from the database.
func (r *Admin) PerformAdminPurgeRoom(
	ctx context.Context,
	roomID string,
) error {
	// Validate we actually got a room ID and nothing else
	if _, err := spec.NewRoomID(roomID); err != nil {
		return err
	}

	logrus.WithField("room_id", roomID).Warn("Purging room from roomserver")
	if err := r.DB.PurgeRoom(ctx, roomID); err != nil {
		logrus.WithField("room_id", roomID).WithError(err).Warn("Failed to purge room from roomserver")
		return err
	}

	logrus.WithField("room_id", roomID).Warn("Room purged from roomserver, informing other components")

	return r.Inputer.OutputProducer.ProduceRoomEvents(roomID, []api.OutputEvent{
		{
			Type: api.OutputTypePurgeRoom,
			PurgeRoom: &api.OutputPurgeRoom{
				RoomID: roomID,
			},
		},
	})
}

func (r *Admin) PerformAdminDownloadState(
	ctx context.Context,
	roomID, userID string, serverName spec.ServerName,
) error {
	fullUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return err
	}
	senderDomain := fullUserID.Domain()

	roomInfo, err := r.DB.RoomInfo(ctx, roomID)
	if err != nil {
		return err
	}

	if roomInfo == nil || roomInfo.IsStub() {
		return eventutil.ErrRoomNoExists{}
	}

	fwdExtremities, _, _, err := r.DB.LatestEventIDs(ctx, roomInfo.RoomNID)
	if err != nil {
		return err
	}

	// Fetch den's own current, locally-resolved state up front. We need it
	// for two things: (1) as the basis for building/authing the corrective
	// event itself, and (2) merged into the state we install, so that state
	// keys den already has right (most notably its own users' memberships)
	// aren't clobbered/dropped just because serverName's snapshot predates
	// them - see fetchStateToInstall for why that matters.
	var localState api.QueryLatestEventsAndStateResponse
	if err = r.Queryer.QueryLatestEventsAndState(ctx, &api.QueryLatestEventsAndStateRequest{
		RoomID: roomID,
	}, &localState); err != nil {
		return fmt.Errorf("r.Queryer.QueryLatestEventsAndState: %w", err)
	}

	authEvents, stateEvents, stateIDs, err := r.fetchStateToInstall(ctx, roomID, serverName, roomInfo, fwdExtremities, localState.StateEvents)
	if err != nil {
		return err
	}

	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return err
	}
	senderID, err := r.Queryer.QuerySenderIDForUser(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return err
	} else if senderID == nil {
		return fmt.Errorf("sender ID not found for %s in %s", *fullUserID, *validRoomID)
	}
	proto := &gomatrixserverlib.ProtoEvent{
		Type:     "org.matrix.dendrite.state_download",
		SenderID: string(*senderID),
		RoomID:   roomID,
		Content:  spec.RawJSON("{}"),
	}

	eventsNeeded, err := gomatrixserverlib.StateNeededForProtoEvent(proto)
	if err != nil {
		return fmt.Errorf("gomatrixserverlib.StateNeededForProtoEvent: %w", err)
	}

	// Build/auth this event against den's own current, locally-resolved
	// state (fetched above as localState) - NOT the state we just fetched
	// from serverName in fetchStateToInstall. That remote state can predate
	// the requester's own membership, which would make this event fail its
	// own auth check before it can apply anything.
	identity, err := r.Cfg.Matrix.SigningIdentityFor(senderDomain)
	if err != nil {
		return err
	}

	ev, err := eventutil.BuildEvent(ctx, proto, identity, time.Now(), &eventsNeeded, &localState)
	if err != nil {
		return fmt.Errorf("eventutil.BuildEvent: %w", err)
	}

	// Submit the outliers (auth chain + state events) as their own request
	// first, and check the result before touching the state snapshot. If
	// we folded these into the same InputRoomEvents call as the final
	// KindNew event below, a failure fetching/storing any individual
	// outlier would be masked: InputRoomEvents only reports back the last
	// error it saw across the whole batch, so a late success (e.g. the
	// final state_download event, which doesn't depend on every outlier
	// having stored cleanly) could silently swallow an earlier outlier
	// failure. That would let this "heal" operation report success while
	// quietly building the new state snapshot on top of missing data -
	// exactly the kind of silent partial failure that got a real room
	// stuck in the first place. Fail loudly instead.
	outlierReq := &api.InputRoomEventsRequest{Asynchronous: false}
	outlierRes := &api.InputRoomEventsResponse{}
	for _, authEvent := range append(authEvents, stateEvents...) {
		outlierReq.InputRoomEvents = append(outlierReq.InputRoomEvents, api.InputRoomEvent{
			Kind:  api.KindOutlier,
			Event: authEvent,
		})
	}
	r.Inputer.InputRoomEvents(ctx, outlierReq, outlierRes)
	if outlierRes.ErrMsg != "" {
		return fmt.Errorf("failed to store %d auth/state events needed to rebuild room state, aborting before touching the current snapshot: %w", len(outlierReq.InputRoomEvents), outlierRes.Err())
	}

	stateReq := &api.InputRoomEventsRequest{Asynchronous: false}
	stateRes := &api.InputRoomEventsResponse{}
	stateReq.InputRoomEvents = append(stateReq.InputRoomEvents, api.InputRoomEvent{
		Kind:          api.KindNew,
		Event:         ev,
		Origin:        r.Cfg.Matrix.ServerName,
		HasState:      true,
		StateEventIDs: stateIDs,
		SendAsServer:  string(r.Cfg.Matrix.ServerName),
	})
	r.Inputer.InputRoomEvents(ctx, stateReq, stateRes)
	if stateRes.ErrMsg != "" {
		return stateRes.Err()
	}

	return nil
}

// fetchStateToInstall fetches the room's state from serverName at each of
// the given forward extremities, verifies signatures, and merges it with
// den's own local state (localStateEvents), preferring *local* wherever
// local has an entry for a given (type, state_key) tuple and only falling
// back to the remote-fetched value for tuples local doesn't have at all.
//
// That priority matters because GET /state returns the room's state as of
// *before* the queried event. If a forward extremity is itself a recent
// membership change - e.g. the admin operation's own requester having only
// just joined - the remote server's response reflects the state *before*
// that join, i.e. it can still show the requester as merely invited, not
// joined. Preferring remote there would silently regress the requester's
// own membership and make the corrective event fail its own auth check
// before it can apply anything - confirmed in production: this exact
// scenario ("eventauth: sender ... not in room") survived an earlier
// version of this fix that only filled gaps missing from remote, because
// remote wasn't missing that tuple - it just had a stale (pre-join) value
// for it. Local is always at least as fresh as remote for anything den has
// already legitimately processed, so local should win on conflicts; remote
// is only needed to fill in tuples (like other users' memberships den never
// received) that local doesn't have at all.
func (r *Admin) fetchStateToInstall(
	ctx context.Context,
	roomID string,
	serverName spec.ServerName,
	roomInfo *types.RoomInfo,
	fwdExtremities []string,
	localStateEvents []*types.HeaderedEvent,
) (authEvents, stateEvents []*types.HeaderedEvent, stateIDs []string, err error) {
	// Keyed by "type\x00state_key". stateEventMap is seeded from local state
	// first so local always wins ties; remote-fetched events below only fill
	// in tuples this map doesn't already have a state_key entry for.
	stateTupleOf := make(map[string]string) // "type\x00state_key" -> event ID, for events currently in stateEventMap
	authEventMap := map[string]gomatrixserverlib.PDU{}
	stateEventMap := map[string]gomatrixserverlib.PDU{}

	for _, ev := range localStateEvents {
		stateEventMap[ev.EventID()] = ev.PDU
		if sk := ev.StateKey(); sk != nil {
			stateTupleOf[ev.Type()+"\x00"+*sk] = ev.EventID()
		}
	}

	for _, fwdExtremity := range fwdExtremities {
		var state gomatrixserverlib.StateResponse
		state, err = r.Inputer.FSAPI.LookupState(ctx, r.Inputer.ServerName, serverName, roomID, fwdExtremity, roomInfo.RoomVersion)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("r.Inputer.FSAPI.LookupState (%q): %s", fwdExtremity, err)
		}
		for _, authEvent := range state.GetAuthEvents().UntrustedEvents(roomInfo.RoomVersion) {
			if err = gomatrixserverlib.VerifyEventSignatures(ctx, authEvent, r.Inputer.KeyRing, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.Queryer.QueryUserIDForSender(ctx, roomID, senderID)
			}); err != nil {
				continue
			}
			authEventMap[authEvent.EventID()] = authEvent
		}
		for _, stateEvent := range state.GetStateEvents().UntrustedEvents(roomInfo.RoomVersion) {
			if err = gomatrixserverlib.VerifyEventSignatures(ctx, stateEvent, r.Inputer.KeyRing, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
				return r.Queryer.QueryUserIDForSender(ctx, roomID, senderID)
			}); err != nil {
				continue
			}
			sk := stateEvent.StateKey()
			if sk == nil {
				stateEventMap[stateEvent.EventID()] = stateEvent
				continue
			}
			tuple := stateEvent.Type() + "\x00" + *sk
			if _, haveLocal := stateTupleOf[tuple]; haveLocal {
				// Local already covers this slot - it wins, per the
				// function comment. Don't add the remote version at all.
				continue
			}
			stateEventMap[stateEvent.EventID()] = stateEvent
			stateTupleOf[tuple] = stateEvent.EventID()
		}
	}

	authEvents = make([]*types.HeaderedEvent, 0, len(authEventMap))
	stateEvents = make([]*types.HeaderedEvent, 0, len(stateEventMap))
	stateIDs = make([]string, 0, len(stateEventMap))

	for _, authEvent := range authEventMap {
		authEvents = append(authEvents, &types.HeaderedEvent{PDU: authEvent})
	}
	for _, stateEvent := range stateEventMap {
		stateEvents = append(stateEvents, &types.HeaderedEvent{PDU: stateEvent})
		stateIDs = append(stateIDs, stateEvent.EventID())
	}

	return authEvents, stateEvents, stateIDs, nil
}

// PerformAdminBridgeState submits an ordinary new event from userID whose
// prev_events explicitly include den's current forward extremities AND the
// given extraEventIDs: events den already holds, with ancestry den can
// verify, but which aren't currently ancestors of den's forward
// extremities (the shape of the three stuck den.nutra.tk events after
// backfill connected them to the room's graph without making them part of
// current state).
//
// Unlike PerformAdminDownloadState, this does NOT set HasState. It goes
// through completely ordinary state resolution - the same code path a real
// client message uses - which correctly folds both branches together when
// neither side has a competing value for a given state key (see
// roomState.LoadCombinedStateAfterEvents / calculateStateAfterManyEvents).
//
// This only works for a userID den currently recognises as a room member:
// helpers.CheckForSoftFail authorises the event against den's CURRENT state
// snapshot, independent of the event's own prev_events, so an event from a
// user den doesn't currently recognise will always soft-fail regardless of
// ancestry - confirmed locally via TestDisconnectedJoinerSendsMessage,
// where an event from the disconnected user soft-fails but an otherwise
// identical event from a currently-recognised member is accepted and
// correctly restores the disconnected user to current state.
//
// This is genuinely ordinary, not a fabricated DAG edge: every ID in
// extraEventIDs must already be present in den's database as a connected
// part of the room's graph (state_snapshot_nid != 0, i.e. state resolution
// has already run for it) - enforced below by refusing anything den can't
// verify. It's exactly what a client naturally produces when its
// prev_events happen to span a branch that's been sitting disconnected.
//
// It IS outward-facing: the resulting event is signed by den and federated
// to every other server in the room like any other event. Call sites should
// treat this as an irreversible, visible action, not a local repair.
func (r *Admin) PerformAdminBridgeState(
	ctx context.Context,
	roomID, userID string,
	extraEventIDs []string,
) error {
	fullUserID, err := spec.NewUserID(userID, true)
	if err != nil {
		return err
	}
	senderDomain := fullUserID.Domain()

	roomInfo, err := r.DB.RoomInfo(ctx, roomID)
	if err != nil {
		return err
	}
	if roomInfo == nil || roomInfo.IsStub() {
		return eventutil.ErrRoomNoExists{}
	}

	var localState api.QueryLatestEventsAndStateResponse
	if err = r.Queryer.QueryLatestEventsAndState(ctx, &api.QueryLatestEventsAndStateRequest{
		RoomID: roomID,
	}, &localState); err != nil {
		return fmt.Errorf("r.Queryer.QueryLatestEventsAndState: %w", err)
	}

	// Refuse anything den can't verify is actually connected to the room's
	// graph - this is what keeps this from being a way to smuggle in a
	// fabricated ancestry claim. First confirm den actually has each event
	// at all (EventsFromIDs silently omits IDs it doesn't have, so a length
	// mismatch means at least one is unknown).
	knownExtra, err := r.DB.EventsFromIDs(ctx, roomInfo, extraEventIDs)
	if err != nil {
		return fmt.Errorf("r.DB.EventsFromIDs: %w", err)
	}
	if len(knownExtra) != len(extraEventIDs) {
		return fmt.Errorf("one or more of the given event IDs are not known to den - refusing to reference them")
	}
	// Then confirm each is a connected part of the room's graph, not a bare
	// outlier. StateAtEventIDs itself errors loudly (MissingStateError) if
	// any non-create event it finds has BeforeStateSnapshotNID == 0, i.e.
	// state resolution has never run for it - exactly the "not vouched for"
	// case this guard exists to catch.
	if _, err = r.DB.StateAtEventIDs(ctx, extraEventIDs); err != nil {
		return fmt.Errorf("one or more of the given event IDs are not a connected part of the room's graph den can verify: %w", err)
	}

	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return err
	}
	senderID, err := r.Queryer.QuerySenderIDForUser(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return err
	} else if senderID == nil {
		return fmt.Errorf("sender ID not found for %s in %s", *fullUserID, *validRoomID)
	}

	proto := &gomatrixserverlib.ProtoEvent{
		Type:     "m.room.message",
		SenderID: string(*senderID),
		RoomID:   roomID,
	}
	if proto.Content, err = json.Marshal(map[string]any{
		"msgtype": "m.notice",
		"body":    "(state repair: bridging previously-disconnected room history back into current state)",
	}); err != nil {
		return fmt.Errorf("json.Marshal: %w", err)
	}

	eventsNeeded, err := gomatrixserverlib.StateNeededForProtoEvent(proto)
	if err != nil {
		return fmt.Errorf("gomatrixserverlib.StateNeededForProtoEvent: %w", err)
	}

	// Extend den's own latest events with the extra IDs so
	// eventutil.BuildEvent's prev_events span both branches.
	bridgingState := localState
	bridgingState.LatestEvents = append(append([]string{}, localState.LatestEvents...), extraEventIDs...)

	identity, err := r.Cfg.Matrix.SigningIdentityFor(senderDomain)
	if err != nil {
		return err
	}

	ev, err := eventutil.BuildEvent(ctx, proto, identity, time.Now(), &eventsNeeded, &bridgingState)
	if err != nil {
		return fmt.Errorf("eventutil.BuildEvent: %w", err)
	}

	inputReq := &api.InputRoomEventsRequest{
		InputRoomEvents: []api.InputRoomEvent{{
			Kind:         api.KindNew,
			Event:        ev,
			Origin:       r.Cfg.Matrix.ServerName,
			SendAsServer: string(r.Cfg.Matrix.ServerName),
		}},
		Asynchronous: false,
	}
	inputRes := &api.InputRoomEventsResponse{}
	r.Inputer.InputRoomEvents(ctx, inputReq, inputRes)
	if inputRes.ErrMsg != "" {
		return inputRes.Err()
	}
	return nil
}

func (r *Admin) PerformAdminDeleteEventReport(ctx context.Context, reportID uint64) error {
	return r.DB.AdminDeleteEventReport(ctx, reportID)
}

func (r *Admin) AdminQueryEmptyRooms(ctx context.Context) ([]string, error) {
	return r.DB.EmptyRooms(ctx)
}
