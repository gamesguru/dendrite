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
	"errors"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	fedapi "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/internal/input"
	"codefloe.com/pat-s/zendrite/roomserver/internal/query"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/storage/shared"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/process"
)

type Admin struct {
	DB      storage.Database
	Cfg     *config.RoomServer
	Queryer *query.Queryer
	Inputer *input.Inputer
	Leaver  *Leaver
	// RSAPI is used where a signing identity must be resolved per-room
	// rather than per-server-domain (see PerformAdminBridgeState) - pseudo-ID
	// rooms sign events with a per-room identity, not the server's own.
	RSAPI api.RoomserverInternalAPI
	// PurgeTracker coalesces auto-purges and lets rejoins wait for them.
	PurgeTracker *PurgeTracker
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
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	allRooms := make([]string, 0, len(roomIDs)+len(inviteRoomIDs))
	allRooms = append(allRooms, roomIDs...)
	allRooms = append(allRooms, inviteRoomIDs...)
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

// AutoPurgeRoom asynchronously purges roomID via PerformAdminPurgeRoom,
// tracking it in the PurgeTracker so that concurrent rejoin attempts can
// wait for the purge to complete. Coalesces concurrent calls for the same
// roomID. The flag check is the caller's responsibility — see
// ScheduleAutoPurgeIfEmpty and the startup sweep.
//
// Purging outlives the caller request but remains bound to the application lifetime.
func (r *Admin) AutoPurgeRoom(ctx context.Context, roomID, reason string) {
	_, owner := r.PurgeTracker.BeginPurge(roomID)
	if !owner {
		return
	}
	if !process.Go(process.Lifetime(ctx), func() {
		defer r.PurgeTracker.FinishPurge(roomID)
		// Detach from the caller's context: the caller may be a short-lived
		// handler (e.g. NATS message dispatch) whose ctx is canceled as soon
		// as the handler returns. The purge itself is an autonomous, in-flight
		// operation that remains bound to the application lifetime.
		ctx := process.Lifetime(ctx)
		fields := logrus.Fields{"room_id": roomID, "reason": reason}
		if err := r.PerformAdminPurgeRoom(ctx, roomID); err != nil {
			logrus.WithError(err).WithFields(fields).Warn("Auto-purge of empty room failed")
			return
		}
		logrus.WithFields(fields).Info("Auto-purged empty room")
	}) {
		r.PurgeTracker.FinishPurge(roomID)
	}
}

func (r *Admin) PerformAdminDownloadState(
	ctx context.Context,
	roomID, userID string, serverName spec.ServerName,
) error {
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

	// Store the fetched auth/state events first so the repair snapshot can be
	// built from local data only. We do not create a synthetic event here.
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
		return fmt.Errorf("failed to store %d auth/state events needed to rebuild room state: %w", len(outlierReq.InputRoomEvents), outlierRes.Err())
	}

	if err := r.installRepairedState(ctx, roomID, roomInfo, localState.StateEvents, stateIDs); err != nil {
		return err
	}

	return nil
}

func (r *Admin) installRepairedState(
	ctx context.Context,
	roomID string,
	roomInfo *types.RoomInfo,
	oldStateEvents []*types.HeaderedEvent,
	newStateEventIDs []string,
) (err error) {
	oldStateEntryIDs := make([]string, 0, len(oldStateEvents))
	for _, ev := range oldStateEvents {
		oldStateEntryIDs = append(oldStateEntryIDs, ev.EventID())
	}

	oldStateEntries, err := r.DB.StateEntriesForEventIDs(ctx, oldStateEntryIDs, true)
	if err != nil {
		return fmt.Errorf("r.DB.StateEntriesForEventIDs(old): %w", err)
	}
	newStateEntries, err := r.DB.StateEntriesForEventIDs(ctx, newStateEventIDs, true)
	if err != nil {
		return fmt.Errorf("r.DB.StateEntriesForEventIDs(new): %w", err)
	}

	oldStateEntries = types.DeduplicateStateEntries(append([]types.StateEntry(nil), oldStateEntries...))
	newStateEntries = types.DeduplicateStateEntries(append([]types.StateEntry(nil), newStateEntries...))

	removed, added := diffStateEntries(oldStateEntries, newStateEntries)

	updater, err := r.DB.GetRoomUpdater(ctx, roomInfo)
	if err != nil {
		return fmt.Errorf("r.DB.GetRoomUpdater: %w", err)
	}
	var succeeded bool
	defer sqlutil.EndTransactionWithCheck(updater, &succeeded, &err)

	stateSnapshotNID, err := updater.AddState(ctx, roomInfo.RoomNID, newStateEntries)
	if err != nil {
		return fmt.Errorf("updater.AddState: %w", err)
	}

	latestEvents := updater.LatestEvents()
	for _, latest := range latestEvents {
		if err = updater.SetState(ctx, latest.EventNID, stateSnapshotNID); err != nil {
			return fmt.Errorf("updater.SetState(%d): %w", latest.EventNID, err)
		}
	}

	if err = updater.SetCurrentStateSnapshotNID(stateSnapshotNID); err != nil {
		return fmt.Errorf("updater.SetCurrentStateSnapshotNID: %w", err)
	}

	if err = r.repairMemberships(ctx, roomID, roomInfo, updater, removed, added); err != nil {
		return err
	}

	succeeded = true
	return nil
}

func (r *Admin) repairMemberships(
	ctx context.Context,
	roomID string,
	roomInfo *types.RoomInfo,
	updater *shared.RoomUpdater,
	removed, added []types.StateEntry,
) error {
	changes := pairUpStateEntries(removed, added)
	if len(changes) == 0 {
		return nil
	}

	var eventNIDs []types.EventNID
	stateKeyNIDs := make(map[types.EventStateKeyNID]struct{})
	for _, change := range changes {
		if change.EventTypeNID != types.MRoomMemberNID {
			continue
		}
		if change.removedEventNID != 0 {
			eventNIDs = append(eventNIDs, change.removedEventNID)
		}
		if change.addedEventNID != 0 {
			eventNIDs = append(eventNIDs, change.addedEventNID)
		}
		stateKeyNIDs[change.EventStateKeyNID] = struct{}{}
	}

	membershipEvents, err := r.DB.Events(ctx, roomInfo.RoomVersion, eventNIDs)
	if err != nil {
		return fmt.Errorf("r.DB.Events: %w", err)
	}

	membershipEventMap := make(map[types.EventNID]types.Event, len(membershipEvents))
	for _, event := range membershipEvents {
		membershipEventMap[event.EventNID] = event
	}

	stateKeyList := make([]types.EventStateKeyNID, 0, len(stateKeyNIDs))
	for nid := range stateKeyNIDs {
		stateKeyList = append(stateKeyList, nid)
	}
	stateKeyMap, err := r.DB.EventStateKeys(ctx, stateKeyList)
	if err != nil {
		return fmt.Errorf("r.DB.EventStateKeys: %w", err)
	}

	validRoomID, err := spec.NewRoomID(roomID)
	if err != nil {
		return err
	}

	for _, change := range changes {
		if change.EventTypeNID != types.MRoomMemberNID {
			continue
		}

		stateKey, ok := stateKeyMap[change.EventStateKeyNID]
		if !ok {
			continue
		}

		targetLocal := false
		if userID, queryErr := r.Queryer.QueryUserIDForSender(ctx, *validRoomID, spec.SenderID(stateKey)); queryErr == nil && userID != nil {
			targetLocal = userID.Domain() == r.Cfg.Matrix.ServerName
		}

		targetUpdater, err := updater.MembershipUpdater(change.EventStateKeyNID, targetLocal)
		if err != nil {
			return fmt.Errorf("updater.MembershipUpdater: %w", err)
		}

		switch {
		case change.addedEventNID != 0:
			event, ok := membershipEventMap[change.addedEventNID]
			if !ok {
				return fmt.Errorf("missing membership event for event NID %d", change.addedEventNID)
			}
			newMembership, membershipErr := membershipStateForEvent(event)
			if membershipErr != nil {
				return membershipErr
			}
			if _, _, err = targetUpdater.Update(newMembership, &event, false); err != nil {
				return fmt.Errorf("targetUpdater.Update: %w", err)
			}
		case change.removedEventNID != 0:
			if err = targetUpdater.Delete(); err != nil {
				return fmt.Errorf("targetUpdater.Delete: %w", err)
			}
		}
	}

	return nil
}

func pairUpStateEntries(removed, added []types.StateEntry) []stateChange {
	tuples := make(map[types.StateKeyTuple]stateChange)
	changes := []stateChange{}

	for _, add := range added {
		if change, ok := tuples[add.StateKeyTuple]; ok {
			change.addedEventNID = add.EventNID
			tuples[add.StateKeyTuple] = change
		} else {
			tuples[add.StateKeyTuple] = stateChange{StateKeyTuple: add.StateKeyTuple, addedEventNID: add.EventNID}
		}
	}

	for _, remove := range removed {
		if change, ok := tuples[remove.StateKeyTuple]; ok {
			change.removedEventNID = remove.EventNID
			tuples[remove.StateKeyTuple] = change
		} else {
			tuples[remove.StateKeyTuple] = stateChange{StateKeyTuple: remove.StateKeyTuple, removedEventNID: remove.EventNID}
		}
	}

	for _, change := range tuples {
		changes = append(changes, change)
	}

	return changes
}

func diffStateEntries(oldEntries, newEntries []types.StateEntry) (removed, added []types.StateEntry) {
	oldMap := make(map[types.StateKeyTuple]types.StateEntry, len(oldEntries))
	for _, entry := range oldEntries {
		oldMap[entry.StateKeyTuple] = entry
	}
	newMap := make(map[types.StateKeyTuple]types.StateEntry, len(newEntries))
	for _, entry := range newEntries {
		newMap[entry.StateKeyTuple] = entry
	}

	for key, oldEntry := range oldMap {
		newEntry, ok := newMap[key]
		if !ok {
			removed = append(removed, oldEntry)
			continue
		}
		if oldEntry.EventNID != newEntry.EventNID {
			removed = append(removed, oldEntry)
			added = append(added, newEntry)
		}
	}
	for key, newEntry := range newMap {
		if _, ok := oldMap[key]; !ok {
			added = append(added, newEntry)
		}
	}
	return
}

type stateChange struct {
	types.StateKeyTuple
	removedEventNID types.EventNID
	addedEventNID   types.EventNID
}

func membershipStateForEvent(event types.Event) (tables.MembershipState, error) {
	membership, err := event.Membership()
	if err != nil {
		return 0, err
	}
	switch membership {
	case spec.Join:
		return tables.MembershipStateJoin, nil
	case spec.Invite:
		return tables.MembershipStateInvite, nil
	case spec.Leave, spec.Ban:
		return tables.MembershipStateLeaveOrBan, nil
	case spec.Knock:
		return tables.MembershipStateKnock, nil
	default:
		return 0, fmt.Errorf("unsupported membership state %q", membership)
	}
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
// before it can apply anything.
//
// An earlier version of this fix only filled gaps missing from remote, so
// the failure ("eventauth: sender not in room") survived it.
// Remote wasn't missing that tuple; it had a stale (pre-join) value for it.
// Local is always at least as fresh as remote for anything den has already
// legitimately processed, so local should win on conflicts; remote
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
			return nil, nil, nil, fmt.Errorf("r.Inputer.FSAPI.LookupState (%q): %w", fwdExtremity, err)
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
// This only works for a userID den currently recognizes as a room member:
// helpers.CheckForSoftFail authorizes the event against den's CURRENT state
// snapshot, independent of the event's own prev_events, so an event from a
// user den doesn't currently recognize will always soft-fail regardless of
// ancestry - confirmed locally via TestDisconnectedJoinerSendsMessage,
// where an event from the disconnected user soft-fails but an otherwise
// identical event from a currently-recognized member is accepted and
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
	// EventsFromIDs looks up by event ID alone - it isn't room-scoped, and
	// parses each event using roomID's room version regardless of which room
	// it actually belongs to. Refuse anything that isn't genuinely in this
	// room; otherwise a foreign-room event ID could pass every check above
	// and end up referenced as this room's ancestry.
	for _, ev := range knownExtra {
		if ev.RoomID().String() != roomID {
			return fmt.Errorf("event %s belongs to room %s, not %s - refusing to reference it", ev.EventID(), ev.RoomID().String(), roomID)
		}
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

	// Custom, non-message type (matching PerformAdminDownloadState's
	// org.matrix.dendrite.state_download below) - ordinary clients don't
	// recognize it and won't render it in the timeline. Nothing about the
	// bridging mechanism requires a message specifically; only that this be
	// an ordinary, non-state KindNew event (see the function doc comment).
	proto := &gomatrixserverlib.ProtoEvent{
		Type:     "org.matrix.dendrite.state_bridge",
		SenderID: string(*senderID),
		RoomID:   roomID,
		Content:  spec.RawJSON("{}"),
	}

	eventsNeeded, err := gomatrixserverlib.StateNeededForProtoEvent(proto)
	if err != nil {
		return fmt.Errorf("gomatrixserverlib.StateNeededForProtoEvent: %w", err)
	}

	// Extend den's own latest events with the extra IDs so
	// eventutil.BuildEvent's prev_events span both branches. addPrevEventsToEvent
	// silently truncates to 20 prev_events (truncateAuthAndPrevEvents in
	// internal/eventutil/events.go) - since extraEventIDs are appended after
	// den's own latest events, a truncation would silently drop the very IDs
	// this call exists to bridge in. Refuse loudly instead of building an
	// event that quietly doesn't do what was asked.
	const maxPrevEvents = 20
	bridgingState := localState
	bridgingState.LatestEvents = append(append([]string{}, localState.LatestEvents...), extraEventIDs...)
	if len(bridgingState.LatestEvents) > maxPrevEvents {
		return fmt.Errorf(
			"room's current extremities (%d) plus the requested extra event IDs (%d) exceed the %d prev_events an event can carry - reduce extraEventIDs or bridge in smaller batches",
			len(localState.LatestEvents), len(extraEventIDs), maxPrevEvents,
		)
	}

	// Room-aware, not r.Cfg.Matrix.SigningIdentityFor(senderDomain): pseudo-ID
	// rooms sign events with a per-room identity that the plain server-domain
	// lookup won't resolve correctly.
	identity, err := r.RSAPI.SigningIdentityFor(ctx, *validRoomID, *fullUserID)
	if err != nil {
		return fmt.Errorf("r.RSAPI.SigningIdentityFor: %w", err)
	}

	ev, err := eventutil.BuildEvent(ctx, proto, &identity, time.Now(), &eventsNeeded, &bridgingState)
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

// PerformUserErasure marks a local user as erased, so that their events are
// served redacted to anyone who was not in the room when they were sent
// (MSC4025), or clears the mark when the account is reactivated.
func (r *Admin) PerformUserErasure(ctx context.Context, userID spec.UserID, erased bool) error {
	if !r.Cfg.Matrix.IsLocalServerName(userID.Domain()) {
		return fmt.Errorf("user %q is not local", userID.String())
	}
	return r.DB.SetUserErased(ctx, userID.String(), erased)
}

// PerformAdminFetchEvent fetches eventID from serverName via federation /event,
// verifies its signatures, and submits it as an ordinary new event (so missing
// prev_events are fetched and auth/state resolution run as for a /send PDU).
// It is not re-federated onwards (SendAsServer is empty). Use it to recover an
// event another server has but which never reached us.
func (r *Admin) PerformAdminFetchEvent(
	ctx context.Context,
	roomID, eventID string,
	serverName spec.ServerName,
) (string, error) {
	roomInfo, err := r.DB.RoomInfo(ctx, roomID)
	if err != nil {
		return "", err
	}
	if roomInfo == nil || roomInfo.IsStub() {
		return "", eventutil.ErrRoomNoExists{}
	}
	verImpl, err := gomatrixserverlib.GetRoomVersion(roomInfo.RoomVersion)
	if err != nil {
		return "", err
	}
	txn, err := r.Inputer.FSAPI.GetEvent(ctx, r.Cfg.Matrix.ServerName, serverName, eventID)
	if err != nil {
		return "", fmt.Errorf("fetching %s from %s: %w", eventID, serverName, err)
	}
	if len(txn.PDUs) == 0 {
		return "", fmt.Errorf("%s returned no PDU for %s", serverName, eventID)
	}
	ev, err := verImpl.NewEventFromUntrustedJSON(txn.PDUs[0])
	if err != nil {
		return "", fmt.Errorf("parsing event: %w", err)
	}
	if ev.EventID() != eventID {
		return "", fmt.Errorf("%s returned event %s, wanted %s", serverName, ev.EventID(), eventID)
	}
	if ev.RoomID().String() != roomID {
		return "", fmt.Errorf("event %s belongs to room %s, not %s", eventID, ev.RoomID().String(), roomID)
	}
	return r.ingestAdminEvent(ctx, ev, serverName)
}

// ingestAdminEvent verifies ev's signatures, submits it as a new event and
// reads back whether it was stored: "accepted", "rejected" or "not_stored".
func (r *Admin) ingestAdminEvent(
	ctx context.Context, ev gomatrixserverlib.PDU, serverName spec.ServerName,
) (string, error) {
	eventID := ev.EventID()
	if err := r.verifyAdminEventSignatures(ctx, ev); err != nil {
		return "", fmt.Errorf("signature check failed: %w", err)
	}
	inputRes := &api.InputRoomEventsResponse{}
	r.Inputer.InputRoomEvents(ctx, &api.InputRoomEventsRequest{
		InputRoomEvents: []api.InputRoomEvent{{
			Kind:   api.KindNew,
			Event:  &types.HeaderedEvent{PDU: ev},
			Origin: serverName,
		}},
		Asynchronous: false,
		VirtualHost:  r.Cfg.Matrix.ServerName,
	}, inputRes)
	if inputRes.ErrMsg != "" {
		return "", inputRes.Err()
	}
	// InputRoomEvents succeeds even if the event ended up rejected, so read
	// back what was actually persisted. (Soft-failed events are stored as
	// accepted but excluded from current state; that is not distinguished here.)
	states, err := r.DB.StateAtEventIDs(ctx, []string{eventID})
	if err != nil {
		var missingErr types.MissingEventError
		if errors.As(err, &missingErr) {
			return "not_stored", nil
		}
		return "", fmt.Errorf("reading back stored event: %w", err)
	}
	if len(states) == 0 {
		return "not_stored", nil
	}
	if states[0].IsRejected {
		return "rejected", nil
	}
	return "accepted", nil
}

func (r *Admin) verifyAdminEventSignatures(ctx context.Context, ev gomatrixserverlib.PDU) error {
	return gomatrixserverlib.VerifyEventSignatures(ctx, ev, r.Inputer.KeyRing, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return r.Queryer.QueryUserIDForSender(ctx, roomID, senderID)
	})
}

// adminDuplicateOutcome reports the import result for an event ID that was
// already ingested. The duplicate's signature is re-checked, because a bad
// copy must be reported as an error rather than silently accepted as a
// duplicate of the good one.
func adminDuplicateOutcome(ev gomatrixserverlib.PDU, verify func(gomatrixserverlib.PDU) error) string {
	outcome := "duplicate"
	if err := verify(ev); err != nil {
		outcome = "error: signature check failed: " + err.Error()
	}
	return outcome
}

// maxAdminImportEvents bounds one import so it finishes inside the server's
// write timeout.
const maxAdminImportEvents = 5000

// PerformAdminImportEvents ingests PDUs supplied by the operator (for example
// from a JSONL export) into roomID. Each event must belong to the room, parse
// for its room version and carry valid signatures; events are submitted as new
// events in ascending depth order so their prev_events arrive first. The origin
// is the server asked for any prev or auth events the batch does not contain.
// One failing event does not stop the rest.
func (r *Admin) PerformAdminImportEvents(
	ctx context.Context, roomID string, pdus []json.RawMessage, origin spec.ServerName,
) ([]api.AdminImportResult, error) {
	if len(pdus) > maxAdminImportEvents {
		return nil, fmt.Errorf("at most %d events per import, got %d", maxAdminImportEvents, len(pdus))
	}
	roomInfo, err := r.DB.RoomInfo(ctx, roomID)
	if err != nil {
		return nil, err
	}
	if roomInfo == nil || roomInfo.IsStub() {
		return nil, eventutil.ErrRoomNoExists{}
	}
	verImpl, err := gomatrixserverlib.GetRoomVersion(roomInfo.RoomVersion)
	if err != nil {
		return nil, err
	}
	type parsed struct {
		line int
		ev   gomatrixserverlib.PDU
	}
	results := make([]api.AdminImportResult, 0, len(pdus))
	events := make([]parsed, 0, len(pdus))
	// IDs already ingested without error. A copy that failed (for example a
	// bad signature) must not suppress a valid copy of the same event.
	done := make(map[string]struct{}, len(pdus))
	for i, raw := range pdus {
		line := i + 1
		ev, parseErr := verImpl.NewEventFromUntrustedJSON(raw)
		switch {
		case parseErr != nil:
			results = append(results, api.AdminImportResult{Line: line, Outcome: "invalid: " + parseErr.Error()})
		case ev.RoomID().String() != roomID:
			results = append(results, api.AdminImportResult{
				Line: line, EventID: ev.EventID(), Outcome: "invalid: event belongs to room " + ev.RoomID().String(),
			})
		default:
			events = append(events, parsed{line: line, ev: ev})
		}
	}
	sort.SliceStable(events, func(i, j int) bool { return events[i].ev.Depth() < events[j].ev.Depth() })
	for _, p := range events {
		if _, dup := done[p.ev.EventID()]; dup {
			outcome := adminDuplicateOutcome(p.ev, func(ev gomatrixserverlib.PDU) error {
				return r.verifyAdminEventSignatures(ctx, ev)
			})
			results = append(results, api.AdminImportResult{Line: p.line, EventID: p.ev.EventID(), Outcome: outcome})
			continue
		}
		outcome, ingestErr := r.ingestAdminEvent(ctx, p.ev, origin)
		if ingestErr != nil {
			outcome = "error: " + ingestErr.Error()
		} else {
			done[p.ev.EventID()] = struct{}{}
		}
		results = append(results, api.AdminImportResult{Line: p.line, EventID: p.ev.EventID(), Outcome: outcome})
	}
	sort.SliceStable(results, func(i, j int) bool { return results[i].Line < results[j].Line })
	return results, nil
}

// maxRefetchServers bounds how many servers are asked for one event.
const maxRefetchServers = 8

// Outcomes of PerformAdminRefetchEvents.
const (
	RefetchOutcomeRefetched = "refetched"
	// RefetchOutcomeSigUnverified marks a hash-valid copy whose signatures could
	// not be verified (for example the origin's keys are unreachable).
	RefetchOutcomeSigUnverified = "refetched_signature_unverified"
	RefetchOutcomeAlreadyFull   = "already_full"
	RefetchOutcomeHashMismatch  = "hash_mismatch"
	RefetchOutcomeNotFound      = "not_found"
	RefetchOutcomeForbidden     = "forbidden"
	RefetchOutcomeUnreachable   = "origin_unreachable"
)

// PerformAdminRefetchEvents fetches the complete copy of each event whose
// stored JSON is hash-pruned (pruned but not a genuine redaction) from its
// origin server, requires it to pass the content hash and signature checks,
// and stores it as an outlier, which the storage layer lets replace a pruned
// copy. Nothing is emitted to other components: the event is not new. With
// dryRun nothing is written.
func (r *Admin) PerformAdminRefetchEvents(
	ctx context.Context, events []api.AdminRefetchEvent, dryRun bool,
) map[string]string {
	results := make(map[string]string, len(events))
	for _, e := range events {
		if _, done := results[e.EventID]; done || e.EventID == "" {
			continue
		}
		outcome, err := r.refetchEvent(ctx, e, dryRun)
		if err != nil {
			logrus.WithError(err).WithFields(logrus.Fields{
				"room_id": e.RoomID, "event_id": e.EventID,
			}).Warn("refetch of pruned event failed")
			outcome = "error: " + err.Error()
		}
		results[e.EventID] = outcome
	}
	return results
}

func (r *Admin) refetchEvent(ctx context.Context, e api.AdminRefetchEvent, dryRun bool) (string, error) {
	roomInfo, err := r.DB.RoomInfo(ctx, e.RoomID)
	if err != nil {
		return "", err
	}
	if roomInfo == nil || roomInfo.IsStub() {
		return "", eventutil.ErrRoomNoExists{}
	}
	meta, err := r.DB.EventNIDs(ctx, []string{e.EventID})
	if err != nil {
		return "", err
	}
	stored, ok := meta[e.EventID]
	if !ok {
		return "", fmt.Errorf("event %s is not stored", e.EventID)
	}
	have, err := r.DB.Events(ctx, roomInfo.RoomVersion, []types.EventNID{stored.EventNID})
	if err != nil {
		return "", err
	}
	if len(have) != 1 {
		return "", fmt.Errorf("event %s could not be loaded", e.EventID)
	}
	if !have[0].Redacted() || gjson.GetBytes(have[0].Unsigned(), "redacted_because").Exists() {
		return RefetchOutcomeAlreadyFull, nil
	}

	servers := r.refetchServerCandidates(ctx, e, have[0], roomInfo)
	if len(servers) == 0 {
		return "", fmt.Errorf("no server to ask for %s", e.EventID)
	}
	if len(servers) > maxRefetchServers {
		servers = servers[:maxRefetchServers]
	}
	verImpl, err := gomatrixserverlib.GetRoomVersion(roomInfo.RoomVersion)
	if err != nil {
		return "", err
	}
	var lastErr error
	sawPruned := false
	sawNotFound := false
	sawForbidden := false
	var unreachable []string
	for _, server := range servers {
		txn, ferr := r.Inputer.FSAPI.GetEvent(ctx, r.Cfg.Matrix.ServerName, server, e.EventID)
		if ferr != nil {
			var httpErr spec.HTTPError
			if errors.As(ferr, &httpErr) {
				switch httpErr.Code {
				case http.StatusNotFound:
					sawNotFound = true
					continue
				case http.StatusForbidden:
					sawForbidden = true
					continue
				}
			}
			unreachable = append(unreachable, string(server))
			lastErr = fmt.Errorf("fetching %s from %s failed: %w", e.EventID, server, ferr)
			continue
		}
		if len(txn.PDUs) == 0 {
			sawNotFound = true
			continue
		}
		ev, perr := verImpl.NewEventFromUntrustedJSON(txn.PDUs[0])
		if perr != nil {
			lastErr = fmt.Errorf("parsing event from %s: %w", server, perr)
			continue
		}
		if ev.EventID() != e.EventID || ev.RoomID().String() != e.RoomID {
			lastErr = fmt.Errorf("%s returned a different event", server)
			continue
		}
		if ev.Redacted() {
			// This server has only a pruned copy too; try the next one.
			sawPruned = true
			continue
		}
		// Content comes from the fetched copy. unsigned and signatures are not
		// part of the event's identity: keep this store's own. Redaction keeps
		// the original signatures, so the local ones sign the restored content.
		merged := ev.JSON()
		var merr error
		for _, key := range []string{"unsigned", "signatures"} {
			var local gjson.Result
			if key == "unsigned" {
				local = gjson.ParseBytes(have[0].Unsigned())
			} else {
				local = gjson.GetBytes(have[0].JSON(), key)
			}
			if local.Exists() {
				merged, merr = sjson.SetRawBytes(merged, key, []byte(local.Raw))
			} else if key == "unsigned" {
				merged, merr = sjson.DeleteBytes(merged, key)
			}
			if merr != nil {
				return "", fmt.Errorf("merging %s: %w", key, merr)
			}
		}
		localSignatures := gjson.GetBytes(have[0].JSON(), "signatures")
		fetchedSignatures := gjson.GetBytes(ev.JSON(), "signatures")
		if (roomInfo.RoomVersion == gomatrixserverlib.RoomVersionV1 || roomInfo.RoomVersion == gomatrixserverlib.RoomVersionV2) &&
			(!localSignatures.IsObject() || len(localSignatures.Map()) == 0) &&
			(!fetchedSignatures.IsObject() || len(fetchedSignatures.Map()) == 0) {
			lastErr = fmt.Errorf("room version %s requires a signed event when the stored copy has no signatures", roomInfo.RoomVersion)
			continue
		}
		ev, perr = verImpl.NewEventFromTrustedJSON(merged, false)
		if perr != nil {
			lastErr = fmt.Errorf("merged copy from %s no longer parses cleanly: %w", server, perr)
			continue
		}
		if ev.Redacted() {
			lastErr = fmt.Errorf("merged copy from %s is still pruned", server)
			continue
		}
		// The merged copy carries this store's own signatures, so a failure here
		// (for example the origin's keys are unreachable) is reported, not fatal.
		signed := RefetchOutcomeRefetched
		if verr := r.verifyAdminEventSignatures(ctx, ev); verr != nil {
			signed = RefetchOutcomeSigUnverified
		}
		if dryRun {
			return "would_" + signed, nil
		}
		inputRes := &api.InputRoomEventsResponse{}
		r.Inputer.InputRoomEvents(ctx, &api.InputRoomEventsRequest{
			InputRoomEvents: []api.InputRoomEvent{{
				Kind: api.KindOutlier, Event: &types.HeaderedEvent{PDU: ev}, Origin: server,
			}},
			Asynchronous: false,
			VirtualHost:  r.Cfg.Matrix.ServerName,
		}, inputRes)
		if inputRes.ErrMsg != "" {
			return "", inputRes.Err()
		}
		after, aerr := r.DB.Events(ctx, roomInfo.RoomVersion, []types.EventNID{stored.EventNID})
		if aerr != nil || len(after) != 1 || after[0].Redacted() {
			return "", fmt.Errorf("event %s is still pruned after the refetch", e.EventID)
		}
		return signed, nil
	}
	switch {
	case len(unreachable) > 0:
		return "", fmt.Errorf("no verified copy found; unreachable candidates: %s; last error: %w", strings.Join(unreachable, ", "), lastErr)
	case sawPruned && !sawNotFound && !sawForbidden:
		return RefetchOutcomeHashMismatch, nil
	case sawForbidden && !sawNotFound && !sawPruned:
		return RefetchOutcomeForbidden, nil
	case sawNotFound && !sawForbidden && !sawPruned:
		return RefetchOutcomeNotFound, nil
	case lastErr != nil:
		return "", lastErr
	}
	return "", fmt.Errorf("no candidate server returned a verifiable copy")
}

// refetchServerCandidates orders sources from most likely to hold the event to
// least likely. For old events, historical residents are more useful than
// current residents: they may have retained the event even if current members
// joined later or pruned their history.
func (r *Admin) refetchServerCandidates(
	ctx context.Context, e api.AdminRefetchEvent, event gomatrixserverlib.PDU, roomInfo *types.RoomInfo,
) []spec.ServerName {
	servers := make([]spec.ServerName, 0, 16)
	add := func(candidate spec.ServerName) {
		if candidate == "" {
			return
		}
		for _, existing := range servers {
			if existing == candidate {
				return
			}
		}
		servers = append(servers, candidate)
	}
	if e.Server != "" {
		add(e.Server)
	}
	for _, candidate := range e.Servers {
		add(candidate)
	}

	// Reuse backfill's historical membership and history-visibility selection.
	prevEventIDs := event.PrevEventIDs()
	if len(prevEventIDs) > 0 {
		requester := newBackfillRequester(
			r.DB, r.Inputer.FSAPI, r.Queryer, r.Cfg.Matrix.ServerName,
			r.Cfg.Matrix.IsLocalServerName,
			map[string][]string{event.EventID(): prevEventIDs}, nil, roomInfo.RoomVersion,
		)
		requester.roomNID = roomInfo.RoomNID
		historical := requester.ServersAtEvent(ctx, e.RoomID, prevEventIDs[0])
		sort.Slice(historical, func(i, j int) bool { return historical[i] < historical[j] })
		for _, candidate := range historical {
			add(candidate)
		}
	}

	if sender, err := r.Queryer.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID()); err == nil && sender != nil {
		add(sender.Domain())
	}
	if create, err := r.DB.GetStateEvent(ctx, e.RoomID, spec.MRoomCreate, ""); err == nil && create != nil {
		if creator, err := r.Queryer.QueryUserIDForSender(ctx, create.RoomID(), create.SenderID()); err == nil && creator != nil {
			add(creator.Domain())
		}
	}
	if roomID, err := spec.NewRoomID(e.RoomID); err == nil {
		add(eventutil.RoomIDDomain(*roomID))
	}

	hosts := &fedapi.QueryJoinedHostServerNamesInRoomResponse{}
	if err := r.Inputer.FSAPI.QueryJoinedHostServerNamesInRoom(ctx, &fedapi.QueryJoinedHostServerNamesInRoomRequest{
		RoomID: e.RoomID, ExcludeSelf: true, ExcludeBlacklisted: true,
	}, hosts); err == nil {
		for _, candidate := range hosts.ServerNames {
			add(candidate)
		}
	}
	if len(servers) > maxRefetchServers {
		servers = servers[:maxRefetchServers]
	}
	return servers
}
