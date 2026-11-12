package consumers

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/internal/pushgateway"
	"codefloe.com/pat-s/zendrite/internal/pushrules"
	"codefloe.com/pat-s/zendrite/internal/receipts"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/producers"
	"codefloe.com/pat-s/zendrite/userapi/storage"
	"codefloe.com/pat-s/zendrite/userapi/storage/tables"
	userAPITypes "codefloe.com/pat-s/zendrite/userapi/types"
	"codefloe.com/pat-s/zendrite/userapi/util"
)

type OutputRoomEventConsumer struct {
	ctx          context.Context
	cfg          *config.UserAPI
	rsAPI        rsapi.UserRoomserverAPI
	jetstream    nats.JetStreamContext
	durable      string
	db           storage.UserDatabase
	keyDB        storage.KeyDatabase
	topic        string
	pgClient     pushgateway.Client
	syncProducer *producers.SyncAPI
	msgCounts    map[spec.ServerName]userAPITypes.MessageStats
	roomCounts   map[spec.ServerName]map[string]bool // map from serverName to map from rommID to "isEncrypted"
	lastUpdate   time.Time
	countsLock   sync.Mutex
	serverName   spec.ServerName
}

var errRetryPushRuleCopy = errors.New("retry room event after push rule copy failure")

func NewOutputRoomEventConsumer(
	process *process.ProcessContext,
	cfg *config.UserAPI,
	js nats.JetStreamContext,
	store storage.UserDatabase,
	keyDB storage.KeyDatabase,
	pgClient pushgateway.Client,
	rsAPI rsapi.UserRoomserverAPI,
	syncProducer *producers.SyncAPI,
) *OutputRoomEventConsumer {
	return &OutputRoomEventConsumer{
		ctx:       process.Context(),
		cfg:       cfg,
		jetstream: js,
		db:        store,
		keyDB:     keyDB,
		durable:   cfg.Matrix.JetStream.Durable("UserAPIRoomServerConsumer"),
		// Consume the syncapi-forwarded topic, not the raw roomserver output -
		// see producers.PushEventProducer for why.
		topic:        cfg.Matrix.JetStream.Prefixed(jetstream.OutputPushEvent),
		pgClient:     pgClient,
		rsAPI:        rsAPI,
		syncProducer: syncProducer,
		msgCounts:    map[spec.ServerName]userAPITypes.MessageStats{},
		roomCounts:   map[spec.ServerName]map[string]bool{},
		lastUpdate:   time.Now(),
		countsLock:   sync.Mutex{},
		serverName:   cfg.Matrix.ServerName,
	}
}

func (s *OutputRoomEventConsumer) Start() error {
	if err := jetstream.JetStreamConsumer(
		s.ctx, s.jetstream, s.topic, s.durable, 1,
		s.onMessage, nats.DeliverAll(), nats.ManualAck(),
	); err != nil {
		return err
	}
	// This consumer used to read the raw roomserver output. Delete the durables
	// left behind on that stream: OutputRoomEvent is interest-based with no
	// MaxAge, so an abandoned consumer pins every message on it forever. Both
	// names are tried because pre-Pull-era deployments have the unsuffixed one.
	//
	// HA refuses consumer deletion outright, so there the cleanup is an offline
	// migration step. Failing to clean up never aborts startup: the caller
	// panics on a Start error, and an orphaned consumer is not worth that.
	if jetstream.OwnershipOf(s.jetstream) == nil {
		stream := s.cfg.Matrix.JetStream.Prefixed(jetstream.OutputRoomEvent)
		for _, durable := range []string{s.durable + "Pull", s.durable} {
			err := s.jetstream.DeleteConsumer(stream, durable)
			if err != nil && !errors.Is(err, nats.ErrConsumerNotFound) {
				log.WithError(err).Warnf("Failed to clean up old consumer %q on stream %q", durable, stream)
			}
		}
	}
	return nil
}

func (s *OutputRoomEventConsumer) onMessage(ctx context.Context, msgs []*nats.Msg) bool {
	msg := msgs[0] // Guaranteed to exist if onMessage is called
	// Only handle events we care about

	var event *rstypes.HeaderedEvent
	var isNewRoomEvent bool
	switch rsapi.OutputType(msg.Header.Get(jetstream.RoomEventType)) {
	case rsapi.OutputTypeNewRoomEvent:
		isNewRoomEvent = true
		fallthrough
	case rsapi.OutputTypeNewInviteEvent:
		var output rsapi.OutputEvent
		if err := json.Unmarshal(msg.Data, &output); err != nil {
			// If the message was invalid, log it and move on to the next message in the stream
			log.WithError(err).Errorf("roomserver output log: message parse failure")
			return true
		}
		if isNewRoomEvent {
			event = output.NewRoomEvent.Event
			if event != nil {
				s.cancelDelayedStateEvents(ctx, output.NewRoomEvent)
			}
		} else {
			event = output.NewInviteEvent.Event
		}

		if event == nil {
			log.Errorf("userapi consumer: expected event")
			return true
		}

		log.WithFields(log.Fields{
			"event_id":   event.EventID(),
			"event_type": event.Type(),
		}).Tracef("Received message from roomserver: %#v", output)
	case rsapi.OutputTypeRejectedEvents:
		var output rsapi.OutputEvent
		if err := json.Unmarshal(msg.Data, &output); err != nil {
			log.WithError(err).Errorf("roomserver output log: message parse failure")
			return true
		}
		if output.RejectedEvents == nil {
			return true
		}
		if err := s.onRejectedEvents(ctx, *output.RejectedEvents); err != nil {
			log.WithError(err).WithField("room_id", output.RejectedEvents.RoomID).
				Error("userapi consumer: failed to clean up rejected events")
			return false
		}
		return true
	default:
		return true
	}

	if s.cfg.Matrix.ReportStats.Enabled {
		process.Go(ctx, func() { s.storeMessageStats(ctx, event.Type(), string(event.SenderID()), event.RoomID().String()) })
	}

	metadata, err := msg.Metadata()
	if err != nil {
		return true
	}

	if err := s.processMessage(ctx, event, uint64(spec.AsTimestamp(metadata.Timestamp))); err != nil {
		log.WithFields(log.Fields{
			"event_id": event.EventID(),
		}).WithError(err).Errorf("userapi consumer: process room event failure")
		if errors.Is(err, errRetryPushRuleCopy) {
			return false
		}
	}

	return true
}

// onRejectedEvents drops notifications for events the roomserver rejected after
// accepting them, and refreshes the room counts for every affected user.
func (s *OutputRoomEventConsumer) onRejectedEvents(ctx context.Context, msg rsapi.OutputRejectedEvents) error {
	for _, eventID := range msg.EventIDs {
		if _, err := s.db.DeleteNotificationsForEvent(ctx, msg.RoomID, eventID); err != nil {
			return fmt.Errorf("s.db.DeleteNotificationsForEvent: %w", err)
		}
	}
	users, err := s.db.PendingNotificationRefreshes(ctx, msg.RoomID)
	if err != nil {
		return fmt.Errorf("s.db.PendingNotificationRefreshes: %w", err)
	}
	for _, userID := range users {
		if err := s.syncProducer.GetAndSendNotificationData(ctx, userID, msg.RoomID); err != nil {
			return fmt.Errorf("publish notification counts for %s in %s: %w", userID, msg.RoomID, err)
		}
		if err := s.db.CompleteNotificationRefresh(ctx, msg.RoomID, userID); err != nil {
			return fmt.Errorf("s.db.CompleteNotificationRefresh: %w", err)
		}
	}
	return nil
}

// cancelDelayedStateEvents cancels other users' MSC4140 delayed events for
// state that this event has just changed, as Synapse does: a delayed state
// event must not overwrite state that someone else set in the meantime. A
// user's own delayed events survive their own changes, so that clients may
// update the state they will reset later.
func (s *OutputRoomEventConsumer) cancelDelayedStateEvents(ctx context.Context, output *rsapi.OutputNewRoomEvent) {
	event := output.Event
	if event.StateKey() == nil || !slices.Contains(output.AddsStateEventIDs, event.EventID()) {
		return
	}
	sender, err := s.rsAPI.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
	if err != nil || sender == nil {
		log.WithError(err).WithField("event_id", event.EventID()).Warn("userapi consumer: unknown sender, not canceling delayed state events")
		return
	}
	// A remote sender is nobody's own change, so it cancels every local user's.
	var exceptLocalpart string
	var exceptServerName spec.ServerName
	if s.cfg.Matrix.IsLocalServerName(sender.Domain()) {
		exceptLocalpart, exceptServerName = sender.Local(), sender.Domain()
	}
	if err = s.db.CancelDelayedStateEvents(
		ctx, event.RoomID().String(), event.Type(), *event.StateKey(),
		exceptLocalpart, exceptServerName, spec.AsTimestamp(time.Now()),
	); err != nil {
		log.WithError(err).WithField("event_id", event.EventID()).Error("userapi consumer: failed to cancel delayed state events")
	}
}

func (s *OutputRoomEventConsumer) storeMessageStats(ctx context.Context, eventType, eventSender, roomID string) {
	s.countsLock.Lock()
	defer s.countsLock.Unlock()

	// reset the roomCounts on a day change
	if s.lastUpdate.Day() != time.Now().Day() {
		s.roomCounts[s.serverName] = make(map[string]bool)
		s.lastUpdate = time.Now()
	}

	_, sender, err := gomatrixserverlib.SplitID('@', eventSender)
	if err != nil {
		return
	}
	msgCount := s.msgCounts[s.serverName]
	roomCount := s.roomCounts[s.serverName]
	if roomCount == nil {
		roomCount = make(map[string]bool)
	}
	switch eventType {
	case "m.room.message":
		roomCount[roomID] = false
		msgCount.Messages++
		if sender == s.serverName {
			msgCount.SentMessages++
		}
	case "m.room.encrypted":
		roomCount[roomID] = true
		msgCount.MessagesE2EE++
		if sender == s.serverName {
			msgCount.SentMessagesE2EE++
		}
	default:
		return
	}
	s.msgCounts[s.serverName] = msgCount
	s.roomCounts[s.serverName] = roomCount

	for serverName, stats := range s.msgCounts {
		var normalRooms, encryptedRooms int64 = 0, 0
		for _, isEncrypted := range s.roomCounts[s.serverName] {
			if isEncrypted {
				encryptedRooms++
			} else {
				normalRooms++
			}
		}
		err := s.db.UpsertDailyRoomsMessages(ctx, serverName, stats, normalRooms, encryptedRooms)
		if err != nil {
			log.WithError(err).Errorf("failed to upsert daily messages")
		}
		// Clear stats if we successfully stored it
		if err == nil {
			stats.Messages = 0
			stats.SentMessages = 0
			stats.MessagesE2EE = 0
			stats.SentMessagesE2EE = 0
			s.msgCounts[serverName] = stats
		}
	}
}

func (s *OutputRoomEventConsumer) handleRoomUpgrade(ctx context.Context, oldRoomID, newRoomID string, localMembers []*localMembership, roomSize int) error {
	for _, membership := range localMembers {
		// Copy any existing push rules from old -> new room
		changed, err := s.copyPushrules(ctx, oldRoomID, newRoomID, membership.UserID, membership.Localpart, membership.Domain)
		if err != nil {
			return err
		}
		// Inform the SyncAPI about the updated push_rules
		if changed {
			if err = s.syncProducer.SendAccountData(membership.UserID, eventutil.AccountData{
				Type: "m.push_rules",
			}); err != nil {
				return err
			}
		}

		// preserve m.direct room state
		changed, err = s.updateMDirect(ctx, oldRoomID, newRoomID, membership.Localpart, membership.Domain, roomSize)
		if err != nil {
			return err
		}
		// Inform the SyncAPI about the updated m.direct
		if changed {
			if err = s.syncProducer.SendAccountData(membership.UserID, eventutil.AccountData{
				Type: "m.direct",
			}); err != nil {
				return err
			}
		}

		// copy existing m.tag entries, if any
		changed, err = s.copyTags(ctx, oldRoomID, newRoomID, membership.Localpart, membership.Domain)
		if err != nil {
			return err
		}
		// Inform the SyncAPI about the updated m.tag
		if changed {
			if err = s.syncProducer.SendAccountData(membership.UserID, eventutil.AccountData{
				RoomID: newRoomID,
				Type:   "m.tag",
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// copyPushrulesFromPredecessor copies the push rules a user has for the room
// that roomID replaced, if there is one, to roomID.
func (s *OutputRoomEventConsumer) copyPushrulesFromPredecessor(ctx context.Context, roomID string, member *localMembership) error {
	createTuple := gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomCreate}
	var res rsapi.QueryCurrentStateResponse
	if err := s.rsAPI.QueryCurrentState(ctx, &rsapi.QueryCurrentStateRequest{
		RoomID:      roomID,
		StateTuples: []gomatrixserverlib.StateKeyTuple{createTuple},
	}, &res); err != nil {
		return err
	}
	create := res.StateEvents[createTuple]
	if create == nil {
		return nil
	}
	oldRoomID := gjson.GetBytes(create.Content(), "predecessor.room_id").Str
	if oldRoomID == "" {
		return nil
	}
	changed, err := s.copyPushrules(ctx, oldRoomID, roomID, member.UserID, member.Localpart, member.Domain)
	if err != nil || !changed {
		return err
	}
	return s.syncProducer.SendAccountData(member.UserID, eventutil.AccountData{Type: "m.push_rules"})
}

func (s *OutputRoomEventConsumer) copyPushrules(ctx context.Context, oldRoomID, newRoomID, userID, localpart string, serverName spec.ServerName) (hasChanges bool, err error) {
	defer pushrules.LockForUser(userID)()

	pushRules, err := s.db.QueryPushRules(ctx, localpart, serverName)
	if err != nil {
		return false, err
	}
	if pushRules == nil {
		return false, err
	}

	var oldRule *pushrules.Rule
	for _, roomRule := range pushRules.Global.Room {
		switch roomRule.RuleID {
		case newRoomID:
			// Already copied, e.g. on the upgrade and again when the user joined.
			return false, nil
		case oldRoomID:
			oldRule = roomRule
		}
	}
	if oldRule == nil {
		return false, nil
	}

	cpRule := *oldRule
	cpRule.RuleID = newRoomID
	pushRules.Global.Room = append(pushRules.Global.Room, &cpRule)
	rulesBytes, err := json.Marshal(pushRules)
	if err != nil {
		return false, err
	}
	if err = s.db.SaveAccountData(ctx, localpart, serverName, "", "m.push_rules", rulesBytes); err != nil {
		return false, err
	}
	return true, nil
}

// updateMDirect copies the "is_direct" flag from oldRoomID to newROomID.
func (s *OutputRoomEventConsumer) updateMDirect(ctx context.Context, oldRoomID, newRoomID, localpart string, serverName spec.ServerName, roomSize int) (hasChanges bool, err error) {
	// this is most likely not a DM, so skip updating m.direct state
	if roomSize > 2 { //nolint:mnd
		return false, nil
	}
	// Get direct message state
	directChatsRaw, err := s.db.GetAccountDataByType(ctx, localpart, serverName, "", "m.direct")
	if err != nil {
		return false, fmt.Errorf("failed to get m.direct from database: %w", err)
	}
	directChats := gjson.ParseBytes(directChatsRaw)
	newDirectChats := make(map[string][]string)
	// iterate over all userID -> roomIDs
	var found bool
	directChats.ForEach(func(userID, roomIDs gjson.Result) bool {
		for _, roomID := range roomIDs.Array() {
			newDirectChats[userID.Str] = append(newDirectChats[userID.Str], roomID.Str)
			// add the new roomID to m.direct
			if roomID.Str == oldRoomID {
				found = true
				newDirectChats[userID.Str] = append(newDirectChats[userID.Str], newRoomID)
			}
		}
		return true
	})

	// Only hit the database if we found the old room as a DM for this user
	if found {
		var data []byte
		data, err = json.Marshal(newDirectChats)
		if err != nil {
			return false, err
		}
		if err = s.db.SaveAccountData(ctx, localpart, serverName, "", "m.direct", data); err != nil {
			return false, fmt.Errorf("failed to update m.direct state: %w", err)
		}
		return true, nil
	}

	return false, nil
}

func (s *OutputRoomEventConsumer) copyTags(ctx context.Context, oldRoomID, newRoomID, localpart string, serverName spec.ServerName) (hasChanges bool, err error) {
	tag, err := s.db.GetAccountDataByType(ctx, localpart, serverName, oldRoomID, "m.tag")
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return false, err
	}
	if tag == nil {
		return false, nil
	}
	if err := s.db.SaveAccountData(ctx, localpart, serverName, newRoomID, "m.tag", tag); err != nil {
		return false, err
	}
	return true, nil
}

func (s *OutputRoomEventConsumer) processMessage(ctx context.Context, event *rstypes.HeaderedEvent, streamPos uint64) (retErr error) {
	members, roomSize, err := s.localRoomMembers(ctx, event.RoomID().String())
	if err != nil {
		return fmt.Errorf("s.localRoomMembers: %w", err)
	}
	var pushRuleCopyErr error
	// Keep a copy failure even when a later step also fails: onMessage only
	// refuses the message while this error is present, so acknowledging it
	// would permanently drop the predecessor push rule.
	defer func() {
		retErr = errors.Join(pushRuleCopyErr, retErr)
	}()

	switch {
	case event.Type() == spec.MRoomMember:
		cevent, clientEvErr := synctypes.ToClientEvent(event, synctypes.FormatAll, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
			return s.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
		})
		if clientEvErr != nil {
			return clientEvErr
		}
		var member *localMembership
		member, err = newLocalMembership(cevent)
		if err != nil {
			return fmt.Errorf("newLocalMembership: %w", err)
		}
		if member.Membership == spec.Invite && member.Domain == s.cfg.Matrix.ServerName {
			// localRoomMembers only adds joined members. An invite
			// should also be pushed to the target user.
			members = append(members, member)
		}
		if member.Membership == spec.Join && s.cfg.Matrix.IsLocalServerName(member.Domain) {
			// A user joining the room that replaced another keeps their push rules
			// for it, including when the room was upgraded on another server.
			if err = s.copyPushrulesFromPredecessor(ctx, event.RoomID().String(), member); err != nil {
				log.WithError(err).Errorf("UserAPI: failed to copy push rules from the predecessor of a room")
				pushRuleCopyErr = fmt.Errorf("%w: %w", errRetryPushRuleCopy, err)
			}
		}
		if (member.Membership == spec.Leave || member.Membership == spec.Ban) && !s.cfg.Matrix.IsLocalServerName(member.Domain) {
			var shared rsapi.QueryUsersSharingRoomWithLocalResponse
			if err := s.rsAPI.QueryUsersSharingRoomWithLocal(ctx, &rsapi.QueryUsersSharingRoomWithLocalRequest{UserIDs: []string{member.UserID}}, &shared); err == nil && len(shared.UserIDs) == 0 {
				if s.keyDB != nil {
					if err := s.keyDB.DeleteStaleDeviceLists(ctx, []string{member.UserID}); err != nil {
						log.WithError(err).Warnf("failed to delete stale device list for %s", member.UserID)
					}
				}
			}
		}
	case event.Type() == "m.room.tombstone" && event.StateKeyEquals(""):
		// Handle room upgrades
		oldRoomID := event.RoomID().String()
		newRoomID := gjson.GetBytes(event.Content(), "replacement_room").Str
		if err = s.handleRoomUpgrade(ctx, oldRoomID, newRoomID, members, roomSize); err != nil {
			// while inconvenient, this shouldn't stop us from sending push notifications
			log.WithError(err).Errorf("UserAPI: failed to handle room upgrade for users")
		}
	}

	// TODO: run in parallel with localRoomMembers.
	roomName, err := s.roomName(ctx, event)
	if err != nil {
		return fmt.Errorf("s.roomName: %w", err)
	}

	log.WithFields(log.Fields{
		"event_id":    event.EventID(),
		"room_id":     event.RoomID().String(),
		"num_members": len(members),
		"room_size":   roomSize,
	}).Tracef("Notifying members")

	// Notification.UserIsTarget is a per-member field, so we
	// cannot group all users in a single request.
	//
	// TODO: does it have to be set? It's not required, and
	// removing it means we can send all notifications to
	// e.g. Element's Push gateway in one go.
	var notified []string
	for _, mem := range members {
		inserted, err := s.notifyLocal(ctx, event, mem, roomSize, roomName, streamPos)
		if inserted {
			notified = append(notified, mem.UserID)
		}
		if err != nil {
			log.WithFields(log.Fields{
				"localpart": mem.Localpart,
			}).WithError(err).Error("Unable to push to local user")
			continue
		}
	}

	var users []string
	threadID, err := receipts.EventThreadID(ctx, s.rsAPI, event)
	if err == nil {
		users, err = s.db.StoreNotificationEvent(ctx, event.RoomID().String(), event.EventID(), threadID, streamPos)
	}
	// Counts are published once the event's thread is recorded, so that a
	// new notification is counted in its thread rather than the main timeline.
	// They are published even if recording failed, counting the event in the
	// main timeline, because the notifications are already stored and pushed.
	for _, userID := range notified {
		if slices.Contains(users, userID) {
			continue
		}
		if publishErr := s.syncProducer.GetAndSendNotificationData(ctx, userID, event.RoomID().String()); publishErr != nil {
			log.WithField("user_id", userID).WithError(publishErr).Error("Unable to send notification counts")
		}
	}
	if err != nil {
		return err
	}
	for _, userID := range users {
		if err = s.syncProducer.GetAndSendNotificationData(ctx, userID, event.RoomID().String()); err != nil {
			return err
		}
		localpart, domain, err := gomatrixserverlib.SplitID('@', userID)
		if err != nil {
			return err
		}
		if err = util.NotifyUserCountsAsync(ctx, s.pgClient, localpart, domain, s.db); err != nil {
			return err
		}
	}
	return nil
}

type localMembership struct {
	gomatrixserverlib.MemberContent
	UserID    string
	Localpart string
	Domain    spec.ServerName
}

func newLocalMembership(event *synctypes.ClientEvent) (*localMembership, error) {
	if event.StateKey == nil {
		return nil, fmt.Errorf("missing state_key")
	}

	var member localMembership
	if err := json.Unmarshal(event.Content, &member.MemberContent); err != nil {
		return nil, err
	}

	localpart, domain, err := gomatrixserverlib.SplitID('@', *event.StateKey)
	if err != nil {
		return nil, err
	}

	member.UserID = *event.StateKey
	member.Localpart = localpart
	member.Domain = domain
	return &member, nil
}

// localRoomMembers fetches the current local members of a room, and
// the total number of members.
func (s *OutputRoomEventConsumer) localRoomMembers(ctx context.Context, roomID string) ([]*localMembership, int, error) {
	// Get only locally joined users to avoid unmarshalling and caching
	// membership events we only use to calculate the room size.
	req := &rsapi.QueryMembershipsForRoomRequest{
		RoomID:     roomID,
		JoinedOnly: true,
		LocalOnly:  true,
	}
	var res rsapi.QueryMembershipsForRoomResponse
	if err := s.rsAPI.QueryMembershipsForRoom(ctx, req, &res); err != nil {
		return nil, 0, err
	}

	// Since we only queried locally joined users above,
	// we also need to ask the roomserver about the joined user count.
	totalCount, err := s.rsAPI.JoinedUserCount(ctx, roomID)
	if err != nil {
		return nil, 0, err
	}

	var members []*localMembership
	for _, event := range res.JoinEvents {
		// Filter out invalid join events
		if event.StateKey == nil {
			continue
		}
		if *event.StateKey == "" {
			continue
		}
		// We're going to trust the Query from above to really just return
		// local users
		member, err := newLocalMembership(&event)
		if err != nil {
			log.WithError(err).Errorf("Parsing MemberContent")
			continue
		}

		members = append(members, member)
	}

	return members, totalCount, nil
}

// roomName returns the name in the event (if type==m.room.name), or
// looks it up in roomserver. If there is no name,
// m.room.canonical_alias is consulted. Returns an empty string if the
// room has no name.
func (s *OutputRoomEventConsumer) roomName(ctx context.Context, event *rstypes.HeaderedEvent) (string, error) {
	if event.Type() == spec.MRoomName {
		name, err := unmarshalRoomName(event)
		if err != nil {
			return "", err
		}

		if name != "" {
			return name, nil
		}
	}

	// Special case for invites, as we don't store any "current state" for these events,
	// we need to make sure that, if present, the m.room.name is sent as well.
	if event.Type() == spec.MRoomMember &&
		gjson.GetBytes(event.Content(), "membership").Str == "invite" {
		invState := gjson.GetBytes(event.JSON(), "unsigned.invite_room_state")
		for _, ev := range invState.Array() {
			if ev.Get("type").Str == spec.MRoomName {
				name := ev.Get("content.name").Str
				return name, nil
			}
		}
	}

	req := &rsapi.QueryCurrentStateRequest{
		RoomID:      event.RoomID().String(),
		StateTuples: []gomatrixserverlib.StateKeyTuple{roomNameTuple, canonicalAliasTuple},
	}
	var res rsapi.QueryCurrentStateResponse

	if err := s.rsAPI.QueryCurrentState(ctx, req, &res); err != nil {
		return "", nil
	}

	if eventS := res.StateEvents[roomNameTuple]; eventS != nil {
		return unmarshalRoomName(eventS)
	}

	if event.Type() == spec.MRoomCanonicalAlias {
		alias, err := unmarshalCanonicalAlias(event)
		if err != nil {
			return "", err
		}

		if alias != "" {
			return alias, nil
		}
	}

	if event = res.StateEvents[canonicalAliasTuple]; event != nil {
		return unmarshalCanonicalAlias(event)
	}

	return "", nil
}

var (
	canonicalAliasTuple = gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomCanonicalAlias}
	roomNameTuple       = gomatrixserverlib.StateKeyTuple{EventType: spec.MRoomName}
)

func unmarshalRoomName(event *rstypes.HeaderedEvent) (string, error) {
	var nc eventutil.NameContent
	if err := json.Unmarshal(event.Content(), &nc); err != nil {
		return "", fmt.Errorf("unmarshaling NameContent: %w", err)
	}

	return nc.Name, nil
}

func unmarshalCanonicalAlias(event *rstypes.HeaderedEvent) (string, error) {
	var cac eventutil.CanonicalAliasContent
	if err := json.Unmarshal(event.Content(), &cac); err != nil {
		return "", fmt.Errorf("unmarshaling CanonicalAliasContent: %w", err)
	}

	return cac.Alias, nil
}

// notifyLocal finds the right push actions for a local user, given an event.
// It reports whether it stored a notification, even when a later step fails.
func (s *OutputRoomEventConsumer) notifyLocal(ctx context.Context, event *rstypes.HeaderedEvent, mem *localMembership, roomSize int, roomName string, streamPos uint64) (bool, error) {
	actions, err := s.evaluatePushRules(ctx, event, mem, roomSize)
	if err != nil {
		return false, fmt.Errorf("s.evaluatePushRules: %w", err)
	}
	a, tweaks, err := pushrules.ActionsToTweaks(actions)
	if err != nil {
		return false, fmt.Errorf("pushrules.ActionsToTweaks: %w", err)
	}

	if a != pushrules.NotifyAction {
		log.WithFields(log.Fields{
			"event_id":  event.EventID(),
			"room_id":   event.RoomID().String(),
			"localpart": mem.Localpart,
		}).Tracef("Push rule evaluation rejected the event")
		return false, nil
	}

	devicesByURLAndFormat, profileTag, err := s.localPushDevices(ctx, mem.Localpart, mem.Domain, tweaks)
	if err != nil {
		return false, fmt.Errorf("s.localPushDevices: %w", err)
	}
	clientEvent, err := synctypes.ToClientEvent(event, synctypes.FormatSync, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return s.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})
	if err != nil {
		return false, err
	}

	n := &api.Notification{
		Actions: actions,
		// UNSPEC: the spec doesn't say this is a ClientEvent, but the
		// fields seem to match. room_id should be missing, which
		// matches the behavior of FormatSync.
		Event: *clientEvent,
		// TODO: this is per-device, but it's not part of the primary
		// key. So inserting one notification per profile tag doesn't
		// make sense. What is this supposed to be? Sytests require it
		// to "work", but they only use a single device.
		ProfileTag: profileTag,
		RoomID:     event.RoomID().String(),
		TS:         spec.AsTimestamp(time.Now()),
	}
	if err = s.db.InsertNotification(ctx, mem.Localpart, mem.Domain, event.EventID(), streamPos, tweaks, n); err != nil {
		return false, fmt.Errorf("s.db.InsertNotification: %w", err)
	}

	// We do this after InsertNotification. Thus, this should always return >=1.
	userNumUnreadNotifs, err := s.db.GetNotificationCount(ctx, mem.Localpart, mem.Domain, tables.AllNotifications)
	if err != nil {
		return true, fmt.Errorf("s.db.GetNotificationCount: %w", err)
	}

	log.WithFields(log.Fields{
		"event_id":   event.EventID(),
		"room_id":    event.RoomID().String(),
		"localpart":  mem.Localpart,
		"num_urls":   len(devicesByURLAndFormat),
		"num_unread": userNumUnreadNotifs,
	}).Trace("Notifying single member")

	// Push gateways are out of our control, and we cannot risk
	// looking up the server on a misbehaving push gateway. Each user
	// receives a goroutine now that all internal API calls have been
	// made.
	//
	// TODO: think about bounding this to one per user, and what
	// ordering guarantees we must provide.
	process.Go(process.Lifetime(ctx), func() {
		// This background processing cannot be tied to a request.
		// We're generous with the "global" timeout, each HTTP request gets its own context with
		// at most 30 seconds below.
		ctx, cancel := context.WithTimeout(process.Lifetime(ctx), 5*time.Minute) //nolint:mnd
		defer cancel()

		var rejected []*pushgateway.Device
		for url, fmts := range devicesByURLAndFormat {
			for format, devices := range fmts {
				// TODO: support "email".
				if !strings.HasPrefix(url, "http") {
					continue
				}

				// UNSPEC: the specification suggests there can be
				// more than one device per request. There is at least
				// one Sytest that expects one HTTP request per
				// device, rather than per URL. For now, we must
				// notify each one separately.
				// Delivery begins inside the consumer callback, which the
				// fetch and acknowledgement barriers do not cover.
				if err := jetstream.Owned(s.jetstream); err != nil {
					log.WithError(err).Warn("Not notifying pushers: ownership is no longer held")
					return
				}
				for _, dev := range devices {
					// Give each HTTP request its own context.
					httpCtx, httpCancel := context.WithTimeout(ctx, 30*time.Second) //nolint:mnd
					rej, err := s.notifyHTTP(httpCtx, event, url, format, []*pushgateway.Device{dev}, mem.Localpart, roomName, int(userNumUnreadNotifs), tweaks)
					httpCancel()
					if err != nil {
						log.WithFields(log.Fields{
							"event_id":  event.EventID(),
							"localpart": mem.Localpart,
						}).WithError(err).Errorf("Unable to notify HTTP pusher")
						continue
					}
					rejected = append(rejected, rej...)
				}
			}
		}

		if len(rejected) > 0 {
			s.deleteRejectedPushers(ctx, rejected, mem.Localpart, mem.Domain)
		}
	})

	return true, nil
}

// evaluatePushRules fetches and evaluates the push rules of a local
// user. Returns actions (including dont_notify).
func (s *OutputRoomEventConsumer) evaluatePushRules(ctx context.Context, event *rstypes.HeaderedEvent, mem *localMembership, roomSize int) ([]*pushrules.Action, error) {
	user := ""
	sender, err := s.rsAPI.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
	if err == nil {
		user = sender.String()
	}
	if user == mem.UserID {
		// SPEC: Homeservers MUST NOT notify the Push Gateway for
		// events that the user has sent themselves.
		return nil, nil
	}

	// Get accountdata to check if the event.Sender() is ignored by mem.LocalPart
	data, err := s.db.GetAccountDataByType(ctx, mem.Localpart, mem.Domain, "", "m.ignored_user_list")
	if err != nil {
		return nil, err
	}
	if data != nil {
		ignored := types.IgnoredUsers{}
		err = json.Unmarshal(data, &ignored)
		if err != nil {
			return nil, err
		}
		if _, ok := ignored.List[sender.String()]; ok {
			return nil, fmt.Errorf("user %s is ignored", sender.String())
		}
	}
	ruleSets, err := s.db.QueryPushRules(ctx, mem.Localpart, mem.Domain)
	if err != nil {
		return nil, err
	}

	ec := &ruleSetEvalContext{
		ctx:      ctx,
		rsAPI:    s.rsAPI,
		mem:      mem,
		roomID:   event.RoomID().String(),
		roomSize: roomSize,
	}
	eval := pushrules.NewRuleSetEvaluator(ec, &ruleSets.Global)
	rule, err := eval.MatchEvent(event.PDU, func(roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return s.rsAPI.QueryUserIDForSender(ctx, roomID, senderID)
	})
	if err != nil {
		return nil, err
	}
	if rule == nil {
		// SPEC: If no rules match an event, the homeserver MUST NOT
		// notify the Push Gateway for that event.
		return nil, nil
	}

	log.WithFields(log.Fields{
		"event_id":  event.EventID(),
		"room_id":   event.RoomID().String(),
		"localpart": mem.Localpart,
		"rule_id":   rule.RuleID,
	}).Trace("Matched a push rule")

	return rule.Actions, nil
}

type ruleSetEvalContext struct {
	ctx      context.Context
	rsAPI    rsapi.UserRoomserverAPI
	mem      *localMembership
	roomID   string
	roomSize int
}

func (rse *ruleSetEvalContext) UserDisplayName() string { return rse.mem.DisplayName }

func (rse *ruleSetEvalContext) RoomMemberCount() (int, error) { return rse.roomSize, nil }

func (rse *ruleSetEvalContext) HasPowerLevel(senderID spec.SenderID, levelKey string) (bool, error) {
	req := &rsapi.QueryLatestEventsAndStateRequest{
		RoomID: rse.roomID,
		StateToFetch: []gomatrixserverlib.StateKeyTuple{
			{EventType: spec.MRoomPowerLevels, StateKey: ""},
			{EventType: spec.MRoomCreate, StateKey: ""},
		},
	}
	var res rsapi.QueryLatestEventsAndStateResponse
	if err := rse.rsAPI.QueryLatestEventsAndState(rse.ctx, req, &res); err != nil {
		return false, err
	}
	var createEvent, plEvent *rstypes.HeaderedEvent
	for i, ev := range res.StateEvents {
		if ev.Type() == spec.MRoomCreate {
			createEvent = res.StateEvents[i]
		} else if ev.Type() == spec.MRoomPowerLevels {
			plEvent = res.StateEvents[i]
		}
	}
	if createEvent == nil {
		return false, fmt.Errorf("missing room create event for notification permissions")
	}
	verImpl := gomatrixserverlib.MustGetRoomVersion(createEvent.Version())
	if verImpl.PrivilegedCreators() && slices.Contains(gomatrixserverlib.CreatorsFromCreateEvent(createEvent), string(senderID)) {
		return true, nil
	}
	if plEvent == nil {
		return senderID == createEvent.SenderID(), nil
	}
	plc, err := gomatrixserverlib.NewPowerLevelContentFromEvent(plEvent.PDU)
	if err != nil {
		return false, err
	}
	return plc.UserLevel(senderID) >= plc.NotificationLevel(levelKey), nil
}

// localPushDevices pushes to the configured devices of a local
// user. The map keys are [url][format].
func (s *OutputRoomEventConsumer) localPushDevices(ctx context.Context, localpart string, serverName spec.ServerName, tweaks map[string]any) (map[string]map[string][]*pushgateway.Device, string, error) {
	pusherDevices, err := util.GetPushDevices(ctx, localpart, serverName, tweaks, s.db)
	if err != nil {
		return nil, "", fmt.Errorf("util.GetPushDevices: %w", err)
	}

	var profileTag string
	devicesByURL := make(map[string]map[string][]*pushgateway.Device, len(pusherDevices))
	for _, pusherDevice := range pusherDevices {
		if profileTag == "" {
			profileTag = pusherDevice.Pusher.ProfileTag
		}

		url := pusherDevice.URL
		if devicesByURL[url] == nil {
			devicesByURL[url] = make(map[string][]*pushgateway.Device, 2)
		}
		devicesByURL[url][pusherDevice.Format] = append(devicesByURL[url][pusherDevice.Format], &pusherDevice.Device)
	}

	return devicesByURL, profileTag, nil
}

// notifyHTTP performs a notificatation to a Push Gateway.
func (s *OutputRoomEventConsumer) notifyHTTP(ctx context.Context, event *rstypes.HeaderedEvent, url, format string, devices []*pushgateway.Device, localpart, roomName string, userNumUnreadNotifs int, tweaks map[string]any) ([]*pushgateway.Device, error) {
	logger := log.WithFields(log.Fields{
		"event_id":    event.EventID(),
		"url":         url,
		"localpart":   localpart,
		"num_devices": len(devices),
	})

	// Set high priority so push gateways wake the device immediately
	prio := pushgateway.LowPrio
	if event.Type() == "m.room.encrypted" || tweaks["highlight"] == true || tweaks["sound"] != nil {
		prio = pushgateway.HighPrio
	}

	var req pushgateway.NotifyRequest
	switch format {
	case "event_id_only":
		req = pushgateway.NotifyRequest{
			Notification: pushgateway.Notification{
				Counts: &pushgateway.Counts{
					Unread: userNumUnreadNotifs,
				},
				Devices: devices,
				EventID: event.EventID(),
				Prio:    prio,
				RoomID:  event.RoomID().String(),
			},
		}

	default:
		sender, err := s.rsAPI.QueryUserIDForSender(ctx, event.RoomID(), event.SenderID())
		if err != nil {
			logger.WithError(err).Errorf("Failed to get userID for sender %s", event.SenderID())
			return nil, err
		}
		req = pushgateway.NotifyRequest{
			Notification: pushgateway.Notification{
				Content: event.Content(),
				Counts: &pushgateway.Counts{
					Unread: userNumUnreadNotifs,
				},
				Devices:  devices,
				EventID:  event.EventID(),
				ID:       event.EventID(),
				Prio:     prio,
				RoomID:   event.RoomID().String(),
				RoomName: roomName,
				Sender:   sender.String(),
				Type:     event.Type(),
			},
		}
		if mem, memberErr := event.Membership(); memberErr == nil {
			req.Notification.Membership = mem
		}
		userID, err := spec.NewUserID(fmt.Sprintf("@%s:%s", localpart, s.cfg.Matrix.ServerName), true)
		if err != nil {
			logger.WithError(err).Errorf("Failed to convert local user to userID %s", localpart)
			return nil, err
		}
		localSender, err := s.rsAPI.QuerySenderIDForUser(ctx, event.RoomID(), *userID)
		if err != nil {
			logger.WithError(err).Errorf("Failed to get local user senderID for room %s: %s", userID.String(), event.RoomID().String())
			return nil, err
		} else if localSender == nil {
			logger.WithError(err).Errorf("Failed to get local user senderID for room %s: %s", userID.String(), event.RoomID().String())
			return nil, fmt.Errorf("no sender ID for user %s in %s", userID.String(), event.RoomID().String())
		}
		if event.StateKey() != nil && *event.StateKey() == string(*localSender) {
			req.Notification.UserIsTarget = true
		}
	}

	logger.Tracef("Notifying push gateway %s", url)
	var res pushgateway.NotifyResponse
	if err := s.pgClient.Notify(ctx, url, &req, &res); err != nil {
		logger.WithError(err).Errorf("Failed to notify push gateway %s", url)
		return nil, err
	}
	logger.WithField("num_rejected", len(res.Rejected)).Trace("Push gateway result")

	if len(res.Rejected) == 0 {
		return nil, nil
	}

	devMap := make(map[string]*pushgateway.Device, len(devices))
	for _, d := range devices {
		devMap[d.PushKey] = d
	}
	rejected := make([]*pushgateway.Device, 0, len(res.Rejected))
	for _, pushKey := range res.Rejected {
		d := devMap[pushKey]
		if d != nil {
			rejected = append(rejected, d)
		}
	}

	return rejected, nil
}

// deleteRejectedPushers deletes the pushers associated with the given devices.
func (s *OutputRoomEventConsumer) deleteRejectedPushers(ctx context.Context, devices []*pushgateway.Device, localpart string, serverName spec.ServerName) {
	log.WithFields(log.Fields{
		"localpart":   localpart,
		"app_id0":     devices[0].AppID,
		"num_devices": len(devices),
	}).Warnf("Deleting pushers rejected by the HTTP push gateway")

	for _, d := range devices {
		if err := s.db.RemovePusher(ctx, d.AppID, d.PushKey, localpart, serverName); err != nil {
			log.WithFields(log.Fields{
				"localpart": localpart,
			}).WithError(err).Errorf("Unable to delete rejected pusher")
		}
	}
}
