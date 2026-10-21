package internal

import (
	"context"
	"crypto/ed25519"
	"fmt"
	"slices"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/getsentry/sentry-go"
	"github.com/matrix-org/util"
	"github.com/nats-io/nats.go"
	"github.com/sirupsen/logrus"

	asAPI "codefloe.com/pat-s/zendrite/appservice/api"
	fsAPI "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/roomserver/acls"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/internal/input"
	"codefloe.com/pat-s/zendrite/roomserver/internal/perform"
	"codefloe.com/pat-s/zendrite/roomserver/internal/query"
	"codefloe.com/pat-s/zendrite/roomserver/producers"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/setup/process"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// defaultPurgeWaitTimeout is the maximum time a rejoin attempt will wait for
// an in-flight auto-purge to complete before proceeding.
const defaultPurgeWaitTimeout = 30 * time.Second

// RoomserverInternalAPI is an implementation of api.RoomserverInternalAPI.
type RoomserverInternalAPI struct {
	*input.Inputer
	*query.Queryer
	*perform.Inviter
	*perform.Joiner
	*perform.Peeker
	*perform.InboundPeeker
	*perform.Unpeeker
	*perform.Leaver
	*perform.Publisher
	*perform.Backfiller
	*perform.Forgetter
	*perform.Upgrader
	*perform.Admin
	*perform.Creator
	Knocker                *perform.Knocker
	ProcessContext         *process.ProcessContext
	DB                     storage.Database
	Cfg                    *config.Zendrite
	Cache                  caching.RoomServerCaches
	ServerName             spec.ServerName
	KeyRing                gomatrixserverlib.JSONVerifier
	ServerACLs             *acls.ServerACLs
	fsAPI                  fsAPI.RoomserverFederationAPI
	asAPI                  asAPI.AppServiceInternalAPI
	NATSClient             *nats.Conn
	JetStream              nats.JetStreamContext
	Durable                string
	InputRoomEventTopic    string // JetStream topic for new input room events
	OutputProducer         *producers.RoomEventProducer
	PerspectiveServerNames []spec.ServerName
	enableMetrics          bool
	defaultRoomVersion     gomatrixserverlib.RoomVersion
	PartialStateTracker    *PartialStateTracker
	PurgeTracker           *perform.PurgeTracker

	// joinedDuringPartialState holds, per room, the users who joined after our partial join.
	joinedDuringPartialState sync.Map
	unPartialStatedHooksMu   sync.Mutex
	unPartialStatedHooks   []func(ctx context.Context, roomID string)
}

func NewRoomserverAPI(
	processContext *process.ProcessContext, zendriteCfg *config.Zendrite, roomserverDB storage.Database,
	js nats.JetStreamContext, nc *nats.Conn, caches caching.RoomServerCaches, enableMetrics bool,
) *RoomserverInternalAPI {
	var perspectiveServerNames []spec.ServerName
	for _, kp := range zendriteCfg.FederationAPI.KeyPerspectives {
		perspectiveServerNames = append(perspectiveServerNames, kp.ServerName)
	}

	serverACLs := acls.NewServerACLs(roomserverDB)
	producer := &producers.RoomEventProducer{
		Topic:     zendriteCfg.Global.JetStream.Prefixed(jetstream.OutputRoomEvent),
		JetStream: js,
		ACLs:      serverACLs,
	}
	a := &RoomserverInternalAPI{
		ProcessContext:         processContext,
		DB:                     roomserverDB,
		Cfg:                    zendriteCfg,
		Cache:                  caches,
		ServerName:             zendriteCfg.Global.ServerName,
		PerspectiveServerNames: perspectiveServerNames,
		InputRoomEventTopic:    zendriteCfg.Global.JetStream.Prefixed(jetstream.InputRoomEvent),
		OutputProducer:         producer,
		JetStream:              js,
		NATSClient:             nc,
		Durable:                zendriteCfg.Global.JetStream.Durable("RoomserverInputConsumer"),
		ServerACLs:             serverACLs,
		enableMetrics:          enableMetrics,
		defaultRoomVersion:     zendriteCfg.RoomServer.DefaultRoomVersion,
		PartialStateTracker:    NewPartialStateTracker(),
		PurgeTracker:           perform.NewPurgeTracker(),
		// perform-er structs + queryer struct get initialized when we have a federation sender to use
	}
	return a
}

// SetFederationInputAPI passes in a federation input API reference so that we can
// avoid the chicken-and-egg problem of both the roomserver input API and the
// federation input API being interdependent.
func (r *RoomserverInternalAPI) SetFederationAPI(fsAPI fsAPI.RoomserverFederationAPI, keyRing *gomatrixserverlib.KeyRing) {
	r.fsAPI = fsAPI
	r.KeyRing = keyRing

	r.Queryer = &query.Queryer{
		DB:                r.DB,
		Cache:             r.Cache,
		IsLocalServerName: r.Cfg.Global.IsLocalServerName,
		ServerACLs:        r.ServerACLs,
		Cfg:               r.Cfg,
		FSAPI:             fsAPI,
	}

	r.Inputer = &input.Inputer{
		Cfg:                 &r.Cfg.RoomServer,
		ProcessContext:      r.ProcessContext,
		DB:                  r.DB,
		InputRoomEventTopic: r.InputRoomEventTopic,
		OutputProducer:      r.OutputProducer,
		JetStream:           r.JetStream,
		NATSClient:          r.NATSClient,
		Durable:             nats.Durable(r.Durable),
		ServerName:          r.ServerName,
		SigningIdentity:     r.SigningIdentityFor,
		FSAPI:               fsAPI,
		RSAPI:               r,
		KeyRing:             keyRing,
		ACLs:                r.ServerACLs,
		Queryer:             r.Queryer,
		EnableMetrics:       r.enableMetrics,
	}
	r.Inviter = &perform.Inviter{
		DB:      r.DB,
		Cfg:     &r.Cfg.RoomServer,
		FSAPI:   r.fsAPI,
		RSAPI:   r,
		Inputer: r.Inputer,
	}
	r.Joiner = &perform.Joiner{
		Cfg:              &r.Cfg.RoomServer,
		DB:               r.DB,
		FSAPI:            r.fsAPI,
		RSAPI:            r,
		Inputer:          r.Inputer,
		Queryer:          r.Queryer,
		PurgeTracker:     r.PurgeTracker,
		PurgeWaitTimeout: defaultPurgeWaitTimeout,
	}
	r.Peeker = &perform.Peeker{
		ServerName: r.ServerName,
		Cfg:        &r.Cfg.RoomServer,
		DB:         r.DB,
		FSAPI:      r.fsAPI,
		Inputer:    r.Inputer,
	}
	r.InboundPeeker = &perform.InboundPeeker{
		DB:      r.DB,
		Inputer: r.Inputer,
	}
	r.Unpeeker = &perform.Unpeeker{
		ServerName: r.ServerName,
		Cfg:        &r.Cfg.RoomServer,
		FSAPI:      r.fsAPI,
		Inputer:    r.Inputer,
	}
	r.Knocker = &perform.Knocker{
		Cfg:     &r.Cfg.RoomServer,
		DB:      r.DB,
		FSAPI:   r.fsAPI,
		RSAPI:   r,
		Inputer: r.Inputer,
	}
	r.Leaver = &perform.Leaver{
		Cfg:     &r.Cfg.RoomServer,
		DB:      r.DB,
		FSAPI:   r.fsAPI,
		RSAPI:   r,
		Inputer: r.Inputer,
	}
	r.Publisher = &perform.Publisher{
		DB: r.DB,
	}
	r.Backfiller = &perform.Backfiller{
		IsLocalServerName: r.Cfg.Global.IsLocalServerName,
		DB:                r.DB,
		FSAPI:             r.fsAPI,
		Querier:           r.Queryer,
		KeyRing:           r.KeyRing,
		// Perspective servers are trusted to not lie about server keys, so we will also
		// prefer these servers when backfilling (assuming they are in the room) rather
		// than trying random servers
		PreferServers: r.PerspectiveServerNames,
	}
	r.Forgetter = &perform.Forgetter{
		DB:    r.DB,
		RSAPI: r,
	}
	r.Upgrader = &perform.Upgrader{
		Cfg:    &r.Cfg.RoomServer,
		URSAPI: r,
	}
	r.Admin = &perform.Admin{
		DB:           r.DB,
		Cfg:          &r.Cfg.RoomServer,
		Inputer:      r.Inputer,
		Queryer:      r.Queryer,
		Leaver:       r.Leaver,
		RSAPI:        r,
		PurgeTracker: r.PurgeTracker,
	}
	r.Creator = &perform.Creator{
		DB:    r.DB,
		Cfg:   &r.Cfg.RoomServer,
		RSAPI: r,
	}

	if err := r.Start(); err != nil {
		logrus.WithError(err).Panic("failed to start roomserver input API")
	}

	// When auto-purge is enabled, sweep any rooms that are already eligible
	// at startup (e.g. left over from a previous run, or from a crash
	// mid-purge during the previous shutdown).
	if r.Cfg.RoomServer.AutoPurgeEnabled() {
		r.ProcessContext.Go(func() {
			if _, err := r.RunEmptyRoomsSweep(r.ProcessContext.Context()); err != nil {
				logrus.WithError(err).Warn("startup empty-rooms sweep failed")
			}
		})
	}
}

// RunEmptyRoomsSweep enumerates all rooms eligible for auto-purge under
// the configured mode and schedules an async auto-purge for each. Returns
// the number of rooms scheduled. Safe to call multiple times;
// AutoPurgeRoom coalesces duplicates. Returns 0 for AutoPurgeNever.
func (r *RoomserverInternalAPI) RunEmptyRoomsSweep(ctx context.Context) (int, error) {
	rooms, err := r.DB.PurgeableRooms(ctx, r.Cfg.RoomServer.AutoPurgeMode)
	if err != nil {
		return 0, err
	}
	for _, roomID := range rooms {
		r.AutoPurgeRoom(ctx, roomID, "startup_sweep")
	}
	if len(rooms) > 0 {
		logrus.WithField("count", len(rooms)).Info("startup empty-rooms sweep scheduled purges")
	}
	return len(rooms), nil
}

// ScheduleAutoPurgeIfEligible looks up the roomInfo for the given roomID
// and dispatches to Inputer.ScheduleAutoPurgeIfEmpty which respects the
// configured AutoPurgeMode. This is the entry point used from non-event
// code paths such as /forget where there is no roomInfo on hand.
func (r *RoomserverInternalAPI) ScheduleAutoPurgeIfEligible(ctx context.Context, roomID string) {
	if !r.Cfg.RoomServer.AutoPurgeEnabled() {
		return
	}
	roomInfo, err := r.DB.RoomInfo(ctx, roomID)
	if err != nil {
		logrus.WithError(err).WithField("room_id", roomID).Warn("auto-purge: failed to fetch room info")
		return
	}
	if roomInfo == nil {
		return
	}
	r.ScheduleAutoPurgeIfEmpty(ctx, roomInfo)
}

func (r *RoomserverInternalAPI) SetUserAPI(userAPI userapi.RoomserverUserAPI) {
	r.Leaver.UserAPI = userAPI
	r.Inputer.UserAPI = userAPI
}

func (r *RoomserverInternalAPI) SetAppserviceAPI(asAPI asAPI.AppServiceInternalAPI) {
	r.asAPI = asAPI
}

func (r *RoomserverInternalAPI) DefaultRoomVersion() gomatrixserverlib.RoomVersion {
	return r.defaultRoomVersion
}

// AutoForgetOnLeaveEnabled reports whether the server is configured to
// automatically forget rooms when a local user transitions to leave or
// ban. Used by the /capabilities endpoint to advertise
// m.forget_forced_upon_leave.
func (r *RoomserverInternalAPI) AutoForgetOnLeaveEnabled() bool {
	return r.Cfg.RoomServer.AutoForgetOnLeave
}

func (r *RoomserverInternalAPI) IsKnownRoom(ctx context.Context, roomID spec.RoomID) (bool, error) {
	return r.Inviter.IsKnownRoom(ctx, roomID)
}

func (r *RoomserverInternalAPI) StateQuerier() gomatrixserverlib.StateQuerier {
	return r.Inviter.StateQuerier()
}

func (r *RoomserverInternalAPI) HandleInvite(
	ctx context.Context, inviteEvent *types.HeaderedEvent,
) error {
	outputEvents, err := r.ProcessInviteMembership(ctx, inviteEvent)
	if err != nil {
		return err
	}
	return r.OutputProducer.ProduceRoomEvents(inviteEvent.RoomID().String(), outputEvents)
}

func (r *RoomserverInternalAPI) RetractPendingMembership(
	ctx context.Context, event *types.HeaderedEvent,
) (bool, error) {
	outputEvents, applied, err := r.Leaver.RetractPendingMembership(ctx, event)
	if err != nil || !applied {
		return applied, err
	}
	return true, r.OutputProducer.ProduceRoomEvents(event.RoomID().String(), outputEvents)
}

func (r *RoomserverInternalAPI) PerformCreateRoom(
	ctx context.Context, userID spec.UserID, roomID spec.RoomID, createRequest *api.PerformCreateRoomRequest,
) (string, *util.JSONResponse) {
	return r.Creator.PerformCreateRoom(ctx, userID, roomID, createRequest)
}

func (r *RoomserverInternalAPI) PerformInvite(
	ctx context.Context,
	req *api.PerformInviteRequest,
) error {
	return r.Inviter.PerformInvite(ctx, req)
}

func (r *RoomserverInternalAPI) PerformLeave(
	ctx context.Context,
	req *api.PerformLeaveRequest,
	res *api.PerformLeaveResponse,
) error {
	outputEvents, err := r.Leaver.PerformLeave(ctx, req, res)
	if err != nil {
		sentry.CaptureException(err)
		return err
	}
	if len(outputEvents) == 0 {
		return nil
	}
	return r.OutputProducer.ProduceRoomEvents(req.RoomID, outputEvents)
}

func (r *RoomserverInternalAPI) PerformKnock(
	ctx context.Context,
	req *api.PerformKnockRequest,
) (string, error) {
	roomID, outputEvents, err := r.Knocker.PerformKnock(ctx, req)
	if err != nil {
		return "", err
	}
	if len(outputEvents) > 0 {
		if err = r.OutputProducer.ProduceRoomEvents(roomID, outputEvents); err != nil {
			return "", err
		}
	}
	return roomID, nil
}

func (r *RoomserverInternalAPI) PerformForget(
	ctx context.Context,
	req *api.PerformForgetRequest,
	resp *api.PerformForgetResponse,
) error {
	return r.Forgetter.PerformForget(ctx, req, resp)
}

// GetOrCreateUserRoomPrivateKey gets the user room key for the specified user. If no key exists yet, a new one is created.
func (r *RoomserverInternalAPI) GetOrCreateUserRoomPrivateKey(ctx context.Context, userID spec.UserID, roomID spec.RoomID) (ed25519.PrivateKey, error) {
	key, err := r.DB.SelectUserRoomPrivateKey(ctx, userID, roomID)
	if err != nil {
		return nil, err
	}
	// no key found, create one
	if len(key) == 0 {
		_, key, err = ed25519.GenerateKey(nil)
		if err != nil {
			return nil, err
		}
		key, err = r.DB.InsertUserRoomPrivatePublicKey(ctx, userID, roomID, key)
		if err != nil {
			return nil, err
		}
	}
	return key, nil
}

func (r *RoomserverInternalAPI) StoreUserRoomPublicKey(ctx context.Context, senderID spec.SenderID, userID spec.UserID, roomID spec.RoomID) error {
	pubKeyBytes, err := senderID.RawBytes()
	if err != nil {
		return err
	}
	_, err = r.DB.InsertUserRoomPublicKey(ctx, userID, roomID, ed25519.PublicKey(pubKeyBytes))
	return err
}

func (r *RoomserverInternalAPI) SigningIdentityFor(ctx context.Context, roomID spec.RoomID, senderID spec.UserID) (fclient.SigningIdentity, error) {
	roomVersion, ok := r.Cache.GetRoomVersion(roomID.String())
	if !ok {
		roomInfo, err := r.DB.RoomInfo(ctx, roomID.String())
		if err != nil {
			return fclient.SigningIdentity{}, err
		}
		if roomInfo != nil {
			roomVersion = roomInfo.RoomVersion
		}
	}
	if roomVersion == gomatrixserverlib.RoomVersionPseudoIDs {
		privKey, err := r.GetOrCreateUserRoomPrivateKey(ctx, senderID, roomID)
		if err != nil {
			return fclient.SigningIdentity{}, err
		}
		return fclient.SigningIdentity{
			PrivateKey: privKey,
			KeyID:      "ed25519:1",
			ServerName: spec.ServerName(spec.SenderIDFromPseudoIDKey(privKey)),
		}, nil
	}
	identity, err := r.Cfg.Global.SigningIdentityFor(senderID.Domain())
	if err != nil {
		return fclient.SigningIdentity{}, err
	}
	return *identity, err
}

func (r *RoomserverInternalAPI) AssignRoomNID(ctx context.Context, roomID spec.RoomID, roomVersion gomatrixserverlib.RoomVersion) (roomNID types.RoomNID, err error) {
	return r.DB.AssignRoomNID(ctx, roomID, roomVersion)
}

func (r *RoomserverInternalAPI) InsertReportedEvent(
	ctx context.Context,
	roomID, eventID, reportingUserID, reason string,
	score int64,
) (int64, error) {
	return r.DB.InsertReportedEvent(ctx, roomID, eventID, reportingUserID, reason, score)
}

// InsertReportedRoom stores a room report (MSC4151).
func (r *RoomserverInternalAPI) InsertReportedRoom(ctx context.Context, roomID, reportingUserID, reason string) (int64, error) {
	return r.DB.InsertReportedRoom(ctx, roomID, reportingUserID, reason)
}

// QueryAdminRoomReports lists room reports for the admin API (MSC4151).
func (r *RoomserverInternalAPI) QueryAdminRoomReports(
	ctx context.Context, from, limit uint64, backwards bool, userID, roomID string,
) ([]api.QueryAdminRoomReportsResponse, int64, error) {
	return r.DB.QueryAdminRoomReports(ctx, from, limit, backwards, userID, roomID)
}

// MSC3706 Partial State Join methods.

// SetRoomPartialState marks a room as having partial state after a faster join.
func (r *RoomserverInternalAPI) SetRoomPartialState(ctx context.Context, roomNID types.RoomNID, joinEventID string, joinedVia string, serversInRoom []string, deviceListStreamID int64) error {
	var joinEventNID types.EventNID
	if joinEventID != "" {
		nids, err := r.DB.EventNIDs(ctx, []string{joinEventID})
		if err != nil {
			return fmt.Errorf("look up join event %s: %w", joinEventID, err)
		}
		joinEventNID = nids[joinEventID].EventNID
	}
	return r.DB.SetRoomPartialState(ctx, roomNID, joinEventNID, joinedVia, serversInRoom, deviceListStreamID)
}

// GetPartialStateJoinEventID returns the join event that started the partial state.
func (r *RoomserverInternalAPI) GetPartialStateJoinEventID(ctx context.Context, roomNID types.RoomNID) (string, error) {
	return r.DB.GetPartialStateJoinEventID(ctx, roomNID)
}

// IsRoomPartialState returns true if the room has partial state.
func (r *RoomserverInternalAPI) IsRoomPartialState(ctx context.Context, roomNID types.RoomNID) (bool, error) {
	return r.DB.IsRoomPartialState(ctx, roomNID)
}

// ClearRoomPartialState removes the partial state flag from a room
// Returns the device list stream ID that was stored at join time for device list replay.
func (r *RoomserverInternalAPI) ClearRoomPartialState(ctx context.Context, roomNID types.RoomNID) (int64, error) {
	// The sync API learns of the resync's state through the output stream, a little
	// after the flag is cleared. Keep waiters waiting until it has, instead of
	// letting them read its stale copy: they are released by NotifyUnPartialStated.
	if roomID, err := r.DB.RoomIDFromNID(ctx, roomNID); err == nil && roomID != "" {
		r.PartialStateTracker.MarkSyncPending(roomID, syncCatchUpTimeout)
		// Once the flag is gone there is no telling who joined after us, which the
		// hooks need to know, so note it now.
		if later, laterErr := r.DB.GetMembersJoinedAfterPartialJoin(ctx, roomNID); laterErr == nil {
			r.joinedDuringPartialState.Store(roomID, later)
		}
	}
	return r.DB.ClearRoomPartialState(ctx, roomNID)
}

// syncCatchUpTimeout bounds how long requests wait for the sync API to take in a
// resync's state before they go ahead without it.
const syncCatchUpTimeout = 5 * time.Second

// TakeMembersJoinedDuringPartialState returns the users who joined the room after our
// partial join, as noted when the room's partial state was cleared. ok is false when that
// is not known, for instance after a restart.
func (r *RoomserverInternalAPI) TakeMembersJoinedDuringPartialState(roomID string) (userIDs []string, ok bool) {
	v, ok := r.joinedDuringPartialState.LoadAndDelete(roomID)
	if !ok {
		return nil, false
	}
	userIDs, _ = v.([]string)
	return userIDs, true
}

// RegisterUnPartialStatedHook registers a function to call whenever a room finishes its
// partial state resync.
func (r *RoomserverInternalAPI) RegisterUnPartialStatedHook(hook func(ctx context.Context, roomID string)) {
	r.unPartialStatedHooksMu.Lock()
	defer r.unPartialStatedHooksMu.Unlock()
	r.unPartialStatedHooks = append(r.unPartialStatedHooks, hook)
}

func (r *RoomserverInternalAPI) runUnPartialStatedHooks(roomID string) {
	r.unPartialStatedHooksMu.Lock()
	hooks := slices.Clone(r.unPartialStatedHooks)
	r.unPartialStatedHooksMu.Unlock()
	for _, hook := range hooks {
		hook(r.ProcessContext.Context(), roomID)
	}
}

// AckUnPartialStated is called by the sync API once it has taken in the state of a
// room that finished its resync, releasing the requests waiting for that.
func (r *RoomserverInternalAPI) AckUnPartialStated(roomID string) {
	r.PartialStateTracker.NotifyUnPartialStated(roomID)
}

// GetPartialStateServers returns servers known to be in a partial state room.
func (r *RoomserverInternalAPI) GetPartialStateServers(ctx context.Context, roomNID types.RoomNID) ([]string, error) {
	return r.DB.GetPartialStateServers(ctx, roomNID)
}

// GetPartialStateJoinServer returns the server we joined through for a partial state room.
func (r *RoomserverInternalAPI) GetPartialStateJoinServer(ctx context.Context, roomNID types.RoomNID) (string, error) {
	return r.DB.GetPartialStateJoinServer(ctx, roomNID)
}

// GetPartialStateDeviceListStreamID returns the device list stream ID for a partial state room.
func (r *RoomserverInternalAPI) GetPartialStateDeviceListStreamID(ctx context.Context, roomNID types.RoomNID) (int64, error) {
	return r.DB.GetPartialStateDeviceListStreamID(ctx, roomNID)
}

// GetAllPartialStateRooms returns all rooms with partial state.
func (r *RoomserverInternalAPI) GetAllPartialStateRooms(ctx context.Context) ([]types.RoomNID, error) {
	return r.DB.GetAllPartialStateRooms(ctx)
}

// RoomInfoByNID returns room information for the given room NID.
func (r *RoomserverInternalAPI) RoomInfoByNID(ctx context.Context, roomNID types.RoomNID) (*types.RoomInfo, error) {
	return r.DB.RoomInfoByNID(ctx, roomNID)
}

// LatestEventIDs returns the latest event IDs and state snapshot for a room.
func (r *RoomserverInternalAPI) LatestEventIDs(ctx context.Context, roomNID types.RoomNID) ([]string, types.StateSnapshotNID, int64, error) {
	return r.DB.LatestEventIDs(ctx, roomNID)
}

// RoomIDFromNID returns the room ID for a given room NID.
func (r *RoomserverInternalAPI) RoomIDFromNID(ctx context.Context, roomNID types.RoomNID) (string, error) {
	return r.DB.RoomIDFromNID(ctx, roomNID)
}

// GetPartialStateRoomIDs returns the room IDs of all rooms currently in partial state (MSC3706 faster joins).
// This is used by sync to filter rooms that may have incomplete state.
func (r *RoomserverInternalAPI) GetPartialStateRoomIDs(ctx context.Context) ([]string, error) {
	roomNIDs, err := r.DB.GetAllPartialStateRooms(ctx)
	if err != nil {
		return nil, err
	}
	roomIDs := make([]string, 0, len(roomNIDs))
	for _, nid := range roomNIDs {
		roomID, err := r.DB.RoomIDFromNID(ctx, nid)
		if err != nil {
			// Skip rooms we can't look up
			continue
		}
		roomIDs = append(roomIDs, roomID)
	}
	return roomIDs, nil
}

// AwaitRoomFullState blocks until the room has full state, which is at once for a
// room that never had partial state. It returns when the context ends, however
// long the resync takes.
func (r *RoomserverInternalAPI) AwaitRoomFullState(ctx context.Context, roomID string) error {
	return r.PartialStateTracker.AwaitUnPartialState(ctx, roomID, func(ctx context.Context) (bool, error) {
		info, err := r.DB.RoomInfo(ctx, roomID)
		if err != nil || info == nil {
			return false, err
		}
		return r.DB.IsRoomPartialState(ctx, info.RoomNID)
	})
}

// NotifyUnPartialStated notifies observers that a room has completed its partial state resync.
// It emits an output event for downstream components (like syncapi) to take in the
// resynced state. Callers waiting for full state are released when the sync API
// acknowledges that event, or after a timeout, so that what they read next is
// consistent with the resync. Without an event to wait for they are released at once.
func (r *RoomserverInternalAPI) NotifyUnPartialStated(roomID string) {
	defer r.runUnPartialStatedHooks(roomID)
	release := func() {
		if r.PartialStateTracker != nil {
			r.PartialStateTracker.NotifyUnPartialStated(roomID)
		}
	}

	// Query local joined members to notify downstream components
	ctx := r.ProcessContext.Context()
	membershipsReq := &api.QueryMembershipsForRoomRequest{
		RoomID:     roomID,
		JoinedOnly: true,
		LocalOnly:  true,
	}
	membershipsRes := &api.QueryMembershipsForRoomResponse{}
	if err := r.QueryMembershipsForRoom(ctx, membershipsReq, membershipsRes); err != nil {
		logrus.WithError(err).WithField("room_id", roomID).Error("Failed to query memberships for un-partial-stated room")
		release()
		return
	}

	// Extract user IDs from membership events
	var joinedUserIDs []string
	for _, ev := range membershipsRes.JoinEvents {
		if ev.StateKey != nil && *ev.StateKey != "" {
			joinedUserIDs = append(joinedUserIDs, *ev.StateKey)
		}
	}

	if len(joinedUserIDs) == 0 {
		logrus.WithField("room_id", roomID).Debug("No local members in un-partial-stated room, skipping output event")
		release()
		return
	}

	// Emit output event to notify downstream components
	outputEvent := api.OutputEvent{
		Type: api.OutputTypeUnPartialStatedRoom,
		UnPartialStatedRoom: &api.OutputUnPartialStatedRoom{
			RoomID:        roomID,
			JoinedUserIDs: joinedUserIDs,
		},
	}

	if err := r.OutputProducer.ProduceRoomEvents(roomID, []api.OutputEvent{outputEvent}); err != nil {
		logrus.WithError(err).WithField("room_id", roomID).Error("Failed to produce un-partial-stated room event")
		release()
		return
	}

	// The sync API acknowledges the event when it has taken the state in. Do not
	// hold waiters for good if it never does.
	time.AfterFunc(syncCatchUpTimeout, release)

	logrus.WithFields(logrus.Fields{
		"room_id":    roomID,
		"user_count": len(joinedUserIDs),
	}).Info("Room completed partial state resync, notified downstream components")
}

// UpdateCurrentStateAfterResync updates the current state and memberships after a partial state resync.
// This is called after state events have been stored as outliers via SendStateAsOutliers.
// It creates a new state snapshot from the stored events, calculates the state delta,
// updates the membership table, and notifies downstream components (syncapi).
func (r *RoomserverInternalAPI) UpdateCurrentStateAfterResync(ctx context.Context, roomID string, stateEventIDs, anchorStateIDs, expectedLatestEventIDs []string) error {
	localLeftJoin, roomInfo, err := r.UpdateStateAfterResync(ctx, roomID, stateEventIDs, anchorStateIDs, expectedLatestEventIDs)
	if err != nil {
		return err
	}
	// ScheduleAutoPurgeIfEmpty is called here, after UpdateStateAfterResync's
	// deferred EndTransactionWithCheck has committed the membership changes.
	if localLeftJoin {
		r.ScheduleAutoPurgeIfEmpty(ctx, roomInfo)
	}
	return nil
}
