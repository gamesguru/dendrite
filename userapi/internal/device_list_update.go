// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/gomatrix"
	"github.com/matrix-org/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"

	fedsenderapi "codefloe.com/pat-s/zendrite/federationapi/api"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/userapi/api"
)

var deviceListUpdateCount = prometheus.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "zendrite",
		Subsystem: "keyserver",
		Name:      "device_list_update",
		Help:      "Number of times we have attempted to update device lists from this server",
	},
	[]string{"server"},
)

const requestTimeout = time.Second * 30

func init() { //nolint:gochecknoinits
	prometheus.MustRegister(
		deviceListUpdateCount,
	)
}

// DeviceListUpdater handles device list updates from remote servers.
//
// In the case where we have the prev_id for an update, the updater just stores the update (after acquiring a per-user lock).
// In the case where we do not have the prev_id for an update, the updater marks the user_id as stale and notifies
// a worker to get the latest device list for this user. Note: stream IDs are scoped per user so missing a prev_id
// for a (user, device) does not mean that DEVICE is outdated as the previous ID could be for a different device:
// we have to invalidate all devices for that user. Once the list has been fetched, the per-user lock is acquired and the
// updater stores the latest list along with the latest stream ID.
//
// On startup, the updater spins up N workers which are responsible for querying device keys from remote servers.
// Workers are scoped by homeserver domain, with one worker responsible for many domains, determined by hashing
// mod N the server name. Work is sent via a channel which just serves to "poke" the worker as the data is retrieved
// from the database (which allows us to batch requests to the same server). This has a number of desirable properties:
//   - We guarantee only 1 in-flight /keys/query request per server at any time as there is exactly 1 worker responsible
//     for that domain.
//   - We don't have unbounded growth in proportion to the number of servers (this is more important in a P2P world where
//     we have many many servers)
//   - We can adjust concurrency (at the cost of memory usage) by tuning N, to accommodate mobile devices vs servers.
//
// The downsides are that:
//   - Query requests can get queued behind other servers if they hash to the same worker, even if there are other free
//     workers elsewhere. Whilst suboptimal, provided we cap how long a single request can last (e.g using context timeouts)
//     we guarantee we will get around to it. Also, more users on a given server does not increase the number of requests
//     (as /keys/query allows multiple users to be specified) so being stuck behind matrix.org won't materially be any worse
//     than being stuck behind foo.bar
//
// In the event that the query fails, a lock is acquired and the server name along with the time to wait before retrying is
// set in a map. A restarter goroutine periodically probes this map and injects servers which are ready to be retried.
type DeviceListUpdater struct {
	process *process.ProcessContext
	// A map from user_id to a mutex. Used when we are missing prev IDs so we don't make more than 1
	// request to the remote server and race.
	// TODO: Put in an LRU cache to bound growth
	userIDToMutex map[string]*sync.Mutex
	mu            *sync.Mutex // protects UserIDToMutex

	db          DeviceListUpdaterDatabase
	api         DeviceListUpdaterAPI
	producer    KeyChangeProducer
	fedClient   fedsenderapi.KeyserverFederationAPI
	workerChans []chan spec.ServerName
	thisServer  spec.ServerName

	// When device lists are stale for a user, they get inserted into this map with a channel which `Update` will
	// block on or timeout via a select.
	userIDToChan   map[string]chan bool
	userIDToChanMu *sync.Mutex
	rsAPI          rsapi.KeyserverRoomserverAPI

	isBlacklistedOrBackingOffFn func(s spec.ServerName) (*statistics.ServerStatistics, error)

	// Users whose device list updates arrived while a room had partial state (MSC3706).
	// Who shares a room with whom is unknown until the resync finishes, so these wait
	// for it instead of making a request to the user's server.
	pendingMu    sync.Mutex
	pendingUsers map[string]struct{}
}

// DeviceListUpdaterDatabase is the subset of functionality from storage.Database required for the updater.
// Useful for testing.
type DeviceListUpdaterDatabase interface {
	// StaleDeviceLists returns a list of user IDs ending with the domains provided who have stale device lists.
	// If no domains are given, all user IDs with stale device lists are returned.
	StaleDeviceLists(ctx context.Context, domains []spec.ServerName) ([]string, error)

	// MarkDeviceListStale sets the stale bit for this user to isStale.
	MarkDeviceListStale(ctx context.Context, userID string, isStale bool) error

	// StoreRemoteDeviceKeys persists the given keys. Keys with the same user ID and device ID will be replaced. An empty KeyJSON removes the key
	// for this (user, device). Does not modify the stream ID for keys. User IDs in `clearUserIDs` will have all their device keys deleted prior
	// to insertion - use this when you have a complete snapshot of a user's keys in order to track device deletions correctly.
	StoreRemoteDeviceKeys(ctx context.Context, keys []api.DeviceMessage, clearUserIDs []string) error

	// PrevIDsExists returns true if all prev IDs exist for this user.
	PrevIDsExists(ctx context.Context, userID string, prevIDs []int64) (bool, error)

	// DeviceKeysJSON populates the KeyJSON for the given keys. If any proided `keys` have a `KeyJSON` or `StreamID` already then it will be replaced.
	DeviceKeysJSON(ctx context.Context, keys []api.DeviceMessage) error

	// DeviceKeysForUser returns the device keys for the device IDs given. If the length of deviceIDs is 0, all devices are selected.
	DeviceKeysForUser(ctx context.Context, userID string, deviceIDs []string, includeEmpty bool) ([]api.DeviceMessage, error)

	DeleteStaleDeviceLists(ctx context.Context, userIDs []string) error
}

type DeviceListUpdaterAPI interface {
	PerformUploadDeviceKeys(ctx context.Context, req *api.PerformUploadDeviceKeysRequest, res *api.PerformUploadDeviceKeysResponse)
}

// KeyChangeProducer is the interface for producers.KeyChange useful for testing.
type KeyChangeProducer interface {
	ProduceKeyChanges(keys []api.DeviceMessage) error
}

var deviceListUpdaterBackpressure = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "zendrite",
		Subsystem: "keyserver",
		Name:      "worker_backpressure",
		Help:      "How many device list updater requests are queued",
	},
	[]string{"worker_id"},
)

var deviceListUpdaterServersRetrying = prometheus.NewGaugeVec(
	prometheus.GaugeOpts{
		Namespace: "zendrite",
		Subsystem: "keyserver",
		Name:      "worker_servers_retrying",
		Help:      "How many servers are queued for retry",
	},
	[]string{"worker_id"},
)

// NewDeviceListUpdater creates a new updater which fetches fresh device lists when they go stale.
func NewDeviceListUpdater(
	process *process.ProcessContext, db DeviceListUpdaterDatabase,
	api DeviceListUpdaterAPI, producer KeyChangeProducer,
	fedClient fedsenderapi.KeyserverFederationAPI, numWorkers int,
	rsAPI rsapi.KeyserverRoomserverAPI,
	thisServer spec.ServerName,
	enableMetrics bool,
	isBlacklistedOrBackingOffFn func(s spec.ServerName) (*statistics.ServerStatistics, error),
) *DeviceListUpdater {
	if enableMetrics {
		prometheus.MustRegister(deviceListUpdaterBackpressure, deviceListUpdaterServersRetrying)
	}
	return &DeviceListUpdater{
		process:                     process,
		userIDToMutex:               make(map[string]*sync.Mutex),
		mu:                          &sync.Mutex{},
		pendingUsers:                make(map[string]struct{}),
		db:                          db,
		api:                         api,
		producer:                    producer,
		fedClient:                   fedClient,
		thisServer:                  thisServer,
		workerChans:                 make([]chan spec.ServerName, numWorkers),
		userIDToChan:                make(map[string]chan bool),
		userIDToChanMu:              &sync.Mutex{},
		rsAPI:                       rsAPI,
		isBlacklistedOrBackingOffFn: isBlacklistedOrBackingOffFn,
	}
}

// Start the device list updater, which will try to refresh any stale device lists.
func (u *DeviceListUpdater) Start() error {
	for i := 0; i < len(u.workerChans); i++ {
		// Allocate a small buffer per channel.
		// If the buffer limit is reached, backpressure will cause the processing of EDUs
		// to stop (in this transaction) until key requests can be made.
		ch := make(chan spec.ServerName, 10)
		u.workerChans[i] = ch
		u.process.Go(func() { u.worker(ch, i) })
	}

	staleLists, err := u.db.StaleDeviceLists(u.process.Context(), []spec.ServerName{})
	if err != nil {
		return err
	}

	newStaleLists := dedupeStaleLists(staleLists)
	offset, step := time.Second*10, time.Second //nolint:mnd
	if max := len(newStaleLists); max > 120 {   //nolint:mnd
		step = (time.Second * 120) / time.Duration(max) //nolint:mnd
	}
	process.Stagger(u.process.Context(), newStaleLists, offset, step, func(userID string) {
		u.process.Go(func() { u.notifyWorkers(userID) })
	})
	return nil
}

// CleanUp removes stale device entries for users we don't share a room with anymore.
func (u *DeviceListUpdater) CleanUp() error {
	staleUsers, err := u.db.StaleDeviceLists(u.process.Context(), []spec.ServerName{})
	if err != nil {
		return err
	}

	res := rsapi.QueryLeftUsersResponse{}
	if err = u.rsAPI.QueryLeftUsers(u.process.Context(), &rsapi.QueryLeftUsersRequest{StaleDeviceListUsers: staleUsers}, &res); err != nil {
		return err
	}

	if len(res.LeftUsers) == 0 {
		return nil
	}
	logrus.Debugf("Deleting %d stale device list entries", len(res.LeftUsers))
	return u.db.DeleteStaleDeviceLists(u.process.Context(), res.LeftUsers)
}

func (u *DeviceListUpdater) mutex(userID string) *sync.Mutex {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.userIDToMutex[userID] == nil {
		u.userIDToMutex[userID] = &sync.Mutex{}
	}
	return u.userIDToMutex[userID]
}

// ManualUpdate invalidates the device list for the given user and fetches the latest and tracks it.
// Blocks until the device list is synced or the timeout is reached.
func (u *DeviceListUpdater) ManualUpdate(ctx context.Context, serverName spec.ServerName, userID string) error {
	mu := u.mutex(userID)
	mu.Lock()
	err := u.db.MarkDeviceListStale(ctx, userID, true)
	mu.Unlock()
	if err != nil {
		return fmt.Errorf("ManualUpdate: failed to mark device list for %s as stale: %w", userID, err)
	}
	u.notifyWorkers(userID)
	return nil
}

// Update blocks until the update has been stored in the database. It blocks primarily for satisfying sytest,
// which assumes when /send 200 OKs that the device lists have been updated.
func (u *DeviceListUpdater) Update(ctx context.Context, event gomatrixserverlib.DeviceListUpdateEvent) error {
	isDeviceListStale, err := u.update(ctx, event)
	if err != nil {
		return err
	}
	if isDeviceListStale {
		// poke workers to handle stale device lists
		u.notifyWorkers(event.UserID)
	}
	return nil
}

func (u *DeviceListUpdater) update(ctx context.Context, event gomatrixserverlib.DeviceListUpdateEvent) (bool, error) {
	mu := u.mutex(event.UserID)
	mu.Lock()
	defer mu.Unlock()
	// check if we have the prev IDs
	exists, err := u.db.PrevIDsExists(ctx, event.UserID, event.PrevID)
	if err != nil {
		return false, fmt.Errorf("failed to check prev IDs exist for %s (%s): %w", event.UserID, event.DeviceID, err)
	}
	// if this is the first time we're hearing about this user, sync the device list manually.
	if len(event.PrevID) == 0 {
		exists = false
	}
	util.GetLogger(ctx).WithFields(logrus.Fields{
		"prev_ids_exist": exists,
		"user_id":        event.UserID,
		"device_id":      event.DeviceID,
		"stream_id":      event.StreamID,
		"prev_ids":       event.PrevID,
		"display_name":   event.DeviceDisplayName,
		"deleted":        event.Deleted,
	}).Trace("DeviceListUpdater.Update")

	// While a room has partial state we cannot tell who shares a room with the
	// user, so do not ask their server for the device list: hold the update until
	// the resync finishes, as Synapse does.
	if !exists && !event.Deleted && u.deferWhilePartialState(ctx, event.UserID) {
		return false, nil
	}

	// if we haven't missed anything update the database and notify users
	if exists || event.Deleted {
		k := event.Keys
		if event.Deleted {
			k = nil
		}
		keys := []api.DeviceMessage{
			{
				Type: api.TypeDeviceKeyUpdate,
				DeviceKeys: &api.DeviceKeys{
					DeviceID:    event.DeviceID,
					DisplayName: event.DeviceDisplayName,
					KeyJSON:     k,
					UserID:      event.UserID,
				},
				StreamID: event.StreamID,
			},
		}

		// DeviceKeysJSON will side-effect modify this, so it needs
		// to be a copy, not sharing any pointers with the above.
		deviceKeysCopy := *keys[0].DeviceKeys
		deviceKeysCopy.KeyJSON = nil
		existingKeys := []api.DeviceMessage{
			{
				Type:       keys[0].Type,
				DeviceKeys: &deviceKeysCopy,
				StreamID:   keys[0].StreamID,
			},
		}

		// fetch what keys we had already and only emit changes
		if err = u.db.DeviceKeysJSON(ctx, existingKeys); err != nil {
			// non-fatal, log and continue
			util.GetLogger(ctx).WithError(err).WithField("user_id", event.UserID).Errorf(
				"failed to query device keys json for calculating diffs",
			)
		}

		err = u.db.StoreRemoteDeviceKeys(ctx, keys, nil)
		if err != nil {
			return false, fmt.Errorf("failed to store remote device keys for %s (%s): %w", event.UserID, event.DeviceID, err)
		}

		if err = emitDeviceKeyChanges(u.producer, existingKeys, keys, false); err != nil {
			return false, fmt.Errorf("failed to produce device key changes for %s (%s): %w", event.UserID, event.DeviceID, err)
		}
		return false, nil
	}

	err = u.db.MarkDeviceListStale(ctx, event.UserID, true)
	if err != nil {
		return false, fmt.Errorf("failed to mark device list for %s as stale: %w", event.UserID, err)
	}

	return true, nil
}

// deferWhilePartialState records the user's update as pending and returns true if
// any room has partial state. Failing to find out counts as no.
func (u *DeviceListUpdater) deferWhilePartialState(ctx context.Context, userID string) bool {
	if u.rsAPI == nil {
		return false
	}
	rooms, err := u.rsAPI.GetPartialStateRoomIDs(ctx)
	if err != nil {
		util.GetLogger(ctx).WithError(err).Warn("Failed to check for rooms with partial state")
		return false
	}
	if len(rooms) == 0 {
		return false
	}
	// A user known to be in a fully stated room is handled as usual: no resync is
	// going to say anything more about them.
	if user, parseErr := spec.NewUserID(userID, true); parseErr == nil {
		if joined, roomsErr := u.rsAPI.QueryRoomsForUser(ctx, *user, spec.Join); roomsErr == nil && len(joined) > 0 {
			return false
		}
	}
	u.pendingMu.Lock()
	defer u.pendingMu.Unlock()
	u.pendingUsers[userID] = struct{}{}
	return true
}

// ListenForUnPartialStatedRooms makes the updater act on the device list updates it
// held back once the members of a room are known.
func (u *DeviceListUpdater) ListenForUnPartialStatedRooms() {
	if u.rsAPI == nil {
		return
	}
	u.rsAPI.RegisterUnPartialStatedHook(u.onRoomUnPartialStated)
}

func (u *DeviceListUpdater) onRoomUnPartialStated(ctx context.Context, roomID string) {
	if err := u.HandleRoomUnPartialStated(ctx, roomID); err != nil {
		util.GetLogger(ctx).WithError(err).WithField("room_id", roomID).Error("Failed to handle held back device list updates")
	}
}

// HandleRoomUnPartialStated is called when a room finishes its partial state
// resync. For each user in it whose device list update was held back, it marks the
// device list stale, so that it is fetched when next needed, and announces a key
// change so that the local users who share the room are told the devices changed.
func (u *DeviceListUpdater) HandleRoomUnPartialStated(ctx context.Context, roomID string) error {
	if u.rsAPI == nil {
		return nil
	}

	var res rsapi.QueryMembershipsForRoomResponse
	if err := u.rsAPI.QueryMembershipsForRoom(ctx, &rsapi.QueryMembershipsForRoomRequest{
		RoomID: roomID, JoinedOnly: true,
	}, &res); err != nil {
		return fmt.Errorf("failed to query the members of %s: %w", roomID, err)
	}

	// The servers in the room were only partly known until now, so the device list
	// updates of our users reached only some of them. Announce the lists again, to
	// all the servers that are in the room now.
	if err := u.replayLocalDeviceLists(ctx, res.JoinEvents); err != nil {
		return err
	}

	var changed []string
	u.pendingMu.Lock()
	for _, ev := range res.JoinEvents {
		if ev.StateKey == nil {
			continue
		}
		if _, ok := u.pendingUsers[*ev.StateKey]; ok {
			changed = append(changed, *ev.StateKey)
			delete(u.pendingUsers, *ev.StateKey)
		}
	}
	u.pendingMu.Unlock()

	for _, userID := range changed {
		if err := u.db.MarkDeviceListStale(ctx, userID, true); err != nil {
			return fmt.Errorf("failed to mark device list for %s as stale: %w", userID, err)
		}
		if err := u.producer.ProduceKeyChanges([]api.DeviceMessage{{
			Type:       api.TypeDeviceKeyUpdate,
			DeviceKeys: &api.DeviceKeys{UserID: userID},
		}}); err != nil {
			return fmt.Errorf("failed to announce the device list change of %s: %w", userID, err)
		}
	}
	return nil
}

// replayLocalDeviceLists announces the device lists of the local users among the
// given joins, as the federation sender sends them to the servers of their rooms.
func (u *DeviceListUpdater) replayLocalDeviceLists(ctx context.Context, joins []synctypes.ClientEvent) error {
	for _, ev := range joins {
		if ev.StateKey == nil {
			continue
		}
		user, err := spec.NewUserID(*ev.StateKey, true)
		if err != nil || user.Domain() != u.thisServer {
			continue
		}
		msgs, err := u.db.DeviceKeysForUser(ctx, user.String(), nil, true)
		if err != nil {
			return fmt.Errorf("failed to load the device lists of %s: %w", user.String(), err)
		}
		if len(msgs) == 0 {
			continue
		}
		if err = u.producer.ProduceKeyChanges(msgs); err != nil {
			return fmt.Errorf("failed to announce the device lists of %s: %w", user.String(), err)
		}
	}
	return nil
}

func (u *DeviceListUpdater) notifyWorkers(userID string) {
	_, remoteServer, err := gomatrixserverlib.SplitID('@', userID)
	if err != nil {
		return
	}
	_, err = u.isBlacklistedOrBackingOffFn(remoteServer)
	var federationClientError *fedsenderapi.FederationClientError
	if errors.As(err, &federationClientError) {
		if federationClientError.Blacklisted {
			return
		}
	}

	hash := fnv.New32a()
	_, _ = hash.Write([]byte(remoteServer))
	index := int(int64(hash.Sum32()) % int64(len(u.workerChans)))

	ch := u.assignChannel(userID)
	// Since workerChans are buffered, we only increment here and let the worker
	// decrement it once it is done processing.
	deviceListUpdaterBackpressure.With(prometheus.Labels{"worker_id": strconv.Itoa(index)}).Inc()
	select {
	case u.workerChans[index] <- remoteServer:
	case <-u.process.Context().Done():
		u.clearChannel(userID)
		return
	}
	select {
	case <-ch:
	case <-u.process.Context().Done():
		return
	case <-time.After(10 * time.Second):
		// we don't return an error in this case as it's not a failure condition.
		// we mainly block for the benefit of sytest anyway
	}
}

func (u *DeviceListUpdater) assignChannel(userID string) chan bool {
	u.userIDToChanMu.Lock()
	defer u.userIDToChanMu.Unlock()
	if ch, ok := u.userIDToChan[userID]; ok {
		return ch
	}
	ch := make(chan bool)
	u.userIDToChan[userID] = ch
	return ch
}

func (u *DeviceListUpdater) clearChannel(userID string) {
	u.userIDToChanMu.Lock()
	defer u.userIDToChanMu.Unlock()
	if ch, ok := u.userIDToChan[userID]; ok {
		close(ch)
		delete(u.userIDToChan, userID)
	}
}

func (u *DeviceListUpdater) worker(ch chan spec.ServerName, workerID int) {
	retries := make(map[spec.ServerName]time.Time)
	retriesMu := &sync.Mutex{}
	// restarter goroutine which will inject failed servers into ch when it is time
	u.process.Go(func() {
		var serversToRetry []spec.ServerName
		for {
			// nuke serversToRetry by re-slicing it to be "empty".
			// The capacity of the slice is unchanged, which ensures we can reuse the memory.
			serversToRetry = serversToRetry[:0]

			retriesMu.Lock()
			deviceListUpdaterServersRetrying.With(prometheus.Labels{"worker_id": strconv.Itoa(workerID)}).Set(float64(len(retries)))
			retriesMu.Unlock()
			if !process.Sleep(u.process.Context(), time.Second*2) { //nolint:mnd // Preserve the existing retry interval.
				return
			}

			// -2, so we have space for incoming device list updates over federation
			maxServers := (cap(ch) - len(ch)) - 2 //nolint:mnd
			if maxServers <= 0 {
				continue
			}

			retriesMu.Lock()
			now := time.Now()
			for srv, retryAt := range retries {
				if now.After(retryAt) {
					serversToRetry = append(serversToRetry, srv)
					if maxServers == len(serversToRetry) {
						break
					}
				}
			}

			for _, srv := range serversToRetry {
				delete(retries, srv)
			}
			retriesMu.Unlock()

			for _, srv := range serversToRetry {
				deviceListUpdaterBackpressure.With(prometheus.Labels{"worker_id": strconv.Itoa(workerID)}).Inc()
				select {
				case ch <- srv:
				case <-u.process.Context().Done():
					return
				}
			}
		}
	})
	for {
		var serverName spec.ServerName
		select {
		case serverName = <-ch:
		case <-u.process.Context().Done():
			return
		}
		retriesMu.Lock()
		_, exists := retries[serverName]
		retriesMu.Unlock()

		// If the serverName is coming from retries, maybe it was
		// blacklisted in the meantime.
		_, err := u.isBlacklistedOrBackingOffFn(serverName)
		var federationClientError *fedsenderapi.FederationClientError
		// unwrap errors and check for FederationClientError, if found, federationClientError will be not nil
		errors.As(err, &federationClientError)
		isBlacklisted := federationClientError != nil && federationClientError.Blacklisted

		// Don't retry a server that we're already waiting for or is blacklisted by now.
		if exists || isBlacklisted {
			deviceListUpdaterBackpressure.With(prometheus.Labels{"worker_id": strconv.Itoa(workerID)}).Dec()
			continue
		}
		waitTime, shouldRetry := u.processServer(serverName)
		if shouldRetry {
			retriesMu.Lock()
			if _, exists = retries[serverName]; !exists {
				retries[serverName] = time.Now().Add(waitTime)
			}
			retriesMu.Unlock()
		}
		deviceListUpdaterBackpressure.With(prometheus.Labels{"worker_id": strconv.Itoa(workerID)}).Dec()
	}
}

func (u *DeviceListUpdater) processServer(serverName spec.ServerName) (time.Duration, bool) {
	ctx := u.process.Context()
	// If the process.Context is canceled, there is no need to go further.
	// This avoids spamming the logs when shutting down
	if errors.Is(ctx.Err(), context.Canceled) {
		return defaultWaitTime, false
	}

	logger := util.GetLogger(ctx).WithField("server_name", serverName)
	deviceListUpdateCount.WithLabelValues(string(serverName)).Inc()

	waitTime := defaultWaitTime // How long should we wait to try again?
	successCount := 0           // How many user requests failed?

	userIDs, err := u.db.StaleDeviceLists(ctx, []spec.ServerName{serverName})
	if err != nil {
		logger.WithError(err).Error("Failed to load stale device lists")
		return waitTime, true
	}

	for _, userID := range userIDs {
		userWait, err := u.processServerUser(ctx, serverName, userID)
		if err != nil {
			if userWait > waitTime {
				waitTime = userWait
			}
			break
		}
		successCount++
	}

	allUsersSucceeded := successCount == len(userIDs)
	if !allUsersSucceeded {
		logger.WithFields(logrus.Fields{
			"total":     len(userIDs),
			"succeeded": successCount,
			"failed":    len(userIDs) - successCount,
			"wait_time": waitTime,
		}).Debug("Failed to query device keys for some users")
	}
	return waitTime, !allUsersSucceeded
}

func (u *DeviceListUpdater) processServerUser(ctx context.Context, serverName spec.ServerName, userID string) (time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()

	// If we are processing more than one user per server, this unblocks further calls to Update
	// immediately instead of just after **all** users have been processed.
	defer u.clearChannel(userID)

	logger := util.GetLogger(ctx).WithFields(logrus.Fields{
		"server_name": serverName,
		"user_id":     userID,
	})
	res, err := u.fedClient.GetUserDevices(ctx, u.thisServer, serverName, userID)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return time.Minute * 10, err //nolint:mnd
		}
		var unmarshalErr *json.UnmarshalTypeError
		var syntaxErr *json.SyntaxError
		var fedErr *fedsenderapi.FederationClientError
		var netErr net.Error
		var httpErr gomatrix.HTTPError
		switch {
		case errors.As(err, &unmarshalErr), errors.As(err, &syntaxErr):
			logger.WithError(err).Debugf("Device list update for %q contained invalid JSON", userID)
			return defaultWaitTime, nil
		case errors.As(err, &fedErr):
			if fedErr.RetryAfter > 0 {
				return fedErr.RetryAfter, err
			} else if fedErr.Blacklisted {
				return time.Hour * 8, err //nolint:mnd
			}
		case errors.As(err, &netErr):
			// Use the default waitTime, if it's a timeout.
			// It probably doesn't make sense to try further users.
			if !netErr.Timeout() {
				logger.WithError(netErr).Debug("GetUserDevices returned net.Error")
				return time.Minute * 10, err //nolint:mnd
			}
		case errors.As(err, &httpErr):
			// The remote server returned an error, give it some time to recover.
			// This is to avoid spamming remote servers, which may not be Matrix servers anymore.
			if httpErr.Code >= 300 { //nolint:mnd
				logger.WithError(httpErr).Debug("GetUserDevices returned gomatrix.HTTPError")
				return hourWaitTime, err
			}
		default:
			// Something else failed
			logger.WithError(err).Debugf("GetUserDevices returned unknown error type: %T", err)
			return time.Minute * 10, err //nolint:mnd
		}
	}
	if res.UserID != userID {
		logger.WithError(err).Debugf("User ID %q in device list update response doesn't match expected %q", res.UserID, userID)
		return defaultWaitTime, nil
	}
	if res.MasterKey != nil || res.SelfSigningKey != nil {
		uploadReq := &api.PerformUploadDeviceKeysRequest{
			UserID: userID,
		}
		uploadRes := &api.PerformUploadDeviceKeysResponse{}
		if res.MasterKey != nil {
			if err = sanityCheckKey(*res.MasterKey, userID, fclient.CrossSigningKeyPurposeMaster); err == nil {
				uploadReq.MasterKey = *res.MasterKey
			}
		}
		if res.SelfSigningKey != nil {
			if err = sanityCheckKey(*res.SelfSigningKey, userID, fclient.CrossSigningKeyPurposeSelfSigning); err == nil {
				uploadReq.SelfSigningKey = *res.SelfSigningKey
			}
		}
		u.api.PerformUploadDeviceKeys(ctx, uploadReq, uploadRes)
	}
	err = u.updateDeviceList(&res) //nolint:contextcheck
	if err != nil {
		logger.WithError(err).Error("Fetched device list but failed to store/emit it")
		return defaultWaitTime, err
	}
	return defaultWaitTime, nil
}

func (u *DeviceListUpdater) updateDeviceList(res *fclient.RespUserDevices) error {
	ctx := u.process.Context() // Persistence outlives individual requests, but not ownership.
	keys := make([]api.DeviceMessage, len(res.Devices))
	for i, device := range res.Devices {
		keyJSON, err := json.Marshal(device.Keys)
		if err != nil {
			util.GetLogger(ctx).WithField("keys", device.Keys).Error("failed to marshal keys, skipping device")
			continue
		}
		keys[i] = api.DeviceMessage{
			Type:     api.TypeDeviceKeyUpdate,
			StreamID: res.StreamID,
			DeviceKeys: &api.DeviceKeys{
				DeviceID:    device.DeviceID,
				DisplayName: device.DisplayName,
				UserID:      res.UserID,
				KeyJSON:     keyJSON,
			},
		}
	}

	err := u.db.StoreRemoteDeviceKeys(ctx, keys, []string{res.UserID})
	if err != nil {
		return fmt.Errorf("failed to store remote device keys: %w", err)
	}
	err = u.db.MarkDeviceListStale(ctx, res.UserID, false)
	if err != nil {
		return fmt.Errorf("failed to mark device list as fresh: %w", err)
	}
	// A full stale-list refresh is our recovery path after missing updates, so
	// shared users should be prompted to re-check this user's device list even
	// if the final snapshot happens to match what was already cached locally.
	err = u.producer.ProduceKeyChanges(keys)
	if err != nil {
		return fmt.Errorf("failed to emit key changes for fresh device list: %w", err)
	}
	return nil
}

// dedupeStaleLists de-duplicates the stateList entries using the domain.
// This is used on startup, processServer is getting all users anyway, so
// there is no need to send every user to the workers.
func dedupeStaleLists(staleLists []string) []string {
	seenDomains := make(map[spec.ServerName]struct{})
	newStaleLists := make([]string, 0, len(staleLists))
	for _, userID := range staleLists {
		_, domain, err := gomatrixserverlib.SplitID('@', userID)
		if err != nil {
			// non-fatal and should not block starting up
			continue
		}
		if _, ok := seenDomains[domain]; ok {
			continue
		}
		newStaleLists = append(newStaleLists, userID)
		seenDomains[domain] = struct{}{}
	}
	return newStaleLists
}
