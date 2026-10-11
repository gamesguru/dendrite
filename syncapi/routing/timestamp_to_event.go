package routing

import (
	"cmp"
	"context"
	"math"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

const (
	timestampLookupBatchSize = 100
	// FederationTimestampLookupBatches bounds the events checked for a remote
	// server, which costs a roomserver query each.
	federationTimestampLookupBatches = 10
	// RemoteTimestampServers bounds the servers asked to fill a gap in the
	// local history, which are tried one after the other.
	remoteTimestampServers = 5
	remoteTimestampTimeout = 10 * time.Second
	// RemoteTimestampBackfillLimit is the number of events backfilled from a
	// remote result, so that /context and /messages can use it.
	remoteTimestampBackfillLimit = 20
)

type timestampToEventResponse struct {
	EventID        string         `json:"event_id"`
	OriginServerTS spec.Timestamp `json:"origin_server_ts"`
}

// parseTimestampQuery validates the room ID and the ts and dir parameters
// that both the client and the federation endpoint take.
func parseTimestampQuery(req *http.Request, roomID string) (timestamp int64, backwards bool, errRes *util.JSONResponse) {
	if _, err := spec.NewRoomID(roomID); err != nil {
		return 0, false, &util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("invalid room ID")}
	}
	query := req.URL.Query()
	for _, name := range []string{"ts", "dir"} {
		if !query.Has(name) {
			return 0, false, &util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("missing " + name)}
		}
		if len(query[name]) != 1 {
			return 0, false, &util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("expected one " + name)}
		}
	}
	timestamp, err := strconv.ParseInt(query.Get("ts"), 10, 64)
	if err != nil {
		return 0, false, &util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("ts must be an integer timestamp in milliseconds")}
	}
	direction := query.Get("dir")
	if direction != "f" && direction != "b" {
		return 0, false, &util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("dir must be f or b")}
	}
	return timestamp, direction == "b", nil
}

// closestVisibleEvent returns the event closest to the timestamp in the given
// direction that passes the visible filter, or nil. MaxBatches limits the
// candidate batches checked, and 0 checks them all.
func closestVisibleEvent(
	ctx context.Context, db storage.DatabaseTransaction, roomID string, timestamp int64, backwards bool, maxBatches int,
	visible func([]*rstypes.HeaderedEvent) (*rstypes.HeaderedEvent, error),
) (*rstypes.HeaderedEvent, error) {
	var afterID types.StreamPosition
	for batch := 0; maxBatches == 0 || batch < maxBatches; batch++ {
		candidates, err := db.EventsByTimestamp(ctx, roomID, timestamp, afterID, backwards, timestampLookupBatchSize)
		if err != nil {
			return nil, err
		}
		if len(candidates) == 0 {
			break
		}
		events := make([]*rstypes.HeaderedEvent, len(candidates))
		for i := range candidates {
			events[i] = candidates[i].HeaderedEvent
		}
		event, err := visible(events)
		if err != nil || event != nil {
			return event, err
		}
		// Use both timestamp and stream position so invisible events sharing a timestamp
		// cannot hide a visible event in a subsequent batch.
		last := candidates[len(candidates)-1]
		timestamp, afterID = int64(last.OriginServerTS()), last.StreamPosition
	}
	return nil, nil
}

// TimestampToEvent implements GET /_matrix/client/v1/rooms/{roomId}/timestamp_to_event.
// When the local history may have a gap closer to the timestamp than the
// local result, it asks other servers in the room (MSC3030), unless
// federation is nil.
func TimestampToEvent(
	req *http.Request, device *userapi.Device, roomID string, syncDB storage.Database, rsAPI api.SyncRoomserverAPI,
	cfg *config.SyncAPI, federation fclient.FederationClient,
) util.JSONResponse {
	timestamp, backwards, errRes := parseTimestampQuery(req, roomID)
	if errRes != nil {
		return *errRes
	}
	rawUserID := device.UserID
	if asUserID := req.URL.Query().Get("user_id"); device.AppserviceID != "" && asUserID != "" {
		rawUserID = asUserID
	}
	userID, err := spec.NewUserID(rawUserID, true)
	if err != nil {
		return util.ErrorResponse(err)
	}
	ctx := req.Context()
	visibleTo := func(db storage.DatabaseTransaction) func([]*rstypes.HeaderedEvent) (*rstypes.HeaderedEvent, error) {
		return func(events []*rstypes.HeaderedEvent) (*rstypes.HeaderedEvent, error) {
			visible, err := internal.ApplyHistoryVisibilityFilter(ctx, db, rsAPI, events, nil, *userID, "timestamp_to_event")
			if err != nil || len(visible) == 0 {
				return nil, err
			}
			return visible[0], nil
		}
	}

	db, err := syncDB.NewDatabaseSnapshot(ctx)
	if err != nil {
		return util.ErrorResponse(err)
	}
	local, err := closestVisibleEvent(ctx, db, roomID, timestamp, backwards, 0, visibleTo(db))
	var gapServers []spec.ServerName
	var successor *rstypes.HeaderedEvent
	if err == nil && federation != nil {
		gapServers, successor, err = remoteTimestampServersForGap(ctx, db, cfg, roomID, timestamp, backwards, local)
	}
	if err == nil && successor != nil {
		// Only users who may see the edge of the gap can make the server
		// query others and backfill.
		var visible *rstypes.HeaderedEvent
		if visible, err = visibleTo(db)([]*rstypes.HeaderedEvent{successor}); visible == nil {
			gapServers = nil
		}
	}
	_ = db.Rollback()
	if err != nil {
		return util.ErrorResponse(err)
	}

	if len(gapServers) > 0 {
		lookup := remoteTimestampLookup{
			cfg: cfg, federation: federation, syncDB: syncDB, rsAPI: rsAPI,
			virtualHost: device.UserDomain(), roomID: roomID, successor: successor.EventID(),
		}
		if remote := lookup.closest(ctx, gapServers, timestamp, backwards, local, visibleTo); remote != nil {
			return util.JSONResponse{Code: http.StatusOK, JSON: timestampToEventResponse{EventID: remote.EventID(), OriginServerTS: remote.OriginServerTS()}}
		}
	}
	if local != nil {
		return util.JSONResponse{Code: http.StatusOK, JSON: timestampToEventResponse{EventID: local.EventID(), OriginServerTS: local.OriginServerTS()}}
	}
	return util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("No event was found or you do not have permission to read it")}
}

// remoteTimestampServersForGap returns the servers to ask when the local
// history may lack an event closer to the timestamp than local, together with
// the known event after the gap to backfill from. It returns no servers when
// there is no such gap.
//
// The sync API knows its gaps from the backward extremities, the events whose
// prev_events were never fetched. The missing events predate their
// successor, so the gap can hold a closer event when a successor is later
// than the bound below: the timestamp itself going forwards, or the local
// result going backwards.
func remoteTimestampServersForGap(
	ctx context.Context, db storage.DatabaseTransaction, cfg *config.SyncAPI,
	roomID string, timestamp int64, backwards bool, local *rstypes.HeaderedEvent,
) ([]spec.ServerName, *rstypes.HeaderedEvent, error) {
	if local != nil && int64(local.OriginServerTS()) == timestamp {
		return nil, nil, nil
	}
	extremities, err := db.BackwardExtremitiesForRoom(ctx, roomID)
	if err != nil || len(extremities) == 0 {
		return nil, nil, err
	}
	successorIDs := make([]string, 0, len(extremities))
	for eventID := range extremities {
		successorIDs = append(successorIDs, eventID)
	}
	successors, err := db.Events(ctx, successorIDs)
	if err != nil {
		return nil, nil, err
	}
	bound := timestamp
	if backwards {
		bound = math.MinInt64
		if local != nil {
			bound = int64(local.OriginServerTS())
		}
	}
	// Prefer the earliest successor after the bound, which borders the gap.
	var successor *rstypes.HeaderedEvent
	for _, event := range successors {
		if int64(event.OriginServerTS()) > bound && (successor == nil || event.OriginServerTS() < successor.OriginServerTS()) {
			successor = event
		}
	}
	if successor == nil {
		return nil, nil, nil
	}

	joined, err := db.AllJoinedUsersInRoom(ctx, []string{roomID})
	if err != nil {
		return nil, nil, err
	}
	members := map[spec.ServerName]int{}
	for _, userID := range joined[roomID] {
		_, domain, err := gomatrixserverlib.SplitID('@', userID)
		if err == nil && !cfg.Matrix.IsLocalServerName(domain) {
			members[domain]++
		}
	}
	servers := make([]spec.ServerName, 0, len(members))
	for server := range members {
		servers = append(servers, server)
	}
	// Servers with more members are more likely to have the full history.
	slices.SortFunc(servers, func(a, b spec.ServerName) int {
		return cmp.Or(members[b]-members[a], cmp.Compare(a, b))
	})
	return servers[:min(len(servers), remoteTimestampServers)], successor, nil
}

type remoteTimestampLookup struct {
	cfg         *config.SyncAPI
	federation  fclient.FederationClient
	syncDB      storage.Database
	rsAPI       api.SyncRoomserverAPI
	virtualHost spec.ServerName
	roomID      string
	// successor is a known event after the gap, used as the backward
	// extremity when backfilling the remote result.
	successor string
}

// closest asks the servers in turn for the event closest to the timestamp and
// returns the first answer that is closer than local and visible to the user,
// after backfilling it. It returns nil when no server has a better event.
func (l *remoteTimestampLookup) closest(
	ctx context.Context, servers []spec.ServerName, timestamp int64, backwards bool, local *rstypes.HeaderedEvent,
	visibleTo func(storage.DatabaseTransaction) func([]*rstypes.HeaderedEvent) (*rstypes.HeaderedEvent, error),
) *rstypes.HeaderedEvent {
	logger := util.GetLogger(ctx).WithField("room_id", l.roomID)
	for _, server := range servers {
		res, err := l.query(ctx, server, timestamp, backwards)
		if err != nil {
			logger.WithError(err).WithField("server", server).Debug("Remote timestamp_to_event failed")
			continue
		}
		ts := int64(res.OriginServerTS)
		if (backwards && ts > timestamp) || (!backwards && ts < timestamp) {
			continue
		}
		// Servers in the same room see the same history, so the first answer
		// settles whether the gap holds a closer event.
		if local != nil && absDiff(ts, timestamp) >= absDiff(int64(local.OriginServerTS()), timestamp) {
			return nil
		}
		event, err := l.fetch(ctx, res.EventID)
		if err != nil {
			logger.WithError(err).WithField("event_id", res.EventID).Warn("Failed to backfill remote timestamp_to_event result")
			continue
		}
		if event == nil {
			continue
		}
		db, err := l.syncDB.NewDatabaseSnapshot(ctx)
		if err != nil {
			logger.WithError(err).Warn("Failed to check visibility of remote timestamp_to_event result")
			return nil
		}
		visible, err := visibleTo(db)([]*rstypes.HeaderedEvent{event})
		_ = db.Rollback()
		if err != nil || visible == nil {
			// Other servers would return the same event.
			return nil
		}
		return visible
	}
	return nil
}

func (l *remoteTimestampLookup) query(ctx context.Context, server spec.ServerName, timestamp int64, backwards bool) (*timestampToEventResponse, error) {
	identity, err := l.cfg.Matrix.SigningIdentityFor(l.virtualHost)
	if err != nil {
		return nil, err
	}
	dir := "f"
	if backwards {
		dir = "b"
	}
	query := url.Values{"ts": {strconv.FormatInt(timestamp, 10)}, "dir": {dir}}
	fedReq := fclient.NewFederationRequest(
		http.MethodGet, identity.ServerName, server,
		"/_matrix/federation/v1/timestamp_to_event/"+url.PathEscape(l.roomID)+"?"+query.Encode(),
	)
	if err = fedReq.Sign(identity.ServerName, identity.KeyID, identity.PrivateKey); err != nil {
		return nil, err
	}
	httpReq, err := fedReq.HTTPRequest()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, remoteTimestampTimeout)
	defer cancel()
	var res timestampToEventResponse
	if err = l.federation.DoRequestAndParseResponse(ctx, httpReq, &res); err != nil {
		return nil, err
	}
	return &res, nil
}

// fetch returns the event from the sync API, backfilling it and the events
// before it first when it is unknown.
func (l *remoteTimestampLookup) fetch(ctx context.Context, eventID string) (*rstypes.HeaderedEvent, error) {
	find := func() (*rstypes.HeaderedEvent, error) {
		db, err := l.syncDB.NewDatabaseSnapshot(ctx)
		if err != nil {
			return nil, err
		}
		defer db.Rollback() //nolint:errcheck
		events, err := db.Events(ctx, []string{eventID})
		if err != nil || len(events) == 0 || events[0].RoomID().String() != l.roomID {
			return nil, err
		}
		return events[0], nil
	}
	if event, err := find(); err != nil || event != nil {
		return event, err
	}
	// The backfill requests the remote event and those before it, attributing
	// them to the known successor so the roomserver can pick servers by its state.
	extremities := map[string][]string{l.successor: {eventID}}
	if _, err := backfillEvents(ctx, l.rsAPI, l.syncDB, l.cfg.Matrix.ServerName, l.virtualHost, l.roomID, extremities, remoteTimestampBackfillLimit); err != nil {
		return nil, err
	}
	return find()
}

func absDiff(a, b int64) uint64 {
	if a > b {
		return uint64(a) - uint64(b)
	}
	return uint64(b) - uint64(a)
}

// FederationTimestampToEvent implements
// GET /_matrix/federation/v1/timestamp_to_event/{roomId} (MSC3030). It only
// looks at local events, and returns the closest one the origin server may see.
func FederationTimestampToEvent(
	req *http.Request, origin spec.ServerName, roomID string, syncDB storage.Database, rsAPI api.SyncRoomserverAPI,
) util.JSONResponse {
	timestamp, backwards, errRes := parseTimestampQuery(req, roomID)
	if errRes != nil {
		return *errRes
	}
	ctx := req.Context()
	var banned api.QueryServerBannedFromRoomResponse
	if err := rsAPI.QueryServerBannedFromRoom(ctx, &api.QueryServerBannedFromRoomRequest{RoomID: roomID, ServerName: origin}, &banned); err != nil {
		return util.ErrorResponse(err)
	}
	if banned.Banned {
		return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("Forbidden by server ACLs")}
	}
	db, err := syncDB.NewDatabaseSnapshot(ctx)
	if err != nil {
		return util.ErrorResponse(err)
	}
	defer db.Rollback() //nolint:errcheck
	event, err := closestVisibleEvent(ctx, db, roomID, timestamp, backwards, federationTimestampLookupBatches,
		func(events []*rstypes.HeaderedEvent) (*rstypes.HeaderedEvent, error) {
			for _, event := range events {
				allowed, err := rsAPI.QueryServerAllowedToSeeEvent(ctx, origin, event.EventID(), roomID)
				if err != nil || allowed {
					return event, err
				}
			}
			return nil, nil
		})
	if err != nil {
		return util.ErrorResponse(err)
	}
	if event == nil {
		return util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("No event was found or the server may not see it")}
	}
	return util.JSONResponse{Code: http.StatusOK, JSON: timestampToEventResponse{EventID: event.EventID(), OriginServerTS: event.OriginServerTS()}}
}

// SetupFederation registers the federation endpoints the sync API serves.
func SetupFederation(
	fedMux *httputil.Router, cfg *config.SyncAPI, syncDB storage.Database, rsAPI api.SyncRoomserverAPI,
	keyRing gomatrixserverlib.JSONVerifier,
) {
	fedMux.Handle("/v1/timestamp_to_event/{roomID}", httputil.MakeExternalAPI("federation_timestamp_to_event", func(req *http.Request) util.JSONResponse {
		fedReq, errRes := fclient.VerifyHTTPRequest(req, time.Now(), cfg.Matrix.ServerName, cfg.Matrix.IsLocalServerName, keyRing)
		if fedReq == nil {
			return util.JSONResponse{Code: errRes.Code, JSON: errRes.JSON, Headers: errRes.Headers}
		}
		vars, err := httputil.URLDecodeMapValues(httputil.Vars(req))
		if err != nil {
			return util.ErrorResponse(err)
		}
		return FederationTimestampToEvent(req, fedReq.Origin(), vars["roomID"], syncDB, rsAPI)
	})).Methods(http.MethodGet)
}
