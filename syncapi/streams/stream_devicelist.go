package streams

import (
	"context"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type DeviceListStreamProvider struct {
	DefaultStreamProvider
	rsAPI   api.SyncRoomserverAPI
	userAPI userapi.SyncKeyAPI
	// topologyLookup replaces the database lookup in tests.
	topologyLookup func(ctx context.Context, eventID string) error
}

func (p *DeviceListStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	// A complete sync issues the token before the PDU provider takes its
	// position. Wait for a just-joined partial-state room to reach the sync API
	// first, so the token covers the join and the next sync does not mistake it
	// for a room that was joined and left in the same gap.
	p.waitForPendingPartialJoins(ctx, req)
	return p.LatestPosition(ctx)
}

// waitForPendingPartialJoins waits for the syncing user's membership in rooms
// that are still doing a partial-state resync to reach the sync API. Those
// joins come from the roomserver before its output reaches the sync consumer,
// so without the wait a complete sync can take a token from before the join.
// This is deliberately limited to partial-state rooms: the PDU position alone
// is not a reliable barrier for ordinary joins until the output consumers share
// a position with the sync API.
func (p *DeviceListStreamProvider) waitForPendingPartialJoins(ctx context.Context, req *types.SyncRequest) {
	if p.rsAPI == nil {
		return
	}
	roomIDs, err := p.rsAPI.GetPartialStateRoomIDs(ctx)
	if err != nil {
		req.Log.WithError(err).Warn("Failed to get partial state rooms")
		return
	}
	if len(roomIDs) == 0 {
		return
	}
	userID, err := spec.NewUserID(req.Device.UserID, true)
	if err != nil {
		return
	}
	for _, roomID := range roomIDs {
		var res api.QueryMembershipForUserResponse
		if err := p.rsAPI.QueryMembershipForUser(ctx, &api.QueryMembershipForUserRequest{
			RoomID: roomID,
			UserID: *userID,
		}, &res); err != nil {
			req.Log.WithError(err).WithField("room_id", roomID).Debug("Failed to query membership for a partial state room")
			continue
		}
		if !res.IsInRoom || res.EventID == "" {
			continue
		}
		req.Log.WithFields(logrus.Fields{
			"room_id":  roomID,
			"event_id": res.EventID,
		}).Debug("Waiting for a partial state join to reach the sync API")
		if p.topologyLookup != nil {
			internal.WaitForTopologyEvent(ctx, res.EventID, p.topologyLookup)
		} else {
			internal.WaitForMembershipInTopology(ctx, p.DB, res.EventID)
		}
	}
}

func (p *DeviceListStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	req.Log.WithFields(logrus.Fields{
		"since_pdu_position":         req.Since.PDUPosition,
		"since_device_list_position": req.Since.DeviceListPosition,
		"from":                       from,
		"to":                         to,
		"joined_room_count":          len(req.Response.Rooms.Join),
		"newly_joined_count":         len(req.NewlyJoinedRooms),
	}).Debug("Device list catch-up for an incremental sync")

	var err error
	to, _, err = internal.DeviceListCatchup(ctx, snapshot, p.userAPI, p.rsAPI, req.Device.UserID, req.Response, from, to)
	if err != nil {
		logDeviceListFailure(ctx, req, err)
		return from
	}
	err = internal.DeviceOTKCounts(ctx, p.userAPI, req.Device.UserID, req.Device.ID, req.Response)
	if err != nil {
		logDeviceListFailure(ctx, req, err)
		return from
	}

	return to
}

// logDeviceListFailure reports a failed device list catch-up, which is routine
// when the client gave up on the sync and its request context was canceled.
func logDeviceListFailure(ctx context.Context, req *types.SyncRequest, err error) {
	if ctx.Err() != nil {
		req.Log.WithError(err).Debug("internal.DeviceListCatchup abandoned, the client went away")
		return
	}
	req.Log.WithError(err).Error("internal.DeviceListCatchup failed")
}
