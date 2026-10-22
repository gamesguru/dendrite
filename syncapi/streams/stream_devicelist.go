package streams

import (
	"context"

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
}

func (p *DeviceListStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.LatestPosition(ctx)
}

func (p *DeviceListStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	joinRoomIDs := make([]string, 0, len(req.Response.Rooms.Join))
	for roomID := range req.Response.Rooms.Join {
		joinRoomIDs = append(joinRoomIDs, roomID)
	}
	newlyJoined := make([]string, 0, len(req.NewlyJoinedRooms))
	for roomID := range req.NewlyJoinedRooms {
		newlyJoined = append(newlyJoined, roomID)
	}
	req.Log.WithFields(logrus.Fields{
		"since_pdu_position":         req.Since.PDUPosition,
		"since_device_list_position": req.Since.DeviceListPosition,
		"from":                       from,
		"to":                         to,
		"join_room_ids":              joinRoomIDs,
		"request_newly_joined":       newlyJoined,
	}).Info("Device list catch-up for an incremental sync")

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
