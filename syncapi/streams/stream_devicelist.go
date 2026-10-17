package streams

import (
	"context"

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
