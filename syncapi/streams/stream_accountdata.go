package streams

import (
	"context"
	"time"

	"github.com/element-hq/dendrite/syncapi/storage"
	"github.com/element-hq/dendrite/syncapi/synctypes"
	"github.com/element-hq/dendrite/syncapi/types"
	userapi "github.com/element-hq/dendrite/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

type AccountDataStreamProvider struct {
	DefaultStreamProvider
	userAPI userapi.SyncUserAPI
}

func (p *AccountDataStreamProvider) Setup(
	ctx context.Context, snapshot storage.DatabaseTransaction,
) {
	p.DefaultStreamProvider.Setup(ctx, snapshot)

	p.latestMutex.Lock()
	defer p.latestMutex.Unlock()

	id, err := snapshot.MaxStreamPositionForAccountData(ctx)
	if err != nil {
		panic(err)
	}
	p.latest = id
}

func (p *AccountDataStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	pos := p.IncrementalSync(ctx, snapshot, req, 0, p.LatestPosition(ctx))
	if len(req.Response.AccountData.Events) > 0 {
		return pos
	}
	return p.waitForAccountData(ctx, snapshot, req, pos)
}

func (p *AccountDataStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	r := types.Range{
		From: from,
		To:   to,
	}

	dataTypes, pos, err := snapshot.GetAccountDataInRange(
		ctx, req.Device.UserID, r, &req.Filter.AccountData,
	)
	if err != nil {
		req.Log.WithError(err).Error("p.DB.GetAccountDataInRange failed")
		return from
	}

	// Iterate over the rooms
	for roomID, dataTypes := range dataTypes {
		// For a complete sync, make sure we're only including this room if
		// that room was present in the joined rooms.
		if from == 0 && roomID != "" && !req.IsRoomPresent(roomID) {
			continue
		}

		// Request the missing data from the database
		for _, dataType := range dataTypes {
			dataReq := userapi.QueryAccountDataRequest{
				UserID:   req.Device.UserID,
				RoomID:   roomID,
				DataType: dataType,
			}
			dataRes := userapi.QueryAccountDataResponse{}
			err = p.userAPI.QueryAccountData(ctx, &dataReq, &dataRes)
			if err != nil {
				req.Log.WithError(err).Error("p.userAPI.QueryAccountData failed")
				continue
			}
			if roomID == "" {
				if globalData, ok := dataRes.GlobalAccountData[dataType]; ok {
					req.Response.AccountData.Events = append(
						req.Response.AccountData.Events,
						synctypes.ClientEvent{
							Type:    dataType,
							Content: spec.RawJSON(globalData),
						},
					)
				}
			} else {
				if roomData, ok := dataRes.RoomAccountData[roomID][dataType]; ok {
					joinData, ok := req.Response.Rooms.Join[roomID]
					if !ok {
						joinData = types.NewJoinResponse()
					}
					joinData.AccountData.Events = append(
						joinData.AccountData.Events,
						synctypes.ClientEvent{
							Type:    dataType,
							Content: spec.RawJSON(roomData),
						},
					)
					req.Response.Rooms.Join[roomID] = joinData
				}
			}
		}
	}

	return pos
}

func (p *AccountDataStreamProvider) waitForAccountData(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from types.StreamPosition,
) types.StreamPosition {
	timer := time.NewTimer(50 * time.Millisecond)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()

	pos := from
	for {
		select {
		case <-ctx.Done():
			return pos
		case <-timer.C:
			return pos
		case <-ticker.C:
			latest := p.LatestPosition(ctx)
			if latest <= pos {
				continue
			}
			pos = p.IncrementalSync(ctx, snapshot, req, pos, latest)
			if len(req.Response.AccountData.Events) > 0 {
				return pos
			}
		}
	}
}
