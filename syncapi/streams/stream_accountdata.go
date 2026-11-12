package streams

import (
	"context"
	"encoding/json"
	"regexp"
	"strings"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// isEmptyAccountData reports whether data is an MSC3391 deletion tombstone.
func isEmptyAccountData(data []byte) bool {
	trimmed := strings.TrimSpace(string(data))
	if trimmed == "" || trimmed == "null" {
		return true
	}
	var object map[string]json.RawMessage
	return json.Unmarshal([]byte(trimmed), &object) == nil && len(object) == 0
}

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
	p.ensurePushRules(ctx, req)
	return pos
}

// ensurePushRules adds the user's push rules to a complete sync when the sync
// API has not yet recorded them. Registration creates the rules in the user API
// and tells the sync API asynchronously, so a sync straight after registering
// can run first and would otherwise come back without them.
func (p *AccountDataStreamProvider) ensurePushRules(ctx context.Context, req *types.SyncRequest) {
	for _, ev := range req.Response.AccountData.Events {
		if ev.Type == "m.push_rules" {
			return
		}
	}
	if !accountDataTypeMatches("m.push_rules", &req.Filter.AccountData) || len(req.Response.AccountData.Events) >= req.Filter.AccountData.Limit {
		return
	}
	dataReq := userapi.QueryAccountDataRequest{
		UserID:   req.Device.UserID,
		DataType: "m.push_rules",
	}
	dataRes := userapi.QueryAccountDataResponse{}
	if err := p.userAPI.QueryAccountData(ctx, &dataReq, &dataRes); err != nil {
		req.Log.WithError(err).Error("p.userAPI.QueryAccountData failed for push rules")
		return
	}
	data, ok := dataRes.GlobalAccountData["m.push_rules"]
	if !ok || isEmptyAccountData(data) {
		return
	}
	req.Response.AccountData.Events = append(
		req.Response.AccountData.Events,
		synctypes.ClientEvent{Type: "m.push_rules", Content: spec.RawJSON(data)},
	)
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
					// A complete sync must omit account data that was deleted
					// (MSC3391); only incremental syncs announce the tombstone.
					if from == 0 && isEmptyAccountData(globalData) {
						continue
					}
					req.Response.AccountData.Events = append(
						req.Response.AccountData.Events,
						synctypes.ClientEvent{
							Type:    dataType,
							Content: spec.RawJSON(globalData),
						},
					)
				}
			} else {
				joinData, hasJoinData := req.Response.Rooms.Join[roomID]
				if !hasJoinData {
					joinData = types.NewJoinResponse()
				}
				if roomData, ok := dataRes.RoomAccountData[roomID][dataType]; ok {
					if from == 0 && isEmptyAccountData(roomData) {
						// Keep the key present but the type absent, as MSC3391
						// requires on a complete sync.
						if joinData.AccountData == nil {
							joinData.AccountData = &types.ClientEvents{Events: []synctypes.ClientEvent{}}
						}
						req.Response.Rooms.Join[roomID] = joinData
						continue
					}
					if joinData.AccountData == nil {
						joinData.AccountData = &types.ClientEvents{}
					}
					joinData.AccountData.Events = append(
						joinData.AccountData.Events,
						synctypes.ClientEvent{
							Type:    dataType,
							Content: spec.RawJSON(roomData),
						},
					)
				} else if from > 0 {
					// Preserve the deleted type as a typed MSC3391 tombstone.
					if joinData.AccountData == nil {
						joinData.AccountData = &types.ClientEvents{}
					}
					joinData.AccountData.Events = append(
						joinData.AccountData.Events,
						synctypes.ClientEvent{
							Type:    dataType,
							Content: spec.RawJSON(`{}`),
						},
					)
				}
				if from > 0 || joinData.AccountData != nil {
					req.Response.Rooms.Join[roomID] = joinData
				}
			}
		}
	}

	return pos
}

func accountDataTypeMatches(dataType string, filter *synctypes.EventFilter) bool {
	matches := func(patterns *[]string) bool {
		if patterns == nil {
			return false
		}
		for _, pattern := range *patterns {
			expression := "^" + strings.ReplaceAll(regexp.QuoteMeta(pattern), `\*`, ".*") + "$"
			if matched, _ := regexp.MatchString(expression, dataType); matched {
				return true
			}
		}
		return false
	}
	return (filter.Types == nil || matches(filter.Types)) && !matches(filter.NotTypes)
}
