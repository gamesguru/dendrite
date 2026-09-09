package routing

import (
	"net/http"
	"strconv"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

const timestampLookupBatchSize = 100

type timestampToEventResponse struct {
	EventID        string         `json:"event_id"`
	OriginServerTS spec.Timestamp `json:"origin_server_ts"`
}

// TimestampToEvent implements GET /_matrix/client/v1/rooms/{roomId}/timestamp_to_event.
func TimestampToEvent(req *http.Request, device *userapi.Device, roomID string, syncDB storage.Database, rsAPI api.SyncRoomserverAPI) util.JSONResponse {
	if _, err := spec.NewRoomID(roomID); err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("invalid room ID")}
	}
	query := req.URL.Query()
	for _, name := range []string{"ts", "dir"} {
		if !query.Has(name) {
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("missing " + name)}
		}
		if len(query[name]) != 1 {
			return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("expected one " + name)}
		}
	}
	timestamp, err := strconv.ParseInt(query.Get("ts"), 10, 64)
	if err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("ts must be an integer timestamp in milliseconds")}
	}
	direction := query.Get("dir")
	if direction != "f" && direction != "b" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("dir must be f or b")}
	}
	rawUserID := device.UserID
	if asUserID := query.Get("user_id"); device.AppserviceID != "" && asUserID != "" {
		rawUserID = asUserID
	}
	userID, err := spec.NewUserID(rawUserID, true)
	if err != nil {
		return util.ErrorResponse(err)
	}
	ctx := req.Context()
	db, err := syncDB.NewDatabaseSnapshot(ctx)
	if err != nil {
		return util.ErrorResponse(err)
	}
	defer db.Rollback() //nolint:errcheck

	var afterID types.StreamPosition
	for {
		candidates, err := db.EventsByTimestamp(ctx, roomID, timestamp, afterID, direction == "b", timestampLookupBatchSize)
		if err != nil {
			return util.ErrorResponse(err)
		}
		if len(candidates) == 0 {
			break
		}
		events := make([]*rstypes.HeaderedEvent, len(candidates))
		for i := range candidates {
			events[i] = candidates[i].HeaderedEvent
		}
		visible, err := internal.ApplyHistoryVisibilityFilter(ctx, db, rsAPI, events, nil, *userID, "timestamp_to_event")
		if err != nil {
			return util.ErrorResponse(err)
		}
		if len(visible) > 0 {
			return util.JSONResponse{Code: http.StatusOK, JSON: timestampToEventResponse{EventID: visible[0].EventID(), OriginServerTS: visible[0].OriginServerTS()}}
		}
		// Use both timestamp and stream position so invisible events sharing a timestamp
		// cannot hide a visible event in a subsequent batch.
		last := candidates[len(candidates)-1]
		timestamp, afterID = int64(last.OriginServerTS()), last.StreamPosition
	}
	return util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("No event was found or you do not have permission to read it")}
}
