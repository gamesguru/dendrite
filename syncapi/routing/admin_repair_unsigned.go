package routing

import (
	"errors"
	"net/http"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rsTypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/internal"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// AdminRepairUnsigned handles POST /_zendrite/admin/repairUnsigned/{roomID}.
//
// It finds the state events of a room that have no unsigned.prev_content,
// which is the case for events stored by backfill before the sync API began
// setting it, and rebuilds prev_content, prev_sender and replaces_state from
// the state before each event. Without it, a client shows a profile change as
// a fresh "joined the room".
//
// It only reports what it would change unless apply=true is given. It never
// overwrites an existing prev_content and never touches any other unsigned key.
//
// Writes are conditional on the stored JSON still matching what was read and on
// the event not being redacted, so a concurrent redaction or event rewrite is
// skipped rather than overwritten. A write that loses a race is reported as
// write_skipped; an event found redacted or already annotated on the re-read
// before the write is dropped without being counted. The repair keeps
// no backup, so take a database backup before applying it to a large room.
func AdminRepairUnsigned(req *http.Request, syncDB storage.Database, rsAPI api.SyncRoomserverAPI, device *userapi.Device) util.JSONResponse {
	vars, err := httputil.URLDecodeMapValues(httputil.Vars(req))
	if err != nil {
		return util.ErrorResponse(err)
	}
	roomID, err := spec.NewRoomID(vars["roomID"])
	if err != nil {
		return util.JSONResponse{
			Code: http.StatusBadRequest,
			JSON: spec.InvalidParam("invalid room ID"),
		}
	}
	apply := req.URL.Query().Get("apply") == "true"

	// Reject unknown rooms instead of returning an all-zero report that looks
	// like a room with nothing to repair. The roomserver is the authority here:
	// the repair itself cannot read its state querier for a room it lacks.
	if _, err = rsAPI.QueryRoomVersionForRoom(req.Context(), roomID.String()); err != nil {
		if errors.Is(err, rsTypes.ErrRoomNotFound) {
			return util.JSONResponse{Code: http.StatusNotFound, JSON: spec.NotFound("Room not found")}
		}
		logrus.WithError(err).WithField("room_id", roomID.String()).Error("failed to query room version for repairUnsigned")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}

	report, err := internal.RepairRoomPrevState(req.Context(), syncDB, rsAPI, roomID.String(), apply)
	logger := logrus.WithField("room_id", roomID.String()).WithField("apply", apply).WithField("user_id", device.UserID)
	if err != nil {
		logger.WithError(err).WithField("report", report).Error("repairUnsigned failed")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	logger.WithField("report", report).Info("repairUnsigned finished")
	return util.JSONResponse{Code: http.StatusOK, JSON: report}
}
