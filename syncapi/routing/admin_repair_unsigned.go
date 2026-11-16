package routing

import (
	"net/http"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
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
