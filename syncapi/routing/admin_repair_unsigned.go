package routing

import (
	"context"
	"errors"
	"net/http"
	"sort"

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

// repairUnsignedAllRoom is a room the all-rooms repair could not process.
type repairUnsignedAllFailure struct {
	RoomID string `json:"room_id"`
	Error  string `json:"error"`
}

// repairUnsignedAllTotals sums the per-room reports.
type repairUnsignedAllTotals struct {
	Rooms                 int `json:"rooms"`
	Failed                int `json:"failed"`
	Scanned               int `json:"scanned"`
	StateEvents           int `json:"state_events"`
	Redacted              int `json:"redacted"`
	AlreadyHadPrevContent int `json:"already_had_prev_content"`
	Repairable            int `json:"repairable"`
	Repaired              int `json:"repaired"`
	NoPredecessor         int `json:"no_predecessor"`
	LookupUnavailable     int `json:"lookup_unavailable"`
	WriteSkipped          int `json:"write_skipped"`
	Errors                int `json:"errors"`
}

type repairUnsignedAllResponse struct {
	Apply  bool                              `json:"apply"`
	Totals repairUnsignedAllTotals           `json:"totals"`
	Rooms  []*internal.PrevStateRepairReport `json:"rooms"`
	Failed []repairUnsignedAllFailure        `json:"failed,omitempty"`
}

// repairUnsignedRoomFunc repairs one room, as internal.RepairRoomPrevState does.
type repairUnsignedRoomFunc func(ctx context.Context, roomID string, apply bool) (*internal.PrevStateRepairReport, error)

// repairUnsignedAllRooms runs repair over every room, in order, and sums the
// reports. A room that cannot be processed is listed in Failed and does not
// stop the others. The per-room samples are dropped to keep the response small.
// It stops early, returning what it has so far, if ctx ends.
func repairUnsignedAllRooms(ctx context.Context, roomIDs []string, apply bool, repair repairUnsignedRoomFunc) repairUnsignedAllResponse {
	res := repairUnsignedAllResponse{Apply: apply, Rooms: []*internal.PrevStateRepairReport{}}
	for _, roomID := range roomIDs {
		if ctx.Err() != nil {
			res.Failed = append(res.Failed, repairUnsignedAllFailure{RoomID: roomID, Error: "not attempted: " + ctx.Err().Error()})
			continue
		}
		report, err := repair(ctx, roomID, apply)
		if err != nil || report == nil {
			msg := "no report"
			if err != nil {
				msg = err.Error()
			}
			res.Failed = append(res.Failed, repairUnsignedAllFailure{RoomID: roomID, Error: msg})
			continue
		}
		report.Samples = nil
		res.Rooms = append(res.Rooms, report)
		res.Totals.Scanned += report.Scanned
		res.Totals.StateEvents += report.StateEvents
		res.Totals.Redacted += report.Redacted
		res.Totals.AlreadyHadPrevContent += report.AlreadyHad
		res.Totals.Repairable += report.Repairable
		res.Totals.Repaired += report.Repaired
		res.Totals.NoPredecessor += report.NoPredecessor
		res.Totals.LookupUnavailable += report.LookupUnavailable
		res.Totals.WriteSkipped += report.WriteSkipped
		res.Totals.Errors += report.Errors
	}
	res.Totals.Rooms = len(res.Rooms)
	res.Totals.Failed = len(res.Failed)
	return res
}

// AdminRepairUnsignedAll handles POST /_zendrite/admin/repairUnsigned.
//
// It runs the same repair as AdminRepairUnsigned over every room the sync API
// has a joined member for, and returns one report per room and the totals.
// Like the single-room endpoint it only reports unless apply=true is given, it
// never overwrites an existing prev_content, and it takes no backup: take a
// database backup before applying.
//
// It does not override the roomserver's incomplete-state marker. A room whose
// state snapshots are marked incomplete is reported as lookup_unavailable and
// is left unchanged, exactly as with the single-room endpoint.
//
// The repair is idempotent, so a request that times out can be repeated. A
// reverse proxy in front of the server needs a read timeout long enough for
// every room to be scanned, or the request will be cut off part way.
func AdminRepairUnsignedAll(req *http.Request, syncDB storage.Database, rsAPI api.SyncRoomserverAPI, device *userapi.Device) util.JSONResponse {
	apply := req.URL.Query().Get("apply") == "true"
	ctx := req.Context()

	snapshot, err := syncDB.NewDatabaseSnapshot(ctx)
	if err != nil {
		logrus.WithError(err).Error("failed to open a snapshot for repairUnsigned")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	joined, err := snapshot.AllJoinedUsersInRooms(ctx)
	_ = snapshot.Rollback()
	if err != nil {
		logrus.WithError(err).Error("failed to list rooms for repairUnsigned")
		return util.JSONResponse{Code: http.StatusInternalServerError, JSON: spec.InternalServerError{}}
	}
	roomIDs := make([]string, 0, len(joined))
	for roomID := range joined {
		roomIDs = append(roomIDs, roomID)
	}
	sort.Strings(roomIDs)

	res := repairUnsignedAllRooms(ctx, roomIDs, apply, func(ctx context.Context, roomID string, apply bool) (*internal.PrevStateRepairReport, error) {
		// A room the roomserver does not know has nothing to repair, and the
		// repair cannot read its state, so skip it as the single-room endpoint
		// rejects it.
		if _, err := rsAPI.QueryRoomVersionForRoom(ctx, roomID); err != nil {
			return nil, err
		}
		return internal.RepairRoomPrevState(ctx, syncDB, rsAPI, roomID, apply)
	})
	logrus.WithField("apply", apply).WithField("user_id", device.UserID).WithField("totals", res.Totals).Info("repairUnsigned (all rooms) finished")
	return util.JSONResponse{Code: http.StatusOK, JSON: res}
}
