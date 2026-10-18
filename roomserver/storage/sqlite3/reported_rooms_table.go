// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
)

// Room reports (MSC4151) keep the room and user IDs, so that a report
// outlives a purge of the room.
const reportedRoomsSchema = `
CREATE TABLE IF NOT EXISTS roomserver_reported_rooms
(
	id          INTEGER PRIMARY KEY AUTOINCREMENT,
	room_id     TEXT NOT NULL,
	user_id     TEXT NOT NULL, -- the user reporting the room
	reason      TEXT NOT NULL,
	received_ts BIGINT NOT NULL
);`

const insertReportedRoomSQL = "" +
	"INSERT INTO roomserver_reported_rooms (room_id, user_id, reason, received_ts) VALUES ($1, $2, $3, $4) RETURNING id"

const countReportedRoomsSQL = "" +
	"SELECT COUNT(*) FROM roomserver_reported_rooms WHERE ($1 = '' OR room_id = $1) AND ($2 = '' OR user_id = $2)"

const selectReportedRoomsDescSQL = "" +
	"SELECT id, room_id, user_id, reason, received_ts FROM roomserver_reported_rooms" +
	" WHERE ($1 = '' OR room_id = $1) AND ($2 = '' OR user_id = $2)" +
	" ORDER BY received_ts DESC, id DESC LIMIT $3 OFFSET $4"

const selectReportedRoomsAscSQL = "" +
	"SELECT id, room_id, user_id, reason, received_ts FROM roomserver_reported_rooms" +
	" WHERE ($1 = '' OR room_id = $1) AND ($2 = '' OR user_id = $2)" +
	" ORDER BY received_ts ASC, id ASC LIMIT $3 OFFSET $4"

type reportedRoomsStatements struct {
	insertReportedRoomStmt      *sql.Stmt
	countReportedRoomsStmt      *sql.Stmt
	selectReportedRoomsDescStmt *sql.Stmt
	selectReportedRoomsAscStmt  *sql.Stmt
}

func CreateReportedRoomsTable(db *sql.DB) error {
	_, err := db.Exec(reportedRoomsSchema)
	return err
}

func PrepareReportedRoomsTable(db *sql.DB) (tables.ReportedRooms, error) {
	s := &reportedRoomsStatements{}
	return s, sqlutil.StatementList{
		{&s.insertReportedRoomStmt, insertReportedRoomSQL},
		{&s.countReportedRoomsStmt, countReportedRoomsSQL},
		{&s.selectReportedRoomsDescStmt, selectReportedRoomsDescSQL},
		{&s.selectReportedRoomsAscStmt, selectReportedRoomsAscSQL},
	}.Prepare(db)
}

func (s *reportedRoomsStatements) InsertReportedRoom(
	ctx context.Context, txn *sql.Tx, roomID, userID, reason string,
) (reportID int64, err error) {
	err = sqlutil.TxStmt(txn, s.insertReportedRoomStmt).QueryRowContext(
		ctx, roomID, userID, reason, spec.AsTimestamp(time.Now()),
	).Scan(&reportID)
	return
}

func (s *reportedRoomsStatements) SelectReportedRooms(
	ctx context.Context, txn *sql.Tx, from, limit uint64, backwards bool, userID, roomID string,
) ([]api.QueryAdminRoomReportsResponse, int64, error) {
	var count int64
	if err := sqlutil.TxStmt(txn, s.countReportedRoomsStmt).QueryRowContext(ctx, roomID, userID).Scan(&count); err != nil {
		return nil, 0, err
	}
	stmt := s.selectReportedRoomsAscStmt
	if backwards {
		stmt = s.selectReportedRoomsDescStmt
	}
	rows, err := sqlutil.TxStmt(txn, stmt).QueryContext(ctx, roomID, userID, limit, from)
	if err != nil {
		return nil, 0, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectReportedRooms: failed to close rows")
	reports := []api.QueryAdminRoomReportsResponse{}
	for rows.Next() {
		var report api.QueryAdminRoomReportsResponse
		if err = rows.Scan(&report.ID, &report.RoomID, &report.UserID, &report.Reason, &report.ReceivedTS); err != nil {
			return nil, 0, err
		}
		reports = append(reports, report)
	}
	return reports, count, rows.Err()
}
