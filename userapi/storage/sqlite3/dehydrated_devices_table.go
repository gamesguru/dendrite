// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"encoding/json"
	"time"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/userapi/storage/tables"
)

const dehydratedDevicesSchema = `
CREATE TABLE IF NOT EXISTS userapi_dehydrated_devices (
	user_id TEXT NOT NULL PRIMARY KEY,
	device_id TEXT NOT NULL,
	device_data TEXT NOT NULL,
	created_ts BIGINT NOT NULL
);
`

const insertDehydratedDeviceSQL = "" +
	"INSERT OR REPLACE INTO userapi_dehydrated_devices (user_id, device_id, device_data, created_ts) VALUES ($1, $2, $3, $4)"

const selectDehydratedDeviceSQL = "" +
	"SELECT device_id, device_data FROM userapi_dehydrated_devices WHERE user_id = $1"

const deleteDehydratedDeviceSQL = "" +
	"DELETE FROM userapi_dehydrated_devices WHERE user_id = $1 RETURNING device_id"

type dehydratedDevicesStatements struct {
	insertStmt *sql.Stmt
	selectStmt *sql.Stmt
	deleteStmt *sql.Stmt
}

func NewSQLiteDehydratedDevicesTable(db *sql.DB) (tables.DehydratedDevicesTable, error) {
	s := &dehydratedDevicesStatements{}
	_, err := db.Exec(dehydratedDevicesSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertStmt, insertDehydratedDeviceSQL},
		{&s.selectStmt, selectDehydratedDeviceSQL},
		{&s.deleteStmt, deleteDehydratedDeviceSQL},
	}.Prepare(db)
}

func (s *dehydratedDevicesStatements) InsertDehydratedDevice(
	ctx context.Context, txn *sql.Tx, userID, deviceID string, deviceData json.RawMessage,
) error {
	stmt := sqlutil.TxStmt(txn, s.insertStmt)
	_, err := stmt.ExecContext(ctx, userID, deviceID, string(deviceData), time.Now().UnixMilli())
	return err
}

func (s *dehydratedDevicesStatements) SelectDehydratedDevice(
	ctx context.Context, userID string,
) (deviceID string, deviceData json.RawMessage, err error) {
	var dataStr string
	err = s.selectStmt.QueryRowContext(ctx, userID).Scan(&deviceID, &dataStr)
	if err != nil {
		return "", nil, err
	}
	deviceData = json.RawMessage(dataStr)
	return deviceID, deviceData, nil
}

func (s *dehydratedDevicesStatements) DeleteDehydratedDevice(
	ctx context.Context, txn *sql.Tx, userID string,
) (deviceID string, err error) {
	stmt := sqlutil.TxStmt(txn, s.deleteStmt)
	err = stmt.QueryRowContext(ctx, userID).Scan(&deviceID)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return deviceID, err
}
