// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/userapi/storage/tables"
)

const accountDataSchema = `
-- Stores data about accounts data.
CREATE TABLE IF NOT EXISTS userapi_account_datas (
    -- The Matrix user ID localpart for this account
    localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
    -- The room ID for this data (empty string if not specific to a room)
    room_id TEXT,
    -- The account data type
    type TEXT NOT NULL,
    -- The account data content
    content TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS userapi_account_datas_idx ON userapi_account_datas(localpart, server_name, room_id, type);
`

const insertAccountDataSQL = `
	INSERT INTO userapi_account_datas(localpart, server_name, room_id, type, content) VALUES($1, $2, $3, $4, $5)
	ON CONFLICT (localpart, server_name, room_id, type) DO UPDATE SET content = $5
`

const insertAccountDataIfAbsentSQL = `
	INSERT INTO userapi_account_datas(localpart, server_name, room_id, type, content) VALUES($1, $2, $3, $4, $5)
	ON CONFLICT (localpart, server_name, room_id, type) DO NOTHING
`

const selectAccountDataSQL = "" +
	"SELECT room_id, type, content FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2"

const selectAccountDataByTypeSQL = "" +
	"SELECT content FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND type = $4"

const deleteAccountDataSQL = "" +
	"DELETE FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2"

const deleteAccountDataByTypeSQL = "" +
	"DELETE FROM userapi_account_datas WHERE localpart = $1 AND server_name = $2 AND room_id = $3 AND type = $4"

type accountDataStatements struct {
	db                            *sql.DB
	insertAccountDataStmt         *sql.Stmt
	insertAccountDataIfAbsentStmt *sql.Stmt
	selectAccountDataStmt         *sql.Stmt
	selectAccountDataByTypeStmt   *sql.Stmt
	deleteAccountDataStmt         *sql.Stmt
	deleteAccountDataByTypeStmt   *sql.Stmt
}

func NewSQLiteAccountDataTable(db *sql.DB) (tables.AccountDataTable, error) {
	s := &accountDataStatements{
		db: db,
	}
	_, err := db.Exec(accountDataSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertAccountDataStmt, insertAccountDataSQL},
		{&s.insertAccountDataIfAbsentStmt, insertAccountDataIfAbsentSQL},
		{&s.selectAccountDataStmt, selectAccountDataSQL},
		{&s.selectAccountDataByTypeStmt, selectAccountDataByTypeSQL},
		{&s.deleteAccountDataStmt, deleteAccountDataSQL},
		{&s.deleteAccountDataByTypeStmt, deleteAccountDataByTypeSQL},
	}.Prepare(db)
}

func (s *accountDataStatements) InsertAccountData(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName,
	roomID, dataType string, content json.RawMessage,
) error {
	if strings.TrimSpace(string(content)) == "null" {
		return errors.New("account data content must not be null")
	}
	stmt := sqlutil.TxStmt(txn, s.insertAccountDataStmt)
	_, err := stmt.ExecContext(ctx, localpart, serverName, roomID, dataType, content)
	return err
}

func (s *accountDataStatements) UpdateAccountDataIfUnchanged(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName,
	roomID, dataType string, expected, content json.RawMessage,
) (bool, error) {
	if strings.TrimSpace(string(content)) == "null" {
		return false, errors.New("account data content must not be null")
	}
	var current []byte
	err := sqlutil.TxStmt(txn, s.selectAccountDataByTypeStmt).QueryRowContext(ctx, localpart, serverName, roomID, dataType).Scan(&current)
	switch {
	case err == sql.ErrNoRows:
		if len(expected) != 0 {
			return false, nil
		}
		result, err := sqlutil.TxStmt(txn, s.insertAccountDataIfAbsentStmt).ExecContext(ctx, localpart, serverName, roomID, dataType, content)
		if err != nil {
			return false, err
		}
		updated, err := result.RowsAffected()
		return updated > 0, err
	case err != nil:
		return false, err
	case string(current) != string(expected):
		return false, nil
	}
	if _, err = sqlutil.TxStmt(txn, s.insertAccountDataStmt).ExecContext(ctx, localpart, serverName, roomID, dataType, content); err != nil {
		return false, err
	}
	return true, nil
}

func (s *accountDataStatements) DeleteAccountDataByType(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName,
	roomID, dataType string,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteAccountDataByTypeStmt)
	_, err := stmt.ExecContext(ctx, localpart, serverName, roomID, dataType)
	return err
}

func (s *accountDataStatements) SelectAccountData(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
) (
	/* global */ map[string]json.RawMessage,
	/* rooms */ map[string]map[string]json.RawMessage,
	error,
) {
	rows, err := s.selectAccountDataStmt.QueryContext(ctx, localpart, serverName)
	if err != nil {
		return nil, nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectAccountData: failed to close rows")

	global := map[string]json.RawMessage{}
	rooms := map[string]map[string]json.RawMessage{}

	for rows.Next() {
		var roomID string
		var dataType string
		var content []byte

		if err = rows.Scan(&roomID, &dataType, &content); err != nil {
			return nil, nil, err
		}

		if roomID != "" {
			if _, ok := rooms[roomID]; !ok {
				rooms[roomID] = map[string]json.RawMessage{}
			}
			rooms[roomID][dataType] = content
		} else {
			global[dataType] = content
		}
	}

	return global, rooms, rows.Err()
}

func (s *accountDataStatements) SelectAccountDataByType(
	ctx context.Context,
	localpart string, serverName spec.ServerName,
	roomID, dataType string,
) (data json.RawMessage, err error) {
	var bytes []byte
	stmt := s.selectAccountDataByTypeStmt
	if err = stmt.QueryRowContext(ctx, localpart, serverName, roomID, dataType).Scan(&bytes); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return
	}
	data = json.RawMessage(bytes)
	return
}

func (s *accountDataStatements) DeleteAccountData(
	ctx context.Context, txn *sql.Tx, localpart string, serverName spec.ServerName,
) error {
	_, err := sqlutil.TxStmt(txn, s.deleteAccountDataStmt).ExecContext(ctx, localpart, serverName)
	return err
}
