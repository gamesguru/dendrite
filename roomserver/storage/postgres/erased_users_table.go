// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
)

const erasedUsersSchema = `
-- Local users who asked for erasure when deactivating their account
-- (MSC4025). Their events are served redacted to anyone who was not joined
-- to the room when the event was sent.
CREATE TABLE IF NOT EXISTS roomserver_erased_users (
    user_id TEXT NOT NULL PRIMARY KEY,
    erased_ts BIGINT NOT NULL
);
`

const insertErasedUserSQL = "" +
	"INSERT INTO roomserver_erased_users (user_id, erased_ts) VALUES ($1, $2)" +
	" ON CONFLICT (user_id) DO NOTHING"

const deleteErasedUserSQL = "" +
	"DELETE FROM roomserver_erased_users WHERE user_id = $1"

const selectErasedUsersSQL = "" +
	"SELECT user_id FROM roomserver_erased_users WHERE user_id = ANY($1)"

type erasedUsersStatements struct {
	insertErasedUserStmt  *sql.Stmt
	deleteErasedUserStmt  *sql.Stmt
	selectErasedUsersStmt *sql.Stmt
}

func CreateErasedUsersTable(db *sql.DB) error {
	_, err := db.Exec(erasedUsersSchema)
	return err
}

func PrepareErasedUsersTable(db *sql.DB) (tables.ErasedUsers, error) {
	s := &erasedUsersStatements{}
	return s, sqlutil.StatementList{
		{&s.insertErasedUserStmt, insertErasedUserSQL},
		{&s.deleteErasedUserStmt, deleteErasedUserSQL},
		{&s.selectErasedUsersStmt, selectErasedUsersSQL},
	}.Prepare(db)
}

func (s *erasedUsersStatements) InsertErasedUser(ctx context.Context, txn *sql.Tx, userID string, erasedTS int64) error {
	_, err := sqlutil.TxStmt(txn, s.insertErasedUserStmt).ExecContext(ctx, userID, erasedTS)
	return err
}

func (s *erasedUsersStatements) DeleteErasedUser(ctx context.Context, txn *sql.Tx, userID string) error {
	_, err := sqlutil.TxStmt(txn, s.deleteErasedUserStmt).ExecContext(ctx, userID)
	return err
}

func (s *erasedUsersStatements) SelectErasedUsers(ctx context.Context, txn *sql.Tx, userIDs []string) (map[string]struct{}, error) {
	rows, err := sqlutil.TxStmt(txn, s.selectErasedUsersStmt).QueryContext(ctx, userIDs)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectErasedUsers: rows.close() failed")
	erased := make(map[string]struct{})
	for rows.Next() {
		var userID string
		if err = rows.Scan(&userID); err != nil {
			return nil, err
		}
		erased[userID] = struct{}{}
	}
	return erased, rows.Err()
}
