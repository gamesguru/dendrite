// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only.

package sqlite3

import (
	"context"
	"database/sql"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/userapi/storage/tables"
)

const externalIDsSchema = `
CREATE TABLE IF NOT EXISTS userapi_external_ids (
	provider_id TEXT NOT NULL,
	external_id TEXT NOT NULL,
	localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
	PRIMARY KEY (provider_id, external_id)
);

CREATE INDEX IF NOT EXISTS userapi_external_ids_localpart_idx ON userapi_external_ids(localpart, server_name);
`

const selectLocalpartForExternalIDSQL = "" +
	"SELECT localpart, server_name FROM userapi_external_ids WHERE provider_id = $1 AND external_id = $2"

const insertExternalIDSQL = "" +
	"INSERT INTO userapi_external_ids (provider_id, external_id, localpart, server_name) VALUES ($1, $2, $3, $4)"

type externalIDsStatements struct {
	db                               *sql.DB
	selectLocalpartForExternalIDStmt *sql.Stmt
	insertExternalIDStmt             *sql.Stmt
}

func NewSQLiteExternalIDsTable(db *sql.DB) (tables.ExternalIDsTable, error) {
	s := &externalIDsStatements{
		db: db,
	}
	_, err := db.Exec(externalIDsSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.selectLocalpartForExternalIDStmt, selectLocalpartForExternalIDSQL},
		{&s.insertExternalIDStmt, insertExternalIDSQL},
	}.Prepare(db)
}

func (s *externalIDsStatements) SelectLocalpartForExternalID(
	ctx context.Context, txn *sql.Tx, providerID, externalID string,
) (localpart string, serverName spec.ServerName, err error) {
	stmt := sqlutil.TxStmt(txn, s.selectLocalpartForExternalIDStmt)
	err = stmt.QueryRowContext(ctx, providerID, externalID).Scan(&localpart, &serverName)
	if err == sql.ErrNoRows {
		return "", "", nil
	}
	return
}

func (s *externalIDsStatements) InsertExternalID(
	ctx context.Context, txn *sql.Tx, localpart string, serverName spec.ServerName, providerID, externalID string,
) (err error) {
	stmt := sqlutil.TxStmt(txn, s.insertExternalIDStmt)
	_, err = stmt.ExecContext(ctx, providerID, externalID, localpart, serverName)
	return
}
