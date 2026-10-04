// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"encoding/json"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/userapi/storage/tables"
)

const profileFieldsSchema = `
-- Stores the custom profile fields of accounts (MSC4133). The display name
-- and avatar URL live in userapi_profiles.
CREATE TABLE IF NOT EXISTS userapi_profile_fields (
	localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
	field_name TEXT NOT NULL,
	-- The field's value as JSON
	field_value TEXT NOT NULL,
	PRIMARY KEY (localpart, server_name, field_name)
);
`

const upsertProfileFieldSQL = "" +
	"INSERT INTO userapi_profile_fields (localpart, server_name, field_name, field_value) VALUES ($1, $2, $3, $4)" +
	" ON CONFLICT (localpart, server_name, field_name) DO UPDATE SET field_value = $4"

const selectProfileFieldsSQL = "" +
	"SELECT field_name, field_value FROM userapi_profile_fields WHERE localpart = $1 AND server_name = $2"

const deleteProfileFieldSQL = "" +
	"DELETE FROM userapi_profile_fields WHERE localpart = $1 AND server_name = $2 AND field_name = $3"

const deleteProfileFieldsSQL = "" +
	"DELETE FROM userapi_profile_fields WHERE localpart = $1 AND server_name = $2"

type profileFieldsStatements struct {
	upsertProfileFieldStmt  *sql.Stmt
	selectProfileFieldsStmt *sql.Stmt
	deleteProfileFieldStmt  *sql.Stmt
	deleteProfileFieldsStmt *sql.Stmt
}

func NewSQLiteProfileFieldsTable(db *sql.DB) (tables.ProfileFieldsTable, error) {
	s := &profileFieldsStatements{}
	_, err := db.Exec(profileFieldsSchema)
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.upsertProfileFieldStmt, upsertProfileFieldSQL},
		{&s.selectProfileFieldsStmt, selectProfileFieldsSQL},
		{&s.deleteProfileFieldStmt, deleteProfileFieldSQL},
		{&s.deleteProfileFieldsStmt, deleteProfileFieldsSQL},
	}.Prepare(db)
}

func (s *profileFieldsStatements) UpsertProfileField(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName,
	fieldName string, value json.RawMessage,
) error {
	stmt := sqlutil.TxStmt(txn, s.upsertProfileFieldStmt)
	_, err := stmt.ExecContext(ctx, localpart, serverName, fieldName, string(value))
	return err
}

func (s *profileFieldsStatements) SelectProfileFields(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName,
) (map[string]json.RawMessage, error) {
	stmt := sqlutil.TxStmt(txn, s.selectProfileFieldsStmt)
	rows, err := stmt.QueryContext(ctx, localpart, serverName)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectProfileFields: rows.close() failed")
	var fields map[string]json.RawMessage
	for rows.Next() {
		var name, value string
		if err = rows.Scan(&name, &value); err != nil {
			return nil, err
		}
		if fields == nil {
			fields = map[string]json.RawMessage{}
		}
		fields[name] = json.RawMessage(value)
	}
	return fields, rows.Err()
}

func (s *profileFieldsStatements) DeleteProfileField(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName, fieldName string,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteProfileFieldStmt)
	_, err := stmt.ExecContext(ctx, localpart, serverName, fieldName)
	return err
}

func (s *profileFieldsStatements) DeleteProfileFields(
	ctx context.Context, txn *sql.Tx,
	localpart string, serverName spec.ServerName,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteProfileFieldsStmt)
	_, err := stmt.ExecContext(ctx, localpart, serverName)
	return err
}
