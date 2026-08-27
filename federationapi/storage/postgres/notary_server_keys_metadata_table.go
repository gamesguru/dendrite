// Copyright 2024 New Vector Ltd.
// Copyright 2021 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/federationapi/storage/tables"
	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
)

const notaryServerKeysMetadataSchema = `
CREATE TABLE IF NOT EXISTS federationsender_notary_server_keys_metadata (
    notary_id BIGINT NOT NULL,
	server_name TEXT NOT NULL,
	key_id TEXT NOT NULL,
	UNIQUE (server_name, key_id)
);
`

const upsertServerKeysSQL = "" +
	"INSERT INTO federationsender_notary_server_keys_metadata (notary_id, server_name, key_id) VALUES ($1, $2, $3)" +
	" ON CONFLICT (server_name, key_id) DO UPDATE SET notary_id = $1"

// for a given (server_name, key_id), find the existing notary ID and valid until. Used to check if we will replace it
// JOINs with the json table.
const selectNotaryKeyMetadataSQL = `
	SELECT federationsender_notary_server_keys_metadata.notary_id, valid_until FROM federationsender_notary_server_keys_json
	JOIN federationsender_notary_server_keys_metadata ON
	federationsender_notary_server_keys_metadata.notary_id = federationsender_notary_server_keys_json.notary_id
	WHERE federationsender_notary_server_keys_metadata.server_name = $1 AND federationsender_notary_server_keys_metadata.key_id = $2
`

// select the response which has the highest valid_until value
// JOINs with the json table.
const selectNotaryKeyResponsesSQL = `
	SELECT response_json FROM federationsender_notary_server_keys_json
	WHERE server_name = $1 AND valid_until = (
		SELECT MAX(valid_until) FROM federationsender_notary_server_keys_json WHERE server_name = $1
	)
`

// select the responses which have the given key IDs
// JOINs with the json table.
const selectNotaryKeyResponsesWithKeyIDsSQL = `
	SELECT response_json FROM federationsender_notary_server_keys_json
	JOIN federationsender_notary_server_keys_metadata ON
	federationsender_notary_server_keys_metadata.notary_id = federationsender_notary_server_keys_json.notary_id
	WHERE federationsender_notary_server_keys_json.server_name = $1 AND federationsender_notary_server_keys_metadata.key_id = ANY ($2)
	GROUP BY federationsender_notary_server_keys_json.notary_id
`

// JOINs with the metadata table.
const deleteUnusedServerKeysJSONSQL = `
	DELETE FROM federationsender_notary_server_keys_json WHERE federationsender_notary_server_keys_json.notary_id NOT IN (
		SELECT DISTINCT notary_id FROM federationsender_notary_server_keys_metadata
	)
`

type notaryServerKeysMetadataStatements struct {
	db                                     *sql.DB
	upsertServerKeysStmt                   *sql.Stmt
	selectNotaryKeyResponsesStmt           *sql.Stmt
	selectNotaryKeyResponsesWithKeyIDsStmt *sql.Stmt
	selectNotaryKeyMetadataStmt            *sql.Stmt
	deleteUnusedServerKeysJSONStmt         *sql.Stmt
}

func NewPostgresNotaryServerKeysMetadataTable(db *sql.DB) (s *notaryServerKeysMetadataStatements, err error) {
	s = &notaryServerKeysMetadataStatements{
		db: db,
	}
	_, err = db.Exec(notaryServerKeysMetadataSchema)
	if err != nil {
		return
	}

	return s, sqlutil.StatementList{
		{&s.upsertServerKeysStmt, upsertServerKeysSQL},
		{&s.selectNotaryKeyResponsesStmt, selectNotaryKeyResponsesSQL},
		{&s.selectNotaryKeyResponsesWithKeyIDsStmt, selectNotaryKeyResponsesWithKeyIDsSQL},
		{&s.selectNotaryKeyMetadataStmt, selectNotaryKeyMetadataSQL},
		{&s.deleteUnusedServerKeysJSONStmt, deleteUnusedServerKeysJSONSQL},
	}.Prepare(db)
}

func (s *notaryServerKeysMetadataStatements) UpsertKey(
	ctx context.Context, txn *sql.Tx, serverName spec.ServerName, keyID gomatrixserverlib.KeyID, newNotaryID tables.NotaryID, newValidUntil spec.Timestamp,
) (tables.NotaryID, error) {
	notaryID := newNotaryID
	// see if the existing notary ID a) exists, b) has a longer valid_until
	var existingNotaryID tables.NotaryID
	var existingValidUntil spec.Timestamp
	selectStmt := txn.Stmt(s.selectNotaryKeyMetadataStmt)
	defer selectStmt.Close()
	if err := selectStmt.QueryRowContext(ctx, serverName, keyID).Scan(&existingNotaryID, &existingValidUntil); err != nil {
		if err != sql.ErrNoRows {
			return 0, err
		}
	}
	if existingValidUntil.Time().After(newValidUntil.Time()) {
		// the existing valid_until is valid longer, so use that.
		return existingNotaryID, nil
	}
	// overwrite the notary_id for this (server_name, key_id) tuple
	upsertStmt := txn.Stmt(s.upsertServerKeysStmt)
	defer upsertStmt.Close()
	_, err := upsertStmt.ExecContext(ctx, notaryID, serverName, keyID)
	return notaryID, err
}

func (s *notaryServerKeysMetadataStatements) SelectKeys(ctx context.Context, txn *sql.Tx, serverName spec.ServerName, keyIDs []gomatrixserverlib.KeyID) ([]gomatrixserverlib.ServerKeys, error) {
	var rows *sql.Rows
	var err error
	if len(keyIDs) == 0 {
		stmt := txn.Stmt(s.selectNotaryKeyResponsesStmt)
		defer stmt.Close()
		rows, err = stmt.QueryContext(ctx, string(serverName))
	} else {
		keyIDstr := make([]string, len(keyIDs))
		for i := range keyIDs {
			keyIDstr[i] = string(keyIDs[i])
		}
		stmt := txn.Stmt(s.selectNotaryKeyResponsesWithKeyIDsStmt)
		defer stmt.Close()
		rows, err = stmt.QueryContext(ctx, string(serverName), keyIDstr)
	}
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "selectNotaryKeyResponsesStmt close failed")
	var results []gomatrixserverlib.ServerKeys
	for rows.Next() {
		var sk gomatrixserverlib.ServerKeys
		var raw string
		if err = rows.Scan(&raw); err != nil {
			return nil, err
		}
		if err = json.Unmarshal([]byte(raw), &sk); err != nil {
			return nil, err
		}
		results = append(results, sk)
	}
	return results, rows.Err()
}

func (s *notaryServerKeysMetadataStatements) DeleteOldJSONResponses(ctx context.Context, txn *sql.Tx) error {
	deleteStmt := txn.Stmt(s.deleteUnusedServerKeysJSONStmt)
	defer deleteStmt.Close()
	_, err := deleteStmt.ExecContext(ctx)
	return err
}
