// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/storage/tables"
)

const clientTransactionsSchema = `
-- Maps a client's transaction ID to the event it produced, so that retries
-- are answered with that event instead of storing another one, also when
-- they reach a different process after an HA takeover.
CREATE TABLE IF NOT EXISTS roomserver_client_transactions (
    room_id TEXT NOT NULL,
    sender_id TEXT NOT NULL,
    -- The client session (device) the transaction ID is scoped to.
    session_id BIGINT NOT NULL,
    transaction_id TEXT NOT NULL,
    event_id TEXT NOT NULL,
    -- When the mapping was recorded, in milliseconds, for expiry.
    inserted_ts BIGINT NOT NULL,
    PRIMARY KEY (room_id, sender_id, session_id, transaction_id)
);
CREATE INDEX IF NOT EXISTS roomserver_client_transactions_inserted_ts_idx ON roomserver_client_transactions (inserted_ts);
`

const insertClientTransactionSQL = "" +
	"INSERT INTO roomserver_client_transactions (room_id, sender_id, session_id, transaction_id, event_id, inserted_ts)" +
	" VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT DO NOTHING"

const selectClientTransactionEventIDSQL = "" +
	"SELECT event_id FROM roomserver_client_transactions" +
	" WHERE room_id = $1 AND sender_id = $2 AND session_id = $3 AND transaction_id = $4"

const deleteClientTransactionsBeforeSQL = "" +
	"DELETE FROM roomserver_client_transactions WHERE inserted_ts < $1"

type clientTransactionsStatements struct {
	insertClientTransactionStmt        *sql.Stmt
	selectClientTransactionEventIDStmt *sql.Stmt
	deleteClientTransactionsBeforeStmt *sql.Stmt
}

func CreateClientTransactionsTable(db *sql.DB) error {
	_, err := db.Exec(clientTransactionsSchema)
	return err
}

func PrepareClientTransactionsTable(db *sql.DB) (tables.ClientTransactions, error) {
	s := &clientTransactionsStatements{}
	return s, sqlutil.StatementList{
		{&s.insertClientTransactionStmt, insertClientTransactionSQL},
		{&s.selectClientTransactionEventIDStmt, selectClientTransactionEventIDSQL},
		{&s.deleteClientTransactionsBeforeStmt, deleteClientTransactionsBeforeSQL},
	}.Prepare(db)
}

func (s *clientTransactionsStatements) InsertClientTransaction(
	ctx context.Context, txn *sql.Tx, roomID, senderID string, sessionID int64, transactionID, eventID string, insertedTS int64,
) error {
	_, err := sqlutil.TxStmt(txn, s.insertClientTransactionStmt).ExecContext(
		ctx, roomID, senderID, sessionID, transactionID, eventID, insertedTS,
	)
	return err
}

func (s *clientTransactionsStatements) SelectClientTransactionEventID(
	ctx context.Context, txn *sql.Tx, roomID, senderID string, sessionID int64, transactionID string,
) (eventID string, err error) {
	err = sqlutil.TxStmt(txn, s.selectClientTransactionEventIDStmt).QueryRowContext(
		ctx, roomID, senderID, sessionID, transactionID,
	).Scan(&eventID)
	return eventID, err
}

func (s *clientTransactionsStatements) DeleteClientTransactionsBefore(
	ctx context.Context, txn *sql.Tx, insertedTS int64,
) (int64, error) {
	res, err := sqlutil.TxStmt(txn, s.deleteClientTransactionsBeforeStmt).ExecContext(ctx, insertedTS)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
