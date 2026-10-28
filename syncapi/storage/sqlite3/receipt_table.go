// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/syncapi/storage/sqlite3/deltas"
	"codefloe.com/pat-s/zendrite/syncapi/storage/tables"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

const receiptsSchema = `
-- Stores data about receipts
CREATE TABLE IF NOT EXISTS syncapi_receipts (
	-- The ID
	id BIGINT,
	room_id TEXT NOT NULL,
	receipt_type TEXT NOT NULL,
	user_id TEXT NOT NULL,
	event_id TEXT NOT NULL,
	receipt_ts BIGINT NOT NULL,
	CONSTRAINT syncapi_receipts_unique UNIQUE (room_id, receipt_type, user_id)
);
CREATE INDEX IF NOT EXISTS syncapi_receipts_room_id_idx ON syncapi_receipts(room_id);
`

const upsertReceipt = "" +
	"INSERT INTO syncapi_receipts" +
	" (id, room_id, receipt_type, user_id, event_id, thread_id, receipt_ts)" +
	" VALUES ($1, $2, $3, $4, $5, $6, $7)" +
	" ON CONFLICT (room_id, receipt_type, user_id, thread_id)" +
	" DO UPDATE SET id = CASE" +
	"   WHEN syncapi_receipts.event_id != excluded.event_id THEN excluded.id" +
	"   ELSE syncapi_receipts.id" +
	" END, event_id = excluded.event_id, receipt_ts = excluded.receipt_ts"

const selectUserReceiptEventIDSQL = "" +
	"SELECT event_id FROM syncapi_receipts" +
	" WHERE room_id = $1 AND receipt_type = $2 AND user_id = $3 AND thread_id = $4"

const selectRoomReceipts = "" +
	"SELECT id, room_id, receipt_type, user_id, event_id, thread_id, receipt_ts" +
	" FROM syncapi_receipts" +
	" WHERE id > $1 and room_id in ($2)"

const selectMaxReceiptIDSQL = "" +
	"SELECT MAX(id) FROM syncapi_receipts"

const purgeReceiptsSQL = "" +
	"DELETE FROM syncapi_receipts WHERE room_id = $1"

const selectConnectionReceiptsSQL = "" +
	"SELECT room_id, receipt_type, user_id, last_delivered_event_id, thread_id, last_delivered_ts " +
	"FROM syncapi_sliding_sync_connection_receipts " +
	"WHERE connection_key = $1"

const upsertConnectionReceiptSQL = "" +
	"INSERT INTO syncapi_sliding_sync_connection_receipts " +
	"(connection_key, room_id, receipt_type, user_id, last_delivered_event_id, thread_id, last_delivered_ts) " +
	"VALUES ($1, $2, $3, $4, $5, $6, $7) " +
	"ON CONFLICT (connection_key, room_id, receipt_type, user_id, thread_id) " +
	"DO UPDATE SET last_delivered_event_id = $5, last_delivered_ts = $7"

const deleteConnectionReceiptsSQL = "" +
	"DELETE FROM syncapi_sliding_sync_connection_receipts WHERE connection_key = $1"

const deleteConnectionReceiptsForRoomSQL = "" +
	"DELETE FROM syncapi_sliding_sync_connection_receipts WHERE connection_key = $1 AND room_id = $2"

type receiptStatements struct {
	db                              *sql.DB
	streamIDStatements              *StreamIDStatements
	upsertReceipt                   *sql.Stmt
	selectRoomReceipts              *sql.Stmt
	selectUserReceiptEventID        *sql.Stmt
	selectMaxReceiptID              *sql.Stmt
	purgeReceiptsStmt               *sql.Stmt
	selectConnectionReceipts        *sql.Stmt
	upsertConnectionReceipt         *sql.Stmt
	deleteConnectionReceipts        *sql.Stmt
	deleteConnectionReceiptsForRoom *sql.Stmt
}

// NewSqliteReceiptsTable creates and migrates receipt and Sliding Sync storage,
// then prepares receipt queries. streamID supplies receipt stream positions. Schema, migration,
// and statement preparation errors are returned.
func NewSqliteReceiptsTable(db *sql.DB, streamID *StreamIDStatements) (tables.Receipts, error) {
	_, err := db.Exec(receiptsSchema)
	if err != nil {
		return nil, err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(
		sqlutil.Migration{
			Version: "syncapi: fix sequences",
			Up:      deltas.UpFixSequences,
		},
		sqlutil.Migration{
			Version: "syncapi: create sliding sync tables",
			Up:      deltas.UpCreateSlidingSyncTables,
		},
		sqlutil.Migration{Version: "syncapi: create sliding sync list windows", Up: deltas.UpCreateSlidingSyncListWindows},
		sqlutil.Migration{
			Version: "syncapi: add connection receipts table for sliding sync",
			Up:      deltas.UpAddConnectionReceipts,
		},
		sqlutil.Migration{Version: "syncapi: threaded receipts", Up: deltas.UpThreadedReceipts},
	)
	err = m.Up(context.Background())
	if err != nil {
		return nil, err
	}
	r := &receiptStatements{
		db:                 db,
		streamIDStatements: streamID,
	}
	return r, sqlutil.StatementList{
		{&r.upsertReceipt, upsertReceipt},
		{&r.selectRoomReceipts, selectRoomReceipts},
		{&r.selectUserReceiptEventID, selectUserReceiptEventIDSQL},
		{&r.selectMaxReceiptID, selectMaxReceiptIDSQL},
		{&r.purgeReceiptsStmt, purgeReceiptsSQL},
		{&r.selectConnectionReceipts, selectConnectionReceiptsSQL},
		{&r.upsertConnectionReceipt, upsertConnectionReceiptSQL},
		{&r.deleteConnectionReceipts, deleteConnectionReceiptsSQL},
		{&r.deleteConnectionReceiptsForRoom, deleteConnectionReceiptsForRoomSQL},
	}.Prepare(db)
}

// UpsertReceipt creates new user receipts.
func (r *receiptStatements) UpsertReceipt(ctx context.Context, txn *sql.Tx, roomId, receiptType, userId, eventId, threadID string, timestamp spec.Timestamp) (pos types.StreamPosition, err error) {
	// Always generate a new ID - the CASE expression in SQL will decide whether to use it
	pos, err = r.streamIDStatements.nextReceiptID(ctx, txn)
	if err != nil {
		return
	}
	stmt := sqlutil.TxStmt(txn, r.upsertReceipt)
	_, err = stmt.ExecContext(ctx, pos, roomId, receiptType, userId, eventId, threadID, timestamp)
	return
}

// SelectUserReceiptEventID returns the event the user's receipt is at, or "" if there is none.
func (r *receiptStatements) SelectUserReceiptEventID(ctx context.Context, txn *sql.Tx, roomID, receiptType, userID, threadID string) (string, error) {
	var eventID string
	err := sqlutil.TxStmt(txn, r.selectUserReceiptEventID).QueryRowContext(ctx, roomID, receiptType, userID, threadID).Scan(&eventID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return eventID, err
}

// SelectRoomReceiptsAfter select all receipts for a given room after a specific timestamp.
func (r *receiptStatements) SelectRoomReceiptsAfter(ctx context.Context, txn *sql.Tx, roomIDs []string, streamPos types.StreamPosition) (types.StreamPosition, []types.OutputReceiptEvent, error) {
	if len(roomIDs) == 0 {
		return streamPos, nil, nil
	}
	selectSQL := strings.Replace(selectRoomReceipts, "($2)", sqlutil.QueryVariadicOffset(len(roomIDs), 1), 1)
	var lastPos types.StreamPosition
	params := make([]any, len(roomIDs)+1)
	params[0] = streamPos
	for k, v := range roomIDs {
		params[k+1] = v
	}
	var prep *sql.Stmt
	var err error
	if txn != nil {
		prep, err = txn.PrepareContext(ctx, selectSQL)
	} else {
		prep, err = r.db.PrepareContext(ctx, selectSQL)
	}
	if err != nil {
		return 0, nil, fmt.Errorf("unable to prepare statement: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, prep, "SelectRoomReceiptsAfter: prep.close() failed")
	rows, err := prep.QueryContext(ctx, params...)
	if err != nil {
		return 0, nil, fmt.Errorf("unable to query room receipts: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectRoomReceiptsAfter: rows.close() failed")
	var res []types.OutputReceiptEvent
	for rows.Next() {
		r := types.OutputReceiptEvent{}
		var id types.StreamPosition
		err = rows.Scan(&id, &r.RoomID, &r.Type, &r.UserID, &r.EventID, &r.ThreadID, &r.Timestamp)
		if err != nil {
			return 0, res, fmt.Errorf("unable to scan row to api.Receipts: %w", err)
		}
		res = append(res, r)
		if id > lastPos {
			lastPos = id
		}
	}
	return lastPos, res, rows.Err()
}

func (s *receiptStatements) SelectMaxReceiptID(
	ctx context.Context, txn *sql.Tx,
) (id int64, err error) {
	var nullableID sql.NullInt64
	stmt := sqlutil.TxStmt(txn, s.selectMaxReceiptID)
	err = stmt.QueryRowContext(ctx).Scan(&nullableID)
	if nullableID.Valid {
		id = nullableID.Int64
	}
	return
}

func (s *receiptStatements) PurgeReceipts(
	ctx context.Context, txn *sql.Tx, roomID string,
) error {
	purgeReceiptsStmt := sqlutil.TxStmt(txn, s.purgeReceiptsStmt)
	_, err := purgeReceiptsStmt.ExecContext(ctx, roomID)
	return err
}

// SelectLatestUserReceiptsForConnection returns receipts that have changed since last delivered
// to this connection. Uses event-ID based comparison instead of position tracking.
//
// IMPORTANT: This query is designed to solve the concurrent connection receipt repetition problem
// by tracking what was delivered to EACH connection separately.
//
// Returns receipts for ALL users in the room (not just the requesting user) to support:
// - Read receipts UI (showing where other users have read to)
// - Client-side notification badge logic
//
// Private receipts are filtered in v4_extensions.go, not here.
//
// Algorithm:
// 1. Get latest receipt for ALL users in each room (from syncapi_receipts)
// 2. Get last delivered receipts for this connection (from syncapi_sliding_sync_connection_receipts)
// 3. Compare event_ids - only return receipts where event_id has changed
// 4. Update connection state after delivery (caller's responsibility).
func (s *receiptStatements) SelectLatestUserReceiptsForConnection(
	ctx context.Context,
	txn *sql.Tx,
	connectionKey int64,
	roomIDs []string,
	userID string,
) ([]types.OutputReceiptEvent, error) {
	if len(roomIDs) == 0 {
		return []types.OutputReceiptEvent{}, nil
	}

	_, latest, err := s.SelectRoomReceiptsAfter(ctx, txn, roomIDs, 0)
	if err != nil {
		return nil, err
	}
	latestReceipts := make(map[[4]string]types.OutputReceiptEvent)
	for _, receipt := range latest {
		latestReceipts[[4]string{receipt.RoomID, receipt.Type, receipt.UserID, receipt.ThreadID}] = receipt
	}

	// Step 2: Get what we last delivered to this connection
	selectConnectionReceipts := sqlutil.TxStmt(txn, s.selectConnectionReceipts)
	deliveredRows, err := selectConnectionReceipts.QueryContext(ctx, connectionKey)
	if err != nil {
		return nil, fmt.Errorf("failed to query connection receipts: %w", err)
	}
	defer internal.CloseAndLogIfError(ctx, deliveredRows, "SelectLatestUserReceiptsForConnection: deliveredRows.close() failed")

	lastDelivered := make(map[[4]string]string) // key: room, receipt type, user, thread -> event_id
	for deliveredRows.Next() {
		var roomID, receiptType, userID, eventID, threadID string
		var ts spec.Timestamp
		err = deliveredRows.Scan(&roomID, &receiptType, &userID, &eventID, &threadID, &ts)
		if err != nil {
			return nil, fmt.Errorf("failed to scan connection receipt: %w", err)
		}
		key := [4]string{roomID, receiptType, userID, threadID}
		lastDelivered[key] = eventID
	}
	if err = deliveredRows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate connection receipts: %w", err)
	}

	// Step 3: Compare and return only changed receipts
	var result []types.OutputReceiptEvent
	for key, latest := range latestReceipts {
		lastEventID, exists := lastDelivered[key]
		// Return if: (1) never delivered before, OR (2) event_id has changed
		if !exists || lastEventID != latest.EventID {
			result = append(result, latest)
		}
	}

	return result, nil
}

// UpsertConnectionReceipt updates the last delivered receipt for a connection.
func (s *receiptStatements) UpsertConnectionReceipt(
	ctx context.Context,
	txn *sql.Tx,
	connectionKey int64,
	roomID, receiptType, userID, eventID, threadID string,
	timestamp spec.Timestamp,
) error {
	upsertConnectionReceipt := sqlutil.TxStmt(txn, s.upsertConnectionReceipt)
	_, err := upsertConnectionReceipt.ExecContext(
		ctx, connectionKey, roomID, receiptType, userID, eventID, threadID, timestamp,
	)
	return err
}

// DeleteConnectionReceipts removes all delivered receipt state for a connection.
// This should be called on fresh sync (no pos token) to ensure receipts are re-delivered.
func (s *receiptStatements) DeleteConnectionReceipts(
	ctx context.Context,
	txn *sql.Tx,
	connectionKey int64,
) error {
	deleteConnectionReceipts := sqlutil.TxStmt(txn, s.deleteConnectionReceipts)
	_, err := deleteConnectionReceipts.ExecContext(ctx, connectionKey)
	return err
}

// DeleteConnectionReceiptsForRoom removes delivered receipt state for a specific room on a connection.
// This should be called when timeline expansion occurs (initial:true with expanded_timeline:true)
// to ensure receipts are re-delivered for that room.
func (s *receiptStatements) DeleteConnectionReceiptsForRoom(
	ctx context.Context,
	txn *sql.Tx,
	connectionKey int64,
	roomID string,
) error {
	deleteConnectionReceiptsForRoom := sqlutil.TxStmt(txn, s.deleteConnectionReceiptsForRoom)
	_, err := deleteConnectionReceiptsForRoom.ExecContext(ctx, connectionKey, roomID)
	return err
}
