// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/storage/tables"
)

const delayedEventsSchema = `
-- Stores the events that clients scheduled to be sent into a room later
-- (MSC4140), and the outcome of finalized ones until they are purged.
CREATE TABLE IF NOT EXISTS userapi_delayed_events (
	delay_id TEXT NOT NULL PRIMARY KEY,
	localpart TEXT NOT NULL,
	server_name TEXT NOT NULL,
	-- The device that scheduled the event, or '' if there was none
	device_id TEXT NOT NULL,
	room_id TEXT NOT NULL,
	event_type TEXT NOT NULL,
	-- NULL for message events
	state_key TEXT,
	content TEXT NOT NULL,
	-- An application service's massaged timestamp, or 0 to use the send time
	origin_server_ts BIGINT NOT NULL,
	delay_ms BIGINT NOT NULL,
	-- When the event was scheduled or last restarted
	delayed_since_ts BIGINT NOT NULL,
	send_ts BIGINT NOT NULL,
	-- 'scheduled', 'sending' while a send attempt holds it, or 'finalized'
	status TEXT NOT NULL,
	-- The outcome of a finalized event: the ID it was sent as, or the
	-- standard error response that prevented sending. Neither is set for a
	-- canceled event.
	event_id TEXT,
	error TEXT,
	finalized_ts BIGINT
);
CREATE INDEX IF NOT EXISTS userapi_delayed_events_user_idx ON userapi_delayed_events (localpart, server_name);
CREATE INDEX IF NOT EXISTS userapi_delayed_events_send_ts_idx ON userapi_delayed_events (send_ts) WHERE status = 'scheduled';
CREATE INDEX IF NOT EXISTS userapi_delayed_events_state_idx ON userapi_delayed_events (room_id, event_type, state_key)
	WHERE state_key IS NOT NULL AND status = 'scheduled';
`

const delayedEventColumns = "delay_id, localpart, server_name, device_id, room_id, event_type, state_key, content," +
	" origin_server_ts, delay_ms, delayed_since_ts, send_ts, status, event_id, error, finalized_ts, sticky_duration_ms"

const insertDelayedEventSQL = "" +
	"INSERT INTO userapi_delayed_events (delay_id, localpart, server_name, device_id, room_id, event_type, state_key, content," +
	" origin_server_ts, delay_ms, delayed_since_ts, send_ts, sticky_duration_ms, status)" +
	" VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, 'scheduled')"

const selectDelayedEventSQL = "" +
	"SELECT " + delayedEventColumns + " FROM userapi_delayed_events WHERE delay_id = $1"

const selectPendingDelayedEventsForUserSQL = "" +
	"SELECT " + delayedEventColumns + " FROM userapi_delayed_events" +
	" WHERE localpart = $1 AND server_name = $2 AND status <> 'finalized' ORDER BY send_ts, delay_id"

const selectFinalizedDelayedEventsForUserSQL = "" +
	"SELECT " + delayedEventColumns + " FROM userapi_delayed_events" +
	" WHERE localpart = $1 AND server_name = $2 AND status = 'finalized' ORDER BY finalized_ts DESC, delay_id"

const countPendingDelayedEventsSQL = "" +
	"SELECT COUNT(*), COALESCE(MIN(send_ts), 0) FROM userapi_delayed_events" +
	" WHERE localpart = $1 AND server_name = $2 AND status <> 'finalized'"

const restartDelayedEventSQL = "" +
	"UPDATE userapi_delayed_events SET delayed_since_ts = $1, send_ts = $1 + delay_ms" +
	" WHERE delay_id = $2 AND status = 'scheduled'"

const claimDelayedEventSQL = "" +
	"UPDATE userapi_delayed_events SET status = 'sending' WHERE delay_id = $1 AND status = 'scheduled'" +
	" RETURNING " + delayedEventColumns

const claimDueDelayedEventsSQL = "" +
	"UPDATE userapi_delayed_events SET status = 'sending'" +
	" WHERE (status = 'scheduled' AND send_ts <= $1) OR (status = 'sending' AND $2)" +
	" RETURNING " + delayedEventColumns

const unclaimDelayedEventSQL = "" +
	"UPDATE userapi_delayed_events SET status = 'scheduled' WHERE delay_id = $1 AND status = 'sending'"

const finalizeDelayedEventSQL = "" +
	"UPDATE userapi_delayed_events SET status = 'finalized', event_id = $1, error = $2, finalized_ts = $3" +
	" WHERE delay_id = $4 AND status = $5"

const cancelDelayedStateEventsSQL = "" +
	"UPDATE userapi_delayed_events SET status = 'finalized', finalized_ts = $1" +
	" WHERE room_id = $2 AND event_type = $3 AND state_key = $4 AND status = 'scheduled'" +
	" AND NOT (localpart = $5 AND server_name = $6)"

const selectNextSendTSSQL = "" +
	"SELECT MIN(send_ts) FROM userapi_delayed_events WHERE status = 'scheduled'"

const deleteDelayedEventsForUserSQL = "" +
	"DELETE FROM userapi_delayed_events WHERE localpart = $1 AND server_name = $2"

const deleteFinalizedDelayedEventsSQL = "" +
	"DELETE FROM userapi_delayed_events WHERE status = 'finalized' AND finalized_ts < $1"

const deleteExcessFinalizedDelayedEventsSQL = "" +
	"DELETE FROM userapi_delayed_events WHERE delay_id IN (" +
	"SELECT delay_id FROM userapi_delayed_events WHERE localpart = $1 AND server_name = $2 AND status = 'finalized'" +
	" ORDER BY finalized_ts DESC, delay_id OFFSET $3)"

type delayedEventsStatements struct {
	insertDelayedEventStmt                  *sql.Stmt
	selectDelayedEventStmt                  *sql.Stmt
	selectPendingDelayedEventsForUserStmt   *sql.Stmt
	selectFinalizedDelayedEventsForUserStmt *sql.Stmt
	countPendingDelayedEventsStmt           *sql.Stmt
	restartDelayedEventStmt                 *sql.Stmt
	claimDelayedEventStmt                   *sql.Stmt
	claimDueDelayedEventsStmt               *sql.Stmt
	unclaimDelayedEventStmt                 *sql.Stmt
	finalizeDelayedEventStmt                *sql.Stmt
	cancelDelayedStateEventsStmt            *sql.Stmt
	selectNextSendTSStmt                    *sql.Stmt
	deleteDelayedEventsForUserStmt          *sql.Stmt
	deleteFinalizedDelayedEventsStmt        *sql.Stmt
	deleteExcessFinalizedDelayedEventsStmt  *sql.Stmt
}

func NewPostgresDelayedEventsTable(db *sql.DB) (tables.DelayedEventsTable, error) {
	s := &delayedEventsStatements{}
	_, err := db.Exec(delayedEventsSchema)
	if err != nil {
		return nil, err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: "userapi: add delayed event sticky duration",
		Up:      tables.AddDelayedEventStickyDuration,
	})
	if err = m.Up(context.Background()); err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertDelayedEventStmt, insertDelayedEventSQL},
		{&s.selectDelayedEventStmt, selectDelayedEventSQL},
		{&s.selectPendingDelayedEventsForUserStmt, selectPendingDelayedEventsForUserSQL},
		{&s.selectFinalizedDelayedEventsForUserStmt, selectFinalizedDelayedEventsForUserSQL},
		{&s.countPendingDelayedEventsStmt, countPendingDelayedEventsSQL},
		{&s.restartDelayedEventStmt, restartDelayedEventSQL},
		{&s.claimDelayedEventStmt, claimDelayedEventSQL},
		{&s.claimDueDelayedEventsStmt, claimDueDelayedEventsSQL},
		{&s.unclaimDelayedEventStmt, unclaimDelayedEventSQL},
		{&s.finalizeDelayedEventStmt, finalizeDelayedEventSQL},
		{&s.cancelDelayedStateEventsStmt, cancelDelayedStateEventsSQL},
		{&s.selectNextSendTSStmt, selectNextSendTSSQL},
		{&s.deleteDelayedEventsForUserStmt, deleteDelayedEventsForUserSQL},
		{&s.deleteFinalizedDelayedEventsStmt, deleteFinalizedDelayedEventsSQL},
		{&s.deleteExcessFinalizedDelayedEventsStmt, deleteExcessFinalizedDelayedEventsSQL},
	}.Prepare(db)
}

func (s *delayedEventsStatements) InsertDelayedEvent(
	ctx context.Context, txn *sql.Tx, event *api.DelayedEvent,
) error {
	stmt := sqlutil.TxStmt(txn, s.insertDelayedEventStmt)
	_, err := stmt.ExecContext(
		ctx, event.DelayID, event.Localpart, event.ServerName, event.DeviceID,
		event.RoomID, event.Type, event.StateKey, string(event.Content),
		event.OriginServerTS, event.DelayMS, event.DelayedSince, event.SendTS, event.StickyDurationMS,
	)
	return err
}

func (s *delayedEventsStatements) SelectDelayedEvent(
	ctx context.Context, txn *sql.Tx, delayID string,
) (*api.DelayedEvent, error) {
	stmt := sqlutil.TxStmt(txn, s.selectDelayedEventStmt)
	event, err := scanDelayedEvent(stmt.QueryRowContext(ctx, delayID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return event, err
}

func (s *delayedEventsStatements) SelectDelayedEventsForUser(
	ctx context.Context, txn *sql.Tx, localpart string, serverName spec.ServerName, finalized bool,
) ([]api.DelayedEvent, error) {
	stmt := sqlutil.TxStmt(txn, s.selectPendingDelayedEventsForUserStmt)
	if finalized {
		stmt = sqlutil.TxStmt(txn, s.selectFinalizedDelayedEventsForUserStmt)
	}
	rows, err := stmt.QueryContext(ctx, localpart, serverName)
	if err != nil {
		return nil, err
	}
	return scanDelayedEvents(ctx, rows)
}

func (s *delayedEventsStatements) CountPendingDelayedEvents(
	ctx context.Context, txn *sql.Tx, localpart string, serverName spec.ServerName,
) (count int, nextSendTS spec.Timestamp, err error) {
	stmt := sqlutil.TxStmt(txn, s.countPendingDelayedEventsStmt)
	err = stmt.QueryRowContext(ctx, localpart, serverName).Scan(&count, &nextSendTS)
	return count, nextSendTS, err
}

func (s *delayedEventsStatements) RestartDelayedEvent(
	ctx context.Context, txn *sql.Tx, delayID string, now spec.Timestamp,
) (bool, error) {
	stmt := sqlutil.TxStmt(txn, s.restartDelayedEventStmt)
	return execAffectsRow(stmt.ExecContext(ctx, now, delayID))
}

func (s *delayedEventsStatements) ClaimDelayedEvent(
	ctx context.Context, txn *sql.Tx, delayID string,
) (*api.DelayedEvent, error) {
	stmt := sqlutil.TxStmt(txn, s.claimDelayedEventStmt)
	event, err := scanDelayedEvent(stmt.QueryRowContext(ctx, delayID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	return event, err
}

func (s *delayedEventsStatements) ClaimDueDelayedEvents(
	ctx context.Context, txn *sql.Tx, now spec.Timestamp, reclaim bool,
) ([]api.DelayedEvent, error) {
	stmt := sqlutil.TxStmt(txn, s.claimDueDelayedEventsStmt)
	rows, err := stmt.QueryContext(ctx, now, reclaim)
	if err != nil {
		return nil, err
	}
	events, err := scanDelayedEvents(ctx, rows)
	if err != nil {
		return nil, err
	}
	// RETURNING yields rows in no particular order.
	sort.SliceStable(events, func(i, j int) bool {
		if events[i].SendTS != events[j].SendTS {
			return events[i].SendTS < events[j].SendTS
		}
		return events[i].DelayID < events[j].DelayID
	})
	return events, nil
}

func (s *delayedEventsStatements) UnclaimDelayedEvent(
	ctx context.Context, txn *sql.Tx, delayID string,
) error {
	stmt := sqlutil.TxStmt(txn, s.unclaimDelayedEventStmt)
	_, err := stmt.ExecContext(ctx, delayID)
	return err
}

func (s *delayedEventsStatements) FinalizeDelayedEvent(
	ctx context.Context, txn *sql.Tx, delayID string, from api.DelayedEventStatus,
	eventID string, sendErr json.RawMessage, now spec.Timestamp,
) (bool, error) {
	stmt := sqlutil.TxStmt(txn, s.finalizeDelayedEventStmt)
	return execAffectsRow(stmt.ExecContext(ctx, nullString(eventID), nullString(string(sendErr)), now, delayID, from))
}

func (s *delayedEventsStatements) CancelDelayedStateEvents(
	ctx context.Context, txn *sql.Tx, roomID, eventType, stateKey, exceptLocalpart string,
	exceptServerName spec.ServerName, now spec.Timestamp,
) error {
	stmt := sqlutil.TxStmt(txn, s.cancelDelayedStateEventsStmt)
	_, err := stmt.ExecContext(ctx, now, roomID, eventType, stateKey, exceptLocalpart, exceptServerName)
	return err
}

func (s *delayedEventsStatements) SelectNextSendTS(
	ctx context.Context, txn *sql.Tx,
) (spec.Timestamp, bool, error) {
	stmt := sqlutil.TxStmt(txn, s.selectNextSendTSStmt)
	var next sql.NullInt64
	if err := stmt.QueryRowContext(ctx).Scan(&next); err != nil {
		return 0, false, err
	}
	return spec.Timestamp(next.Int64), next.Valid, nil
}

func (s *delayedEventsStatements) DeleteDelayedEventsForUser(
	ctx context.Context, txn *sql.Tx, localpart string, serverName spec.ServerName,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteDelayedEventsForUserStmt)
	_, err := stmt.ExecContext(ctx, localpart, serverName)
	return err
}

func (s *delayedEventsStatements) DeleteFinalizedDelayedEvents(
	ctx context.Context, txn *sql.Tx, before spec.Timestamp,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteFinalizedDelayedEventsStmt)
	_, err := stmt.ExecContext(ctx, before)
	return err
}

func (s *delayedEventsStatements) DeleteExcessFinalizedDelayedEvents(
	ctx context.Context, txn *sql.Tx, localpart string, serverName spec.ServerName, keep int,
) error {
	stmt := sqlutil.TxStmt(txn, s.deleteExcessFinalizedDelayedEventsStmt)
	_, err := stmt.ExecContext(ctx, localpart, serverName, keep)
	return err
}

type rowScanner interface {
	Scan(dest ...any) error
}

func scanDelayedEvent(row rowScanner) (*api.DelayedEvent, error) {
	var event api.DelayedEvent
	var stateKey, eventID, sendErr sql.NullString
	var content string
	var finalizedTS, stickyDurationMS sql.NullInt64
	if err := row.Scan(
		&event.DelayID, &event.Localpart, &event.ServerName, &event.DeviceID,
		&event.RoomID, &event.Type, &stateKey, &content,
		&event.OriginServerTS, &event.DelayMS, &event.DelayedSince, &event.SendTS,
		&event.Status, &eventID, &sendErr, &finalizedTS, &stickyDurationMS,
	); err != nil {
		return nil, err
	}
	if stateKey.Valid {
		event.StateKey = &stateKey.String
	}
	event.Content = json.RawMessage(content)
	event.EventID = eventID.String
	if sendErr.Valid {
		event.Error = json.RawMessage(sendErr.String)
	}
	event.FinalizedTS = spec.Timestamp(finalizedTS.Int64)
	if stickyDurationMS.Valid {
		event.StickyDurationMS = &stickyDurationMS.Int64
	}
	return &event, nil
}

func scanDelayedEvents(ctx context.Context, rows *sql.Rows) ([]api.DelayedEvent, error) {
	defer internal.CloseAndLogIfError(ctx, rows, "scanDelayedEvents: rows.close() failed")
	var events []api.DelayedEvent
	for rows.Next() {
		event, err := scanDelayedEvent(rows)
		if err != nil {
			return nil, err
		}
		events = append(events, *event)
	}
	return events, rows.Err()
}

func execAffectsRow(res sql.Result, err error) (bool, error) {
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

func nullString(s string) sql.NullString {
	return sql.NullString{String: s, Valid: s != ""}
}
