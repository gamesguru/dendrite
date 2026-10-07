// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	log "github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/clientapi/userutil"
	"codefloe.com/pat-s/zendrite/internal"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/storage/sqlite3/deltas"
	"codefloe.com/pat-s/zendrite/userapi/storage/tables"
)

const accountsSchema = `
-- Stores data about accounts.
CREATE TABLE IF NOT EXISTS userapi_accounts (
    -- The Matrix user ID localpart for this account
    localpart TEXT NOT NULL,
    server_name TEXT NOT NULL,
    -- When this account was first created, as a unix timestamp (ms resolution).
    created_ts BIGINT NOT NULL,
    -- The password hash for this account. Can be NULL if this is a passwordless account.
    password_hash TEXT,
    -- Identifies which application service this account belongs to, if any.
    appservice_id TEXT,
    -- If the account is currently active
    is_deactivated BOOLEAN DEFAULT 0,
	-- The account_type (user = 1, guest = 2, admin = 3, appservice = 4, call guest = 5)
	account_type INTEGER NOT NULL
    -- TODO:
    -- upgraded_ts, devices, any email reset stuff?
);

CREATE UNIQUE INDEX IF NOT EXISTS userapi_accounts_idx ON userapi_accounts(localpart, server_name);
`

const insertAccountSQL = "" +
	"INSERT INTO userapi_accounts(localpart, server_name, created_ts, password_hash, appservice_id, account_type) VALUES ($1, $2, $3, $4, $5, $6)"

const updatePasswordSQL = "" +
	"UPDATE userapi_accounts SET password_hash = $1 WHERE localpart = $2 AND server_name = $3"

const deactivateAccountSQL = "" +
	"UPDATE userapi_accounts SET is_deactivated = 1 WHERE localpart = $1 AND server_name = $2"

const reactivateAccountSQL = "" +
	"UPDATE userapi_accounts SET is_deactivated = 0 WHERE localpart = $1 AND server_name = $2"

const selectAccountByLocalpartSQL = "" +
	"SELECT localpart, server_name, appservice_id, account_type FROM userapi_accounts WHERE localpart = $1 AND server_name = $2"

const selectIsDeactivatedSQL = "" +
	"SELECT is_deactivated FROM userapi_accounts WHERE localpart = $1 AND server_name = $2"

const selectPasswordHashSQL = "" +
	"SELECT password_hash FROM userapi_accounts WHERE localpart = $1 AND server_name = $2 AND is_deactivated = 0"

const selectNewNumericLocalpartSQL = "" +
	"SELECT COALESCE(MAX(CAST(localpart AS INT)), 0) FROM userapi_accounts WHERE CAST(localpart AS INT) <> 0 AND server_name = $1"

const countAccountsSQL = "SELECT COUNT(*) FROM userapi_accounts"

const selectActiveAccountsCreatedBeforeSQL = "" +
	"SELECT localpart, server_name FROM userapi_accounts" +
	" WHERE account_type = $1 AND created_ts < $2 AND is_deactivated = 0" +
	" ORDER BY created_ts LIMIT $3"

type accountsStatements struct {
	db                            *sql.DB
	insertAccountStmt             *sql.Stmt
	countAccountsStmt             *sql.Stmt
	updatePasswordStmt            *sql.Stmt
	deactivateAccountStmt         *sql.Stmt
	reactivateAccountStmt         *sql.Stmt
	selectAccountByLocalpartStmt  *sql.Stmt
	selectIsDeactivatedStmt       *sql.Stmt
	selectPasswordHashStmt        *sql.Stmt
	selectNewNumericLocalpartStmt *sql.Stmt
	selectActiveCreatedBeforeStmt *sql.Stmt
	serverName                    spec.ServerName
}

func NewSQLiteAccountsTable(db *sql.DB, serverName spec.ServerName) (tables.AccountsTable, error) {
	s := &accountsStatements{
		db:         db,
		serverName: serverName,
	}
	_, err := db.Exec(accountsSchema)
	if err != nil {
		return nil, err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations([]sqlutil.Migration{
		{
			Version: "userapi: add is active",
			Up:      deltas.UpIsActive,
			Down:    deltas.DownIsActive,
		},
		{
			Version: "userapi: add account type",
			Up:      deltas.UpAddAccountType,
			Down:    deltas.DownAddAccountType,
		},
	}...)
	err = m.Up(context.Background())
	if err != nil {
		return nil, err
	}
	return s, sqlutil.StatementList{
		{&s.insertAccountStmt, insertAccountSQL},
		{&s.countAccountsStmt, countAccountsSQL},
		{&s.updatePasswordStmt, updatePasswordSQL},
		{&s.deactivateAccountStmt, deactivateAccountSQL},
		{&s.reactivateAccountStmt, reactivateAccountSQL},
		{&s.selectAccountByLocalpartStmt, selectAccountByLocalpartSQL},
		{&s.selectIsDeactivatedStmt, selectIsDeactivatedSQL},
		{&s.selectPasswordHashStmt, selectPasswordHashSQL},
		{&s.selectNewNumericLocalpartStmt, selectNewNumericLocalpartSQL},
		{&s.selectActiveCreatedBeforeStmt, selectActiveAccountsCreatedBeforeSQL},
	}.Prepare(db)
}

func (s *accountsStatements) CountAccounts(ctx context.Context) (int64, error) {
	var count int64
	err := s.countAccountsStmt.QueryRowContext(ctx).Scan(&count)
	return count, err
}

// insertAccount creates a new account. 'hash' should be the password hash for this account. If it is missing,
// this account will be passwordless. Returns an error if this account already exists. Returns the account
// on success.
func (s *accountsStatements) InsertAccount(
	ctx context.Context, txn *sql.Tx, localpart string, serverName spec.ServerName,
	hash, appserviceID string, accountType api.AccountType,
) (*api.Account, error) {
	createdTimeMS := time.Now().UnixNano() / 1000000 //nolint:mnd
	stmt := s.insertAccountStmt

	txnStmt := sqlutil.TxStmt(txn, stmt)
	var err error
	if accountType != api.AccountTypeAppService {
		_, err = txnStmt.ExecContext(ctx, localpart, serverName, createdTimeMS, hash, nil, accountType)
	} else {
		_, err = txnStmt.ExecContext(ctx, localpart, serverName, createdTimeMS, hash, appserviceID, accountType)
	}
	if err != nil {
		return nil, err
	}

	return &api.Account{
		Localpart:    localpart,
		UserID:       userutil.MakeUserID(localpart, serverName),
		ServerName:   serverName,
		AppServiceID: appserviceID,
		AccountType:  accountType,
	}, nil
}

func (s *accountsStatements) UpdatePassword(
	ctx context.Context, localpart string, serverName spec.ServerName,
	passwordHash string,
) (err error) {
	_, err = s.updatePasswordStmt.ExecContext(ctx, passwordHash, localpart, serverName)
	return
}

func (s *accountsStatements) DeactivateAccount(
	ctx context.Context, localpart string, serverName spec.ServerName,
) (err error) {
	_, err = s.deactivateAccountStmt.ExecContext(ctx, localpart, serverName)
	return
}

// ReactivateAccount clears the deactivated flag and reports sql.ErrNoRows
// when no account matches, so callers need no separate existence lookup.
func (s *accountsStatements) ReactivateAccount(
	ctx context.Context, localpart string, serverName spec.ServerName,
) error {
	result, err := s.reactivateAccountStmt.ExecContext(ctx, localpart, serverName)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *accountsStatements) SelectPasswordHash(
	ctx context.Context, localpart string, serverName spec.ServerName,
) (hash string, err error) {
	err = s.selectPasswordHashStmt.QueryRowContext(ctx, localpart, serverName).Scan(&hash)
	return
}

func (s *accountsStatements) SelectAccountByLocalpart(
	ctx context.Context, localpart string, serverName spec.ServerName,
) (*api.Account, error) {
	var appserviceIDPtr sql.NullString
	var acc api.Account

	stmt := s.selectAccountByLocalpartStmt
	err := stmt.QueryRowContext(ctx, localpart, serverName).Scan(&acc.Localpart, &acc.ServerName, &appserviceIDPtr, &acc.AccountType)
	if err != nil {
		if err != sql.ErrNoRows {
			log.WithError(err).Error("Unable to retrieve user from the db")
		}
		return nil, err
	}
	if appserviceIDPtr.Valid {
		acc.AppServiceID = appserviceIDPtr.String
	}

	acc.UserID = userutil.MakeUserID(acc.Localpart, acc.ServerName)
	return &acc, nil
}

func (s *accountsStatements) SelectIsDeactivated(
	ctx context.Context, localpart string, serverName spec.ServerName,
) (bool, error) {
	var deactivated bool
	err := s.selectIsDeactivatedStmt.QueryRowContext(ctx, localpart, serverName).Scan(&deactivated)
	return deactivated, err
}

func (s *accountsStatements) SelectNewNumericLocalpart(
	ctx context.Context, txn *sql.Tx, serverName spec.ServerName,
) (id int64, err error) {
	stmt := s.selectNewNumericLocalpartStmt
	if txn != nil {
		stmt = sqlutil.TxStmt(txn, stmt)
	}
	err = stmt.QueryRowContext(ctx, serverName).Scan(&id)
	if err == sql.ErrNoRows {
		return 1, nil
	}
	return id + 1, err
}

func (s *accountsStatements) SelectActiveAccountsCreatedBefore(
	ctx context.Context, accountType api.AccountType, before spec.Timestamp, limit int,
) ([]api.Account, error) {
	rows, err := s.selectActiveCreatedBeforeStmt.QueryContext(ctx, accountType, before, limit)
	if err != nil {
		return nil, err
	}
	defer internal.CloseAndLogIfError(ctx, rows, "SelectActiveAccountsCreatedBefore: rows.close() failed")
	var accounts []api.Account
	for rows.Next() {
		acc := api.Account{AccountType: accountType}
		if err = rows.Scan(&acc.Localpart, &acc.ServerName); err != nil {
			return nil, err
		}
		acc.UserID = userutil.MakeUserID(acc.Localpart, acc.ServerName)
		accounts = append(accounts, acc)
	}
	return accounts, rows.Err()
}
