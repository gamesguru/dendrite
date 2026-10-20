// Copyright 2024 New Vector Ltd.
// Copyright 2019, 2020 The Matrix.org Foundation C.I.C.
// Copyright 2017, 2018 New Vector Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sqlite3

import (
	"context"
	"database/sql"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/mediaapi/storage/sqlite3/deltas"
	"codefloe.com/pat-s/zendrite/mediaapi/storage/tables"
	"codefloe.com/pat-s/zendrite/mediaapi/types"
)

const mediaSchema = `
-- The media_repository table holds metadata for each media file stored and accessible to the local server,
-- the actual file is stored separately.
CREATE TABLE IF NOT EXISTS mediaapi_media_repository (
    -- The id used to refer to the media.
    -- For uploads to this server this is a base64-encoded sha256 hash of the file data
    -- For media from remote servers, this can be any unique identifier string
    media_id TEXT NOT NULL,
    -- The origin of the media as requested by the client. Should be a homeserver domain.
    media_origin TEXT NOT NULL,
    -- The MIME-type of the media file as specified when uploading.
    content_type TEXT NOT NULL,
    -- Size of the media file in bytes.
    file_size_bytes INTEGER NOT NULL,
    -- When the content was uploaded in UNIX epoch ms.
    creation_ts INTEGER NOT NULL,
    -- The file name with which the media was uploaded.
    upload_name TEXT NOT NULL,
    -- Alternate RFC 4648 unpadded base64 encoding string representation of a SHA-256 hash sum of the file data.
    base64hash TEXT NOT NULL,
    -- The user who uploaded the file. Should be a Matrix user ID.
    user_id TEXT NOT NULL,
    -- For media IDs created with POST /create whose content has not been
    -- uploaded yet: when the reservation expires in UNIX epoch ms. 0 once uploaded.
    unused_expires_ts INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS mediaapi_media_repository_index ON mediaapi_media_repository (media_id, media_origin);
`

const insertMediaSQL = `
INSERT INTO mediaapi_media_repository (media_id, media_origin, content_type, file_size_bytes, creation_ts, upload_name, base64hash, user_id, unused_expires_ts)
    VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
`

const selectMediaSQL = `
SELECT content_type, file_size_bytes, creation_ts, upload_name, base64hash, user_id, unused_expires_ts FROM mediaapi_media_repository WHERE media_id = $1 AND media_origin = $2
`

// Pending media has no content yet, so it must never be matched by hash.
const selectMediaByHashSQL = `
SELECT content_type, file_size_bytes, creation_ts, upload_name, media_id, user_id FROM mediaapi_media_repository WHERE base64hash = $1 AND media_origin = $2 AND unused_expires_ts = 0
`

const countPendingMediaSQL = `
SELECT COUNT(*) FROM mediaapi_media_repository WHERE user_id = $1 AND unused_expires_ts > $2
`

// The conditional WHERE makes the upload succeed at most once, even when
// several requests race on different replicas.
const updatePendingMediaSQL = `
UPDATE mediaapi_media_repository
    SET content_type = $1, file_size_bytes = $2, creation_ts = $3, upload_name = $4, base64hash = $5, unused_expires_ts = 0
    WHERE media_id = $6 AND media_origin = $7 AND user_id = $8 AND unused_expires_ts > $9
`

const deleteExpiredPendingMediaSQL = `
DELETE FROM mediaapi_media_repository WHERE unused_expires_ts > 0 AND unused_expires_ts <= $1
`

const deleteMediaSQL = `
DELETE FROM mediaapi_media_repository WHERE media_id = $1 AND media_origin = $2
`

type mediaStatements struct {
	db                    *sql.DB
	insertMediaStmt       *sql.Stmt
	selectMediaStmt       *sql.Stmt
	selectMediaByHashStmt *sql.Stmt
	deleteMediaStmt       *sql.Stmt

	countPendingMediaStmt         *sql.Stmt
	updatePendingMediaStmt        *sql.Stmt
	deleteExpiredPendingMediaStmt *sql.Stmt
}

func NewSQLiteMediaRepositoryTable(db *sql.DB) (tables.MediaRepository, error) {
	s := &mediaStatements{
		db: db,
	}
	_, err := db.Exec(mediaSchema)
	if err != nil {
		return nil, err
	}
	m := sqlutil.NewMigrator(db)
	m.AddMigrations(sqlutil.Migration{
		Version: "mediaapi: add unused_expires_ts for asynchronous uploads",
		Up:      deltas.UpAsyncUploads,
		Down:    deltas.DownAsyncUploads,
	})
	if err = m.Up(context.Background()); err != nil {
		return nil, err
	}

	return s, sqlutil.StatementList{
		{&s.insertMediaStmt, insertMediaSQL},
		{&s.selectMediaStmt, selectMediaSQL},
		{&s.selectMediaByHashStmt, selectMediaByHashSQL},
		{&s.deleteMediaStmt, deleteMediaSQL},
		{&s.countPendingMediaStmt, countPendingMediaSQL},
		{&s.updatePendingMediaStmt, updatePendingMediaSQL},
		{&s.deleteExpiredPendingMediaStmt, deleteExpiredPendingMediaSQL},
	}.Prepare(db)
}

func (s *mediaStatements) InsertMedia(
	ctx context.Context, txn *sql.Tx, mediaMetadata *types.MediaMetadata,
) error {
	mediaMetadata.CreationTimestamp = spec.AsTimestamp(time.Now())
	insertStmt := sqlutil.TxStmtContext(ctx, txn, s.insertMediaStmt)
	_, err := insertStmt.ExecContext(
		ctx,
		mediaMetadata.MediaID,
		mediaMetadata.Origin,
		mediaMetadata.ContentType,
		mediaMetadata.FileSizeBytes,
		mediaMetadata.CreationTimestamp,
		mediaMetadata.UploadName,
		mediaMetadata.Base64Hash,
		mediaMetadata.UserID,
		mediaMetadata.UnusedExpiresAt,
	)
	return err
}

func (s *mediaStatements) SelectMedia(
	ctx context.Context, txn *sql.Tx, mediaID types.MediaID, mediaOrigin spec.ServerName,
) (*types.MediaMetadata, error) {
	mediaMetadata := types.MediaMetadata{
		MediaID: mediaID,
		Origin:  mediaOrigin,
	}
	selectStmt := sqlutil.TxStmtContext(ctx, txn, s.selectMediaStmt)
	err := selectStmt.QueryRowContext(
		ctx, mediaMetadata.MediaID, mediaMetadata.Origin,
	).Scan(
		&mediaMetadata.ContentType,
		&mediaMetadata.FileSizeBytes,
		&mediaMetadata.CreationTimestamp,
		&mediaMetadata.UploadName,
		&mediaMetadata.Base64Hash,
		&mediaMetadata.UserID,
		&mediaMetadata.UnusedExpiresAt,
	)
	return &mediaMetadata, err
}

func (s *mediaStatements) DeleteMedia(
	ctx context.Context, txn *sql.Tx, mediaID types.MediaID, mediaOrigin spec.ServerName,
) error {
	stmt := sqlutil.TxStmtContext(ctx, txn, s.deleteMediaStmt)
	_, err := stmt.ExecContext(ctx, mediaID, mediaOrigin)
	return err
}

func (s *mediaStatements) SelectMediaByHash(
	ctx context.Context, txn *sql.Tx, mediaHash types.Base64Hash, mediaOrigin spec.ServerName,
) (*types.MediaMetadata, error) {
	mediaMetadata := types.MediaMetadata{
		Base64Hash: mediaHash,
		Origin:     mediaOrigin,
	}
	selectByHashStmt := sqlutil.TxStmtContext(ctx, txn, s.selectMediaByHashStmt)
	err := selectByHashStmt.QueryRowContext(
		ctx, mediaMetadata.Base64Hash, mediaMetadata.Origin,
	).Scan(
		&mediaMetadata.ContentType,
		&mediaMetadata.FileSizeBytes,
		&mediaMetadata.CreationTimestamp,
		&mediaMetadata.UploadName,
		&mediaMetadata.MediaID,
		&mediaMetadata.UserID,
	)
	return &mediaMetadata, err
}

func (s *mediaStatements) CountPendingMedia(
	ctx context.Context, txn *sql.Tx, userID types.MatrixUserID, now spec.Timestamp,
) (count int64, err error) {
	stmt := sqlutil.TxStmtContext(ctx, txn, s.countPendingMediaStmt)
	err = stmt.QueryRowContext(ctx, userID, now).Scan(&count)
	return count, err
}

func (s *mediaStatements) UpdatePendingMedia(
	ctx context.Context, txn *sql.Tx, mediaMetadata *types.MediaMetadata, now spec.Timestamp,
) (bool, error) {
	stmt := sqlutil.TxStmtContext(ctx, txn, s.updatePendingMediaStmt)
	res, err := stmt.ExecContext(
		ctx,
		mediaMetadata.ContentType,
		mediaMetadata.FileSizeBytes,
		now,
		mediaMetadata.UploadName,
		mediaMetadata.Base64Hash,
		mediaMetadata.MediaID,
		mediaMetadata.Origin,
		mediaMetadata.UserID,
		now,
	)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if affected == 0 {
		return false, nil
	}
	mediaMetadata.CreationTimestamp = now
	mediaMetadata.UnusedExpiresAt = 0
	return true, nil
}

func (s *mediaStatements) DeleteExpiredPendingMedia(
	ctx context.Context, txn *sql.Tx, now spec.Timestamp,
) (int64, error) {
	stmt := sqlutil.TxStmtContext(ctx, txn, s.deleteExpiredPendingMediaStmt)
	res, err := stmt.ExecContext(ctx, now)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
