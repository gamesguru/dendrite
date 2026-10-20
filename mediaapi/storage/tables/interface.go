// Copyright 2024 New Vector Ltd.
// Copyright 2022 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package tables

import (
	"context"
	"database/sql"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/mediaapi/types"
)

type Thumbnails interface {
	InsertThumbnail(ctx context.Context, txn *sql.Tx, thumbnailMetadata *types.ThumbnailMetadata) error
	SelectThumbnail(
		ctx context.Context, txn *sql.Tx,
		mediaID types.MediaID, mediaOrigin spec.ServerName,
		width, height int,
		resizeMethod string,
	) (*types.ThumbnailMetadata, error)
	SelectThumbnails(
		ctx context.Context, txn *sql.Tx, mediaID types.MediaID,
		mediaOrigin spec.ServerName,
	) ([]*types.ThumbnailMetadata, error)
}

type MediaRepository interface {
	InsertMedia(ctx context.Context, txn *sql.Tx, mediaMetadata *types.MediaMetadata) error
	SelectMedia(ctx context.Context, txn *sql.Tx, mediaID types.MediaID, mediaOrigin spec.ServerName) (*types.MediaMetadata, error)
	SelectMediaByHash(
		ctx context.Context, txn *sql.Tx,
		mediaHash types.Base64Hash, mediaOrigin spec.ServerName,
	) (*types.MediaMetadata, error)
	DeleteMedia(ctx context.Context, txn *sql.Tx, mediaID types.MediaID, mediaOrigin spec.ServerName) error
	// CountPendingMedia counts the user's media IDs that were created but are
	// neither uploaded nor expired at now.
	CountPendingMedia(ctx context.Context, txn *sql.Tx, userID types.MatrixUserID, now spec.Timestamp) (int64, error)
	// UpdatePendingMedia stores the uploaded content's metadata on a pending
	// media ID created by the same user. It returns false without changing
	// anything if the media ID is not pending for that user or has expired.
	UpdatePendingMedia(ctx context.Context, txn *sql.Tx, mediaMetadata *types.MediaMetadata, now spec.Timestamp) (bool, error)
	// DeleteExpiredPendingMedia deletes pending media IDs that expired at or before now.
	DeleteExpiredPendingMedia(ctx context.Context, txn *sql.Tx, now spec.Timestamp) (int64, error)
}
