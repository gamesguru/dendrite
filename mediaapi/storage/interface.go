// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package storage

import (
	"context"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/mediaapi/types"
)

type Database interface {
	MediaRepository
	Thumbnails
}

type MediaRepository interface {
	StoreMediaMetadata(ctx context.Context, mediaMetadata *types.MediaMetadata) error
	GetMediaMetadata(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName) (*types.MediaMetadata, error)
	GetMediaMetadataByHash(ctx context.Context, mediaHash types.Base64Hash, mediaOrigin spec.ServerName) (*types.MediaMetadata, error)
	DeleteMediaMetadata(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName) error
	// StorePendingMedia reserves a media ID created with POST /create. It
	// returns types.ErrTooManyPendingUploads if the user already holds
	// maxPending unexpired pending media IDs.
	StorePendingMedia(ctx context.Context, mediaMetadata *types.MediaMetadata, maxPending int) error
	// CompletePendingMedia stores the content metadata of an upload to a
	// pending media ID. It returns false if the media ID is no longer pending
	// for the uploading user, e.g. because a concurrent upload won.
	CompletePendingMedia(ctx context.Context, mediaMetadata *types.MediaMetadata) (bool, error)
	// DeleteExpiredPendingMedia deletes pending media IDs whose reservation expired.
	DeleteExpiredPendingMedia(ctx context.Context) (int64, error)
}

type Thumbnails interface {
	StoreThumbnail(ctx context.Context, thumbnailMetadata *types.ThumbnailMetadata) error
	GetThumbnail(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName, width, height int, resizeMethod string) (*types.ThumbnailMetadata, error)
	GetThumbnails(ctx context.Context, mediaID types.MediaID, mediaOrigin spec.ServerName) ([]*types.ThumbnailMetadata, error)
}
