// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package filestore

import (
	"context"
	"errors"
	"io"

	"codefloe.com/pat-s/zendrite/mediaapi/types"
)

// ErrNotFound is returned when a requested file or thumbnail does not exist.
var ErrNotFound = errors.New("file not found")

// FileStore abstracts persistent storage for media files and their thumbnails.
// Implementations must be safe for concurrent use.
type FileStore interface {
	// WriteTemp writes r to a temporary location, computes its SHA-256 hash and
	// returns the hash, size, and a temporary path that can be passed to Store.
	// The caller must invoke cleanup when done, even if Store is called.
	WriteTemp(ctx context.Context, r io.Reader) (hash types.Base64Hash, size types.FileSizeBytes, tmpPath types.Path, cleanup func(), err error)

	// Store moves the temporary file identified by tmpPath to its final location
	// keyed by hash. It returns true if a file with the same hash and size was
	// already present. The temporary file is removed on success.
	Store(ctx context.Context, tmpPath types.Path, hash types.Base64Hash, size types.FileSizeBytes) (duplicate bool, err error)

	// Open returns a reader for the file identified by hash.
	Open(ctx context.Context, hash types.Base64Hash) (io.ReadCloser, types.FileSizeBytes, error)

	// Stat returns the size of the file and whether it exists.
	Stat(ctx context.Context, hash types.Base64Hash) (types.FileSizeBytes, bool, error)

	// Delete removes the file identified by hash.
	Delete(ctx context.Context, hash types.Base64Hash) error

	// LocalPath returns a local filesystem path for the file identified by hash,
	// downloading it to a temporary file if necessary. The caller must invoke
	// cleanup when done.
	LocalPath(ctx context.Context, hash types.Base64Hash) (string, func(), error)

	// OpenThumbnail returns a reader for the requested thumbnail.
	OpenThumbnail(ctx context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize) (io.ReadCloser, types.FileSizeBytes, error)

	// StatThumbnail returns the size and existence of the requested thumbnail.
	StatThumbnail(ctx context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize) (types.FileSizeBytes, bool, error)

	// StoreThumbnail stores a thumbnail from a local temporary file and returns
	// the stored size.
	StoreThumbnail(ctx context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize, tmpPath types.Path, fileSize types.FileSizeBytes) error

	// DeleteThumbnail removes the requested thumbnail.
	DeleteThumbnail(ctx context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize) error
}
