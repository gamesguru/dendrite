// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/mediaapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

// TestMediaContentHeaders covers Content-Type and Content-Disposition in the
// media repository (MSC2701, MSC2702): uploads without a Content-Type are
// stored as application/octet-stream, and thumbnails are served inline with
// a file name.
func TestMediaContentHeaders(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		e := newAsyncUploadEnv(t, dbType)
		e.cfg.MaxFileSizeBytes = 1 << 20
		e.cfg.MaxThumbnailGenerators = 10

		untyped := e.uploadImage(t, []byte("some bytes"), "")
		rec := e.download(untyped, "", e.pendingUploads)
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, "application/octet-stream", rec.Header().Get("Content-Type"))
		assert.Equal(t, `attachment; filename=image`, rec.Header().Get("Content-Disposition"))

		// A generated thumbnail.
		animated := e.uploadImage(t, encodeGIF(t, 3), "image/gif")
		rec = e.thumbnail(animated, "")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, "image/jpeg", rec.Header().Get("Content-Type"))
		assert.Equal(t, `inline; filename="thumbnail.jpg"`, rec.Header().Get("Content-Disposition"))

		// An original that is small enough to be its own thumbnail.
		still := e.uploadImage(t, encodeGIF(t, 1), "image/gif")
		rec = e.thumbnail(still, "")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, "image/gif", rec.Header().Get("Content-Type"))
		assert.Equal(t, `inline; filename="thumbnail.gif"`, rec.Header().Get("Content-Disposition"))
	})
}

func TestResponseContentType(t *testing.T) {
	assert.Equal(t, defaultContentType, responseContentType(&types.MediaMetadata{}))
	assert.Equal(t, types.ContentType("image/png"), responseContentType(&types.MediaMetadata{ContentType: "image/png"}))
}

func TestThumbnailDisposition(t *testing.T) {
	assert.Equal(t, `inline; filename="thumbnail.png"`, thumbnailDisposition("image/png"))
	// Unsafe types are never served inline, even as a thumbnail.
	assert.Equal(t, `attachment; filename="thumbnail"`, thumbnailDisposition("image/svg+xml"))
}
