// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"bytes"
	"image"
	"image/color"
	"image/color/palette"
	"image/gif"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/mediaapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

func encodeGIF(t *testing.T, frames int) []byte {
	t.Helper()
	anim := &gif.GIF{}
	for i := range frames {
		img := image.NewPaletted(image.Rect(0, 0, 8, 8), palette.Plan9)
		img.Set(i, i, color.White)
		anim.Image = append(anim.Image, img)
		anim.Delay = append(anim.Delay, 10)
	}
	var buf bytes.Buffer
	require.NoError(t, gif.EncodeAll(&buf, anim))
	return buf.Bytes()
}

func (e *asyncUploadEnv) uploadImage(t *testing.T, data []byte, contentType string) types.MediaID {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/_matrix/media/v3/upload?filename=image", bytes.NewReader(data))
	req.Header.Set("Content-Type", contentType)
	res := Upload(req, e.cfg, asyncAlice, e.db, e.fileStore, e.thumbnails)
	require.Equal(t, http.StatusOK, res.Code, "%+v", res.JSON)
	uploaded, ok := res.JSON.(uploadResponse)
	require.True(t, ok, "%+v", res.JSON)
	return types.MediaID(strings.TrimPrefix(uploaded.ContentURI, "mxc://localhost/"))
}

func (e *asyncUploadEnv) thumbnail(mediaID types.MediaID, query string) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v1/media/thumbnail/localhost/"+string(mediaID)+"?width=32&height=32&method=scale"+query, nil)
	Download(
		rec, req, "localhost", mediaID, e.cfg, e.db, e.fileStore, nil, nil,
		e.remote, e.thumbnails, e.pendingUploads, true, "", false,
	)
	return rec
}

// TestAnimatedThumbnails checks MSC2705 for a thumbnail request that is
// larger than the image, which the image itself used to answer: an animated
// image is only served when animated=true, and otherwise replaced by a still
// rendition. Still images are served as they are.
func TestAnimatedThumbnails(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		e := newAsyncUploadEnv(t, dbType)
		e.cfg.MaxFileSizeBytes = 1 << 20
		e.cfg.MaxThumbnailGenerators = 10
		animated := e.uploadImage(t, encodeGIF(t, 3), "image/gif")
		still := e.uploadImage(t, encodeGIF(t, 1), "image/gif")

		for _, query := range []string{"", "&animated=false"} {
			rec := e.thumbnail(animated, query)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			assert.Equal(t, "image/jpeg", rec.Header().Get("Content-Type"), "query %q", query)
		}

		rec := e.thumbnail(animated, "&animated=true")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, "image/gif", rec.Header().Get("Content-Type"))
		assert.Equal(t, encodeGIF(t, 3), rec.Body.Bytes())

		rec = e.thumbnail(still, "")
		require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
		assert.Equal(t, "image/gif", rec.Header().Get("Content-Type"))
	})
}

// TestAnimatedThumbnailBusy returns 429 rather than the animated original
// when no thumbnail generator is free.
func TestAnimatedThumbnailBusy(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		e := newAsyncUploadEnv(t, dbType)
		e.cfg.MaxFileSizeBytes = 1 << 20
		animated := e.uploadImage(t, encodeGIF(t, 2), "image/gif")
		e.cfg.MaxThumbnailGenerators = 0
		rec := e.thumbnail(animated, "")
		requireRecorderErrCode(t, rec, http.StatusTooManyRequests, spec.ErrorLimitExceeded)
		assert.Equal(t, "1", rec.Header().Get("Retry-After"))
	})
}
