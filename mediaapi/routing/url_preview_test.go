// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"bytes"
	"encoding/json"
	"image"
	"image/png"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/mediaapi/types"
	"codefloe.com/pat-s/zendrite/test"
)

func testPNG(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, png.Encode(&buf, image.NewRGBA(image.Rect(0, 0, 3, 2))))
	return buf.Bytes()
}

// previewSite serves a few pages for URL preview tests and counts requests.
func previewSite(t *testing.T) (*httptest.Server, *atomic.Int32) {
	t.Helper()
	var hits atomic.Int32
	img := testPNG(t)
	mux := http.NewServeMux()
	mux.HandleFunc("/page", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = w.Write([]byte(`<!doctype html><html><head>
<title>Ignored title</title>
<meta property="og:title" content="Open Graph title">
<meta property="og:description" content="A description">
<meta property="og:image" content="/img.png">
<meta property="og:site_name" content="Example">
</head><body><meta property="og:late" content="ignored"></body></html>`))
	})
	mux.HandleFunc("/plain", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><head><title> Plain title </title><meta name="description" content="Plain description"></head></html>`))
	})
	mux.HandleFunc("/latin1", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=iso-8859-1")
		_, _ = w.Write([]byte("<html><head><title>Caf\xe9</title></head></html>"))
	})
	mux.HandleFunc("/broken-image", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		_, _ = w.Write([]byte(`<html><head><meta property="og:title" content="T"><meta property="og:image" content="/missing.png"></head></html>`))
	})
	mux.HandleFunc("/img.png", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(img)
	})
	mux.HandleFunc("/gone", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		mux.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server, &hits
}

func (e *asyncUploadEnv) previewer(t *testing.T, whitelist, blacklist []string) *URLPreviewer {
	t.Helper()
	e.cfg.MaxFileSizeBytes = 1 << 20
	e.cfg.URLPreview.Enabled = true
	e.cfg.URLPreview.IPRangeWhitelist = whitelist
	e.cfg.URLPreview.IPRangeBlacklist = blacklist
	p, err := NewURLPreviewer(e.cfg, e.db, e.fileStore, e.thumbnails)
	require.NoError(t, err)
	return p
}

func preview(t *testing.T, p *URLPreviewer, target string) (int, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v1/media/preview_url?url="+url.QueryEscape(target)+"&ts=1", nil)
	res := p.Preview(req, asyncAlice)
	body, err := json.Marshal(res.JSON)
	require.NoError(t, err)
	var out map[string]any
	require.NoError(t, json.Unmarshal(body, &out))
	return res.Code, out
}

func TestURLPreview(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		e := newAsyncUploadEnv(t, dbType)
		site, hits := previewSite(t)
		p := e.previewer(t, []string{"127.0.0.0/8", "::1/128"}, nil)

		t.Run("open graph", func(t *testing.T) {
			code, got := preview(t, p, site.URL+"/page#fragment")
			require.Equal(t, http.StatusOK, code, got)
			assert.Equal(t, "Open Graph title", got["og:title"])
			assert.Equal(t, "A description", got["og:description"])
			assert.Equal(t, "Example", got["og:site_name"])
			assert.NotContains(t, got, "og:late", "only the head is read")
			assert.Equal(t, "image/png", got["og:image:type"])
			assert.EqualValues(t, len(testPNG(t)), got["matrix:image:size"])
			assert.EqualValues(t, 3, got["og:image:width"])
			assert.EqualValues(t, 2, got["og:image:height"])

			mxc, _ := got["og:image"].(string)
			mediaID, ok := strings.CutPrefix(mxc, "mxc://localhost/")
			require.True(t, ok, mxc)
			rec := e.download(types.MediaID(mediaID), "", e.pendingUploads)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, testPNG(t), rec.Body.Bytes())
		})

		t.Run("cached", func(t *testing.T) {
			before := hits.Load()
			code, got := preview(t, p, site.URL+"/page")
			require.Equal(t, http.StatusOK, code)
			assert.Equal(t, "Open Graph title", got["og:title"])
			assert.Equal(t, before, hits.Load(), "a cached preview must not fetch again")

			p.now = func() time.Time { return time.Now().Add(2 * time.Hour) }
			defer func() { p.now = time.Now }()
			_, _ = preview(t, p, site.URL+"/page")
			assert.Greater(t, hits.Load(), before, "an expired preview is fetched again")
		})

		t.Run("title and description fallback", func(t *testing.T) {
			code, got := preview(t, p, site.URL+"/plain")
			require.Equal(t, http.StatusOK, code)
			assert.Equal(t, map[string]any{"og:title": "Plain title", "og:description": "Plain description"}, got)
		})

		t.Run("charset", func(t *testing.T) {
			_, got := preview(t, p, site.URL+"/latin1")
			assert.Equal(t, "Café", got["og:title"])
		})

		t.Run("image", func(t *testing.T) {
			code, got := preview(t, p, site.URL+"/img.png")
			require.Equal(t, http.StatusOK, code)
			assert.Equal(t, "image/png", got["og:image:type"])
			assert.Contains(t, got["og:image"], "mxc://localhost/")
		})

		t.Run("missing page image", func(t *testing.T) {
			code, got := preview(t, p, site.URL+"/broken-image")
			require.Equal(t, http.StatusOK, code)
			assert.Equal(t, map[string]any{"og:title": "T"}, got)
		})

		t.Run("error status", func(t *testing.T) {
			code, got := preview(t, p, site.URL+"/gone")
			assert.Equal(t, http.StatusBadGateway, code, got)
		})

		t.Run("invalid URLs", func(t *testing.T) {
			for _, target := range []string{"ftp://example.org/", "/relative", "mailto:a@example.org"} {
				code, got := preview(t, p, target)
				assert.Equal(t, http.StatusBadRequest, code, target)
				assert.Equal(t, string(spec.ErrorInvalidParam), got["errcode"], target)
			}
			res := p.Preview(httptest.NewRequest(http.MethodGet, "/_matrix/client/v1/media/preview_url", nil), asyncAlice)
			assert.Equal(t, http.StatusBadRequest, res.Code)
		})
	})
}

// TestURLPreviewBlockedAddresses refuses loopback by default, and when a
// blacklist entry covers a whitelisted range.
func TestURLPreviewBlockedAddresses(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		site, hits := previewSite(t)
		for name, lists := range map[string][2][]string{
			"default":                   {nil, nil},
			"blacklist beats whitelist": {{"127.0.0.0/8", "::1/128"}, {"127.0.0.1/32", "::1/128"}},
		} {
			t.Run(name, func(t *testing.T) {
				p := newAsyncUploadEnv(t, dbType).previewer(t, lists[0], lists[1])
				code, got := preview(t, p, site.URL+"/page")
				assert.Equal(t, http.StatusForbidden, code, got)
				assert.Equal(t, string(spec.ErrorForbidden), got["errcode"])
			})
		}
		assert.Zero(t, hits.Load(), "blocked addresses must never be contacted")
	})
}

func TestURLPreviewAddressAllowed(t *testing.T) {
	t.Parallel()
	blacklist := []netip.Prefix{netip.MustParsePrefix("203.0.113.0/24")}
	whitelist := []netip.Prefix{netip.MustParsePrefix("10.1.0.0/16")}
	for addr, want := range map[string]bool{
		"8.8.8.8":         true,
		"203.0.113.7":     false,
		"10.1.2.3":        true,
		"10.2.0.1":        false,
		"127.0.0.1":       false,
		"169.254.169.254": false,
		"100.64.0.1":      false,
		"::ffff:10.1.2.3": true,
		"fd00::1":         false,
		"2001:4860::8888": true,
	} {
		assert.Equal(t, want, urlPreviewAddressAllowed(netip.MustParseAddr(addr), blacklist, whitelist), addr)
	}
}

func TestParseOpenGraphTruncates(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("é", urlPreviewMaxValue)
	og := parseOpenGraph(strings.NewReader(`<head><meta property="og:title" content="` + long + `"></head>`))
	title := og["og:title"]
	assert.True(t, strings.HasSuffix(title, "…"))
	assert.LessOrEqual(t, len(title), urlPreviewMaxValue+len("…"))
	assert.True(t, strings.HasPrefix(long, strings.TrimSuffix(title, "…")), "cut at a rune boundary")
}
