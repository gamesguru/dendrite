// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"image"
	"io"
	"mime"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/html"
	"golang.org/x/net/html/charset"
	"golang.org/x/sync/singleflight"

	"codefloe.com/pat-s/zendrite/mediaapi/storage"
	"codefloe.com/pat-s/zendrite/mediaapi/storage/filestore"
	"codefloe.com/pat-s/zendrite/mediaapi/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

const (
	urlPreviewTimeout     = 20 * time.Second
	urlPreviewMaxRedirect = 5
	urlPreviewUserAgent   = "Zendrite URL preview (+https://codefloe.com/pat-s/zendrite)"
	// UrlPreviewMaxCache bounds the number of cached previews.
	urlPreviewMaxCache = 4096
	// UrlPreviewMaxValue truncates long OpenGraph values.
	urlPreviewMaxValue = 1000
	// UrlPreviewMaxKeys bounds the OpenGraph keys taken from a page.
	urlPreviewMaxKeys = 50
)

// errBlockedAddress reports that a URL resolved to an address URL previews
// may not fetch.
var errBlockedAddress = errors.New("address is blocked for URL previews")

// URLPreviewer serves GET /_matrix/client/v1/media/preview_url.
type URLPreviewer struct {
	cfg       *config.MediaAPI
	db        storage.Database
	fileStore filestore.FileStore
	thumbs    *types.ActiveThumbnailGeneration
	client    *http.Client
	flight    singleflight.Group

	mu    sync.Mutex
	cache map[string]urlPreviewEntry
	now   func() time.Time
}

type urlPreviewEntry struct {
	preview map[string]any
	expires time.Time
}

// NewURLPreviewer returns a previewer, or an error if the configured IP
// ranges cannot be parsed.
func NewURLPreviewer(
	cfg *config.MediaAPI, db storage.Database, fileStore filestore.FileStore,
	thumbs *types.ActiveThumbnailGeneration,
) (*URLPreviewer, error) {
	blacklist, whitelist, err := cfg.URLPreview.IPRanges()
	if err != nil {
		return nil, err
	}
	return &URLPreviewer{
		cfg:       cfg,
		db:        db,
		fileStore: fileStore,
		thumbs:    thumbs,
		client:    newURLPreviewClient(blacklist, whitelist),
		cache:     make(map[string]urlPreviewEntry),
		now:       time.Now,
	}, nil
}

// urlPreviewAddressAllowed applies the blacklist, then the whitelist, and
// otherwise only allows public addresses.
func urlPreviewAddressAllowed(addr netip.Addr, blacklist, whitelist []netip.Prefix) bool {
	addr = addr.Unmap()
	for _, prefix := range blacklist {
		if prefix.Contains(addr) {
			return false
		}
	}
	for _, prefix := range whitelist {
		if prefix.Contains(addr) {
			return true
		}
	}
	return isPublicIP(addr.AsSlice())
}

func newURLPreviewClient(blacklist, whitelist []netip.Prefix) *http.Client {
	dialer := &net.Dialer{
		Timeout: mediaRedirectDialTimeout,
		// Checking the address being dialed, rather than the host name, also
		// covers redirects and DNS rebinding.
		Control: func(_, address string, _ syscall.RawConn) error {
			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}
			addr, err := netip.ParseAddr(host)
			if err != nil || !urlPreviewAddressAllowed(addr, blacklist, whitelist) {
				return fmt.Errorf("%w: %s", errBlockedAddress, host)
			}
			return nil
		},
	}
	return &http.Client{
		Timeout: urlPreviewTimeout,
		Transport: &http.Transport{
			// No proxy: it would connect on our behalf and bypass the check.
			Proxy:                 nil,
			DialContext:           dialer.DialContext,
			TLSHandshakeTimeout:   mediaRedirectDialTimeout,
			ResponseHeaderTimeout: urlPreviewTimeout,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) > urlPreviewMaxRedirect {
				return errors.New("too many redirects")
			}
			return checkPreviewURL(req.URL)
		},
	}
}

func checkPreviewURL(u *url.URL) error {
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("unsupported URL scheme %q", u.Scheme)
	}
	if u.Hostname() == "" {
		return errors.New("URL has no host")
	}
	return nil
}

// Preview handles a preview request. The ts parameter is accepted and
// ignored: the spec lets the server return a newer preview.
func (p *URLPreviewer) Preview(req *http.Request, dev *userapi.Device) util.JSONResponse {
	rawURL := req.URL.Query().Get("url")
	if rawURL == "" {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.MissingParam("Missing url parameter")}
	}
	target, err := url.Parse(rawURL)
	if err == nil {
		err = checkPreviewURL(target)
	}
	if err != nil {
		return util.JSONResponse{Code: http.StatusBadRequest, JSON: spec.InvalidParam("Invalid URL: only http and https URLs can be previewed")}
	}
	target.Fragment = ""
	key := target.String()

	if preview, ok := p.cached(key); ok {
		return util.JSONResponse{Code: http.StatusOK, JSON: preview}
	}
	// Concurrent requests for the same URL share one fetch. Its context is
	// not canceled with the request, so that one client going away doesn't
	// fail the rest.
	shared := context.WithoutCancel(req.Context())
	result, err, _ := p.flight.Do(key, func() (any, error) {
		ctx, cancel := context.WithTimeout(shared, urlPreviewTimeout)
		defer cancel()
		preview, fetchErr := p.fetch(ctx, target, dev)
		if fetchErr != nil {
			return nil, fetchErr
		}
		p.store(key, preview)
		return preview, nil
	})
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).WithField("url", key).Info("URL preview failed")
		if errors.Is(err, errBlockedAddress) {
			return util.JSONResponse{Code: http.StatusForbidden, JSON: spec.Forbidden("URL previews are not allowed for this address")}
		}
		return util.JSONResponse{Code: http.StatusBadGateway, JSON: spec.Unknown("Failed to fetch the URL")}
	}
	preview, _ := result.(map[string]any)
	return util.JSONResponse{Code: http.StatusOK, JSON: preview}
}

func (p *URLPreviewer) cached(key string) (map[string]any, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	entry, ok := p.cache[key]
	if !ok {
		return nil, false
	}
	if p.now().After(entry.expires) {
		delete(p.cache, key)
		return nil, false
	}
	return entry.preview, true
}

func (p *URLPreviewer) store(key string, preview map[string]any) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	if len(p.cache) >= urlPreviewMaxCache {
		for k, entry := range p.cache {
			if now.After(entry.expires) {
				delete(p.cache, k)
			}
		}
		// Still full: drop arbitrary entries rather than growing.
		for k := range p.cache {
			if len(p.cache) < urlPreviewMaxCache {
				break
			}
			delete(p.cache, k)
		}
	}
	p.cache[key] = urlPreviewEntry{preview: preview, expires: now.Add(p.cfg.URLPreview.CacheTTL())}
}

// get fetches a URL, failing for error statuses.
func (p *URLPreviewer) get(ctx context.Context, target *url.URL) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, target.String(), nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", urlPreviewUserAgent)
	req.Header.Set("Accept", "text/html, application/xhtml+xml, image/*;q=0.9, */*;q=0.8")
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return resp, nil
}

// fetch builds the preview of a URL: the OpenGraph data of an HTML page, or
// the image itself for an image.
func (p *URLPreviewer) fetch(ctx context.Context, target *url.URL, dev *userapi.Device) (map[string]any, error) {
	resp, err := p.get(ctx, target)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	preview := map[string]any{}
	switch {
	case strings.HasPrefix(mediaType, "image/"):
		if err = p.addImage(ctx, preview, resp.Body, mediaType, dev); err != nil {
			return nil, err
		}
	case mediaType == "text/html" || mediaType == "application/xhtml+xml":
		body := io.LimitReader(resp.Body, int64(p.cfg.URLPreview.MaxPageSize()))
		reader, err := charset.NewReader(body, resp.Header.Get("Content-Type"))
		if err != nil {
			return nil, err
		}
		og := parseOpenGraph(reader)
		imageURL, hasImage := og["og:image"]
		delete(og, "og:image")
		for k, v := range og {
			preview[k] = v
		}
		if hasImage {
			// A page whose image cannot be fetched still gets a preview.
			if err = p.fetchImage(ctx, preview, resp.Request.URL, imageURL, dev); err != nil {
				util.GetLogger(ctx).WithError(err).WithField("image", imageURL).Debug("URL preview image failed")
			}
		}
	}
	return preview, nil
}

func (p *URLPreviewer) fetchImage(ctx context.Context, preview map[string]any, base *url.URL, rawURL string, dev *userapi.Device) error {
	ref, err := url.Parse(rawURL)
	if err != nil {
		return err
	}
	imageURL := base.ResolveReference(ref)
	if err = checkPreviewURL(imageURL); err != nil {
		return err
	}
	resp, err := p.get(ctx, imageURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	mediaType, _, _ := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if !strings.HasPrefix(mediaType, "image/") {
		return fmt.Errorf("not an image: %q", mediaType)
	}
	return p.addImage(ctx, preview, resp.Body, mediaType, dev)
}

// addImage stores an image as local media and describes it in the preview.
func (p *URLPreviewer) addImage(ctx context.Context, preview map[string]any, body io.Reader, mediaType string, dev *userapi.Device) error {
	// Reading one byte more than allowed tells a too large image apart.
	limit := int64(p.cfg.MaxFileSizeBytes)
	if limit <= 0 {
		limit = int64(config.DefaultMaxFileSizeBytes)
	}
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > limit {
		return errors.New("image is too large")
	}
	r := &uploadRequest{
		MediaMetadata: &types.MediaMetadata{
			Origin:        p.cfg.Matrix.ServerName,
			ContentType:   types.ContentType(mediaType),
			FileSizeBytes: types.FileSizeBytes(len(data)),
			UploadName:    "preview",
			UserID:        types.MatrixUserID(dev.UserID),
		},
		Logger: util.GetLogger(ctx).WithFields(log.Fields{"user_id": dev.UserID, "purpose": "url_preview"}),
	}
	if resErr := r.doUpload(ctx, bytes.NewReader(data), p.cfg, p.db, p.fileStore, p.thumbs); resErr != nil {
		return fmt.Errorf("storing preview image failed with status %d", resErr.Code)
	}
	preview["og:image"] = fmt.Sprintf("mxc://%s/%s", p.cfg.Matrix.ServerName, r.MediaMetadata.MediaID)
	preview["og:image:type"] = mediaType
	preview["matrix:image:size"] = len(data)
	if cfg, _, err := image.DecodeConfig(bytes.NewReader(data)); err == nil {
		preview["og:image:width"] = cfg.Width
		preview["og:image:height"] = cfg.Height
	}
	return nil
}

// parseOpenGraph reads the OpenGraph properties of a page's head, falling
// back to its title and description.
func parseOpenGraph(r io.Reader) map[string]string {
	og := map[string]string{}
	var title, description string
	inTitle := false
	tokenizer := html.NewTokenizer(r)
	for {
		switch tokenizer.Next() {
		case html.ErrorToken:
			return finishOpenGraph(og, title, description)
		case html.StartTagToken, html.SelfClosingTagToken:
			name, hasAttr := tokenizer.TagName()
			switch string(name) {
			case "body":
				// OpenGraph data lives in the head.
				return finishOpenGraph(og, title, description)
			case "title":
				inTitle = true
			case "meta":
				if !hasAttr {
					continue
				}
				var property, metaName, content string
				for {
					key, value, more := tokenizer.TagAttr()
					switch string(key) {
					case "property":
						property = strings.TrimSpace(string(value))
					case "name":
						metaName = strings.ToLower(strings.TrimSpace(string(value)))
					case "content":
						content = strings.TrimSpace(string(value))
					}
					if !more {
						break
					}
				}
				if property == "" && strings.HasPrefix(metaName, "og:") {
					property = metaName
				}
				switch {
				case strings.HasPrefix(property, "og:") && content != "":
					if _, seen := og[property]; !seen && len(og) < urlPreviewMaxKeys {
						og[property] = truncate(content)
					}
				case metaName == "description" && description == "":
					description = truncate(content)
				}
			}
		case html.TextToken:
			if inTitle && title == "" {
				title = truncate(strings.TrimSpace(string(tokenizer.Text())))
			}
		case html.EndTagToken:
			name, _ := tokenizer.TagName()
			switch string(name) {
			case "title":
				inTitle = false
			case "head":
				return finishOpenGraph(og, title, description)
			}
		}
	}
}

func finishOpenGraph(og map[string]string, title, description string) map[string]string {
	if _, ok := og["og:title"]; !ok && title != "" {
		og["og:title"] = title
	}
	if _, ok := og["og:description"]; !ok && description != "" {
		og["og:description"] = description
	}
	return og
}

func truncate(value string) string {
	if len(value) <= urlPreviewMaxValue {
		return value
	}
	// Cut at a rune boundary.
	cut := urlPreviewMaxValue
	for cut > 0 && !utf8.RuneStart(value[cut]) {
		cut--
	}
	return value[:cut] + "…"
}
