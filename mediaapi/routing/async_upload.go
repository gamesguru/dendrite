// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	log "github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/mediaapi/storage"
	"codefloe.com/pat-s/zendrite/mediaapi/storage/filestore"
	"codefloe.com/pat-s/zendrite/mediaapi/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

const (
	// DefaultPendingDownloadTimeout is the spec default of timeout_ms.
	defaultPendingDownloadTimeout = 20 * time.Second
	// PendingUploadPollInterval bounds how long a waiting download misses an
	// upload that completed on another replica, which the in-process
	// notification can't see.
	pendingUploadPollInterval = 500 * time.Millisecond
	// PendingMediaSweepInterval is how often expired pending media IDs are
	// deleted. Expired IDs are treated as unknown before they're swept.
	pendingMediaSweepInterval = time.Hour
)

// errNotYetUploaded is returned by a download of media that was created with
// POST /create but whose content didn't arrive within timeout_ms.
var errNotYetUploaded = errors.New("media has not been uploaded yet")

// createResponse is the response to POST /_matrix/media/v1/create.
type createResponse struct {
	ContentURI      string         `json:"content_uri"`
	UnusedExpiresAt spec.Timestamp `json:"unused_expires_at"`
}

// PendingUploads wakes downloads waiting for pending media on this process
// when its upload completes here. Downloads also poll the database, so an
// upload completed on another replica is still seen.
type PendingUploads struct {
	mu      sync.Mutex
	waiters map[string]*pendingUploadWaiters
}

type pendingUploadWaiters struct {
	ch    chan struct{}
	count int
}

// NewPendingUploads returns an empty PendingUploads.
func NewPendingUploads() *PendingUploads {
	return &PendingUploads{waiters: map[string]*pendingUploadWaiters{}}
}

// wait returns a channel that is closed once the upload for key completes on
// this process. The caller must call release when it stops waiting.
func (p *PendingUploads) wait(key string) (ch <-chan struct{}, release func()) {
	p.mu.Lock()
	defer p.mu.Unlock()
	w, ok := p.waiters[key]
	if !ok {
		w = &pendingUploadWaiters{ch: make(chan struct{})}
		p.waiters[key] = w
	}
	w.count++
	return w.ch, func() {
		p.mu.Lock()
		defer p.mu.Unlock()
		w.count--
		if w.count == 0 && p.waiters[key] == w {
			delete(p.waiters, key)
		}
	}
}

// notify wakes all downloads waiting for key.
func (p *PendingUploads) notify(key string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if w, ok := p.waiters[key]; ok {
		close(w.ch)
		delete(p.waiters, key)
	}
}

func mxcKey(origin spec.ServerName, mediaID types.MediaID) string {
	return "mxc://" + string(origin) + "/" + string(mediaID)
}

// CreateMedia implements POST /_matrix/media/v1/create. It reserves a media ID
// for the user, whose content is uploaded later with PUT /upload.
func CreateMedia(req *http.Request, cfg *config.MediaAPI, dev *userapi.Device, db storage.Database) util.JSONResponse {
	r := &uploadRequest{
		MediaMetadata: &types.MediaMetadata{
			Origin: cfg.Matrix.ServerName,
			UserID: types.MatrixUserID(dev.UserID),
		},
		Logger: util.GetLogger(req.Context()).WithField("Origin", cfg.Matrix.ServerName),
	}
	mediaID, err := r.generateMediaID(req.Context(), db)
	if err != nil {
		r.Logger.WithError(err).Error("Failed to generate media ID for created media")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	r.MediaMetadata.MediaID = mediaID
	r.MediaMetadata.UnusedExpiresAt = spec.AsTimestamp(time.Now().Add(cfg.AsyncUploads.Expiry()))

	err = db.StorePendingMedia(req.Context(), r.MediaMetadata, cfg.AsyncUploads.MaxPending())
	if errors.Is(err, types.ErrTooManyPendingUploads) {
		return util.JSONResponse{
			Code: http.StatusTooManyRequests,
			JSON: spec.MatrixError{
				ErrCode: spec.ErrorLimitExceeded,
				Err:     "Too many pending media uploads, upload or wait for them to expire first.",
			},
		}
	}
	if err != nil {
		r.Logger.WithError(err).Error("Failed to store created media")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: createResponse{
			ContentURI:      mxcKey(cfg.Matrix.ServerName, mediaID),
			UnusedExpiresAt: r.MediaMetadata.UnusedExpiresAt,
		},
	}
}

// UploadToPending implements PUT /upload/{serverName}/{mediaId}, which
// uploads the content of a media ID created with POST /create.
func UploadToPending(
	req *http.Request,
	cfg *config.MediaAPI,
	dev *userapi.Device,
	db storage.Database,
	fileStore filestore.FileStore,
	activeThumbnailGeneration *types.ActiveThumbnailGeneration,
	pendingUploads *PendingUploads,
	serverName spec.ServerName,
	mediaID types.MediaID,
) util.JSONResponse {
	unknown := util.JSONResponse{
		Code: http.StatusNotFound,
		JSON: spec.NotFound("Unknown media ID"),
	}
	if serverName != cfg.Matrix.ServerName || !mediaIDRegex.MatchString(string(mediaID)) {
		return unknown
	}

	existing, err := db.GetMediaMetadata(req.Context(), mediaID, serverName)
	if err != nil {
		util.GetLogger(req.Context()).WithError(err).Error("Failed to look up created media")
		return util.JSONResponse{
			Code: http.StatusInternalServerError,
			JSON: spec.InternalServerError{},
		}
	}
	if resErr := pendingUploadError(existing, dev.UserID); resErr != nil {
		return *resErr
	}

	r, resErr := parseAndValidateRequest(req, cfg, dev)
	if resErr != nil {
		return *resErr
	}
	r.MediaMetadata.MediaID = mediaID
	r.Logger = r.Logger.WithField("media_id", mediaID)

	if resErr = r.doUploadToPending(req.Context(), req.Body, cfg, db, fileStore, activeThumbnailGeneration); resErr != nil {
		return *resErr
	}
	pendingUploads.notify(mxcKey(serverName, mediaID))

	return util.JSONResponse{
		Code: http.StatusOK,
		JSON: struct{}{},
	}
}

// pendingUploadError returns the error for uploading to existing as userID,
// or nil if the upload may go ahead.
func pendingUploadError(existing *types.MediaMetadata, userID string) *util.JSONResponse {
	switch {
	case existing == nil:
		return &util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Unknown media ID"),
		}
	case string(existing.UserID) != userID:
		return &util.JSONResponse{
			Code: http.StatusForbidden,
			JSON: spec.Forbidden("This media ID was created by another user"),
		}
	case !existing.IsPending():
		return &util.JSONResponse{
			Code: http.StatusConflict,
			JSON: spec.MatrixError{ErrCode: "M_CANNOT_OVERWRITE_MEDIA", Err: "Media already uploaded"},
		}
	case pendingExpired(existing):
		return &util.JSONResponse{
			Code: http.StatusNotFound,
			JSON: spec.NotFound("Media ID has expired"),
		}
	}
	return nil
}

func pendingExpired(m *types.MediaMetadata) bool {
	return m.IsPending() && m.UnusedExpiresAt <= spec.AsTimestamp(time.Now())
}

// doUploadToPending stores the request body as the content of the pending
// media ID in r.MediaMetadata.
func (r *uploadRequest) doUploadToPending(
	ctx context.Context,
	reqReader io.Reader,
	cfg *config.MediaAPI,
	db storage.Database,
	fileStore filestore.FileStore,
	activeThumbnailGeneration *types.ActiveThumbnailGeneration,
) *util.JSONResponse {
	hash, bytesWritten, tmpDir, cleanup, resErr := r.receiveFile(ctx, reqReader, cfg, fileStore)
	if resErr != nil {
		return resErr
	}
	defer cleanup()

	r.MediaMetadata.FileSizeBytes = bytesWritten
	r.MediaMetadata.Base64Hash = hash
	r.Logger.WithFields(log.Fields{
		"Base64Hash":    r.MediaMetadata.Base64Hash,
		"UploadName":    r.MediaMetadata.UploadName,
		"FileSizeBytes": r.MediaMetadata.FileSizeBytes,
		"ContentType":   r.MediaMetadata.ContentType,
	}).Info("File uploaded to created media ID")

	return r.storeFileAndMetadata(
		ctx, tmpDir, fileStore, db, cfg.ThumbnailSizes,
		activeThumbnailGeneration, cfg.MaxThumbnailGenerators, true,
	)
}

// pendingTimeout parses the timeout_ms query parameter of a download, capped
// at the configured maximum.
func pendingTimeout(req *http.Request, cfg *config.MediaAPI) time.Duration {
	timeout := defaultPendingDownloadTimeout
	if v := req.URL.Query().Get("timeout_ms"); v != "" {
		if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
			timeout = time.Duration(max(ms, 0)) * time.Millisecond
		}
	}
	return min(timeout, cfg.AsyncUploads.MaxWait())
}

// waitForPendingUpload waits until the content of the pending media ID in
// r.MediaMetadata is uploaded and returns its metadata. It returns nil if the
// media ID expired or disappeared, and errNotYetUploaded on timeout.
func (r *downloadRequest) waitForPendingUpload(
	ctx context.Context,
	db storage.Database,
	metadata *types.MediaMetadata,
) (*types.MediaMetadata, error) {
	key := mxcKey(r.MediaMetadata.Origin, r.MediaMetadata.MediaID)
	deadline := time.Now().Add(r.pendingTimeout)
	r.Logger.WithField("timeout", r.pendingTimeout).Debug("Waiting for created media to be uploaded")
	for {
		if metadata == nil || pendingExpired(metadata) {
			return nil, nil
		}
		if !metadata.IsPending() {
			return metadata, nil
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, errNotYetUploaded
		}

		var uploaded <-chan struct{}
		release := func() {}
		if r.pendingUploads != nil {
			uploaded, release = r.pendingUploads.wait(key)
		}
		timer := time.NewTimer(min(remaining, pendingUploadPollInterval))
		select {
		case <-ctx.Done():
			timer.Stop()
			release()
			return nil, ctx.Err()
		case <-uploaded:
		case <-timer.C:
		}
		timer.Stop()
		release()

		var err error
		metadata, err = db.GetMediaMetadata(ctx, r.MediaMetadata.MediaID, r.MediaMetadata.Origin)
		if err != nil {
			return nil, fmt.Errorf("db.GetMediaMetadata: %w", err)
		}
	}
}

// SweepExpiredPendingMedia periodically deletes pending media IDs whose
// reservation expired, until ctx is done. The delete only matches expired
// rows, so every replica may run it.
func SweepExpiredPendingMedia(ctx context.Context, db storage.Database) {
	ticker := time.NewTicker(pendingMediaSweepInterval)
	defer ticker.Stop()
	for {
		deleted, err := db.DeleteExpiredPendingMedia(ctx)
		switch {
		case err != nil && ctx.Err() == nil:
			log.WithError(err).Warn("Failed to delete expired pending media")
		case deleted > 0:
			log.WithField("count", deleted).Debug("Deleted expired pending media")
		}
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}
