// Copyright 2024 New Vector Ltd.
// Copyright 2017 Vector Creations Ltd
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package routing

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"

	federationrouting "codefloe.com/pat-s/zendrite/federationapi/routing"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/mediaapi/storage"
	"codefloe.com/pat-s/zendrite/mediaapi/storage/filestore"
	"codefloe.com/pat-s/zendrite/mediaapi/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// configResponse is the response to GET /_matrix/media/r0/config
// https://matrix.org/docs/spec/client_server/latest#get-matrix-media-r0-config
type configResponse struct {
	UploadSize *config.FileSizeBytes `json:"m.upload.size,omitempty"`
}

// Setup registers the media API HTTP handlers
//
// Due to Setup being used to call many other functions, a gocyclo nolint is
// applied:
//
//nolint:gocyclo
func Setup(
	routers httputil.Routers,
	cfg *config.Zendrite,
	db storage.Database,
	fileStore filestore.FileStore,
	userAPI userapi.MediaUserAPI,
	client *fclient.Client,
	federationClient fclient.FederationClient,
	keyRing gomatrixserverlib.JSONVerifier, contexts ...context.Context,
) {
	rateLimits := httputil.NewRateLimits(&cfg.ClientAPI.RateLimiting, contexts...)

	v3mux := routers.Media.PathPrefix("/{apiversion:(?:r0|v1|v3)}/").Subrouter()
	v1mux := routers.Client.PathPrefix("/v1/media/").Subrouter()
	v1fedMux := routers.Federation.PathPrefix("/v1/media/").Subrouter()

	activeThumbnailGeneration := &types.ActiveThumbnailGeneration{
		PathToResult: map[string]*types.ThumbnailGenerationResult{},
	}
	pendingUploads := NewPendingUploads()

	uploadHandler := httputil.MakeAuthAPI(
		"upload", userAPI,
		func(req *http.Request, dev *userapi.Device) util.JSONResponse {
			if r := rateLimits.Limit(req, dev); r != nil {
				return *r
			}
			return Upload(req, &cfg.MediaAPI, dev, db, fileStore, activeThumbnailGeneration)
		},
	)

	// MSC2246 asynchronous uploads: reserve an mxc:// URI, upload later.
	createHandler := httputil.MakeAuthAPI(
		"create", userAPI,
		func(req *http.Request, dev *userapi.Device) util.JSONResponse {
			if r := rateLimits.Limit(req, dev); r != nil {
				return *r
			}
			return CreateMedia(req, &cfg.MediaAPI, dev, db)
		},
	)

	uploadToPendingHandler := httputil.MakeAuthAPI(
		"upload_pending", userAPI,
		func(req *http.Request, dev *userapi.Device) util.JSONResponse {
			if r := rateLimits.Limit(req, dev); r != nil {
				return *r
			}
			vars, err := httputil.URLDecodeMapValues(httputil.Vars(req))
			if err != nil {
				return util.ErrorResponse(err)
			}
			return UploadToPending(
				req, &cfg.MediaAPI, dev, db, fileStore, activeThumbnailGeneration, pendingUploads,
				spec.ServerName(vars["serverName"]), types.MediaID(vars["mediaId"]),
			)
		},
	)

	configHandler := httputil.MakeAuthAPI("config", userAPI, func(req *http.Request, device *userapi.Device) util.JSONResponse {
		if r := rateLimits.Limit(req, device); r != nil {
			return *r
		}
		respondSize := &cfg.MediaAPI.MaxFileSizeBytes
		if cfg.MediaAPI.MaxFileSizeBytes == 0 {
			respondSize = nil
		}
		return util.JSONResponse{
			Code: http.StatusOK,
			JSON: configResponse{UploadSize: respondSize},
		}
	})

	v3mux.Handle("/upload", uploadHandler).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/upload/{serverName}/{mediaId}", uploadToPendingHandler).Methods(http.MethodPut, http.MethodOptions)
	routers.Media.Handle("/v1/create", createHandler).Methods(http.MethodPost, http.MethodOptions)
	v3mux.Handle("/config", configHandler).Methods(http.MethodGet, http.MethodOptions)

	activeRemoteRequests := &types.ActiveRemoteRequests{
		MXCToResult: map[string]*types.RemoteRequestResult{},
	}

	downloadHandler := makeDownloadAPI("download_unauthed", &cfg.MediaAPI, rateLimits, db, fileStore, client, federationClient, activeRemoteRequests, activeThumbnailGeneration, pendingUploads, false)
	v3mux.Handle("/download/{serverName}/{mediaId}", downloadHandler).Methods(http.MethodGet, http.MethodOptions)
	v3mux.Handle("/download/{serverName}/{mediaId}/{downloadName}", downloadHandler).Methods(http.MethodGet, http.MethodOptions)

	v3mux.Handle(
		"/thumbnail/{serverName}/{mediaId}",
		makeDownloadAPI("thumbnail_unauthed", &cfg.MediaAPI, rateLimits, db, fileStore, client, federationClient, activeRemoteRequests, activeThumbnailGeneration, pendingUploads, false),
	).Methods(http.MethodGet, http.MethodOptions)

	// v1 client endpoints requiring auth. Call guests may download media to
	// display it in the rooms they joined, but not upload any.
	downloadHandlerAuthed := httputil.MakeHTTPAPI("download", userAPI, cfg.Global.Metrics.Enabled, makeDownloadAPI("download_authed_client", &cfg.MediaAPI, rateLimits, db, fileStore, client, federationClient, activeRemoteRequests, activeThumbnailGeneration, pendingUploads, false), httputil.WithAuth(), httputil.WithAllowCallGuests())
	v1mux.Handle("/config", configHandler).Methods(http.MethodGet, http.MethodOptions)
	v1mux.Handle("/download/{serverName}/{mediaId}", downloadHandlerAuthed).Methods(http.MethodGet, http.MethodOptions)
	v1mux.Handle("/download/{serverName}/{mediaId}/{downloadName}", downloadHandlerAuthed).Methods(http.MethodGet, http.MethodOptions)

	v1mux.Handle(
		"/thumbnail/{serverName}/{mediaId}",
		httputil.MakeHTTPAPI("thumbnail", userAPI, cfg.Global.Metrics.Enabled, makeDownloadAPI("thumbnail_authed_client", &cfg.MediaAPI, rateLimits, db, fileStore, client, federationClient, activeRemoteRequests, activeThumbnailGeneration, pendingUploads, false), httputil.WithAuth(), httputil.WithAllowCallGuests()),
	).Methods(http.MethodGet, http.MethodOptions)

	// URL previews. Disabled, the routes stay unregistered and answer 404
	// M_UNRECOGNIZED, as in Synapse. Config verification has already
	// checked the IP ranges.
	if cfg.MediaAPI.URLPreview.Enabled {
		previewer, err := NewURLPreviewer(&cfg.MediaAPI, db, fileStore, activeThumbnailGeneration)
		if err != nil {
			logrus.WithError(err).Fatal("invalid media_api.url_preview configuration")
		}
		previewHandler := httputil.MakeAuthAPI("preview_url", userAPI, func(req *http.Request, dev *userapi.Device) util.JSONResponse {
			if r := rateLimits.Limit(req, dev); r != nil {
				return *r
			}
			return previewer.Preview(req, dev)
		})
		v1mux.Handle("/preview_url", previewHandler).Methods(http.MethodGet, http.MethodOptions)
		// Deprecated by MSC3916, kept for older clients.
		v3mux.Handle("/preview_url", previewHandler).Methods(http.MethodGet, http.MethodOptions)
	}

	// same, but for federation
	v1fedMux.Handle("/download/{mediaId}", federationrouting.MakeFedHTTPAPI(
		cfg.Global.ServerName, cfg.Global.IsLocalServerName, keyRing,
		makeDownloadAPI("download_authed_federation", &cfg.MediaAPI, rateLimits, db, fileStore, client, federationClient, activeRemoteRequests, activeThumbnailGeneration, pendingUploads, true),
	)).Methods(http.MethodGet, http.MethodOptions)
	v1fedMux.Handle("/thumbnail/{mediaId}", federationrouting.MakeFedHTTPAPI(
		cfg.Global.ServerName, cfg.Global.IsLocalServerName, keyRing,
		makeDownloadAPI("thumbnail_authed_federation", &cfg.MediaAPI, rateLimits, db, fileStore, client, federationClient, activeRemoteRequests, activeThumbnailGeneration, pendingUploads, true),
	)).Methods(http.MethodGet, http.MethodOptions)
}

var thumbnailCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "zendrite",
		Subsystem: "mediaapi",
		Name:      "thumbnail",
		Help:      "Total number of media_api requests for thumbnails",
	},
	[]string{"code", "type"},
)

var thumbnailSize = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "zendrite",
		Subsystem: "mediaapi",
		Name:      "thumbnail_size_bytes",
		Help:      "Total size of media_api requests for thumbnails",
		Buckets:   []float64{50, 100, 200, 500, 900, 1500, 3000, 6000},
	},
	[]string{"code", "type"},
)

var downloadCounter = promauto.NewCounterVec(
	prometheus.CounterOpts{
		Namespace: "zendrite",
		Subsystem: "mediaapi",
		Name:      "download",
		Help:      "Total size of media_api requests for full downloads",
	},
	[]string{"code", "type"},
)

var downloadSize = promauto.NewHistogramVec(
	prometheus.HistogramOpts{
		Namespace: "zendrite",
		Subsystem: "mediaapi",
		Name:      "download_size_bytes",
		Help:      "Total size of media_api requests for full downloads",
		Buckets:   []float64{1500, 3000, 6000, 10_000, 50_000, 100_000},
	},
	[]string{"code", "type"},
)

func makeDownloadAPI(
	name string,
	cfg *config.MediaAPI,
	rateLimits *httputil.RateLimits,
	db storage.Database,
	fileStore filestore.FileStore,
	client *fclient.Client,
	fedClient fclient.FederationClient,
	activeRemoteRequests *types.ActiveRemoteRequests,
	activeThumbnailGeneration *types.ActiveThumbnailGeneration,
	pendingUploads *PendingUploads,
	forFederation bool,
) http.HandlerFunc {
	var counterVec *prometheus.CounterVec
	var sizeVec *prometheus.HistogramVec
	var requestType string
	if cfg.Matrix.Metrics.Enabled {
		split := strings.Split(name, "_")
		// The first part of the split is either "download" or "thumbnail"
		name = split[0]
		// The remainder of the split is something like "authed_download" or "unauthed_thumbnail", etc.
		// This is used to curry the metrics with the given types.
		requestType = strings.Join(split[1:], "_")

		counterVec = thumbnailCounter
		sizeVec = thumbnailSize
		if name != "thumbnail" {
			counterVec = downloadCounter
			sizeVec = downloadSize
		}
	}
	httpHandler := func(w http.ResponseWriter, req *http.Request) {
		req = util.RequestWithLogging(req)

		// Set internal headers returned regardless of the outcome of the request
		util.SetCORSHeaders(w)
		w.Header().Set("Cross-Origin-Resource-Policy", "cross-origin")
		// Content-Type will be overridden in case of returning file data, else we respond with JSON-formatted errors
		w.Header().Set("Content-Type", "application/json")

		// Ratelimit requests
		// NOTSPEC: The spec says everything at /media/ should be rate limited, but this causes issues with thumbnails (#2243)
		if name != "thumbnail" {
			if r := rateLimits.Limit(req, nil); r != nil {
				for header, value := range r.Headers {
					w.Header().Set(header, value)
				}
				w.WriteHeader(r.Code)
				if err := json.NewEncoder(w).Encode(r.JSON); err != nil {
					return
				}
				return
			}
		}

		vars, _ := httputil.URLDecodeMapValues(httputil.Vars(req))
		serverName := spec.ServerName(vars["serverName"])

		// For the purposes of loop avoidance, we will return a 404 if allow_remote is set to
		// false in the query string and the target server name isn't our own.
		// https://github.com/matrix-org/matrix-doc/pull/1265
		if allowRemote := req.URL.Query().Get("allow_remote"); strings.ToLower(allowRemote) == "false" {
			if serverName != cfg.Matrix.ServerName {
				w.WriteHeader(http.StatusNotFound)
				return
			}
		}

		// allow_redirect (MSC3860) is accepted and ignored: media is always
		// served from our own store, never redirected.

		// Cache media for at least one day.
		w.Header().Set("Cache-Control", "public,max-age=86400,s-maxage=86400")

		Download(
			w,
			req,
			serverName,
			types.MediaID(vars["mediaId"]),
			cfg,
			db,
			fileStore,
			client,
			fedClient,
			activeRemoteRequests,
			activeThumbnailGeneration,
			pendingUploads,
			strings.HasPrefix(name, "thumbnail"),
			vars["downloadName"],
			forFederation,
		)
	}

	var handlerFunc http.HandlerFunc
	if counterVec != nil {
		counterVec = counterVec.MustCurryWith(prometheus.Labels{"type": requestType})
		sizeVec2 := sizeVec.MustCurryWith(prometheus.Labels{"type": requestType})
		handlerFunc = promhttp.InstrumentHandlerCounter(counterVec, http.HandlerFunc(httpHandler))
		handlerFunc = promhttp.InstrumentHandlerResponseSize(sizeVec2, handlerFunc).ServeHTTP
	} else {
		handlerFunc = http.HandlerFunc(httpHandler)
	}
	return handlerFunc
}
