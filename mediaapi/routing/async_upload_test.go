package routing

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/mediaapi/storage"
	"codefloe.com/pat-s/zendrite/mediaapi/storage/filestore"
	"codefloe.com/pat-s/zendrite/mediaapi/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

var (
	asyncAlice = &userapi.Device{UserID: "@alice:localhost"}
	asyncBob   = &userapi.Device{UserID: "@bob:localhost"}
)

// asyncUploadEnv is a media API with its own database and file store.
type asyncUploadEnv struct {
	cfg            *config.MediaAPI
	db             storage.Database
	fileStore      filestore.FileStore
	thumbnails     *types.ActiveThumbnailGeneration
	remote         *types.ActiveRemoteRequests
	pendingUploads *PendingUploads
}

func newAsyncUploadEnv(t *testing.T, dbType test.DBType) *asyncUploadEnv {
	t.Helper()
	connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
	t.Cleanup(closeDB)
	cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	db, err := storage.NewMediaAPIDatasource(cm, &config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	})
	require.NoError(t, err)
	basePath := t.TempDir()
	fileStore, err := filestore.NewLocalFileStore(basePath)
	require.NoError(t, err)
	return &asyncUploadEnv{
		cfg: &config.MediaAPI{
			Matrix:           &config.Global{ServerName: "localhost"},
			MaxFileSizeBytes: 1000,
			AbsBasePath:      config.Path(basePath),
		},
		db:             db,
		fileStore:      fileStore,
		thumbnails:     &types.ActiveThumbnailGeneration{PathToResult: map[string]*types.ThumbnailGenerationResult{}},
		remote:         &types.ActiveRemoteRequests{MXCToResult: map[string]*types.RemoteRequestResult{}},
		pendingUploads: NewPendingUploads(),
	}
}

func (e *asyncUploadEnv) create(t *testing.T, dev *userapi.Device) (types.MediaID, createResponse) {
	t.Helper()
	res := CreateMedia(httptest.NewRequest(http.MethodPost, "/_matrix/media/v1/create", nil), e.cfg, dev, e.db)
	require.Equal(t, http.StatusOK, res.Code, "%+v", res.JSON)
	created, ok := res.JSON.(createResponse)
	require.True(t, ok, "%+v", res.JSON)
	mediaID, found := strings.CutPrefix(created.ContentURI, "mxc://localhost/")
	require.True(t, found, created.ContentURI)
	return types.MediaID(mediaID), created
}

func (e *asyncUploadEnv) put(dev *userapi.Device, serverName spec.ServerName, mediaID types.MediaID, body string) util.JSONResponse {
	req := httptest.NewRequest(
		http.MethodPut,
		"/_matrix/media/v3/upload/"+string(serverName)+"/"+string(mediaID)+"?filename=hello.txt",
		strings.NewReader(body),
	)
	req.Header.Set("Content-Type", "text/plain")
	return UploadToPending(req, e.cfg, dev, e.db, e.fileStore, e.thumbnails, e.pendingUploads, serverName, mediaID)
}

func (e *asyncUploadEnv) download(mediaID types.MediaID, query string, pendingUploads *PendingUploads) *httptest.ResponseRecorder {
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v1/media/download/localhost/"+string(mediaID)+query, nil)
	Download(
		rec, req, "localhost", mediaID, e.cfg, e.db, e.fileStore, nil, nil,
		e.remote, e.thumbnails, pendingUploads, false, "", false,
	)
	return rec
}

func requireErrCode(t *testing.T, res util.JSONResponse, code int, errcode spec.MatrixErrorCode) {
	t.Helper()
	require.Equal(t, code, res.Code, "%+v", res.JSON)
	body, err := json.Marshal(res.JSON)
	require.NoError(t, err)
	var matrixErr spec.MatrixError
	require.NoError(t, json.Unmarshal(body, &matrixErr))
	require.Equal(t, errcode, matrixErr.ErrCode)
}

func requireRecorderErrCode(t *testing.T, rec *httptest.ResponseRecorder, code int, errcode spec.MatrixErrorCode) {
	t.Helper()
	require.Equal(t, code, rec.Code, rec.Body.String())
	var matrixErr spec.MatrixError
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &matrixErr))
	require.Equal(t, errcode, matrixErr.ErrCode)
}

func TestAsyncUpload(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		e := newAsyncUploadEnv(t, dbType)

		t.Run("create, upload and download", func(t *testing.T) {
			before := spec.AsTimestamp(time.Now().Add(config.DefaultAsyncUploadUnusedExpiry))
			mediaID, created := e.create(t, asyncAlice)
			require.GreaterOrEqual(t, created.UnusedExpiresAt, before)

			res := e.put(asyncAlice, "localhost", mediaID, "hello")
			require.Equal(t, http.StatusOK, res.Code, "%+v", res.JSON)
			require.Equal(t, struct{}{}, res.JSON)

			rec := e.download(mediaID, "", e.pendingUploads)
			require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			require.Equal(t, "hello", rec.Body.String())
			require.Equal(t, "text/plain", rec.Header().Get("Content-Type"))
			require.Contains(t, rec.Header().Get("Content-Disposition"), "hello.txt")
		})

		t.Run("upload by another user is forbidden", func(t *testing.T) {
			mediaID, _ := e.create(t, asyncAlice)
			requireErrCode(t, e.put(asyncBob, "localhost", mediaID, "hello"), http.StatusForbidden, spec.ErrorForbidden)
		})

		t.Run("second upload cannot overwrite", func(t *testing.T) {
			mediaID, _ := e.create(t, asyncAlice)
			require.Equal(t, http.StatusOK, e.put(asyncAlice, "localhost", mediaID, "first").Code)
			requireErrCode(t, e.put(asyncAlice, "localhost", mediaID, "second"), http.StatusConflict, "M_CANNOT_OVERWRITE_MEDIA")

			rec := e.download(mediaID, "", e.pendingUploads)
			require.Equal(t, "first", rec.Body.String())
		})

		t.Run("concurrent uploads succeed once", func(t *testing.T) {
			mediaID, _ := e.create(t, asyncAlice)
			var wg sync.WaitGroup
			codes := make([]int, 4)
			for i := range codes {
				wg.Add(1)
				go func() {
					defer wg.Done()
					codes[i] = e.put(asyncAlice, "localhost", mediaID, strings.Repeat("x", i+1)).Code
				}()
			}
			wg.Wait()
			ok := 0
			for _, code := range codes {
				if code == http.StatusOK {
					ok++
				} else {
					require.Equal(t, http.StatusConflict, code)
				}
			}
			require.Equal(t, 1, ok, "codes: %v", codes)
			require.Equal(t, http.StatusOK, e.download(mediaID, "", e.pendingUploads).Code)
		})

		t.Run("unknown, remote and uploaded media IDs are not found", func(t *testing.T) {
			requireErrCode(t, e.put(asyncAlice, "localhost", "unknown", "hello"), http.StatusNotFound, spec.ErrorNotFound)
			mediaID, _ := e.create(t, asyncAlice)
			requireErrCode(t, e.put(asyncAlice, "remote.example", mediaID, "hello"), http.StatusNotFound, spec.ErrorNotFound)
		})

		t.Run("too large upload", func(t *testing.T) {
			mediaID, _ := e.create(t, asyncAlice)
			requireErrCode(t, e.put(asyncAlice, "localhost", mediaID, strings.Repeat("x", 1001)), http.StatusRequestEntityTooLarge, spec.ErrorTooLarge)
			// The reservation is still usable.
			require.Equal(t, http.StatusOK, e.put(asyncAlice, "localhost", mediaID, "hello").Code)
		})

		t.Run("pending download times out", func(t *testing.T) {
			mediaID, _ := e.create(t, asyncAlice)
			start := time.Now()
			rec := e.download(mediaID, "?timeout_ms=100", e.pendingUploads)
			require.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
			requireRecorderErrCode(t, rec, http.StatusGatewayTimeout, "M_NOT_YET_UPLOADED")
			require.Equal(t, "no-store", rec.Header().Get("Cache-Control"))
		})

		t.Run("pending download completes when the upload arrives", func(t *testing.T) {
			for name, waiterUploads := range map[string]*PendingUploads{
				"same replica":  e.pendingUploads,
				"other replica": NewPendingUploads(), // only sees the upload by polling
			} {
				t.Run(name, func(t *testing.T) {
					mediaID, _ := e.create(t, asyncAlice)
					done := make(chan *httptest.ResponseRecorder)
					go func() { done <- e.download(mediaID, "?timeout_ms=10000", waiterUploads) }()
					time.Sleep(50 * time.Millisecond)
					require.Equal(t, http.StatusOK, e.put(asyncAlice, "localhost", mediaID, "late").Code)
					select {
					case rec := <-done:
						require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
						require.Equal(t, "late", rec.Body.String())
					case <-time.After(5 * time.Second):
						t.Fatal("download did not complete after the upload")
					}
				})
			}
		})
	})
}

func TestAsyncUploadExpiry(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		e := newAsyncUploadEnv(t, dbType)
		e.cfg.AsyncUploads.UnusedExpiry = 20 * time.Millisecond
		mediaID, _ := e.create(t, asyncAlice)
		time.Sleep(40 * time.Millisecond)

		requireErrCode(t, e.put(asyncAlice, "localhost", mediaID, "hello"), http.StatusNotFound, spec.ErrorNotFound)
		requireRecorderErrCode(t, e.download(mediaID, "?timeout_ms=1000", e.pendingUploads), http.StatusNotFound, spec.ErrorNotFound)

		// Expired reservations don't count towards the limit.
		e.cfg.AsyncUploads.MaxPendingPerUser = 1
		e.cfg.AsyncUploads.UnusedExpiry = time.Hour
		e.create(t, asyncAlice)
	})
}

func TestAsyncUploadPendingLimit(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		e := newAsyncUploadEnv(t, dbType)
		e.cfg.AsyncUploads.MaxPendingPerUser = 2
		first, _ := e.create(t, asyncAlice)
		e.create(t, asyncAlice)

		res := CreateMedia(httptest.NewRequest(http.MethodPost, "/_matrix/media/v1/create", nil), e.cfg, asyncAlice, e.db)
		requireErrCode(t, res, http.StatusTooManyRequests, spec.ErrorLimitExceeded)
		e.create(t, asyncBob)

		// Uploading frees a slot.
		require.Equal(t, http.StatusOK, e.put(asyncAlice, "localhost", first, "hello").Code)
		e.create(t, asyncAlice)
	})
}

func TestAsyncUploadDeduplication(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		e := newAsyncUploadEnv(t, dbType)
		e.create(t, asyncAlice)

		// A regular upload must not be deduplicated against pending media.
		req := httptest.NewRequest(http.MethodPost, "/_matrix/media/v3/upload?filename=a.txt", strings.NewReader("same"))
		req.Header.Set("Content-Type", "text/plain")
		res := Upload(req, e.cfg, asyncAlice, e.db, e.fileStore, e.thumbnails)
		require.Equal(t, http.StatusOK, res.Code, "%+v", res.JSON)
		uploadRes, ok := res.JSON.(uploadResponse)
		require.True(t, ok, "%+v", res.JSON)
		uploaded := strings.TrimPrefix(uploadRes.ContentURI, "mxc://localhost/")
		require.Equal(t, "same", e.download(types.MediaID(uploaded), "", e.pendingUploads).Body.String())

		// Uploading identical content to a created media ID shares the file.
		mediaID, _ := e.create(t, asyncAlice)
		require.Equal(t, http.StatusOK, e.put(asyncAlice, "localhost", mediaID, "same").Code)
		require.Equal(t, "same", e.download(mediaID, "", e.pendingUploads).Body.String())
		require.Equal(t, "same", e.download(types.MediaID(uploaded), "", e.pendingUploads).Body.String())
	})
}

func TestPendingTimeout(t *testing.T) {
	cfg := &config.MediaAPI{}
	for query, want := range map[string]time.Duration{
		"":                     defaultPendingDownloadTimeout,
		"?timeout_ms=1500":     1500 * time.Millisecond,
		"?timeout_ms=-5":       0,
		"?timeout_ms=bogus":    defaultPendingDownloadTimeout,
		"?timeout_ms=10000000": config.DefaultAsyncUploadMaxDownloadWait,
	} {
		req := httptest.NewRequest(http.MethodGet, "/download/localhost/abc"+query, nil)
		require.Equal(t, want, pendingTimeout(req, cfg), query)
	}
}

func TestPendingUploadsRelease(t *testing.T) {
	p := NewPendingUploads()
	_, release := p.wait("mxc://localhost/a")
	_, release2 := p.wait("mxc://localhost/a")
	release()
	release2()
	require.Empty(t, p.waiters, "released waiters must not leak")

	ch, release := p.wait("mxc://localhost/a")
	defer release()
	p.notify("mxc://localhost/a")
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("notify did not wake the waiter")
	}
}
