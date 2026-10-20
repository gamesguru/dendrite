package routing

import (
	"context"
	"fmt"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httptest"
	"net/textproto"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/fclient"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/matrix-org/util"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/mediaapi/storage/filestore"
	"codefloe.com/pat-s/zendrite/mediaapi/types"
	"codefloe.com/pat-s/zendrite/setup/config"
)

// allowLoopbackRedirects lets media redirects reach httptest servers.
func allowLoopbackRedirects(t *testing.T) {
	previous := mediaRedirectClient
	mediaRedirectClient = newMediaRedirectClient(true)
	t.Cleanup(func() { mediaRedirectClient = previous })
}

// redirectingMediaServer answers like a federation media endpoint whose media
// part points at location instead of containing the media.
func redirectingMediaServer(t *testing.T, location string) *httptest.Server {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mw := multipart.NewWriter(w)
		w.Header().Set("Content-Type", "multipart/mixed; boundary="+mw.Boundary())
		jsonPart, err := mw.CreatePart(textproto.MIMEHeader{"Content-Type": {"application/json"}})
		require.NoError(t, err)
		_, err = jsonPart.Write([]byte("{}"))
		require.NoError(t, err)
		_, err = mw.CreatePart(textproto.MIMEHeader{"Location": {location}})
		require.NoError(t, err)
		require.NoError(t, mw.Close())
	}))
	t.Cleanup(srv.Close)
	return srv
}

// mediaTarget serves the media a redirect points at, after redirects hops.
func mediaTarget(t *testing.T, body string) *httptest.Server {
	var srv *httptest.Server
	srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var hops int
		if _, err := fmt.Sscanf(req.URL.Query().Get("hops"), "%d", &hops); err == nil && hops > 0 {
			http.Redirect(w, req, fmt.Sprintf("%s/media?hops=%d", srv.URL, hops-1), http.StatusFound)
			return
		}
		w.Header().Set("Content-Type", "image/png")
		w.Header().Set("Content-Disposition", `attachment; filename="pic.png"`)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

func fetchMultipart(t *testing.T, srv *httptest.Server, maxSize int64) (*downloadRequest, *mediaPart, error) {
	t.Helper()
	r := &downloadRequest{
		MediaMetadata: &types.MediaMetadata{MediaID: "abc", Origin: "remote.example"},
		Logger:        util.GetLogger(context.Background()),
	}
	resp, err := srv.Client().Get(srv.URL)
	require.NoError(t, err)
	t.Cleanup(func() { resp.Body.Close() })
	part, err := parseMultipartResponse(context.Background(), r, resp, config.FileSizeBytes(maxSize))
	if part != nil {
		t.Cleanup(part.Close)
	}
	return r, part, err
}

func TestMediaRedirectFollowed(t *testing.T) {
	allowLoopbackRedirects(t)
	target := mediaTarget(t, "png bytes")

	for name, location := range map[string]string{
		"direct":         target.URL + "/media",
		"redirect chain": target.URL + "/media?hops=3",
	} {
		t.Run(name, func(t *testing.T) {
			r, part, err := fetchMultipart(t, redirectingMediaServer(t, location), 1000)
			require.NoError(t, err)
			got, err := io.ReadAll(part.reader)
			require.NoError(t, err)
			require.Equal(t, "png bytes", string(got))
			require.Equal(t, int64(len("png bytes")), part.contentLength)
			require.Equal(t, types.ContentType("image/png"), r.MediaMetadata.ContentType)
			require.Equal(t, `attachment; filename="pic.png"`, part.disposition)
		})
	}
}

func TestMediaRedirectRejected(t *testing.T) {
	t.Run("too many hops", func(t *testing.T) {
		allowLoopbackRedirects(t)
		target := mediaTarget(t, "png bytes")
		_, _, err := fetchMultipart(t, redirectingMediaServer(t, fmt.Sprintf("%s/media?hops=%d", target.URL, maxMediaRedirects+1)), 1000)
		require.ErrorContains(t, err, "too many media redirects")
	})
	t.Run("too large", func(t *testing.T) {
		allowLoopbackRedirects(t)
		target := mediaTarget(t, "png bytes")
		_, _, err := fetchMultipart(t, redirectingMediaServer(t, target.URL+"/media"), 4)
		require.ErrorContains(t, err, "exceeds locally configured max media size")
	})
	t.Run("unsupported scheme", func(t *testing.T) {
		allowLoopbackRedirects(t)
		_, _, err := fetchMultipart(t, redirectingMediaServer(t, "file:///etc/passwd"), 1000)
		require.ErrorContains(t, err, "unsupported media redirect scheme")
	})
	t.Run("non-public address", func(t *testing.T) {
		// The default client must not fetch from internal addresses.
		target := mediaTarget(t, "secret")
		_, _, err := fetchMultipart(t, redirectingMediaServer(t, target.URL+"/media"), 1000)
		require.ErrorContains(t, err, "non-public address")
	})
}

func TestIsPublicIP(t *testing.T) {
	for addr, want := range map[string]bool{
		"1.1.1.1":         true,
		"2606:4700::1111": true,
		"127.0.0.1":       false,
		"10.1.2.3":        false,
		"192.168.1.1":     false,
		"169.254.169.254": false,
		"100.64.1.1":      false,
		"0.0.0.0":         false,
		"::1":             false,
		"fd00::1":         false,
		"fe80::1":         false,
	} {
		ip := net.ParseIP(addr)
		require.NotNil(t, ip, addr)
		require.Equal(t, want, isPublicIP(ip), addr)
	}
}

// fakeMediaFedClient answers DownloadMedia from an httptest server.
type fakeMediaFedClient struct {
	fclient.FederationClient
	url string
}

func (f *fakeMediaFedClient) DownloadMedia(ctx context.Context, _, _ spec.ServerName, _ string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.url, nil)
	if err != nil {
		return nil, err
	}
	return http.DefaultClient.Do(req)
}

func TestFetchRemoteFileFollowsRedirect(t *testing.T) {
	allowLoopbackRedirects(t)
	target := mediaTarget(t, "png bytes")
	fed := redirectingMediaServer(t, target.URL+"/media")
	fileStore, err := filestore.NewLocalFileStore(t.TempDir())
	require.NoError(t, err)

	r := &downloadRequest{
		MediaMetadata: &types.MediaMetadata{MediaID: "abc", Origin: "remote.example"},
		Logger:        util.GetLogger(context.Background()),
		fedClient:     &fakeMediaFedClient{url: fed.URL},
		origin:        "localhost",
	}
	_, err = r.fetchRemoteFile(context.Background(), nil, fileStore, 1000)
	require.NoError(t, err)
	require.Equal(t, types.ContentType("image/png"), r.MediaMetadata.ContentType)
	require.Equal(t, types.Filename("pic.png"), r.MediaMetadata.UploadName)
	require.Equal(t, types.FileSizeBytes(len("png bytes")), r.MediaMetadata.FileSizeBytes)
	file, _, err := fileStore.Open(context.Background(), r.MediaMetadata.Base64Hash)
	require.NoError(t, err)
	defer file.Close()
	got, err := io.ReadAll(file)
	require.NoError(t, err)
	require.Equal(t, "png bytes", string(got))
}

func TestFetchRemoteFileNotYetUploaded(t *testing.T) {
	fed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusGatewayTimeout)
		_, _ = w.Write([]byte(`{"errcode":"M_NOT_YET_UPLOADED","error":"Content has not yet been uploaded"}`))
	}))
	defer fed.Close()
	fileStore, err := filestore.NewLocalFileStore(t.TempDir())
	require.NoError(t, err)

	r := &downloadRequest{
		MediaMetadata: &types.MediaMetadata{MediaID: "abc", Origin: "remote.invalid"},
		Logger:        util.GetLogger(context.Background()),
		fedClient:     &fakeMediaFedClient{url: fed.URL},
		origin:        "localhost",
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = r.fetchRemoteFile(ctx, fclient.NewClient(), fileStore, 1000)
	require.ErrorIs(t, err, errNotYetUploaded)
}
