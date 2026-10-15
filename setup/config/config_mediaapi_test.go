package config

import (
	"testing"
	"time"

	"github.com/goccy/go-yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMediaAPIAsyncUploads(t *testing.T) {
	var c MediaAPI
	c.Defaults(DefaultOpts{})
	require.Equal(t, 24*time.Hour, c.AsyncUploads.Expiry())
	require.Equal(t, 10, c.AsyncUploads.MaxPending())
	require.Equal(t, time.Minute, c.AsyncUploads.MaxWait())

	require.NoError(t, yaml.Unmarshal([]byte(`
async_uploads:
  unused_expiry: 2h
  max_pending_per_user: 3
  max_download_wait: 30s
`), &c))
	require.Equal(t, 2*time.Hour, c.AsyncUploads.Expiry())
	require.Equal(t, 3, c.AsyncUploads.MaxPending())
	require.Equal(t, 30*time.Second, c.AsyncUploads.MaxWait())

	// Unset values, e.g. in a config built in code, fall back to the defaults.
	require.Equal(t, DefaultAsyncUploadUnusedExpiry, AsyncUploads{}.Expiry())
	require.Equal(t, DefaultAsyncUploadMaxPendingPerUser, AsyncUploads{}.MaxPending())
	require.Equal(t, DefaultAsyncUploadMaxDownloadWait, AsyncUploads{}.MaxWait())

	c.Matrix = &Global{DatabaseOptions: DatabaseOptions{ConnectionString: "file:test.db"}}
	c.BasePath = "./media_store"
	errs := ConfigErrors{}
	c.Verify(&errs)
	require.Empty(t, errs)
	c.AsyncUploads.MaxPendingPerUser = -1
	c.Verify(&errs)
	require.Len(t, errs, 1)
}

func TestURLPreviewConfig(t *testing.T) {
	t.Parallel()
	var c MediaAPI
	c.Defaults(DefaultOpts{})
	assert.False(t, c.URLPreview.Enabled, "URL previews are opt-in")
	assert.Equal(t, DefaultURLPreviewMaxPageSizeBytes, c.URLPreview.MaxPageSize())
	assert.Equal(t, DefaultURLPreviewCacheDuration, c.URLPreview.CacheTTL())

	c.URLPreview.IPRangeWhitelist = []string{"10.0.0.0/8", "fd00::/8"}
	c.URLPreview.IPRangeBlacklist = []string{"10.1.0.0/16"}
	_, _, err := c.URLPreview.IPRanges()
	assert.NoError(t, err)

	c.URLPreview.IPRangeBlacklist = []string{"10.1.0.0"}
	_, _, err = c.URLPreview.IPRanges()
	assert.ErrorContains(t, err, "media_api.url_preview.ip_range_blacklist[0]")
}
