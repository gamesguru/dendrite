package config

import (
	"fmt"
)

type MediaAPI struct {
	Matrix *Global `yaml:"-"`

	// The MediaAPI database stores information about files uploaded and downloaded
	// by local users. It is only accessed by the MediaAPI.
	Database DatabaseOptions `yaml:"database,omitempty"`

	// The base path to where the media files will be stored. May be relative or absolute.
	//
	// Deprecated: use storage.local.base_path instead. Kept for backwards compatibility.
	BasePath Path `yaml:"base_path"`

	// The absolute base path to where media files will be stored.
	AbsBasePath Path `yaml:"-"`

	// Storage configures where media files and thumbnails are persisted.
	// If unset, the local filesystem backend is used and base_path is honored.
	Storage MediaStorage `yaml:"storage,omitempty"`

	// The maximum file size in bytes that is allowed to be stored on this server.
	// Note: if max_file_size_bytes is set to 0, the size is unlimited.
	// Note: if max_file_size_bytes is not set, it will default to 10485760 (10MB)
	MaxFileSizeBytes FileSizeBytes `yaml:"max_file_size_bytes,omitempty"`

	// Whether to dynamically generate thumbnails on-the-fly if the requested resolution is not already generated
	DynamicThumbnails bool `yaml:"dynamic_thumbnails"`

	// The maximum number of simultaneous thumbnail generators. default: 10
	MaxThumbnailGenerators int `yaml:"max_thumbnail_generators"`

	// A list of thumbnail sizes to be pre-generated for downloaded remote / uploaded content
	ThumbnailSizes []ThumbnailSize `yaml:"thumbnail_sizes"`
}

// MediaStorage selects the backend used to persist media files.
type MediaStorage struct {
	// Type is either "local" or "s3".
	Type string `yaml:"type"`
	// Local contains options for the local filesystem backend.
	Local LocalStorage `yaml:"local,omitempty"`
	// S3 contains options for the S3-compatible backend.
	S3 S3Storage `yaml:"s3,omitempty"`
}

// LocalStorage contains configuration for the local filesystem backend.
type LocalStorage struct {
	// BasePath is the path where media files are stored.
	BasePath Path `yaml:"base_path"`
}

// S3Storage contains configuration for the S3-compatible backend.
type S3Storage struct {
	// Endpoint is the S3 endpoint URL, e.g. "https://s3.amazonaws.com" or
	// "http://localhost:9000". If empty, the AWS regional endpoint is used.
	// The scheme (http/https) must be included.
	Endpoint string `yaml:"endpoint"`
	// Region is the AWS region, e.g. "us-east-1". Defaults to "us-east-1".
	Region string `yaml:"region"`
	// Bucket is the S3 bucket name.
	Bucket string `yaml:"bucket"`
	// AccessKeyID is the AWS access key ID.
	AccessKeyID string `yaml:"access_key_id"`
	// SecretAccessKey is the AWS secret access key.
	SecretAccessKey string `yaml:"secret_access_key"`
	// SessionToken is an optional AWS session token.
	SessionToken string `yaml:"session_token"`
	// PathStyle forces path-style S3 URLs instead of virtual-hosted-style.
	PathStyle bool `yaml:"path_style"`
	// Prefix is prepended to all object keys.
	Prefix string `yaml:"prefix"`
}

// DefaultMaxFileSizeBytes defines the default file size allowed in transfers.
var DefaultMaxFileSizeBytes = FileSizeBytes(10485760) //nolint:mnd

func (c *MediaAPI) Defaults(opts DefaultOpts) {
	c.MaxFileSizeBytes = DefaultMaxFileSizeBytes
	c.MaxThumbnailGenerators = 10
	if opts.Generate {
		c.ThumbnailSizes = []ThumbnailSize{
			{
				Width:        32, //nolint:mnd
				Height:       32, //nolint:mnd
				ResizeMethod: "crop",
			},
			{
				Width:        96, //nolint:mnd
				Height:       96, //nolint:mnd
				ResizeMethod: "crop",
			},
			{
				Width:        640, //nolint:mnd
				Height:       480, //nolint:mnd
				ResizeMethod: "scale",
			},
		}
		if !opts.SingleDatabase {
			c.Database.ConnectionString = "file:mediaapi.db"
		}
		c.BasePath = "./media_store"
		c.Storage.Type = "local"
		c.Storage.Local.BasePath = c.BasePath
	}
}

// StorageType returns the configured storage backend, normalising an empty
// value to "local" for backwards compatibility.
func (c *MediaAPI) StorageType() string {
	if c.Storage.Type == "" {
		return "local"
	}
	return c.Storage.Type
}

func (c *MediaAPI) Verify(configErrs *ConfigErrors) {
	checkPositive(configErrs, "media_api.max_file_size_bytes", int64(c.MaxFileSizeBytes))
	checkPositive(configErrs, "media_api.max_thumbnail_generators", int64(c.MaxThumbnailGenerators))

	for i, size := range c.ThumbnailSizes {
		checkPositive(configErrs, fmt.Sprintf("media_api.thumbnail_sizes[%d].width", i), int64(size.Width))
		checkPositive(configErrs, fmt.Sprintf("media_api.thumbnail_sizes[%d].height", i), int64(size.Height))
	}

	switch c.StorageType() {
	case "local":
		basePath := string(c.Storage.Local.BasePath)
		if basePath == "" {
			basePath = string(c.BasePath)
		}
		checkNotEmpty(configErrs, "media_api.storage.local.base_path", basePath)
	case "s3":
		checkNotEmpty(configErrs, "media_api.storage.s3.bucket", c.Storage.S3.Bucket)
		// Region is optional and defaults to us-east-1 in NewS3FileStore.
	default:
		configErrs.Add(fmt.Sprintf("invalid media_api.storage.type: %q", c.Storage.Type))
	}

	if c.Matrix.DatabaseOptions.ConnectionString == "" {
		checkNotEmpty(configErrs, "media_api.database.connection_string", string(c.Database.ConnectionString))
	}
}
