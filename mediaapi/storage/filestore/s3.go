// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package filestore

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"

	"codefloe.com/pat-s/zendrite/mediaapi/types"
)

// S3FileStore stores media files in an S3-compatible object storage bucket.
type S3FileStore struct {
	client       *s3.Client
	bucket       string
	prefix       string
	tempBasePath string
}

// S3Config contains the configuration for an S3 file store.
type S3Config struct {
	Endpoint        string
	Region          string
	Bucket          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	PathStyle       bool
	Prefix          string
}

// NewS3FileStore creates an S3-backed file store.
func NewS3FileStore(ctx context.Context, cfg S3Config) (*S3FileStore, error) {
	if cfg.Bucket == "" {
		return nil, fmt.Errorf("s3 bucket is required")
	}
	if cfg.Region == "" {
		cfg.Region = "us-east-1"
	}

	opts := []func(*awsconfig.LoadOptions) error{
		awsconfig.WithRegion(cfg.Region),
	}
	if cfg.AccessKeyID != "" && cfg.SecretAccessKey != "" {
		opts = append(opts, awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(cfg.AccessKeyID, cfg.SecretAccessKey, cfg.SessionToken),
		))
	}

	awsCfg, err := awsconfig.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to load aws config: %w", err)
	}

	s3Opts := []func(*s3.Options){
		func(o *s3.Options) {
			if cfg.Endpoint != "" {
				o.BaseEndpoint = aws.String(cfg.Endpoint)
			}
			o.UsePathStyle = cfg.PathStyle
		},
	}
	client := s3.NewFromConfig(awsCfg, s3Opts...)

	return &S3FileStore{
		client:       client,
		bucket:       cfg.Bucket,
		prefix:       cfg.Prefix,
		tempBasePath: os.TempDir(),
	}, nil
}

// WriteTemp writes r to a temporary file and returns its hash, size, and path.
func (s *S3FileStore) WriteTemp(_ context.Context, r io.Reader) (types.Base64Hash, types.FileSizeBytes, types.Path, func(), error) {
	tmpDir, err := os.MkdirTemp(s.tempBasePath, "zendrite-media-")
	if err != nil {
		return "", -1, "", nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	cleanup := func() { RemoveDir(types.Path(tmpDir)) }

	contentPath := filepath.Join(tmpDir, "content")
	file, err := os.Create(contentPath)
	if err != nil {
		cleanup()
		return "", -1, "", nil, fmt.Errorf("failed to create temp file: %w", err)
	}

	writer := bufio.NewWriter(file)
	hasher := sha256.New()
	teeReader := io.TeeReader(r, hasher)
	bytesWritten, err := io.Copy(writer, teeReader)
	flushErr := writer.Flush()
	closeErr := file.Close()

	if err != nil && !errors.Is(err, io.EOF) {
		cleanup()
		return "", -1, "", nil, fmt.Errorf("failed to write temp file: %w", err)
	}
	if flushErr != nil {
		cleanup()
		return "", -1, "", nil, fmt.Errorf("failed to flush temp file: %w", flushErr)
	}
	if closeErr != nil {
		cleanup()
		return "", -1, "", nil, fmt.Errorf("failed to close temp file: %w", closeErr)
	}

	hash := types.Base64Hash(base64.RawURLEncoding.EncodeToString(hasher.Sum(nil)))
	return hash, types.FileSizeBytes(bytesWritten), types.Path(tmpDir), cleanup, nil
}

// Store uploads the temporary file to S3 keyed by hash. It returns true if an
// object with the same hash and size already exists.
func (s *S3FileStore) Store(ctx context.Context, tmpPath types.Path, hash types.Base64Hash, size types.FileSizeBytes) (bool, error) {
	defer RemoveDir(tmpPath)

	key, err := s.objectKey(hash)
	if err != nil {
		return false, err
	}
	existingSize, exists, err := s.headObject(ctx, key)
	if err != nil {
		return false, err
	}
	if exists {
		if existingSize == size {
			return true, nil
		}
		return false, fmt.Errorf("uploaded file with hash collision but different file size (%v)", key)
	}

	contentPath := filepath.Join(string(tmpPath), "content")
	file, err := os.Open(contentPath)
	if err != nil {
		return false, fmt.Errorf("failed to open temp file: %w", err)
	}
	defer file.Close()

	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          file,
		ContentLength: aws.Int64(int64(size)),
	})
	if err != nil {
		return false, fmt.Errorf("failed to put object: %w", err)
	}
	return false, nil
}

// Open returns a reader for the object identified by hash.
func (s *S3FileStore) Open(ctx context.Context, hash types.Base64Hash) (io.ReadCloser, types.FileSizeBytes, error) {
	key, err := s.objectKey(hash)
	if err != nil {
		return nil, 0, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var notFound *s3types.NoSuchKey
		if errors.As(err, &notFound) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, fmt.Errorf("failed to get object: %w", err)
	}
	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return out.Body, types.FileSizeBytes(size), nil
}

// Stat returns the size of the object and whether it exists.
func (s *S3FileStore) Stat(ctx context.Context, hash types.Base64Hash) (types.FileSizeBytes, bool, error) {
	key, err := s.objectKey(hash)
	if err != nil {
		return 0, false, err
	}
	return s.headObject(ctx, key)
}

// Delete removes the object identified by hash.
func (s *S3FileStore) Delete(ctx context.Context, hash types.Base64Hash) error {
	key, err := s.objectKey(hash)
	if err != nil {
		return err
	}
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete object: %w", err)
	}
	return nil
}

// LocalPath downloads the object to a temporary file and returns the local path.
func (s *S3FileStore) LocalPath(ctx context.Context, hash types.Base64Hash) (string, func(), error) {
	reader, _, err := s.Open(ctx, hash)
	if err != nil {
		return "", nil, err
	}
	defer reader.Close()

	tmpDir, err := os.MkdirTemp(s.tempBasePath, "zendrite-media-local-")
	if err != nil {
		return "", nil, fmt.Errorf("failed to create temp dir: %w", err)
	}
	cleanup := func() { RemoveDir(types.Path(tmpDir)) }

	contentPath := filepath.Join(tmpDir, "content")
	file, err := os.Create(contentPath)
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("failed to create local temp file: %w", err)
	}

	_, err = io.Copy(file, reader)
	closeErr := file.Close()
	if err != nil {
		cleanup()
		return "", nil, fmt.Errorf("failed to download object to temp file: %w", err)
	}
	if closeErr != nil {
		cleanup()
		return "", nil, fmt.Errorf("failed to close local temp file: %w", closeErr)
	}

	return contentPath, cleanup, nil
}

// OpenThumbnail returns a reader for the requested thumbnail.
func (s *S3FileStore) OpenThumbnail(ctx context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize) (io.ReadCloser, types.FileSizeBytes, error) {
	key, err := s.thumbnailKey(mediaHash, size)
	if err != nil {
		return nil, 0, err
	}
	out, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var notFound *s3types.NoSuchKey
		if errors.As(err, &notFound) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, fmt.Errorf("failed to get thumbnail object: %w", err)
	}
	sizeBytes := int64(0)
	if out.ContentLength != nil {
		sizeBytes = *out.ContentLength
	}
	return out.Body, types.FileSizeBytes(sizeBytes), nil
}

// StatThumbnail returns the size and existence of the requested thumbnail.
func (s *S3FileStore) StatThumbnail(ctx context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize) (types.FileSizeBytes, bool, error) {
	key, err := s.thumbnailKey(mediaHash, size)
	if err != nil {
		return 0, false, err
	}
	return s.headObject(ctx, key)
}

// StoreThumbnail uploads a thumbnail from a local temporary file.
func (s *S3FileStore) StoreThumbnail(ctx context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize, tmpPath types.Path, fileSize types.FileSizeBytes) error {
	contentPath := filepath.Join(string(tmpPath), "content")
	file, err := os.Open(contentPath)
	if err != nil {
		return fmt.Errorf("failed to open thumbnail temp file: %w", err)
	}
	defer file.Close()

	key, err := s.thumbnailKey(mediaHash, size)
	if err != nil {
		return err
	}
	_, err = s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          file,
		ContentLength: aws.Int64(int64(fileSize)),
	})
	if err != nil {
		return fmt.Errorf("failed to put thumbnail object: %w", err)
	}
	return nil
}

// DeleteThumbnail removes the requested thumbnail.
func (s *S3FileStore) DeleteThumbnail(ctx context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize) error {
	key, err := s.thumbnailKey(mediaHash, size)
	if err != nil {
		return err
	}
	_, err = s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to delete thumbnail object: %w", err)
	}
	return nil
}

func (s *S3FileStore) headObject(ctx context.Context, key string) (types.FileSizeBytes, bool, error) {
	out, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		var notFound *s3types.NotFound
		if errors.As(err, &notFound) {
			return 0, false, nil
		}
		return 0, false, fmt.Errorf("failed to head object: %w", err)
	}
	size := int64(0)
	if out.ContentLength != nil {
		size = *out.ContentLength
	}
	return types.FileSizeBytes(size), true, nil
}

func (s *S3FileStore) objectKey(hash types.Base64Hash) (string, error) {
	if len(hash) < 3 { //nolint:mnd
		return "", fmt.Errorf("invalid hash (too short - min 3 characters): %q", hash)
	}
	key := fmt.Sprintf("media/%s/%s/%s/file", string(hash[0:1]), string(hash[1:2]), string(hash[2:]))
	if s.prefix != "" {
		key = s.prefix + "/" + key
	}
	return key, nil
}

func (s *S3FileStore) thumbnailKey(mediaHash types.Base64Hash, size types.ThumbnailSize) (string, error) {
	if len(mediaHash) < 3 { //nolint:mnd
		return "", fmt.Errorf("invalid hash (too short - min 3 characters): %q", mediaHash)
	}
	key := fmt.Sprintf("media/%s/%s/%s/thumbnail-%dx%d-%s",
		string(mediaHash[0:1]), string(mediaHash[1:2]), string(mediaHash[2:]),
		size.Width, size.Height, size.ResizeMethod)
	if s.prefix != "" {
		key = s.prefix + "/" + key
	}
	return key, nil
}
