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
	"strings"

	"codefloe.com/pat-s/zendrite/mediaapi/types"
)

// LocalFileStore stores media files on the local filesystem.
type LocalFileStore struct {
	basePath string
}

// NewLocalFileStore creates a local file store rooted at basePath.
func NewLocalFileStore(basePath string) (*LocalFileStore, error) {
	abs, err := filepath.Abs(basePath)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve base path: %w", err)
	}
	return &LocalFileStore{basePath: abs}, nil
}

// WriteTemp writes r to a temporary file under the store's base path and
// returns its SHA-256 hash, size, and the temporary directory path.
func (s *LocalFileStore) WriteTemp(_ context.Context, r io.Reader) (types.Base64Hash, types.FileSizeBytes, types.Path, func(), error) {
	tmpDir, err := s.createTempDir()
	if err != nil {
		return "", -1, "", nil, err
	}
	cleanup := func() { RemoveDir(tmpDir) }

	contentPath := filepath.Join(string(tmpDir), "content")
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
	return hash, types.FileSizeBytes(bytesWritten), tmpDir, cleanup, nil
}

// Store moves the temporary file identified by tmpPath to its final hash-based
// location. It returns true if a file with the same hash and size already exists.
func (s *LocalFileStore) Store(_ context.Context, tmpPath types.Path, hash types.Base64Hash, size types.FileSizeBytes) (bool, error) {
	defer RemoveDir(tmpPath)

	finalPath, err := s.pathFromHash(hash)
	if err != nil {
		return false, err
	}

	if stat, err := os.Stat(finalPath); err == nil {
		if stat.Size() == int64(size) {
			return true, nil
		}
		return false, fmt.Errorf("downloaded file with hash collision but different file size (%v)", finalPath)
	}

	src := filepath.Join(string(tmpPath), "content")
	dstDir := filepath.Dir(finalPath)
	if err := os.MkdirAll(dstDir, 0o770); err != nil { //nolint:mnd
		return false, fmt.Errorf("failed to make directory: %w", err)
	}
	if err := os.Rename(src, finalPath); err != nil {
		return false, fmt.Errorf("failed to move file to final destination (%v): %w", finalPath, err)
	}
	return false, nil
}

// Open returns a reader for the file identified by hash.
func (s *LocalFileStore) Open(_ context.Context, hash types.Base64Hash) (io.ReadCloser, types.FileSizeBytes, error) {
	path, err := s.pathFromHash(hash)
	if err != nil {
		return nil, 0, err
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, fmt.Errorf("failed to open file: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, fmt.Errorf("failed to stat file: %w", err)
	}
	return file, types.FileSizeBytes(stat.Size()), nil
}

// Stat returns the size of the file and whether it exists.
func (s *LocalFileStore) Stat(_ context.Context, hash types.Base64Hash) (types.FileSizeBytes, bool, error) {
	path, err := s.pathFromHash(hash)
	if err != nil {
		return 0, false, err
	}
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to stat file: %w", err)
	}
	return types.FileSizeBytes(stat.Size()), true, nil
}

// Delete removes the file identified by hash.
func (s *LocalFileStore) Delete(_ context.Context, hash types.Base64Hash) error {
	path, err := s.pathFromHash(hash)
	if err != nil {
		return err
	}
	return os.Remove(path)
}

// LocalPath returns the local filesystem path for the file identified by hash.
func (s *LocalFileStore) LocalPath(_ context.Context, hash types.Base64Hash) (string, func(), error) {
	path, err := s.pathFromHash(hash)
	if err != nil {
		return "", nil, err
	}
	return path, func() {}, nil
}

// OpenThumbnail returns a reader for the requested thumbnail.
func (s *LocalFileStore) OpenThumbnail(_ context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize) (io.ReadCloser, types.FileSizeBytes, error) {
	path, err := s.thumbnailPath(mediaHash, size)
	if err != nil {
		return nil, 0, err
	}
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, 0, ErrNotFound
		}
		return nil, 0, fmt.Errorf("failed to open thumbnail: %w", err)
	}
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, 0, fmt.Errorf("failed to stat thumbnail: %w", err)
	}
	return file, types.FileSizeBytes(stat.Size()), nil
}

// StatThumbnail returns the size and existence of the requested thumbnail.
func (s *LocalFileStore) StatThumbnail(_ context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize) (types.FileSizeBytes, bool, error) {
	path, err := s.thumbnailPath(mediaHash, size)
	if err != nil {
		return 0, false, err
	}
	stat, err := os.Stat(path)
	if os.IsNotExist(err) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, fmt.Errorf("failed to stat thumbnail: %w", err)
	}
	return types.FileSizeBytes(stat.Size()), true, nil
}

// StoreThumbnail stores a thumbnail from a local temporary file.
func (s *LocalFileStore) StoreThumbnail(_ context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize, tmpPath types.Path, fileSize types.FileSizeBytes) error {
	path, err := s.thumbnailPath(mediaHash, size)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o770); err != nil { //nolint:mnd
		return fmt.Errorf("failed to make thumbnail directory: %w", err)
	}

	src := filepath.Join(string(tmpPath), "content")
	if err := os.Rename(src, path); err != nil {
		return fmt.Errorf("failed to move thumbnail to final destination (%v): %w", path, err)
	}
	_ = fileSize
	return nil
}

// DeleteThumbnail removes the requested thumbnail.
func (s *LocalFileStore) DeleteThumbnail(_ context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize) error {
	path, err := s.thumbnailPath(mediaHash, size)
	if err != nil {
		return err
	}
	return os.Remove(path)
}

func (s *LocalFileStore) createTempDir() (types.Path, error) {
	baseTmpDir := filepath.Join(s.basePath, "tmp")
	if err := os.MkdirAll(baseTmpDir, 0o770); err != nil { //nolint:mnd
		return "", fmt.Errorf("failed to create base temp dir: %w", err)
	}
	tmpDir, err := os.MkdirTemp(baseTmpDir, "")
	if err != nil {
		return "", fmt.Errorf("failed to create temp dir: %w", err)
	}
	return types.Path(tmpDir), nil
}

func (s *LocalFileStore) pathFromHash(hash types.Base64Hash) (string, error) {
	if len(hash) < 3 { //nolint:mnd
		return "", fmt.Errorf("invalid hash (too short - min 3 characters): %q", hash)
	}
	if len(hash) > 255 { //nolint:mnd
		return "", fmt.Errorf("invalid hash (too long - max 255 characters): %q", hash)
	}

	path, err := filepath.Abs(filepath.Join(
		s.basePath,
		string(hash[0:1]),
		string(hash[1:2]),
		string(hash[2:]),
		"file",
	))
	if err != nil {
		return "", fmt.Errorf("unable to construct file path: %w", err)
	}
	if !strings.HasPrefix(path, s.basePath) {
		return "", fmt.Errorf("invalid file path (not within base path %v): %v", s.basePath, path)
	}
	return path, nil
}

func (s *LocalFileStore) thumbnailPath(mediaHash types.Base64Hash, size types.ThumbnailSize) (string, error) {
	mediaPath, err := s.pathFromHash(mediaHash)
	if err != nil {
		return "", err
	}
	return filepath.Join(
		filepath.Dir(mediaPath),
		fmt.Sprintf("thumbnail-%dx%d-%s", size.Width, size.Height, size.ResizeMethod),
	), nil
}

// RemoveDir removes a directory and ignores not-exist errors.
func RemoveDir(dir types.Path) {
	_ = os.RemoveAll(string(dir))
}
