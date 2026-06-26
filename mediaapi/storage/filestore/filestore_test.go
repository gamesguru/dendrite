// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package filestore

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"codefloe.com/pat-s/zendrite/mediaapi/types"
)

func TestLocalFileStore(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	store, err := NewLocalFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewLocalFileStore failed: %v", err)
	}

	content := "hello world"
	hash, size, tmpPath, cleanup, err := store.WriteTemp(ctx, strings.NewReader(content))
	if err != nil {
		t.Fatalf("WriteTemp failed: %v", err)
	}
	defer cleanup()

	if size != types.FileSizeBytes(len(content)) {
		t.Fatalf("expected size %d, got %d", len(content), size)
	}

	duplicate, err := store.Store(ctx, tmpPath, hash, size)
	if err != nil {
		t.Fatalf("Store failed: %v", err)
	}
	if duplicate {
		t.Fatalf("did not expect duplicate on first store")
	}

	// Storing the same file again should report a duplicate.
	_, _, tmpPath2, cleanup2, err := store.WriteTemp(ctx, strings.NewReader(content))
	if err != nil {
		t.Fatalf("WriteTemp failed: %v", err)
	}
	defer cleanup2()
	duplicate, err = store.Store(ctx, tmpPath2, hash, size)
	if err != nil {
		t.Fatalf("Store duplicate failed: %v", err)
	}
	if !duplicate {
		t.Fatalf("expected duplicate on second store")
	}

	gotSize, exists, err := store.Stat(ctx, hash)
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if !exists {
		t.Fatalf("expected file to exist")
	}
	if gotSize != size {
		t.Fatalf("expected size %d, got %d", size, gotSize)
	}

	reader, gotSize2, err := store.Open(ctx, hash)
	if err != nil {
		t.Fatalf("Open failed: %v", err)
	}
	defer reader.Close()
	if gotSize2 != size {
		t.Fatalf("expected size %d, got %d", size, gotSize2)
	}
	gotContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(gotContent) != content {
		t.Fatalf("expected content %q, got %q", content, string(gotContent))
	}

	localPath, cleanupLocal, err := store.LocalPath(ctx, hash)
	if err != nil {
		t.Fatalf("LocalPath failed: %v", err)
	}
	defer cleanupLocal()
	if _, err := os.Stat(localPath); err != nil {
		t.Fatalf("local path does not exist: %v", err)
	}
}

func TestLocalFileStore_NotFound(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	store, err := NewLocalFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewLocalFileStore failed: %v", err)
	}

	_, exists, err := store.Stat(ctx, types.Base64Hash("nonexistent"))
	if err != nil {
		t.Fatalf("Stat failed: %v", err)
	}
	if exists {
		t.Fatalf("expected file to not exist")
	}

	_, _, err = store.Open(ctx, types.Base64Hash("nonexistent"))
	if err == nil {
		t.Fatalf("expected error for missing file")
	}
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}
}

func TestLocalFileStore_Thumbnail(t *testing.T) {
	ctx := context.Background()
	tmpDir := t.TempDir()

	store, err := NewLocalFileStore(tmpDir)
	if err != nil {
		t.Fatalf("NewLocalFileStore failed: %v", err)
	}

	mediaContent := "media"
	mediaHash, _, mediaTmp, cleanup, err := store.WriteTemp(ctx, strings.NewReader(mediaContent))
	if err != nil {
		t.Fatalf("WriteTemp failed: %v", err)
	}
	defer cleanup()
	if _, err := store.Store(ctx, mediaTmp, mediaHash, types.FileSizeBytes(len(mediaContent))); err != nil {
		t.Fatalf("Store failed: %v", err)
	}

	thumbContent := "thumbnail"
	thumbSize := types.ThumbnailSize{Width: 32, Height: 32, ResizeMethod: types.Crop}

	thumbTmp := t.TempDir()
	thumbPath := filepath.Join(thumbTmp, "content")
	if err := os.WriteFile(thumbPath, []byte(thumbContent), 0o600); err != nil {
		t.Fatalf("WriteFile failed: %v", err)
	}

	if err := store.StoreThumbnail(ctx, mediaHash, thumbSize, types.Path(thumbTmp), types.FileSizeBytes(len(thumbContent))); err != nil {
		t.Fatalf("StoreThumbnail failed: %v", err)
	}

	gotSize, exists, err := store.StatThumbnail(ctx, mediaHash, thumbSize)
	if err != nil {
		t.Fatalf("StatThumbnail failed: %v", err)
	}
	if !exists {
		t.Fatalf("expected thumbnail to exist")
	}
	if gotSize != types.FileSizeBytes(len(thumbContent)) {
		t.Fatalf("expected thumbnail size %d, got %d", len(thumbContent), gotSize)
	}

	reader, gotSize2, err := store.OpenThumbnail(ctx, mediaHash, thumbSize)
	if err != nil {
		t.Fatalf("OpenThumbnail failed: %v", err)
	}
	defer reader.Close()
	if gotSize2 != types.FileSizeBytes(len(thumbContent)) {
		t.Fatalf("expected thumbnail size %d, got %d", len(thumbContent), gotSize2)
	}
	gotContent, err := io.ReadAll(reader)
	if err != nil {
		t.Fatalf("ReadAll failed: %v", err)
	}
	if string(gotContent) != thumbContent {
		t.Fatalf("expected thumbnail content %q, got %q", thumbContent, string(gotContent))
	}
}
