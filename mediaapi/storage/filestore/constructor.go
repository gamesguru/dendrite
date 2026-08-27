// Copyright 2026 New Vector Ltd.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package filestore

import (
	"context"
	"fmt"

	"codefloe.com/pat-s/zendrite/setup/config"
)

// NewFileStore creates a FileStore from the MediaAPI configuration.
func NewFileStore(ctx context.Context, cfg *config.MediaAPI) (FileStore, error) {
	switch cfg.StorageType() {
	case "local":
		basePath := string(cfg.Storage.Local.BasePath)
		if basePath == "" {
			basePath = string(cfg.AbsBasePath)
		}
		return NewLocalFileStore(basePath)
	case "s3":
		s3Cfg := S3Config{
			Endpoint:        cfg.Storage.S3.Endpoint,
			Region:          cfg.Storage.S3.Region,
			Bucket:          cfg.Storage.S3.Bucket,
			AccessKeyID:     cfg.Storage.S3.AccessKeyID,
			SecretAccessKey: cfg.Storage.S3.SecretAccessKey,
			SessionToken:    cfg.Storage.S3.SessionToken,
			PathStyle:       cfg.Storage.S3.PathStyle,
			Prefix:          cfg.Storage.S3.Prefix,
		}
		return NewS3FileStore(ctx, s3Cfg)
	default:
		return nil, fmt.Errorf("unknown media storage type: %q", cfg.Storage.Type)
	}
}
