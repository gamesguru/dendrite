package storage_test

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/mediaapi/storage"
	"codefloe.com/pat-s/zendrite/mediaapi/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

func mustCreateDatabase(t *testing.T, dbType test.DBType) (storage.Database, func()) {
	connStr, close := test.PrepareDBConnectionString(t, dbType)
	cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	db, err := storage.NewMediaAPIDatasource(cm, &config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	})
	if err != nil {
		t.Fatalf("NewSyncServerDatasource returned %s", err)
	}
	return db, close
}

func TestMediaRepository(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, close := mustCreateDatabase(t, dbType)
		defer close()
		ctx := context.Background()
		t.Run("can insert media & query media", func(t *testing.T) {
			metadata := &types.MediaMetadata{
				MediaID:       "testing",
				Origin:        "localhost",
				ContentType:   "image/png",
				FileSizeBytes: 10,
				UploadName:    "upload test",
				Base64Hash:    "dGVzdGluZw==",
				UserID:        "@alice:localhost",
			}
			if err := db.StoreMediaMetadata(ctx, metadata); err != nil {
				t.Fatalf("unable to store media metadata: %v", err)
			}
			// query by media id
			gotMetadata, err := db.GetMediaMetadata(ctx, metadata.MediaID, metadata.Origin)
			if err != nil {
				t.Fatalf("unable to query media metadata: %v", err)
			}
			if !reflect.DeepEqual(metadata, gotMetadata) {
				t.Fatalf("expected metadata %+v, got %v", metadata, gotMetadata)
			}
			// query by media hash
			gotMetadata, err = db.GetMediaMetadataByHash(ctx, metadata.Base64Hash, metadata.Origin)
			if err != nil {
				t.Fatalf("unable to query media metadata by hash: %v", err)
			}
			if !reflect.DeepEqual(metadata, gotMetadata) {
				t.Fatalf("expected metadata %+v, got %v", metadata, gotMetadata)
			}
		})
	})
}

func TestThumbnailsStorage(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, close := mustCreateDatabase(t, dbType)
		defer close()
		ctx := context.Background()
		t.Run("can insert thumbnails & query media", func(t *testing.T) {
			thumbnails := []*types.ThumbnailMetadata{
				{
					MediaMetadata: &types.MediaMetadata{
						MediaID:       "testing",
						Origin:        "localhost",
						ContentType:   "image/png",
						FileSizeBytes: 6,
					},
					ThumbnailSize: types.ThumbnailSize{
						Width:        5,
						Height:       5,
						ResizeMethod: types.Crop,
					},
				},
				{
					MediaMetadata: &types.MediaMetadata{
						MediaID:       "testing",
						Origin:        "localhost",
						ContentType:   "image/png",
						FileSizeBytes: 7,
					},
					ThumbnailSize: types.ThumbnailSize{
						Width:        1,
						Height:       1,
						ResizeMethod: types.Scale,
					},
				},
			}
			for i := range thumbnails {
				if err := db.StoreThumbnail(ctx, thumbnails[i]); err != nil {
					t.Fatalf("unable to store thumbnail metadata: %v", err)
				}
			}
			// query by single thumbnail
			gotMetadata, err := db.GetThumbnail(ctx,
				thumbnails[0].MediaMetadata.MediaID,
				thumbnails[0].MediaMetadata.Origin,
				thumbnails[0].ThumbnailSize.Width, thumbnails[0].ThumbnailSize.Height,
				thumbnails[0].ThumbnailSize.ResizeMethod,
			)
			if err != nil {
				t.Fatalf("unable to query thumbnail metadata: %v", err)
			}
			if !reflect.DeepEqual(thumbnails[0].MediaMetadata, gotMetadata.MediaMetadata) {
				t.Fatalf("expected metadata %+v, got %+v", thumbnails[0].MediaMetadata, gotMetadata.MediaMetadata)
			}
			if !reflect.DeepEqual(thumbnails[0].ThumbnailSize, gotMetadata.ThumbnailSize) {
				t.Fatalf("expected metadata %+v, got %+v", thumbnails[0].MediaMetadata, gotMetadata.MediaMetadata)
			}
			// query by all thumbnails
			gotMediadatas, err := db.GetThumbnails(ctx, thumbnails[0].MediaMetadata.MediaID, thumbnails[0].MediaMetadata.Origin)
			if err != nil {
				t.Fatalf("unable to query media metadata by hash: %v", err)
			}
			if len(gotMediadatas) != len(thumbnails) {
				t.Fatalf("expected %d stored thumbnail metadata, got %d", len(thumbnails), len(gotMediadatas))
			}
			for i := range gotMediadatas {
				// metadata may be returned in a different order than it was stored, perform a search
				metaDataMatches := func() bool {
					for _, t := range thumbnails {
						if reflect.DeepEqual(t.MediaMetadata, gotMediadatas[i].MediaMetadata) && reflect.DeepEqual(t.ThumbnailSize, gotMediadatas[i].ThumbnailSize) {
							return true
						}
					}
					return false
				}

				if !metaDataMatches() {
					t.Fatalf("expected metadata %+v, got %+v", thumbnails[i].MediaMetadata, gotMediadatas[i].MediaMetadata)
				}
			}
		})
	})
}

func TestPendingMedia(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, close := mustCreateDatabase(t, dbType)
		defer close()
		ctx := context.Background()
		now := spec.AsTimestamp(time.Now())
		alice := types.MatrixUserID("@alice:localhost")
		pending := func(id types.MediaID, expiresAt spec.Timestamp) *types.MediaMetadata {
			return &types.MediaMetadata{
				MediaID:         id,
				Origin:          "localhost",
				UserID:          alice,
				UnusedExpiresAt: expiresAt,
			}
		}

		// The per-user limit only counts unexpired pending media.
		require.NoError(t, db.StorePendingMedia(ctx, pending("expired", now-1000), 2))
		require.NoError(t, db.StorePendingMedia(ctx, pending("first", now+60_000), 2))
		require.NoError(t, db.StorePendingMedia(ctx, pending("second", now+60_000), 2))
		require.ErrorIs(t, db.StorePendingMedia(ctx, pending("third", now+60_000), 2), types.ErrTooManyPendingUploads)
		require.NoError(t, db.StorePendingMedia(ctx, &types.MediaMetadata{
			MediaID: "bob", Origin: "localhost", UserID: "@bob:localhost", UnusedExpiresAt: now + 60_000,
		}, 2), "the limit is per user")

		got, err := db.GetMediaMetadata(ctx, "first", "localhost")
		require.NoError(t, err)
		require.True(t, got.IsPending())
		require.Equal(t, now+60_000, got.UnusedExpiresAt)

		// Pending media must never be matched by hash.
		withHash := pending("hashed", now+60_000)
		withHash.UserID = "@carol:localhost"
		withHash.Base64Hash = "cGVuZGluZw"
		require.NoError(t, db.StorePendingMedia(ctx, withHash, 2))
		byHash, err := db.GetMediaMetadataByHash(ctx, "cGVuZGluZw", "localhost")
		require.NoError(t, err)
		require.Nil(t, byHash)

		content := &types.MediaMetadata{
			MediaID:       "first",
			Origin:        "localhost",
			ContentType:   "text/plain",
			FileSizeBytes: 5,
			UploadName:    "hello.txt",
			Base64Hash:    "aGVsbG8",
			UserID:        "@mallory:localhost",
		}
		updated, err := db.CompletePendingMedia(ctx, content)
		require.NoError(t, err)
		require.False(t, updated, "another user must not complete the upload")

		content.UserID = alice
		updated, err = db.CompletePendingMedia(ctx, content)
		require.NoError(t, err)
		require.True(t, updated)
		updated, err = db.CompletePendingMedia(ctx, content)
		require.NoError(t, err)
		require.False(t, updated, "uploaded media must not be overwritten")

		got, err = db.GetMediaMetadata(ctx, "first", "localhost")
		require.NoError(t, err)
		require.False(t, got.IsPending())
		require.Equal(t, content.Base64Hash, got.Base64Hash)
		require.Equal(t, content.ContentType, got.ContentType)
		require.Equal(t, content.UploadName, got.UploadName)
		require.Equal(t, content.FileSizeBytes, got.FileSizeBytes)
		byHash, err = db.GetMediaMetadataByHash(ctx, content.Base64Hash, "localhost")
		require.NoError(t, err)
		require.NotNil(t, byHash)
		require.Equal(t, types.MediaID("first"), byHash.MediaID)

		// An expired reservation can't be completed and is swept.
		expiredContent := *content
		expiredContent.MediaID = "expired"
		updated, err = db.CompletePendingMedia(ctx, &expiredContent)
		require.NoError(t, err)
		require.False(t, updated)

		deleted, err := db.DeleteExpiredPendingMedia(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(1), deleted)
		deleted, err = db.DeleteExpiredPendingMedia(ctx)
		require.NoError(t, err)
		require.Equal(t, int64(0), deleted, "the sweep must be idempotent")
		got, err = db.GetMediaMetadata(ctx, "expired", "localhost")
		require.NoError(t, err)
		require.Nil(t, got)
		for _, id := range []types.MediaID{"first", "second", "bob", "hashed"} {
			got, err = db.GetMediaMetadata(ctx, id, "localhost")
			require.NoError(t, err)
			require.NotNil(t, got, "%s must survive the sweep", id)
		}
	})
}

// TestPendingMediaMigration checks that a media repository created before
// asynchronous uploads gains the unused_expires_ts column, and that existing
// media is treated as uploaded.
func TestPendingMediaMigration(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		connStr, close := test.PrepareDBConnectionString(t, dbType)
		defer close()
		opts := &config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		rawDB, _, err := cm.Connection(opts)
		require.NoError(t, err)
		_, err = rawDB.Exec(`CREATE TABLE mediaapi_media_repository (
    media_id TEXT NOT NULL,
    media_origin TEXT NOT NULL,
    content_type TEXT NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    creation_ts BIGINT NOT NULL,
    upload_name TEXT NOT NULL,
    base64hash TEXT NOT NULL,
    user_id TEXT NOT NULL
);
CREATE UNIQUE INDEX mediaapi_media_repository_index ON mediaapi_media_repository (media_id, media_origin);
INSERT INTO mediaapi_media_repository VALUES ('old', 'localhost', 'text/plain', 3, 1, 'old.txt', 'b2xk', '@alice:localhost');`)
		require.NoError(t, err)

		db, err := storage.NewMediaAPIDatasource(cm, opts)
		require.NoError(t, err)
		ctx := context.Background()
		got, err := db.GetMediaMetadata(ctx, "old", "localhost")
		require.NoError(t, err)
		require.NotNil(t, got)
		require.False(t, got.IsPending())
		byHash, err := db.GetMediaMetadataByHash(ctx, "b2xk", "localhost")
		require.NoError(t, err)
		require.NotNil(t, byHash)
		require.NoError(t, db.StorePendingMedia(ctx, &types.MediaMetadata{
			MediaID: "new", Origin: "localhost", UserID: "@alice:localhost",
			UnusedExpiresAt: spec.AsTimestamp(time.Now().Add(time.Hour)),
		}, 1))
	})
}
