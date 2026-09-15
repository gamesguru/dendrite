package filestore

import (
	"context"
	"fmt"
	"io"

	"codefloe.com/pat-s/zendrite/internal/ownership"
	"codefloe.com/pat-s/zendrite/mediaapi/types"
)

// Guarded binds a file store's mutable operations to one authorized
// activation. Media has no server-side ownership check of its own, so this is
// the boundary a retired owner's late thumbnail replacement, deletion, or
// purge must fail at.
//
// Reads stay open. Serving media that already exists is not an authoritative
// effect, and a draining candidate still answers in-flight downloads.
func Guarded(store FileStore, owner ownership.Owner) FileStore {
	return &guardedFileStore{FileStore: store, owner: owner}
}

type guardedFileStore struct {
	FileStore
	owner ownership.Owner
}

func (g *guardedFileStore) owned(effect string) error {
	if g.owner == nil {
		return fmt.Errorf("%w: media %s refused: ownership has not been established", ownership.ErrDispatchRefused, effect)
	}
	if err := g.owner.Valid(); err != nil {
		return fmt.Errorf("%w: media %s refused: %w", ownership.ErrDispatchRefused, effect, err)
	}
	return nil
}

func (g *guardedFileStore) WriteTemp(ctx context.Context, r io.Reader) (types.Base64Hash, types.FileSizeBytes, types.Path, func(), error) {
	if err := g.owned("upload"); err != nil {
		return "", -1, "", func() {}, err
	}
	return g.FileStore.WriteTemp(ctx, r)
}

func (g *guardedFileStore) Store(ctx context.Context, tmpPath types.Path, hash types.Base64Hash, size types.FileSizeBytes) (bool, error) {
	if err := g.owned("publication"); err != nil {
		return false, err
	}
	return g.FileStore.Store(ctx, tmpPath, hash, size)
}

func (g *guardedFileStore) StoreThumbnail(ctx context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize, tmpPath types.Path, fileSize types.FileSizeBytes) error {
	if err := g.owned("thumbnail replacement"); err != nil {
		return err
	}
	return g.FileStore.StoreThumbnail(ctx, mediaHash, size, tmpPath, fileSize)
}

func (g *guardedFileStore) Delete(ctx context.Context, hash types.Base64Hash) error {
	if err := g.owned("deletion"); err != nil {
		return err
	}
	return g.FileStore.Delete(ctx, hash)
}

func (g *guardedFileStore) DeleteThumbnail(ctx context.Context, mediaHash types.Base64Hash, size types.ThumbnailSize) error {
	if err := g.owned("thumbnail deletion"); err != nil {
		return err
	}
	return g.FileStore.DeleteThumbnail(ctx, mediaHash, size)
}
