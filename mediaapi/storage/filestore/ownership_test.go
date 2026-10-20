package filestore

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/mediaapi/types"
)

type testOwner struct{ err error }

func (o *testOwner) Generation() int64 { return 1 }
func (o *testOwner) Identity() string  { return "candidate-a" }
func (o *testOwner) Valid() error      { return o.err }

func guardedStore(t *testing.T, owner *testOwner) FileStore {
	t.Helper()
	local, err := NewLocalFileStore(t.TempDir())
	require.NoError(t, err)
	return Guarded(local, owner)
}

func TestGuardedStoreRefusesMutationsWithoutOwnership(t *testing.T) {
	retired := errors.New("ownership generation is retired")
	guarded := guardedStore(t, &testOwner{err: retired})

	_, _, _, _, err := guarded.WriteTemp(t.Context(), strings.NewReader("content")) //nolint:dogsled // FileStore.WriteTemp reports hash, size, path and cleanup.
	require.ErrorIs(t, err, retired)
	_, err = guarded.Store(t.Context(), "tmp", "hash", 7)
	require.ErrorIs(t, err, retired)
	require.ErrorIs(t, guarded.StoreThumbnail(t.Context(), "hash", types.ThumbnailSize{Width: 32, Height: 32}, "tmp", 7), retired)
	require.ErrorIs(t, guarded.Delete(t.Context(), "hash"), retired)
	require.ErrorIs(t, guarded.DeleteThumbnail(t.Context(), "hash", types.ThumbnailSize{Width: 32, Height: 32}), retired)
}

func TestGuardedStoreAllowsReadsWithoutOwnership(t *testing.T) {
	owner := &testOwner{}
	guarded := guardedStore(t, owner)
	hash, size, tmpPath, cleanup, err := guarded.WriteTemp(t.Context(), strings.NewReader("content"))
	require.NoError(t, err)
	defer cleanup()
	_, err = guarded.Store(t.Context(), tmpPath, hash, size)
	require.NoError(t, err)

	// Serving media that already exists is not an authoritative effect, so a
	// draining candidate can still answer reads while it shuts down.
	owner.err = errors.New("ownership generation is retired")
	_, exists, err := guarded.Stat(t.Context(), hash)
	require.NoError(t, err)
	require.True(t, exists)
	reader, _, err := guarded.Open(t.Context(), hash)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
}
