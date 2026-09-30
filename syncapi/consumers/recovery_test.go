package consumers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/fulltext"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/test"
)

type recoveryDatabase struct {
	storage.Database
	pages   []map[int64]rstypes.HeaderedEvent
	cursors []int64
	err     error
	// failures fails this many reads before serving pages.
	failures int
}

func (d *recoveryDatabase) ReIndex(_ context.Context, _, after int64) (map[int64]rstypes.HeaderedEvent, error) {
	d.cursors = append(d.cursors, after)
	if d.err != nil {
		return nil, d.err
	}
	if d.failures > 0 {
		d.failures--
		return nil, errors.New("database unavailable")
	}
	if len(d.pages) == 0 {
		return nil, nil
	}
	page := d.pages[0]
	d.pages = d.pages[1:]
	return page, nil
}

type recoveryIndex struct {
	fulltext.Indexer
	elements []fulltext.IndexElement
	err      error
}

func (i *recoveryIndex) Index(elements ...fulltext.IndexElement) error {
	i.elements = append(i.elements, elements...)
	return i.err
}

func TestSearchRecovery(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	event := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "recover searchable content", "msgtype": "m.text"})
	for _, tc := range []struct {
		name            string
		dbErr, indexErr error
		canceled        bool
	}{
		{name: "success"}, {name: "database failure", dbErr: errors.New("database unavailable")}, {name: "index failure", indexErr: errors.New("disk full")}, {name: "ownership lost", canceled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := &recoveryDatabase{pages: []map[int64]rstypes.HeaderedEvent{{9: *event, 3: *event, 7: *event}}, err: tc.dbErr}
			index := &recoveryIndex{err: tc.indexErr}
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			if tc.canceled {
				cancel(context.Canceled)
			}
			err := RecoverSearch(ctx, db, index)
			if tc.dbErr != nil || tc.indexErr != nil || tc.canceled {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, []int64{0, 9}, db.cursors)
			require.Len(t, index.elements, 3)
			require.Equal(t, "recover searchable content", index.elements[0].Content)
		})
	}
}

func TestRecoveredEventsAreSearchable(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	event := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "recover searchable content", "msgtype": "m.text"})
	db := &recoveryDatabase{pages: []map[int64]rstypes.HeaderedEvent{{42: *event}}}
	index, err := fulltext.New(nil, config.Fulltext{InMemory: true, Language: "en"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	require.NoError(t, RecoverSearch(t.Context(), db, index))
	result, err := index.Search("searchable", []string{event.RoomID().String()}, nil, 10, 0, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Total)
	require.Equal(t, event.EventID(), result.Hits[0].ID)
}

func TestRecoveryRemovesEditedOriginalsAcrossBatches(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	original := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "originaltoken", "msgtype": "m.text"})
	edit := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "replacementtoken", "msgtype": "m.text", "m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": original.EventID()}})
	// Backfill can assign the original a later stream position than its edit.
	db := &recoveryDatabase{pages: []map[int64]rstypes.HeaderedEvent{{1: *edit}, {2: *original}}}
	index, err := fulltext.New(nil, config.Fulltext{InMemory: true, Language: "en"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	require.NoError(t, RecoverSearch(t.Context(), db, index))
	for _, tc := range []struct {
		term  string
		count uint64
	}{{"originaltoken", 0}, {"replacementtoken", 1}} {
		result, searchErr := index.Search(tc.term, []string{room.ID}, nil, 10, 0, false)
		require.NoError(t, searchErr)
		require.Equal(t, tc.count, result.Total)
	}
}

func TestRebuildSearchRetriesThenAdmitsSearches(t *testing.T) {
	retry := searchRebuildRetry
	searchRebuildRetry = time.Millisecond
	t.Cleanup(func() { searchRebuildRetry = retry })

	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	event := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "rebuilt after a failure", "msgtype": "m.text"})
	db := &recoveryDatabase{pages: []map[int64]rstypes.HeaderedEvent{{5: *event}}, failures: 2}
	fts, err := fulltext.New(nil, config.Fulltext{InMemory: true, Language: "en"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, fts.Close()) })
	recovering := fulltext.NewRecovering(fts)

	RebuildSearch(t.Context(), db, recovering)
	require.True(t, recovering.Recovered())
	require.Equal(t, []int64{0, 0, 0, 5}, db.cursors)
	result, err := recovering.Search("failure", []string{room.ID}, nil, 10, 0, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Total)
}

func TestRebuildSearchStopsWhenOwnershipIsLost(t *testing.T) {
	ctx, cancel := context.WithCancelCause(t.Context())
	cancel(errors.New("ownership lost"))
	recovering := fulltext.NewRecovering(&recoveryIndex{})
	RebuildSearch(ctx, &recoveryDatabase{err: errors.New("refused")}, recovering)
	require.False(t, recovering.Recovered())
}
