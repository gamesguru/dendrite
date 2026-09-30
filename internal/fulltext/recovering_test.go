package fulltext_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/fulltext"
)

func searchable(eventID, body string) fulltext.IndexElement {
	e := fulltext.IndexElement{EventID: eventID, RoomID: "!room:test", Content: body}
	e.SetContentType("m.room.message")
	return e
}

func TestRecoveringRefusesSearchesUntilFinished(t *testing.T) {
	fts, _ := mustOpenIndex(t, "")
	recovering := fulltext.NewRecovering(fts)
	require.NoError(t, recovering.Index(searchable("$live", "livetoken")))

	_, err := recovering.Search("livetoken", []string{"!room:test"}, nil, 10, 0, false)
	require.ErrorIs(t, err, fulltext.ErrRecovering)
	require.False(t, recovering.Recovered())

	require.NoError(t, recovering.Finish())
	require.True(t, recovering.Recovered())
	result, err := recovering.Search("livetoken", []string{"!room:test"}, nil, 10, 0, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Total)
}

// A rebuild can read an event before a consumer applies its redaction, and
// write it after the consumer removed it from the index.
func TestRecoveringReplaysDeletionsTheRebuildUndid(t *testing.T) {
	fts, _ := mustOpenIndex(t, "")
	recovering := fulltext.NewRecovering(fts)

	require.NoError(t, recovering.Delete("$redacted"))
	require.NoError(t, fts.Index(searchable("$redacted", "secrettoken"), searchable("$kept", "secrettoken")))

	require.NoError(t, recovering.Finish())
	result, err := recovering.Search("secrettoken", []string{"!room:test"}, nil, 10, 0, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Total)
	require.Equal(t, "$kept", result.Hits[0].ID)

	// Deletions after the rebuild apply directly, as before.
	require.NoError(t, recovering.Delete("$kept"))
	result, err = recovering.Search("secrettoken", []string{"!room:test"}, nil, 10, 0, false)
	require.NoError(t, err)
	require.EqualValues(t, 0, result.Total)
}
