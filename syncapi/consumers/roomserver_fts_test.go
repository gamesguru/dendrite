package consumers

import (
	"testing"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/fulltext"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
)

// Events without a target must not fail indexing, or the consumer NAKs them
// and the roomserver redelivers them forever.
func TestWriteFTSSkipsEventsWithoutTarget(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	index, err := fulltext.New(nil, config.Fulltext{InMemory: true, Language: "en"})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, index.Close()) })
	s := &OutputRoomEventConsumer{cfg: &config.SyncAPI{Fulltext: config.Fulltext{Enabled: true}}, fts: index}

	redaction := room.CreateEvent(t, alice, spec.MRoomRedaction, map[string]any{})
	require.Empty(t, redaction.Redacts())
	require.NoError(t, s.writeFTS(redaction, 1))

	edit := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "edited", "msgtype": "m.text", "m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": ""}})
	s.cfg.Matrix = &config.Global{HA: config.HA{Enabled: true}}
	require.NoError(t, s.writeFTS(edit, 2))
	result, err := index.Search("edited", []string{room.ID}, nil, 10, 0, false)
	require.NoError(t, err)
	require.EqualValues(t, 1, result.Total)
}
