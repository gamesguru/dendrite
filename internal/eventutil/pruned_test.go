package eventutil

import (
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/sjson"
)

func buildSignedMessage(t *testing.T) []byte {
	t.Helper()
	_, priv, err := ed25519.GenerateKey(nil)
	require.NoError(t, err)
	verImpl, err := gomatrixserverlib.GetRoomVersion(gomatrixserverlib.RoomVersionV10)
	require.NoError(t, err)
	b := verImpl.NewEventBuilderFromProtoEvent(&gomatrixserverlib.ProtoEvent{
		SenderID: "@u:example.org",
		RoomID:   "!r:example.org",
		Type:     "m.room.message",
		Depth:    1,
	})
	require.NoError(t, b.SetContent(map[string]any{"body": "hello"}))
	ev, err := b.Build(time.Now(), "example.org", "ed25519:1", priv)
	require.NoError(t, err)
	return ev.JSON()
}

func parseV10(t *testing.T, raw []byte) gomatrixserverlib.PDU {
	t.Helper()
	verImpl, err := gomatrixserverlib.GetRoomVersion(gomatrixserverlib.RoomVersionV10)
	require.NoError(t, err)
	ev, err := verImpl.NewEventFromUntrustedJSON(raw)
	require.NoError(t, err)
	return ev
}

func TestLooksHashPruned(t *testing.T) {
	raw := buildSignedMessage(t)

	t.Run("full event", func(t *testing.T) {
		require.False(t, LooksHashPruned(raw, parseV10(t, raw)))
	})

	tampered, err := sjson.SetBytes(raw, "content.body", "changed")
	require.NoError(t, err)

	t.Run("content hash failure", func(t *testing.T) {
		ev := parseV10(t, tampered)
		require.True(t, ev.Redacted())
		require.True(t, LooksHashPruned(tampered, ev))
	})

	t.Run("genuine redaction records its cause", func(t *testing.T) {
		withCause, err := sjson.SetBytes(tampered, "unsigned.redacted_because", map[string]any{"type": "m.room.redaction"})
		require.NoError(t, err)
		require.False(t, LooksHashPruned(withCause, parseV10(t, withCause)))
	})

	t.Run("nil", func(t *testing.T) {
		require.False(t, LooksHashPruned(nil, nil))
	})
}
