package input

import (
	"context"
	"strings"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
)

func TestVerifyFetchedEventSignaturesMalformedContent(t *testing.T) {
	const eventID = "$malformed"
	eventJSON := []byte(`{
		"auth_events": [],
		"content": "not an object",
		"depth": 1,
		"hashes": {},
		"origin_server_ts": 1,
		"prev_events": [],
		"room_id": "!room:example.com",
		"sender": "@alice:example.com",
		"signatures": {},
		"type": "m.room.message"
	}`)

	event, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10).
		NewEventFromTrustedJSON(eventJSON, false)
	if err != nil {
		t.Fatalf("NewEventFromTrustedJSON: %s", err)
	}

	err = verifyFetchedEventSignatures(
		context.Background(),
		event,
		eventID,
		gomatrixserverlib.JSONVerifierSelf{},
		func(spec.RoomID, spec.SenderID) (*spec.UserID, error) {
			return spec.NewUserID("@alice:example.com", true)
		},
	)
	if err == nil {
		t.Fatal("expected malformed content to fail signature verification")
	}
	if !strings.Contains(err.Error(), eventID) {
		t.Fatalf("error %q does not identify expected event %q", err, eventID)
	}
	if !strings.Contains(err.Error(), "cannot unmarshal string") {
		t.Fatalf("error %q does not report malformed content", err)
	}
}
