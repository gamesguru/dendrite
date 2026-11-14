//go:build rezzy && linux && cgo

package eventhash

import (
	"strings"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
)

// With rezzy installed, a real event keeps its content and ID.
func TestInstalledHooksKeepRealEvent(t *testing.T) {
	if err := Install(); err != nil {
		t.Fatal(err)
	}
	event := realV12Event(t)
	if event.EventID() != v12EventID {
		t.Fatalf("event ID = %s, want %s", event.EventID(), v12EventID)
	}
	if event.Redacted() {
		t.Fatal("real event was redacted")
	}
}

// A content hash mismatch redacts the event under rezzy's verdict too.
func TestInstalledHooksRedactTamperedEvent(t *testing.T) {
	if err := Install(); err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(v12Event, "A nice sunny day", "A nasty rainy day", 1)
	impl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV12)
	event, err := impl.NewEventFromUntrustedJSON([]byte(tampered))
	if err != nil {
		t.Fatal(err)
	}
	if !event.Redacted() {
		t.Fatal("tampered event was not redacted")
	}
	if strings.Contains(string(event.Content()), "rainy") {
		t.Fatal("redacted event kept its content")
	}
}

// rezzy and the built-in gmsl code agree on MSC4014 event IDs.
func TestPseudoIDsEventIDMatchesBuiltin(t *testing.T) {
	const ev = `{"type":"m.room.message","room_id":"!r:example.org","sender":"abcdefghijklmnopqrstuvwxyz0123456789ABCDEFG","content":{"body":"x"},"origin_server_ts":1,"depth":1,"prev_events":[],"auth_events":[]}`
	impl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionPseudoIDs)
	gomatrixserverlib.SetEventIDHook(nil)
	defer func() { _ = install() }()
	builtin, err := impl.NewEventFromTrustedJSON([]byte(ev), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := install(); err != nil {
		t.Fatal(err)
	}
	hooked, err := impl.NewEventFromTrustedJSON([]byte(ev), false)
	if err != nil {
		t.Fatal(err)
	}
	if builtin.EventID() != hooked.EventID() {
		t.Fatalf("builtin %s, rezzy %s", builtin.EventID(), hooked.EventID())
	}
}
