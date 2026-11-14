package eventhash

import (
	"strings"
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
)

// A real room version 12 event from matrix.org.
const v12Event = `{"auth_events":["$sbusQdJu8vp4mYghGgIdFrz9K_UbSNVmVr3UBxLM2Mk","$SbBcrKoU8Wo-rq9hH6NORFEcD7jrSgLkyYBgvlvNZVc"],"content":{"body":"A nice sunny day spent near the window.","m.mentions":{},"msgtype":"m.text"},"depth":265,"hashes":{"sha256":"BN2Mpld9Ox01qmZjEh+2SXjc3hf67Z+UrUvCc/TdJoc"},"origin_server_ts":1763426725083,"prev_events":["$YgQCx1FMxaNlpl8CoymZCmPLcE9YJ95kwnp8VBHJiqY"],"room_id":"!L58ME6ufiP49v97UIOBIpvWKEgj4912JmECPuDzlvCI","sender":"@chtr-x:matrix.org","signatures":{"matrix.org":{"ed25519:a_RXGa":"EKGxFhkSSIlattBwQDE9V1moJdRz6FGd4tC+u43q6z0/8v3oys743hvIStLWTO0LYR1GrntNOHyztbUaIVQOAw"}},"type":"m.room.message","unsigned":{"age_ts":1763426725083}}`

const v12EventID = "$IYh3j5DDlpXmmErAivzq4d1PVT0DxegxvvKtlWVDoFE"

func realV12Event(t *testing.T) gomatrixserverlib.PDU {
	t.Helper()
	impl, err := gomatrixserverlib.GetRoomVersion(gomatrixserverlib.RoomVersionV12)
	if err != nil {
		t.Fatal(err)
	}
	event, err := impl.NewEventFromTrustedJSON([]byte(v12Event), false)
	if err != nil {
		t.Fatal(err)
	}
	return event
}

func TestCheckAgreesOnRealEvent(t *testing.T) {
	event := realV12Event(t)
	if event.EventID() != v12EventID {
		t.Fatalf("gomatrixserverlib event ID = %s, want %s", event.EventID(), v12EventID)
	}
	if err := Check(event); err != nil {
		t.Fatalf("Check (rezzy linked: %v): %v", Enabled, err)
	}
}

func TestCheckContentHashAgreesOnRealEvent(t *testing.T) {
	if err := CheckContentHash(realV12Event(t)); err != nil {
		t.Fatalf("CheckContentHash (rezzy linked: %v): %v", Enabled, err)
	}
}

func TestCheckContentHashRejectsTamperedEvent(t *testing.T) {
	if !Enabled {
		t.Skip("rezzy is not linked")
	}
	impl, err := gomatrixserverlib.GetRoomVersion(gomatrixserverlib.RoomVersionV12)
	if err != nil {
		t.Fatal(err)
	}
	tampered := strings.Replace(v12Event, "A nice sunny day spent near the window.", "A tampered message.", 1)
	if tampered == v12Event {
		t.Fatal("tamper did not change the event")
	}
	event, err := impl.NewEventFromTrustedJSON([]byte(tampered), false)
	if err != nil {
		t.Fatal(err)
	}
	if err := CheckContentHash(event); err == nil {
		t.Fatal("CheckContentHash accepted a tampered event")
	}
}

func TestCheckStateHAMTAgreesWithEntries(t *testing.T) {
	entries := []StateEntry{
		{Type: "m.room.create", StateKey: "", EventID: "$create"},
		{Type: "m.room.member", StateKey: "@alice:example.org", EventID: "$alice"},
		{Type: "m.room.member", StateKey: "@bob:example.org", EventID: "$bob"},
		{Type: "m.room.power_levels", StateKey: "", EventID: "$power"},
		{Type: "m.room.topic", StateKey: "", EventID: "$topic"},
	}
	if err := CheckStateHAMT("!room:example.org", entries); err != nil {
		t.Fatalf("CheckStateHAMT (rezzy linked: %v): %v", Enabled, err)
	}
}

func TestCheckStateHAMTRejectsDuplicateKeys(t *testing.T) {
	if !Enabled {
		t.Skip("rezzy is not linked")
	}
	entries := []StateEntry{
		{Type: "m.room.topic", StateKey: "", EventID: "$one"},
		{Type: "m.room.topic", StateKey: "", EventID: "$two"},
	}
	if err := CheckStateHAMT("!room:example.org", entries); err == nil {
		t.Fatal("CheckStateHAMT accepted duplicate state keys")
	}
}
