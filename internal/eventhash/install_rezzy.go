//go:build rezzy && linux && cgo

package eventhash

import (
	"fmt"
	"sync"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/rezzy/rezzy-ffi/go/rezzy"
)

// selfTestEvent is an arbitrary room v10 event. Its ID is computed with the
// built-in gomatrixserverlib code before the hooks are installed, so a rezzy
// that is linked but broken is caught at startup instead of failing events.
const selfTestEvent = `{"type":"m.room.message","room_id":"!selftest:example.org","sender":"@a:example.org","content":{"body":"x","msgtype":"m.text"},"origin_server_ts":1,"depth":1,"prev_events":[],"auth_events":[]}`

var (
	installOnce sync.Once
	installErr  error
)

func init() {
	if err := Install(); err != nil {
		panic("eventhash: rezzy is linked but not usable: " + err.Error())
	}
}

// Install makes rezzy the source of event IDs and content hash verdicts.
// Call it during server startup before parsing events. It self-tests the
// native library before installing hooks, so unsupported builds fail early.
func Install() error {
	installOnce.Do(func() { installErr = install() })
	return installErr
}

func install() error {
	ver := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	ev, err := ver.NewEventFromTrustedJSON([]byte(selfTestEvent), false)
	if err != nil {
		return fmt.Errorf("self-test event: %w", err)
	}
	want := ev.EventID()
	got, err := rezzy.EventID(string(gomatrixserverlib.RoomVersionV10), []byte(selfTestEvent))
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("self-test event ID is %s, gomatrixserverlib has %s", got, want)
	}

	gomatrixserverlib.SetEventIDHook(func(v gomatrixserverlib.RoomVersion, eventJSON []byte) (string, error) {
		if !rezzyDerivesIDs(v) {
			return "", gomatrixserverlib.ErrUseBuiltin
		}
		return rezzy.EventID(string(v), eventJSON)
	})
	gomatrixserverlib.SetContentHashHook(func(v gomatrixserverlib.RoomVersion, eventJSON []byte) error {
		if v == gomatrixserverlib.RoomVersionV1 || v == gomatrixserverlib.RoomVersionV2 || !gomatrixserverlib.StableRoomVersion(v) {
			return gomatrixserverlib.ErrUseBuiltin
		}
		return rezzy.VerifyContentHash(string(v), eventJSON)
	})
	return nil
}

// rezzyDerivesIDs reports the stable room versions whose event IDs can be
// derived from the event. Experimental room versions use GMSL's implementation.
func rezzyDerivesIDs(v gomatrixserverlib.RoomVersion) bool {
	switch v {
	case gomatrixserverlib.RoomVersionV3, gomatrixserverlib.RoomVersionV4,
		gomatrixserverlib.RoomVersionV5, gomatrixserverlib.RoomVersionV6,
		gomatrixserverlib.RoomVersionV7, gomatrixserverlib.RoomVersionV8,
		gomatrixserverlib.RoomVersionV9, gomatrixserverlib.RoomVersionV10,
		gomatrixserverlib.RoomVersionV11, gomatrixserverlib.RoomVersionV12:
		return true
	default:
		return false
	}
}
