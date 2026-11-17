package eventutil

import (
	"github.com/gamesguru/gomatrixserverlib"
	"github.com/tidwall/gjson"
)

// LooksHashPruned reports whether a federation event, as received in raw, looks like
// a copy whose content hash failed, so parsing reduced it to its redacted form.
//
// The argument ev must be the result of parsing raw. Parsing discards `unsigned`, so the raw
// bytes are the only place `unsigned.redacted_because` can still be seen.
//
// This is a heuristic, not proof: a genuinely redacted event from a peer that omits
// `unsigned` is indistinguishable from a hash failure. Callers must therefore prefer
// another source and only then accept the copy, never reject it.
func LooksHashPruned(raw []byte, ev gomatrixserverlib.PDU) bool {
	if ev == nil || !ev.Redacted() {
		return false
	}
	return !gjson.GetBytes(raw, "unsigned.redacted_because").Exists() &&
		!gjson.GetBytes(raw, "unsigned.redacted_by").Exists()
}
