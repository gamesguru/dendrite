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
// another source and only then accept the copy, never reject it. Because a genuine
// redaction also matches, a match is only a reason to try one more source: log it at
// debug and do not let it drive an unbounded retry loop.
func LooksHashPruned(raw []byte, ev gomatrixserverlib.PDU) bool {
	if ev == nil || !ev.Redacted() {
		return false
	}
	return !gjson.GetBytes(raw, "unsigned.redacted_because").Exists() &&
		!gjson.GetBytes(raw, "unsigned.redacted_by").Exists()
}

// StoredCopyLooksHashPruned reports whether the stored JSON of an event is a hash-pruned copy.
//
// Events loaded from the database carry Redacted() only when `unsigned.redacted_because` is
// present, so that flag cannot identify a stored pruned copy. Instead the stored JSON is parsed
// again as untrusted input: a pruned copy fails its content hash and is redacted again, while a
// complete copy passes. A genuine redaction is excluded by its `unsigned.redacted_because`.
func StoredCopyLooksHashPruned(verImpl gomatrixserverlib.IRoomVersion, stored []byte) bool {
	if gjson.GetBytes(stored, "unsigned.redacted_because").Exists() ||
		gjson.GetBytes(stored, "unsigned.redacted_by").Exists() {
		return false
	}
	ev, err := verImpl.NewEventFromUntrustedJSON(stored)
	return err == nil && ev.Redacted()
}
