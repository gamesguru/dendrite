// Copyright 2024 New Vector Ltd.
// Copyright 2023 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package types

import (
	"unsafe"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
)

// HeaderedEvent is an Event which serializes to the headered form, which includes
// _room_version and _event_id fields.
type HeaderedEvent struct {
	gomatrixserverlib.PDU
	Visibility gomatrixserverlib.HistoryVisibility
	// TODO: Remove this. This is a temporary workaround to store the userID in the syncAPI.
	// 		It really should be the userKey instead.
	UserID           spec.UserID
	StateKeyResolved *string
	// MembershipForRequester is the requesting user's membership at the
	// event, served as unsigned.membership (MSC4115). The sync API sets it
	// on copies of events it returns; it is never stored.
	MembershipForRequester string
}

// RequesterMembership returns the requesting user's membership at the event,
// or "" if it is not known.
func (h *HeaderedEvent) RequesterMembership() string {
	return h.MembershipForRequester
}

func (h *HeaderedEvent) CacheCost() int {
	return int(unsafe.Sizeof(*h)) +
		len(h.EventID()) +
		(cap(h.JSON()) * 2) + //nolint:mnd
		len(h.Version()) +
		1 // redacted bool
}

func (h *HeaderedEvent) MarshalJSON() ([]byte, error) {
	// Room v3+ events compute their ID lazily, and Redact drops the cached one.
	// ToHeaderedJSON reads only the cache, so resolve the ID first or the
	// header is written empty (fixed in gomatrixserverlib#25).
	h.EventID()
	return h.ToHeaderedJSON()
}

func (j *HeaderedEvent) UnmarshalJSON(data []byte) error {
	ev, err := gomatrixserverlib.NewEventFromHeaderedJSON(data, false)
	if err != nil {
		return err
	}
	j.PDU = ev
	return nil
}

// Unwrap returns the event h wraps, however many HeaderedEvents deep. An
// event read through the roomserver cache is already a *HeaderedEvent, so
// wrapping it again nests them, and marshaling the inner one writes the
// internal _room_version and _event_id keys. Anything that serializes a PDU
// for federation must unwrap it first.
func (h *HeaderedEvent) Unwrap() gomatrixserverlib.PDU {
	var pdu gomatrixserverlib.PDU = h
	for {
		headered, ok := pdu.(*HeaderedEvent)
		if !ok || headered == nil {
			return pdu
		}
		pdu = headered.PDU
	}
}

func NewEventJSONsFromHeaderedEvents(hes []*HeaderedEvent) gomatrixserverlib.EventJSONs {
	result := make(gomatrixserverlib.EventJSONs, len(hes))
	for i := range hes {
		result[i] = hes[i].JSON()
	}
	return result
}

// RedactedCopy returns a redacted copy of the event, leaving the event
// itself, which may be shared through a cache, untouched.
func RedactedCopy(event gomatrixserverlib.PDU) (gomatrixserverlib.PDU, error) {
	if headered, ok := event.(*HeaderedEvent); ok {
		event = headered.Unwrap()
	}
	if event.Redacted() {
		return event, nil
	}
	verImpl, err := gomatrixserverlib.GetRoomVersion(event.Version())
	if err != nil {
		return nil, err
	}
	redactedJSON, err := verImpl.RedactEventJSON(event.JSON())
	if err != nil {
		return nil, err
	}
	if redactedJSON, err = gomatrixserverlib.EnforcedCanonicalJSON(redactedJSON, event.Version()); err != nil {
		return nil, err
	}
	// Keep the event ID: Redact would drop it, and an ID computed from the
	// JSON again is wrong for events whose ID is not the reference hash.
	return verImpl.NewEventFromTrustedJSONWithEventID(event.EventID(), redactedJSON, true)
}
