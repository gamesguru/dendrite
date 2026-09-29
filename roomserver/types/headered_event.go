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
}

func (h *HeaderedEvent) CacheCost() int {
	return int(unsafe.Sizeof(*h)) +
		len(h.EventID()) +
		(cap(h.JSON()) * 2) + //nolint:mnd
		len(h.Version()) +
		1 // redacted bool
}

func (h *HeaderedEvent) MarshalJSON() ([]byte, error) {
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
