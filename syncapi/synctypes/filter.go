// Copyright 2024 New Vector Ltd.
// Copyright 2017 Jan Christian Grünhage
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package synctypes

import (
	"errors"

	"github.com/gamesguru/gomatrixserverlib/spec"
)

// Filter is used by clients to specify how the server should filter responses to e.g. sync requests
// Specified by: https://spec.matrix.org/v1.6/client-server-api/#filtering
type Filter struct {
	EventFields []string    `json:"event_fields,omitempty"`
	EventFormat string      `json:"event_format,omitempty"`
	Presence    EventFilter `json:"presence,omitempty"`
	AccountData EventFilter `json:"account_data,omitempty"`
	Room        RoomFilter  `json:"room,omitempty"`
}

// EventFilter is used to define filtering rules for events.
type EventFilter struct {
	Limit      int       `json:"limit,omitempty"`
	NotSenders *[]string `json:"not_senders,omitempty"`
	NotTypes   *[]string `json:"not_types,omitempty"`
	Senders    *[]string `json:"senders,omitempty"`
	Types      *[]string `json:"types,omitempty"`
}

// RoomFilter is used to define filtering rules for room-related events.
type RoomFilter struct {
	NotRooms     *[]string       `json:"not_rooms,omitempty"`
	Rooms        *[]string       `json:"rooms,omitempty"`
	Ephemeral    RoomEventFilter `json:"ephemeral,omitempty"`
	IncludeLeave bool            `json:"include_leave,omitempty"`
	State        StateFilter     `json:"state,omitempty"`
	Timeline     RoomEventFilter `json:"timeline,omitempty"`
	AccountData  RoomEventFilter `json:"account_data,omitempty"`
}

// StateFilter is used to define filtering rules for state events.
type StateFilter struct {
	NotSenders                *[]string `json:"not_senders,omitempty"`
	NotTypes                  *[]string `json:"not_types,omitempty"`
	Senders                   *[]string `json:"senders,omitempty"`
	Types                     *[]string `json:"types,omitempty"`
	LazyLoadMembers           bool      `json:"lazy_load_members,omitempty"`
	IncludeRedundantMembers   bool      `json:"include_redundant_members,omitempty"`
	NotRooms                  *[]string `json:"not_rooms,omitempty"`
	Rooms                     *[]string `json:"rooms,omitempty"`
	Limit                     int       `json:"limit,omitempty"`
	UnreadThreadNotifications bool      `json:"unread_thread_notifications,omitempty"`
	ContainsURL               *bool     `json:"contains_url,omitempty"`
}

// RoomEventFilter is used to define filtering rules for events in rooms.
type RoomEventFilter struct {
	Limit                     int       `json:"limit,omitempty"`
	NotSenders                *[]string `json:"not_senders,omitempty"`
	NotTypes                  *[]string `json:"not_types,omitempty"`
	Senders                   *[]string `json:"senders,omitempty"`
	Types                     *[]string `json:"types,omitempty"`
	LazyLoadMembers           bool      `json:"lazy_load_members,omitempty"`
	IncludeRedundantMembers   bool      `json:"include_redundant_members,omitempty"`
	NotRooms                  *[]string `json:"not_rooms,omitempty"`
	Rooms                     *[]string `json:"rooms,omitempty"`
	UnreadThreadNotifications bool      `json:"unread_thread_notifications,omitempty"`
	ContainsURL               *bool     `json:"contains_url,omitempty"`
	RelationTypes             []string  `json:"org.matrix.msc3874.rel_types,omitempty"`
}

const (
	EventFormatClient     = "client"
	EventFormatFederation = "federation"
)

// Validate checks if the filter contains valid property values.
func (filter *Filter) Validate() error {
	if filter.EventFormat != "" && filter.EventFormat != EventFormatClient && filter.EventFormat != EventFormatFederation {
		return errors.New("bad event_format value. Must be one of [\"client\", \"federation\"]")
	}
	validateRoomIDs := func(name string, roomIDs *[]string) error {
		if roomIDs == nil {
			return nil
		}
		for _, roomID := range *roomIDs {
			if _, err := spec.NewRoomID(roomID); err != nil {
				return errors.New("invalid room ID in " + name)
			}
		}
		return nil
	}
	validateSenders := func(name string, senders *[]string) error {
		if senders == nil {
			return nil
		}
		for _, sender := range *senders {
			if _, err := spec.NewUserID(sender, true); err != nil {
				return errors.New("invalid sender ID in " + name)
			}
		}
		return nil
	}
	for _, item := range []struct {
		name string
		ids  *[]string
	}{
		{"room.rooms", filter.Room.Rooms},
		{"room.not_rooms", filter.Room.NotRooms},
		{"room.state.rooms", filter.Room.State.Rooms},
		{"room.state.not_rooms", filter.Room.State.NotRooms},
		{"room.timeline.rooms", filter.Room.Timeline.Rooms},
		{"room.timeline.not_rooms", filter.Room.Timeline.NotRooms},
		{"room.account_data.rooms", filter.Room.AccountData.Rooms},
		{"room.account_data.not_rooms", filter.Room.AccountData.NotRooms},
		{"room.ephemeral.rooms", filter.Room.Ephemeral.Rooms},
		{"room.ephemeral.not_rooms", filter.Room.Ephemeral.NotRooms},
	} {
		if err := validateRoomIDs(item.name, item.ids); err != nil {
			return err
		}
	}
	for _, item := range []struct {
		name string
		ids  *[]string
	}{
		{"presence.senders", filter.Presence.Senders},
		{"presence.not_senders", filter.Presence.NotSenders},
		{"account_data.senders", filter.AccountData.Senders},
		{"account_data.not_senders", filter.AccountData.NotSenders},
		{"room.state.senders", filter.Room.State.Senders},
		{"room.state.not_senders", filter.Room.State.NotSenders},
		{"room.timeline.senders", filter.Room.Timeline.Senders},
		{"room.timeline.not_senders", filter.Room.Timeline.NotSenders},
		{"room.account_data.senders", filter.Room.AccountData.Senders},
		{"room.account_data.not_senders", filter.Room.AccountData.NotSenders},
		{"room.ephemeral.senders", filter.Room.Ephemeral.Senders},
		{"room.ephemeral.not_senders", filter.Room.Ephemeral.NotSenders},
	} {
		if err := validateSenders(item.name, item.ids); err != nil {
			return err
		}
	}
	return nil
}

// DefaultFilter returns the default filter used by the Matrix server if no filter is provided in
// the request.
func DefaultFilter() Filter {
	return Filter{
		AccountData: DefaultEventFilter(),
		EventFields: nil,
		EventFormat: "client",
		Presence:    DefaultEventFilter(),
		Room: RoomFilter{
			AccountData:  DefaultRoomEventFilter(),
			Ephemeral:    DefaultRoomEventFilter(),
			IncludeLeave: false,
			NotRooms:     nil,
			Rooms:        nil,
			State:        DefaultStateFilter(),
			Timeline:     DefaultRoomEventFilter(),
		},
	}
}

// DefaultEventFilter returns the default event filter used by the Matrix server if no filter is
// provided in the request.
func DefaultEventFilter() EventFilter {
	return EventFilter{
		// parity with synapse: https://github.com/matrix-org/synapse/blob/v1.80.0/synapse/api/filtering.py#L336
		Limit:      10, //nolint:mnd
		NotSenders: nil,
		NotTypes:   nil,
		Senders:    nil,
		Types:      nil,
	}
}

// DefaultStateFilter returns the default state event filter used by the Matrix server if no filter
// is provided in the request.
func DefaultStateFilter() StateFilter {
	return StateFilter{
		NotSenders:              nil,
		NotTypes:                nil,
		Senders:                 nil,
		Types:                   nil,
		LazyLoadMembers:         false,
		IncludeRedundantMembers: false,
		NotRooms:                nil,
		Rooms:                   nil,
		ContainsURL:             nil,
	}
}

// DefaultRoomEventFilter returns the default room event filter used by the Matrix server if no
// filter is provided in the request.
func DefaultRoomEventFilter() RoomEventFilter {
	return RoomEventFilter{
		// parity with synapse: https://github.com/matrix-org/synapse/blob/v1.80.0/synapse/api/filtering.py#L336
		Limit:       10, //nolint:mnd
		NotSenders:  nil,
		NotTypes:    nil,
		Senders:     nil,
		Types:       nil,
		NotRooms:    nil,
		Rooms:       nil,
		ContainsURL: nil,
	}
}
