// Copyright 2026 The Zendrite Authors
// Copyright 2025 Jackmaninov
// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// mockSnapshot implements storage.DatabaseTransaction for testing
// Uses interface embedding - only override methods needed for tests.
type mockSnapshot struct {
	storage.DatabaseTransaction

	// Configurable return values
	membershipForUser  map[string]map[string]mockMembership         // roomID -> userID -> membership
	stateEvents        map[string]map[string]*rstypes.HeaderedEvent // roomID -> "type|stateKey" -> event
	recentEvents       map[string]types.RecentEvents                // roomID -> events
	roomSummaries      map[string]*types.Summary
	maxStreamPositions map[string]types.StreamPosition   // roomID -> position
	inviteEvents       map[string]*rstypes.HeaderedEvent // roomID -> invite event
	retiredInvites     map[string]*rstypes.HeaderedEvent // roomID -> retired invite
	maxInvitePos       types.StreamPosition
	membershipCounts   map[string]map[string]int // roomID -> membership -> count

	// accountDataChanges backs GetAccountDataInRange.
	accountDataChanges []mockAccountDataChange
}

// mockAccountDataChange is one fixture entry for GetAccountDataInRange.
type mockAccountDataChange struct {
	roomID   string
	dataType string
	pos      types.StreamPosition
}

type mockMembership struct {
	membership string
	topoPos    int64
}

// newMockSnapshot creates a new mock snapshot with default empty maps.
func newMockSnapshot() *mockSnapshot {
	return &mockSnapshot{
		membershipForUser:  make(map[string]map[string]mockMembership),
		stateEvents:        make(map[string]map[string]*rstypes.HeaderedEvent),
		recentEvents:       make(map[string]types.RecentEvents),
		roomSummaries:      make(map[string]*types.Summary),
		maxStreamPositions: make(map[string]types.StreamPosition),
		inviteEvents:       make(map[string]*rstypes.HeaderedEvent),
		retiredInvites:     make(map[string]*rstypes.HeaderedEvent),
		membershipCounts:   make(map[string]map[string]int),
	}
}

// SetMembership sets the membership for a user in a room.
func (m *mockSnapshot) SetMembership(roomID, userID, membership string, topoPos int64) {
	if m.membershipForUser[roomID] == nil {
		m.membershipForUser[roomID] = make(map[string]mockMembership)
	}
	m.membershipForUser[roomID][userID] = mockMembership{
		membership: membership,
		topoPos:    topoPos,
	}
}

// SetStateEvent sets a state event for a room.
func (m *mockSnapshot) SetStateEvent(roomID, eventType, stateKey string, event *rstypes.HeaderedEvent) {
	if m.stateEvents[roomID] == nil {
		m.stateEvents[roomID] = make(map[string]*rstypes.HeaderedEvent)
	}
	key := eventType + "|" + stateKey
	m.stateEvents[roomID][key] = event
}

// SetRecentEvents sets recent events for a room.
func (m *mockSnapshot) SetRecentEvents(roomID string, events types.RecentEvents) {
	m.recentEvents[roomID] = events
}

// SetAccountDataChange adds one fixture entry: roomID (empty = global) had
// dataType change at pos.
func (m *mockSnapshot) SetAccountDataChange(roomID, dataType string, pos types.StreamPosition) {
	m.accountDataChanges = append(m.accountDataChanges, mockAccountDataChange{roomID: roomID, dataType: dataType, pos: pos})
}

// GetAccountDataInRange filters the fixtures set via SetAccountDataChange down
// to the requested range, mirroring the real DB's (Low(), High()] semantics:
// exclusive lower bound, inclusive upper bound. An empty result reports
// lastPos as High(), as both real backends do, so callers that advance a token
// on "nothing changed" behave the same here as in production.
func (m *mockSnapshot) GetAccountDataInRange(ctx context.Context, userID string, r types.Range, accountDataFilterPart *synctypes.EventFilter) (map[string][]string, types.StreamPosition, error) {
	lo, hi := r.Low(), r.High()

	result := make(map[string][]string)
	lastPos := lo
	for _, c := range m.accountDataChanges {
		if c.pos <= lo || c.pos > hi {
			continue
		}
		result[c.roomID] = append(result[c.roomID], c.dataType)
		if c.pos > lastPos {
			lastPos = c.pos
		}
	}
	if len(result) == 0 {
		lastPos = hi
	}
	return result, lastPos, nil
}

// SetRoomSummary sets the room summary for a room.
func (m *mockSnapshot) SetRoomSummary(roomID string, summary *types.Summary) {
	m.roomSummaries[roomID] = summary
}

// SetMaxStreamPosition sets the max stream position for a room.
func (m *mockSnapshot) SetMaxStreamPosition(roomID string, pos types.StreamPosition) {
	m.maxStreamPositions[roomID] = pos
}

// SetMembershipCount sets the membership count for a room.
func (m *mockSnapshot) SetMembershipCount(roomID, membership string, count int) {
	if m.membershipCounts[roomID] == nil {
		m.membershipCounts[roomID] = make(map[string]int)
	}
	m.membershipCounts[roomID][membership] = count
}

// Interface implementations.

func (m *mockSnapshot) SelectMembershipForUser(ctx context.Context, roomID, userID string, pos int64) (string, int64, error) {
	if roomUsers, ok := m.membershipForUser[roomID]; ok {
		if membership, ok := roomUsers[userID]; ok {
			// If pos is specified, only return membership if topoPos <= pos
			if membership.topoPos <= pos {
				return membership.membership, membership.topoPos, nil
			}
		}
	}
	return "leave", 0, nil
}

func (m *mockSnapshot) GetStateEvent(ctx context.Context, roomID, evType, stateKey string) (*rstypes.HeaderedEvent, error) {
	if roomEvents, ok := m.stateEvents[roomID]; ok {
		key := evType + "|" + stateKey
		if event, ok := roomEvents[key]; ok {
			return event, nil
		}
	}
	return nil, nil
}

func (m *mockSnapshot) GetStateEventsForRoom(ctx context.Context, roomID string, stateFilterPart *synctypes.StateFilter) ([]*rstypes.HeaderedEvent, error) {
	var events []*rstypes.HeaderedEvent
	if roomEvents, ok := m.stateEvents[roomID]; ok {
		for _, event := range roomEvents {
			events = append(events, event)
		}
	}
	return events, nil
}

// RecentEvents filters the events set via SetRecentEvents down to the requested
// range, mirroring the real DB's (r.To, r.From] semantics (exclusive lower bound,
// inclusive upper bound), so range-computation bugs in callers are actually
// exercised instead of being masked by returning everything unconditionally.
func (m *mockSnapshot) RecentEvents(ctx context.Context, roomIDs []string, r types.Range, eventFilter *synctypes.RoomEventFilter, chronologicalOrder bool, onlySyncEvents bool) (map[string]types.RecentEvents, error) {
	// Low()/High() key off r.Backwards, so an inverted From/To is reported as
	// the empty range the real DB would produce rather than being silently
	// normalised away by magnitude.
	lo, hi := r.Low(), r.High()

	result := make(map[string]types.RecentEvents)
	for _, roomID := range roomIDs {
		all, ok := m.recentEvents[roomID]
		if !ok {
			continue
		}

		filtered := make([]types.StreamEvent, 0, len(all.Events))
		for _, ev := range all.Events {
			if ev.StreamPosition > lo && ev.StreamPosition <= hi {
				filtered = append(filtered, ev)
			}
		}
		sort.Slice(filtered, func(i, j int) bool {
			return filtered[i].StreamPosition < filtered[j].StreamPosition
		})

		limited := false
		if eventFilter != nil && eventFilter.Limit > 0 && len(filtered) > eventFilter.Limit {
			filtered = filtered[len(filtered)-eventFilter.Limit:]
			limited = true
		}

		result[roomID] = types.RecentEvents{Events: filtered, Limited: limited}
	}
	return result, nil
}

func (m *mockSnapshot) GetRoomSummary(ctx context.Context, roomID, userID string) (*types.Summary, error) {
	if summary, ok := m.roomSummaries[roomID]; ok {
		return summary, nil
	}
	return &types.Summary{}, nil
}

func (m *mockSnapshot) MaxStreamPositionsForRooms(ctx context.Context, roomIDs []string) (map[string]types.StreamPosition, error) {
	result := make(map[string]types.StreamPosition)
	for _, roomID := range roomIDs {
		if pos, ok := m.maxStreamPositions[roomID]; ok {
			result[roomID] = pos
		}
	}
	return result, nil
}

func (m *mockSnapshot) InviteEventsInRange(ctx context.Context, targetUserID string, r types.Range) (map[string]*rstypes.HeaderedEvent, map[string]*rstypes.HeaderedEvent, types.StreamPosition, error) {
	return m.inviteEvents, m.retiredInvites, m.maxInvitePos, nil
}

func (m *mockSnapshot) MaxStreamPositionForInvites(ctx context.Context) (types.StreamPosition, error) {
	return m.maxInvitePos, nil
}

func (m *mockSnapshot) MembershipCount(ctx context.Context, roomID, membership string, pos types.StreamPosition) (int, error) {
	if roomCounts, ok := m.membershipCounts[roomID]; ok {
		if count, ok := roomCounts[membership]; ok {
			return count, nil
		}
	}
	return 0, nil
}

func (m *mockSnapshot) EventPositionInTopology(ctx context.Context, eventID string) (types.TopologyToken, error) {
	// Return a simple token for testing
	return types.TopologyToken{Depth: 10, PDUPosition: 100}, nil
}

// Transaction interface methods (no-ops for testing).
func (m *mockSnapshot) Commit() error   { return nil }
func (m *mockSnapshot) Rollback() error { return nil }

// mockRoomserverAPI implements api.SyncRoomserverAPI for testing
// Uses interface embedding to satisfy the interface without implementing all methods.
type mockRoomserverAPI struct {
	roomserverAPI.SyncRoomserverAPI
}

func (m *mockRoomserverAPI) QueryUserIDForSender(ctx context.Context, roomID spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(senderID), true)
}

func (m *mockRoomserverAPI) QuerySenderIDForUser(ctx context.Context, roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	senderID := spec.SenderID(userID.String())
	return &senderID, nil
}

func (m *mockRoomserverAPI) QueryMembershipAtEvent(ctx context.Context, roomID spec.RoomID, eventIDs []string, senderID spec.SenderID) (map[string]*rstypes.HeaderedEvent, error) {
	return map[string]*rstypes.HeaderedEvent{}, nil
}

// mockUserAPI implements userapi.SyncUserAPI for testing.
// Uses interface embedding to satisfy the interface without implementing all methods.
type mockUserAPI struct {
	userapi.SyncUserAPI

	globalAccountData map[string]json.RawMessage            // dataType -> content
	roomAccountData   map[string]map[string]json.RawMessage // roomID -> dataType -> content
}

// QueryAccountData returns the fixtures configured on mockUserAPI regardless
// of which room/type was requested - callers (e.g. processAccountDataExtension)
// are responsible for looking up the specific room/type they need, exactly as
// the real userapi implementation's caller does.
func (m *mockUserAPI) QueryAccountData(ctx context.Context, req *userapi.QueryAccountDataRequest, res *userapi.QueryAccountDataResponse) error {
	res.GlobalAccountData = m.globalAccountData
	res.RoomAccountData = m.roomAccountData
	return nil
}

// createMockStateEvent creates a mock HeaderedEvent for testing
// This is a simple helper that creates events with minimal structure.
func createMockStateEvent(eventType, stateKey, content string) *rstypes.HeaderedEvent {
	// Create a minimal valid event JSON with proper room version format
	eventJSON := []byte(`{
		"type":"` + eventType + `",
		"state_key":"` + stateKey + `",
		"room_id":"!test:localhost",
		"sender":"@test:localhost",
		"event_id":"$test:localhost",
		"origin_server_ts":1234567890,
		"depth":1,
		"prev_events":[],
		"auth_events":[],
		"content":` + content + `
	}`)

	// Use gomatrixserverlib to parse the event
	// Use room version 10 which is commonly used
	verImpl, _ := gomatrixserverlib.GetRoomVersion(gomatrixserverlib.RoomVersionV10)
	event, err := verImpl.NewEventFromTrustedJSON(eventJSON, false)
	if err != nil {
		// Return nil if parsing fails - test will catch this
		return nil
	}

	return &rstypes.HeaderedEvent{PDU: event}
}

// createMockStateStreamEvent creates a mock state StreamEvent (includes
// state_key, which is what makes an event a state event at all), for tests
// exercising state-delta computation via RecentEvents (getRequiredStateDelta).
func createMockStateStreamEvent(eventID, eventType, stateKey string, streamPos types.StreamPosition, content string) types.StreamEvent {
	eventJSON := []byte(`{
		"type":"` + eventType + `",
		"state_key":"` + stateKey + `",
		"room_id":"!test:localhost",
		"sender":"@test:localhost",
		"event_id":"` + eventID + `",
		"origin_server_ts":1234567890,
		"depth":1,
		"prev_events":[],
		"auth_events":[],
		"content":` + content + `
	}`)

	return mockStreamEvent(eventJSON, streamPos, "createMockStateStreamEvent")
}

// createMockTimelineEvent creates a mock non-state (message) StreamEvent at the
// given stream position, for tests exercising getTimelineEvents' range
// computation. Unlike createMockStateStreamEvent, it omits state_key entirely,
// since a present state_key (even "") makes an event a state event.
func createMockTimelineEvent(eventID string, streamPos types.StreamPosition, content string) types.StreamEvent {
	eventJSON := []byte(`{
		"type":"m.room.message",
		"room_id":"!test:localhost",
		"sender":"@test:localhost",
		"event_id":"` + eventID + `",
		"origin_server_ts":1234567890,
		"depth":1,
		"prev_events":[],
		"auth_events":[],
		"content":` + content + `
	}`)

	return mockStreamEvent(eventJSON, streamPos, "createMockTimelineEvent")
}

// mockStreamEvent builds the StreamEvent both fixture helpers return; caller
// only names the panic so a malformed fixture points at the right helper.
func mockStreamEvent(eventJSON []byte, streamPos types.StreamPosition, caller string) types.StreamEvent {
	verImpl, _ := gomatrixserverlib.GetRoomVersion(gomatrixserverlib.RoomVersionV10)
	event, err := verImpl.NewEventFromTrustedJSON(eventJSON, false)
	if err != nil {
		panic(caller + ": failed to build mock event: " + err.Error())
	}

	// World readable, so the timeline's history visibility filter keeps the
	// fixtures without the mocks having to model memberships.
	return types.StreamEvent{
		HeaderedEvent:  &rstypes.HeaderedEvent{PDU: event, Visibility: gomatrixserverlib.HistoryVisibilityWorldReadable},
		StreamPosition: streamPos,
	}
}

// QueryErasedUsers reports that no user is erased (MSC4025).
func (*mockRoomserverAPI) QueryErasedUsers(context.Context, []string) (map[string]struct{}, error) {
	return map[string]struct{}{}, nil
}
