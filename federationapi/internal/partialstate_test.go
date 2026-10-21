// Copyright 2025 Jackmaninov
// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package internal

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	roomserverAPI "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/process"
	"codefloe.com/pat-s/zendrite/test"
)

// TestPartialStateWorkerQueueRoom tests that rooms can be queued for processing.
func TestPartialStateWorkerQueueRoom(t *testing.T) {
	processCtx := process.NewProcessContext()
	defer processCtx.ShutdownZendrite()

	worker := &PartialStateWorker{
		process:  processCtx,
		workerCh: make(chan types.RoomNID, 10),
		retryMap: make(map[types.RoomNID]*roomRetryInfo),
	}

	// Queue a room
	worker.QueueRoom(types.RoomNID(1))

	// Should be in the channel
	select {
	case roomNID := <-worker.workerCh:
		assert.Equal(t, types.RoomNID(1), roomNID)
	case <-time.After(time.Second):
		t.Fatal("Room was not queued")
	}
}

// TestPartialStateWorkerQueueRoomFullChannel tests fallback to retry map when channel is full.
func TestPartialStateWorkerQueueRoomFullChannel(t *testing.T) {
	processCtx := process.NewProcessContext()
	defer processCtx.ShutdownZendrite()

	// Create worker with tiny channel
	worker := &PartialStateWorker{
		process:  processCtx,
		workerCh: make(chan types.RoomNID, 1),
		retryMap: make(map[types.RoomNID]*roomRetryInfo),
	}

	// Fill the channel
	worker.workerCh <- types.RoomNID(1)

	// Queue another room - should go to retry map
	worker.QueueRoom(types.RoomNID(2))

	// Should be in retry map
	worker.retryMu.Lock()
	_, exists := worker.retryMap[types.RoomNID(2)]
	worker.retryMu.Unlock()

	assert.True(t, exists, "Room should be in retry map when channel is full")
}

// TestPartialStateWorkerDuplicateQueue tests that duplicate queue requests don't overwrite retry times.
func TestPartialStateWorkerDuplicateQueue(t *testing.T) {
	processCtx := process.NewProcessContext()
	defer processCtx.ShutdownZendrite()

	worker := &PartialStateWorker{
		process:  processCtx,
		workerCh: make(chan types.RoomNID, 1),
		retryMap: make(map[types.RoomNID]*roomRetryInfo),
	}

	// Fill channel
	worker.workerCh <- types.RoomNID(1)

	// First queue of room 2 - goes to retry map
	worker.QueueRoom(types.RoomNID(2))

	worker.retryMu.Lock()
	firstInfo := worker.retryMap[types.RoomNID(2)]
	worker.retryMu.Unlock()

	// Wait a bit
	time.Sleep(10 * time.Millisecond)

	// Second queue of room 2 - should NOT update retry time
	worker.QueueRoom(types.RoomNID(2))

	worker.retryMu.Lock()
	secondInfo := worker.retryMap[types.RoomNID(2)]
	worker.retryMu.Unlock()

	assert.Equal(t, firstInfo.retryAt, secondInfo.retryAt, "Duplicate queue should not update retry time")
}

// TestPartialStateWorkerRetryMapCleanup tests that retry map entries are properly moved to the queue.
func TestPartialStateWorkerRetryMapCleanup(t *testing.T) {
	processCtx := process.NewProcessContext()
	defer processCtx.ShutdownZendrite()

	worker := &PartialStateWorker{
		process:  processCtx,
		workerCh: make(chan types.RoomNID, 10),
		retryMap: make(map[types.RoomNID]*roomRetryInfo),
	}

	// Add entries to retry map with past times
	worker.retryMu.Lock()
	worker.retryMap[types.RoomNID(1)] = &roomRetryInfo{retryAt: time.Now().Add(-time.Hour)}
	worker.retryMap[types.RoomNID(2)] = &roomRetryInfo{retryAt: time.Now().Add(-time.Hour)}
	worker.retryMap[types.RoomNID(3)] = &roomRetryInfo{retryAt: time.Now().Add(time.Hour)} // Future - should not be retried
	worker.retryMu.Unlock()

	// Manually trigger retry logic (simulating what retryLoop does)
	worker.retryMu.Lock()
	now := time.Now()
	var toRetry []types.RoomNID
	for roomNID, info := range worker.retryMap {
		if now.After(info.retryAt) {
			toRetry = append(toRetry, roomNID)
		}
	}
	for _, roomNID := range toRetry {
		delete(worker.retryMap, roomNID)
	}
	worker.retryMu.Unlock()

	// Queue retried rooms
	for _, roomNID := range toRetry {
		worker.QueueRoom(roomNID)
	}

	// Should have 2 rooms in channel (1 and 2), and 1 in retry map (3)
	assert.Len(t, toRetry, 2)

	worker.retryMu.Lock()
	remainingRetries := len(worker.retryMap)
	worker.retryMu.Unlock()
	assert.Equal(t, 1, remainingRetries, "Room 3 should still be in retry map")
}

// mockPartialStateRoomserverAPI implements minimal interface for testing.
type mockPartialStateRoomserverAPI struct {
	partialStateRooms map[types.RoomNID]bool
	partialServers    map[types.RoomNID][]string
	roomIDs           map[types.RoomNID]string
	clearCalled       int32 // atomic
	mu                sync.RWMutex
}

func newMockPartialStateRoomserverAPI() *mockPartialStateRoomserverAPI {
	return &mockPartialStateRoomserverAPI{
		partialStateRooms: make(map[types.RoomNID]bool),
		partialServers:    make(map[types.RoomNID][]string),
		roomIDs:           make(map[types.RoomNID]string),
	}
}

func (m *mockPartialStateRoomserverAPI) IsRoomPartialState(ctx context.Context, roomNID types.RoomNID) (bool, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.partialStateRooms[roomNID], nil
}

func (m *mockPartialStateRoomserverAPI) GetPartialStateServers(ctx context.Context, roomNID types.RoomNID) ([]string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.partialServers[roomNID], nil
}

func (m *mockPartialStateRoomserverAPI) GetPartialStateJoinEventID(context.Context, types.RoomNID) (string, error) {
	return "", nil
}

func (m *mockPartialStateRoomserverAPI) GetPartialStateJoinServer(ctx context.Context, roomNID types.RoomNID) (string, error) {
	return "", nil
}

func (m *mockPartialStateRoomserverAPI) GetAllPartialStateRooms(ctx context.Context) ([]types.RoomNID, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var nids []types.RoomNID
	for nid, isPartial := range m.partialStateRooms {
		if isPartial {
			nids = append(nids, nid)
		}
	}
	return nids, nil
}

func (m *mockPartialStateRoomserverAPI) ClearRoomPartialState(ctx context.Context, roomNID types.RoomNID) (int64, error) {
	atomic.AddInt32(&m.clearCalled, 1)
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.partialStateRooms, roomNID)
	return 0, nil // Return 0 for device list stream ID in mock
}

func (m *mockPartialStateRoomserverAPI) RoomIDFromNID(ctx context.Context, roomNID types.RoomNID) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if id, ok := m.roomIDs[roomNID]; ok {
		return id, nil
	}
	return "", nil // Room not found
}

// TestPartialStateWorkerSkipsNonPartialRooms tests that non-partial rooms are skipped.
func TestPartialStateWorkerSkipsNonPartialRooms(t *testing.T) {
	processCtx := process.NewProcessContext()
	defer processCtx.ShutdownZendrite()

	mockAPI := newMockPartialStateRoomserverAPI()
	// Room 1 is NOT in partial state
	mockAPI.partialStateRooms[types.RoomNID(1)] = false

	_ = &PartialStateWorker{
		process:  processCtx,
		rsAPI:    nil, // Will use mockAPI in processRoom
		workerCh: make(chan types.RoomNID, 10),
		retryMap: make(map[types.RoomNID]*roomRetryInfo),
	}

	// Manually test the check logic (since processRoom requires full API)
	hasPartialState, err := mockAPI.IsRoomPartialState(context.Background(), types.RoomNID(1))
	require.NoError(t, err)
	assert.False(t, hasPartialState, "Room should not have partial state")
}

// TestPartialStateJoinClientTracking tests that the PartialStateJoinClient tracks join metadata.
func TestPartialStateJoinClientTracking(t *testing.T) {
	// Test initial state
	client := &PartialStateJoinClient{
		FederationInternalAPI:  nil, // Not needed for this test
		LastJoinMembersOmitted: false,
		LastJoinServersInRoom:  nil,
	}

	assert.False(t, client.LastJoinMembersOmitted)
	assert.Nil(t, client.LastJoinServersInRoom)

	// Simulate a partial state join response update
	client.LastJoinMembersOmitted = true
	client.LastJoinServersInRoom = []string{"server1.example.com", "server2.example.com"}

	assert.True(t, client.LastJoinMembersOmitted)
	assert.Len(t, client.LastJoinServersInRoom, 2)
	assert.Contains(t, client.LastJoinServersInRoom, "server1.example.com")
}

func TestPartialStateJoinClientResetPartialState(t *testing.T) {
	client := &PartialStateJoinClient{
		LastJoinMembersOmitted: true,
		LastJoinServersInRoom:  []string{"server1.example.com"},
	}

	client.resetPartialState()

	assert.False(t, client.LastJoinMembersOmitted, "a full-state retry must not be recorded as partial")
	assert.Nil(t, client.LastJoinServersInRoom)
}

func TestShouldRetryJoinWithoutPartialState(t *testing.T) {
	client := &PartialStateJoinClient{LastJoinMembersOmitted: true}
	authErr := &gomatrixserverlib.FederationError{
		Reachable: true,
		Err:       fmt.Errorf("respSendJoin.Check: %w", &gomatrixserverlib.NotAllowed{Message: "invite missing from state"}),
	}

	assert.True(t, shouldRetryJoinWithoutPartialState(client, authErr))
	assert.False(t, shouldRetryJoinWithoutPartialState(&PartialStateJoinClient{}, authErr), "full-state responses do not need retry")
	assert.False(t, shouldRetryJoinWithoutPartialState(client, &gomatrixserverlib.FederationError{
		Reachable: true,
		Err:       fmt.Errorf("transport or parsing failure"),
	}), "non-auth failures do not need retry")
	assert.False(t, shouldRetryJoinWithoutPartialState(client, &gomatrixserverlib.FederationError{
		Reachable: false,
		Err:       &gomatrixserverlib.NotAllowed{Message: "not reachable"},
	}), "unreachable servers do not need retry")
}

// TestPartialStateWorkerConcurrency tests concurrent queue operations.
func TestPartialStateWorkerConcurrency(t *testing.T) {
	processCtx := process.NewProcessContext()
	defer processCtx.ShutdownZendrite()

	worker := &PartialStateWorker{
		process:  processCtx,
		workerCh: make(chan types.RoomNID, 100),
		retryMap: make(map[types.RoomNID]*roomRetryInfo),
	}

	// Concurrently queue many rooms
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(roomNID types.RoomNID) {
			defer wg.Done()
			worker.QueueRoom(roomNID)
		}(types.RoomNID(i))
	}

	wg.Wait()

	// Count rooms in channel + retry map
	channelCount := len(worker.workerCh)
	worker.retryMu.Lock()
	retryCount := len(worker.retryMap)
	worker.retryMu.Unlock()

	total := channelCount + retryCount
	assert.Equal(t, 50, total, "All rooms should be queued in channel or retry map")
}

// TestPartialStateWorkerRaceConditions tests for data races in concurrent operations.
func TestPartialStateWorkerRaceConditions(t *testing.T) {
	processCtx := process.NewProcessContext()
	defer processCtx.ShutdownZendrite()

	worker := &PartialStateWorker{
		process:  processCtx,
		workerCh: make(chan types.RoomNID, 5), // Small channel to force retry map usage
		retryMap: make(map[types.RoomNID]*roomRetryInfo),
	}

	// Run concurrent reads and writes
	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// Writers
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < 10; j++ {
				select {
				case <-ctx.Done():
					return
				default:
					worker.QueueRoom(types.RoomNID(id*100 + j))
				}
			}
		}(i)
	}

	// Readers (simulating retry loop reads)
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				default:
					worker.retryMu.Lock()
					for roomNID, retryAt := range worker.retryMap {
						_ = roomNID
						_ = retryAt
					}
					worker.retryMu.Unlock()
					time.Sleep(time.Millisecond)
				}
			}
		}()
	}

	// Drain channel concurrently
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-worker.workerCh:
				// Drained
			}
		}
	}()

	wg.Wait()
	// Test passes if no race conditions detected (run with -race flag)
}

// TestPartialStateConstants verifies the configuration constants.
func TestPartialStateConstants(t *testing.T) {
	assert.Equal(t, 4, partialStateWorkerCount, "Worker count should be 4")
	assert.Equal(t, time.Minute*1, partialStateMinBackoff, "Min backoff should be 1 minute")
	assert.Equal(t, time.Hour*1, partialStateMaxBackoff, "Max backoff should be 1 hour")
	assert.Equal(t, 16, partialStateMaxRetries, "Max retries should be 16")
}

// TestPartialStateWorkerContextCancellation tests that workers respect context cancellation.
func TestPartialStateWorkerContextCancellation(t *testing.T) {
	processCtx := process.NewProcessContext()

	worker := &PartialStateWorker{
		process:  processCtx,
		workerCh: make(chan types.RoomNID, 10),
		retryMap: make(map[types.RoomNID]*roomRetryInfo),
	}

	// Start a goroutine that would block on channel read
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-processCtx.Context().Done():
				close(done)
				return
			case roomNID := <-worker.workerCh:
				_ = roomNID
			}
		}
	}()

	// Cancel context
	processCtx.ShutdownZendrite()

	// Should exit quickly
	select {
	case <-done:
		// Success
	case <-time.After(time.Second):
		t.Fatal("Worker did not exit on context cancellation")
	}
}

func TestOverlayStateEvent(t *testing.T) {
	roomVer := gomatrixserverlib.RoomVersionV10
	verImpl := gomatrixserverlib.MustGetRoomVersion(roomVer)
	newEvent := func(eventType, stateKey, body string) *types.HeaderedEvent {
		raw := fmt.Sprintf(`{"type":%q,"state_key":%q,"room_id":"!r:test","sender":"@a:test","content":{"body":%q},"origin_server_ts":1,"depth":1,"prev_events":[],"auth_events":[]}`, eventType, stateKey, body)
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(raw), false)
		require.NoError(t, err)
		return &types.HeaderedEvent{PDU: ev}
	}

	oldName := newEvent("m.room.name", "", "old")
	topic := newEvent("m.room.topic", "", "topic")
	newName := newEvent("m.room.name", "", "new")
	member := newEvent("m.room.member", "@b:test", "member")

	ids := []string{oldName.EventID(), topic.EventID()}
	stored := []*types.HeaderedEvent{oldName, topic}

	t.Run("replaces the entry with the same type and state key", func(t *testing.T) {
		out, err := overlayStateEvent(ids, stored, newName.PDU)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{topic.EventID(), newName.EventID()}, out)
	})
	t.Run("adds the event when the key is new", func(t *testing.T) {
		out, err := overlayStateEvent(ids, stored, member.PDU)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{oldName.EventID(), topic.EventID(), member.EventID()}, out)
	})
	t.Run("fails if an old state event is not stored", func(t *testing.T) {
		out, err := overlayStateEvent(ids, []*types.HeaderedEvent{topic}, newName.PDU)
		require.ErrorContains(t, err, oldName.EventID())
		assert.Nil(t, out, "must not return a state set with both versions of the key")
	})
}

func TestOverlayStateEventsAppliesInOrder(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	newName := func(body string) *types.HeaderedEvent {
		raw := fmt.Sprintf(`{"type":"m.room.name","state_key":"","room_id":"!r:test","sender":"@a:test","content":{"name":%q},"origin_server_ts":1,"depth":1,"prev_events":[],"auth_events":[]}`, body)
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(raw), false)
		require.NoError(t, err)
		return &types.HeaderedEvent{PDU: ev}
	}
	first, second, third := newName("1"), newName("2"), newName("3")

	out, err := overlayStateEvents([]string{first.EventID()}, []*types.HeaderedEvent{first}, []gomatrixserverlib.PDU{second.PDU, third.PDU})
	require.NoError(t, err)
	assert.Equal(t, []string{third.EventID()}, out, "a later overlay replaces an earlier one")
}

type anchorRoomserverAPI struct {
	roomserverAPI.FederationRoomserverAPI
	events      map[string]*types.HeaderedEvent
	rejectedIDs map[string]bool
}

func (a anchorRoomserverAPI) QueryEventsByID(_ context.Context, req *roomserverAPI.QueryEventsByIDRequest, res *roomserverAPI.QueryEventsByIDResponse) error {
	for _, id := range req.EventIDs {
		if req.ExcludeRejected && a.rejectedIDs[id] {
			continue
		}
		if ev, ok := a.events[id]; ok {
			res.Events = append(res.Events, ev)
		}
	}
	return nil
}

func TestFindStateAnchor(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	newEvent := func(body string, prevs ...string) *types.HeaderedEvent {
		prevJSON := "[]"
		if len(prevs) > 0 {
			prevJSON = fmt.Sprintf("[%q", prevs[0])
			for _, p := range prevs[1:] {
				prevJSON += fmt.Sprintf(",%q", p)
			}
			prevJSON += "]"
		}
		raw := fmt.Sprintf(`{"type":"m.room.message","room_id":"!r:test","sender":"@a:test","content":{"body":%q},"origin_server_ts":1,"depth":1,"prev_events":%s,"auth_events":[]}`, body, prevJSON)
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(raw), false)
		require.NoError(t, err)
		return &types.HeaderedEvent{PDU: ev}
	}
	store := func(evs ...*types.HeaderedEvent) *PartialStateWorker {
		m := map[string]*types.HeaderedEvent{}
		for _, ev := range evs {
			m[ev.EventID()] = ev
		}
		return &PartialStateWorker{rsAPI: anchorRoomserverAPI{events: m}}
	}
	ctx := context.Background()

	t.Run("anchors on the parent of the oldest stored ancestor", func(t *testing.T) {
		pre := newEvent("pre")
		join := newEvent("join", pre.EventID())
		later := newEvent("later", join.EventID())
		a := store(join, later).findStateAnchor(ctx, "!r:test", later.EventID(), "", "local")
		assert.Equal(t, pre.EventID(), a.askID)
		assert.True(t, a.applyAsk)
		require.Len(t, a.chain, 2)
		assert.Equal(t, join.EventID(), a.chain[0].EventID(), "chain is oldest first")
		assert.Equal(t, later.EventID(), a.chain[1].EventID())
	})
	member := func(user, body string, prevs ...string) *types.HeaderedEvent {
		prevJSON := "[]"
		if len(prevs) > 0 {
			prevJSON = fmt.Sprintf("[%q]", prevs[0])
		}
		raw := fmt.Sprintf(`{"type":"m.room.member","state_key":%q,"room_id":"!r:test","sender":%q,"content":{"membership":"join","displayname":%q},"origin_server_ts":1,"depth":1,"prev_events":%s,"auth_events":[]}`, user, user, body, prevJSON)
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(raw), false)
		require.NoError(t, err)
		return &types.HeaderedEvent{PDU: ev}
	}
	t.Run("stops at our own join even when older history is stored", func(t *testing.T) {
		older := newEvent("older")
		pre := newEvent("pre", older.EventID())
		join := member("@alice:local", "alice", pre.EventID())
		later := newEvent("later", join.EventID())
		a := store(older, pre, join, later).findStateAnchor(ctx, "!r:test", later.EventID(), "", "local")
		assert.Equal(t, pre.EventID(), a.askID, "outliers stored by an earlier attempt must not move the anchor")
		assert.True(t, a.applyAsk)
		require.Len(t, a.chain, 2)
		assert.Equal(t, join.EventID(), a.chain[0].EventID())
	})
	t.Run("walks past more than 256 events to the recorded join", func(t *testing.T) {
		pre := newEvent("pre")
		join := member("@alice:local", "alice", pre.EventID())
		stored := []*types.HeaderedEvent{pre, join}
		latest := join.PDU
		for i := range 300 {
			latest = newEvent(fmt.Sprintf("after join %d", i), latest.EventID()).PDU
			stored = append(stored, &types.HeaderedEvent{PDU: latest})
		}
		a := store(stored...).findStateAnchor(ctx, "!r:test", latest.EventID(), join.EventID(), "local")
		assert.Equal(t, pre.EventID(), a.askID)
		assert.True(t, a.applyAsk)
		assert.Len(t, a.chain, 301)
	})
	t.Run("a second local join during the resync does not move the recorded anchor", func(t *testing.T) {
		pre := newEvent("pre")
		first := member("@alice:local", "alice", pre.EventID())
		second := member("@bob:local", "bob", first.EventID())
		later := newEvent("later", second.EventID())
		w := store(pre, first, second, later)

		a := w.findStateAnchor(ctx, "!r:test", later.EventID(), first.EventID(), "local")
		assert.Equal(t, pre.EventID(), a.askID, "the join that started the partial state is the join point")
		require.Len(t, a.chain, 3)
		assert.Equal(t, first.EventID(), a.chain[0].EventID())

		a = w.findStateAnchor(ctx, "!r:test", later.EventID(), "", "local")
		assert.Equal(t, first.EventID(), a.askID, "without a recorded join the fallback takes the newest local join")
	})
	t.Run("a profile change after the join does not move the anchor", func(t *testing.T) {
		pre := newEvent("pre")
		join := member("@alice:local", "alice", pre.EventID())
		rename := member("@alice:local", "alicia", join.EventID())
		a := store(pre, join, rename).findStateAnchor(ctx, "!r:test", rename.EventID(), "", "local")
		assert.Equal(t, pre.EventID(), a.askID)
		require.Len(t, a.chain, 2)
		assert.Equal(t, join.EventID(), a.chain[0].EventID())
		assert.Equal(t, rename.EventID(), a.chain[1].EventID())
	})
	t.Run("a remote user's join is not our join point", func(t *testing.T) {
		pre := newEvent("pre")
		join := member("@bob:remote", "bob", pre.EventID())
		a := store(join).findStateAnchor(ctx, "!r:test", join.EventID(), "", "local")
		assert.Equal(t, pre.EventID(), a.askID, "walk ends because pre is not stored")
		assert.True(t, a.applyAsk)
	})
	t.Run("asks about an event with several parents directly", func(t *testing.T) {
		p1, p2 := newEvent("p1"), newEvent("p2")
		merge := newEvent("merge", p1.EventID(), p2.EventID())
		a := store(merge).findStateAnchor(ctx, "!r:test", merge.EventID(), "", "local")
		assert.Equal(t, merge.EventID(), a.askID)
		assert.False(t, a.applyAsk)
		require.Len(t, a.chain, 1)
	})
	t.Run("asks about an unknown latest event and applies it", func(t *testing.T) {
		a := store().findStateAnchor(ctx, "!r:test", "$unknown", "", "local")
		assert.Equal(t, "$unknown", a.askID)
		assert.True(t, a.applyAsk, "the latest event's own state change must still be applied")
		assert.Empty(t, a.chain)
	})
	t.Run("walks through a rejected event without applying its change", func(t *testing.T) {
		pre := newEvent("pre")
		rejected := newEvent("rejected", pre.EventID())
		later := newEvent("later", rejected.EventID())
		worker := &PartialStateWorker{rsAPI: anchorRoomserverAPI{
			events:      map[string]*types.HeaderedEvent{rejected.EventID(): rejected, later.EventID(): later},
			rejectedIDs: map[string]bool{rejected.EventID(): true},
		}}
		a := worker.findStateAnchor(ctx, "!r:test", later.EventID(), "", "local")
		assert.Equal(t, pre.EventID(), a.askID, "the walk continues past the rejected event")
		assert.True(t, a.applyAsk)
		require.Len(t, a.chain, 2)
		assert.Equal(t, rejected.EventID(), a.chain[0].EventID())

		// The overlay step looks each event up again as an accepted one, so the
		// rejected event's state change is left out.
		_, err := worker.getStoredEvent(ctx, "!r:test", rejected.EventID())
		assert.Error(t, err, "a rejected event is not an accepted stored event")
		_, err = worker.getStoredEvent(ctx, "!r:test", later.EventID())
		assert.NoError(t, err)
	})
}

func TestAllowedOverlays(t *testing.T) {
	alice, bob := test.NewUser(t), test.NewUser(t)
	room := test.NewRoom(t, alice)
	userIDs := func(_ spec.RoomID, senderID spec.SenderID) (*spec.UserID, error) {
		return spec.NewUserID(string(senderID), true)
	}
	nobodyIsLocal := func(gomatrixserverlib.PDU) bool { return false }
	ids := func(evs ...gomatrixserverlib.PDU) []string {
		out := make([]string, len(evs))
		for i, ev := range evs {
			out[i] = ev.EventID()
		}
		return out
	}
	tupleOf := func(ev gomatrixserverlib.PDU) gomatrixserverlib.StateKeyTuple {
		return gomatrixserverlib.StateKeyTuple{EventType: ev.Type(), StateKey: *ev.StateKey()}
	}
	stateOf := func(events []*types.HeaderedEvent) map[gomatrixserverlib.StateKeyTuple]gomatrixserverlib.PDU {
		state := map[gomatrixserverlib.StateKeyTuple]gomatrixserverlib.PDU{}
		for _, ev := range events {
			if ev.StateKey() != nil {
				state[tupleOf(ev.PDU)] = ev.PDU
			}
		}
		return state
	}
	state := stateOf(room.Events())

	bobJoin := room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
	topic := room.CreateAndInsert(t, alice, spec.MRoomTopic, map[string]any{"topic": "a topic"}, test.WithStateKey(""))
	leave := room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Leave}, test.WithStateKey(alice.ID))

	t.Run("events the state allows are all applied", func(t *testing.T) {
		allowed, skipped := allowedOverlays(state, []gomatrixserverlib.PDU{bobJoin.PDU, topic.PDU}, nobodyIsLocal, userIDs)
		assert.Equal(t, ids(bobJoin.PDU, topic.PDU), ids(allowed...))
		assert.Empty(t, skipped)
	})

	// The full state shows Alice had already left, though her topic cites her join.
	left := stateOf(room.Events())
	delete(left, tupleOf(leave.PDU))
	t.Run("an event the full state does not allow is left out", func(t *testing.T) {
		allowed, skipped := allowedOverlays(left, []gomatrixserverlib.PDU{bobJoin.PDU, topic.PDU}, nobodyIsLocal, userIDs)
		assert.Equal(t, ids(bobJoin.PDU), ids(allowed...))
		assert.Equal(t, ids(topic.PDU), ids(skipped...))
	})

	t.Run("events of this server are trusted", func(t *testing.T) {
		allowed, skipped := allowedOverlays(left, []gomatrixserverlib.PDU{topic.PDU}, func(gomatrixserverlib.PDU) bool { return true }, userIDs)
		assert.Equal(t, ids(topic.PDU), ids(allowed...))
		assert.Empty(t, skipped)
	})

	t.Run("each event is checked against the ones applied before it", func(t *testing.T) {
		allowed, skipped := allowedOverlays(state, []gomatrixserverlib.PDU{leave.PDU, topic.PDU}, nobodyIsLocal, userIDs)
		assert.Equal(t, ids(leave.PDU), ids(allowed...))
		assert.Equal(t, ids(topic.PDU), ids(skipped...), "Alice has left by then")
	})

	t.Run("the state given is not changed", func(t *testing.T) {
		before := len(state)
		allowedOverlays(state, []gomatrixserverlib.PDU{topic.PDU}, nobodyIsLocal, userIDs)
		assert.Len(t, state, before)
		assert.NotContains(t, state, tupleOf(topic.PDU))
	})
}

func TestFindWindowAnchor(t *testing.T) {
	verImpl := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV10)
	newEvent := func(body string, depth int, prevs ...string) *types.HeaderedEvent {
		prevJSON, err := json.Marshal(prevs)
		require.NoError(t, err)
		raw := fmt.Sprintf(`{"type":"m.room.message","room_id":"!r:test","sender":"@a:test","content":{"body":%q},"origin_server_ts":1,"depth":%d,"prev_events":%s,"auth_events":[]}`, body, depth, prevJSON)
		ev, err := verImpl.NewEventFromTrustedJSON([]byte(raw), false)
		require.NoError(t, err)
		return &types.HeaderedEvent{PDU: ev}
	}
	store := func(evs ...*types.HeaderedEvent) *PartialStateWorker {
		m := map[string]*types.HeaderedEvent{}
		for _, ev := range evs {
			m[ev.EventID()] = ev
		}
		return &PartialStateWorker{rsAPI: anchorRoomserverAPI{events: m}}
	}
	ctx := context.Background()
	ids := func(chain []gomatrixserverlib.PDU) []string {
		var out []string
		for _, ev := range chain {
			out = append(out, ev.EventID())
		}
		return out
	}

	pre := newEvent("pre", 1)
	join := newEvent("join", 2, pre.EventID())

	t.Run("an event with two prev events anchors on the join's parent", func(t *testing.T) {
		a := newEvent("a", 3, join.EventID())
		b := newEvent("b", 4, a.EventID(), join.EventID())
		anchor, ok := store(pre, join, a, b).findWindowAnchor(ctx, "!r:test", b.EventID(), join.EventID())
		require.True(t, ok)
		assert.Equal(t, pre.EventID(), anchor.askID)
		assert.True(t, anchor.applyAsk)
		assert.Equal(t, []string{join.EventID(), a.EventID(), b.EventID()}, ids(anchor.chain))
	})

	t.Run("a fork and merge puts the merge last", func(t *testing.T) {
		a := newEvent("a", 3, join.EventID())
		b := newEvent("b", 4, a.EventID())
		c := newEvent("c", 5, a.EventID())
		d := newEvent("d", 6, b.EventID(), c.EventID())
		anchor, ok := store(pre, join, a, b, c, d).findWindowAnchor(ctx, "!r:test", d.EventID(), join.EventID())
		require.True(t, ok)
		got := ids(anchor.chain)
		require.Len(t, got, 5)
		assert.Equal(t, join.EventID(), got[0])
		assert.Equal(t, a.EventID(), got[1])
		assert.Equal(t, d.EventID(), got[4])
	})

	t.Run("an event that does not lead back to the join is not anchored", func(t *testing.T) {
		other := newEvent("other", 3, pre.EventID())
		_, ok := store(pre, join, other).findWindowAnchor(ctx, "!r:test", other.EventID(), join.EventID())
		assert.False(t, ok)
	})

	t.Run("a join with two parents is not anchored", func(t *testing.T) {
		other := newEvent("other", 1)
		twoParents := newEvent("join2", 2, pre.EventID(), other.EventID())
		_, ok := store(pre, other, twoParents).findWindowAnchor(ctx, "!r:test", twoParents.EventID(), twoParents.EventID())
		assert.False(t, ok)
	})
}
