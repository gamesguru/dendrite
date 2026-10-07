// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"sync"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	log "github.com/sirupsen/logrus"

	"codefloe.com/pat-s/zendrite/federationapi/types"
	"codefloe.com/pat-s/zendrite/roomserver/api"
)

// stickySweepInterval is how often the tracker drops the expired sticky events
// of rooms that have seen no sticky events since.
const stickySweepInterval = time.Hour

// stickyTracker remembers this server's unexpired MSC4354 sticky events in
// each room, so that they can be pushed to servers that join the room later.
// It lives in memory, so the sticky events sent before a restart are not
// pushed to servers joining after it: pushing is best effort, as the MSC
// allows, and these servers receive later sticky events as usual.
type stickyTracker struct {
	mu        sync.Mutex
	rooms     map[string][]stickyRef
	lastSweep time.Time
}

type stickyRef struct {
	eventID   string
	origin    spec.ServerName
	expiresAt time.Time
}

func newStickyTracker() *stickyTracker {
	return &stickyTracker{rooms: make(map[string][]stickyRef)}
}

// add remembers a sticky event of this server until it expires.
func (t *stickyTracker) add(roomID string, ref stickyRef, now time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if now.Sub(t.lastSweep) >= stickySweepInterval {
		for id := range t.rooms {
			t.pruneLocked(id, now)
		}
		t.lastSweep = now
	}
	refs := t.pruneLocked(roomID, now)
	for _, existing := range refs {
		if existing.eventID == ref.eventID {
			return
		}
	}
	t.rooms[roomID] = append(refs, ref)
}

// unexpired returns the sticky events of this server in the room that are
// unexpired at now, oldest first.
func (t *stickyTracker) unexpired(roomID string, now time.Time) []stickyRef {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]stickyRef(nil), t.pruneLocked(roomID, now)...)
}

// forget drops the sticky events of a room, as when it is purged.
func (t *stickyTracker) forget(roomID string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.rooms, roomID)
}

func (t *stickyTracker) pruneLocked(roomID string, now time.Time) []stickyRef {
	refs := t.rooms[roomID]
	kept := refs[:0]
	for _, ref := range refs {
		if ref.expiresAt.After(now) {
			kept = append(kept, ref)
		}
	}
	if len(kept) == 0 {
		delete(t.rooms, roomID)
		return nil
	}
	t.rooms[roomID] = kept
	return kept
}

// trackStickyEvent remembers the event if it is an unexpired sticky event
// that this server sent.
func (s *OutputRoomEventConsumer) trackStickyEvent(ore api.OutputNewRoomEvent) {
	if s.sticky == nil {
		return
	}
	origin := spec.ServerName(ore.SendAsServer)
	if !s.cfg.Matrix.IsLocalServerName(origin) {
		return
	}
	now := time.Now()
	if !ore.Event.IsSticky(now, now) {
		return
	}
	s.sticky.add(ore.Event.RoomID().String(), stickyRef{
		eventID:   ore.Event.EventID(),
		origin:    origin,
		expiresAt: ore.Event.StickyEndTime(now),
	}, now)
}

// pushStickyEvents sends this server's unexpired sticky events in the room to
// the servers that the event made join it, as MSC4354 asks. Failures are
// logged: like the rest of the push, sending them is best effort.
func (s *OutputRoomEventConsumer) pushStickyEvents(roomID string, oldJoinedHosts, addsJoinedHosts []types.JoinedHost) {
	if s.sticky == nil || len(addsJoinedHosts) == 0 {
		return
	}
	now := time.Now()
	refs := s.sticky.unexpired(roomID, now)
	if len(refs) == 0 {
		return
	}
	joined := make(map[spec.ServerName]struct{}, len(oldJoinedHosts))
	for _, host := range oldJoinedHosts {
		joined[host.ServerName] = struct{}{}
	}
	var newServers []spec.ServerName
	for _, host := range addsJoinedHosts {
		if _, ok := joined[host.ServerName]; ok || s.cfg.Matrix.IsLocalServerName(host.ServerName) {
			continue
		}
		joined[host.ServerName] = struct{}{}
		newServers = append(newServers, host.ServerName)
	}
	if len(newServers) == 0 {
		return
	}

	eventIDs := make([]string, len(refs))
	for i, ref := range refs {
		eventIDs[i] = ref.eventID
	}
	var res api.QueryEventsByIDResponse
	if err := s.rsAPI.QueryEventsByID(s.ctx, &api.QueryEventsByIDRequest{RoomID: roomID, EventIDs: eventIDs}, &res); err != nil {
		log.WithError(err).WithField("room_id", roomID).Warn("Failed to load sticky events for newly joined servers")
		return
	}
	byID := make(map[string]int, len(res.Events))
	for i, ev := range res.Events {
		byID[ev.EventID()] = i
	}
	for _, ref := range refs {
		i, ok := byID[ref.eventID]
		// A redacted event is no longer sticky.
		if !ok || !res.Events[i].IsSticky(now, now) {
			continue
		}
		if err := s.queues.SendEvent(res.Events[i], ref.origin, newServers); err != nil {
			log.WithError(err).WithFields(log.Fields{
				"room_id":  roomID,
				"event_id": ref.eventID,
			}).Warn("Failed to push a sticky event to newly joined servers")
		}
	}
}
