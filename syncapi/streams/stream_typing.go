package streams

import (
	"context"
	"encoding/json"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

// typingWaitInterval bounds how long a sync waits for typing state to arrive.
// It is a variable so tests can widen the window instead of racing the timer.
var typingWaitInterval = 50 * time.Millisecond

type TypingStreamProvider struct {
	DefaultStreamProvider
	EDUCache *caching.EDUCache
}

func (p *TypingStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	_, position := p.addTypingEvents(req, 0)
	return position
}

func (p *TypingStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	if added, latest := p.addTypingEvents(req, from); added {
		return latest
	}
	if to > from {
		return to
	}
	return p.waitForTypingEvents(ctx, req, from, true)
}

func (p *TypingStreamProvider) waitForTypingEvents(
	ctx context.Context,
	req *types.SyncRequest,
	from types.StreamPosition,
	allowImmediate bool,
) types.StreamPosition {
	to := types.StreamPosition(p.EDUCache.GetLatestSyncPosition())
	if added, latest := p.addTypingEvents(req, from); added {
		return latest
	}
	if allowImmediate && req.Timeout <= 0 {
		return to
	}

	wait := typingWaitInterval
	if req.Timeout > 0 && req.Timeout < wait {
		wait = req.Timeout
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	ticker := time.NewTicker(5 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return to
		case <-timer.C:
			return to
		case <-ticker.C:
			to = types.StreamPosition(p.EDUCache.GetLatestSyncPosition())
			if added, latest := p.addTypingEvents(req, from); added {
				return latest
			}
		}
	}
}

func (p *TypingStreamProvider) addTypingEvents(
	req *types.SyncRequest,
	from types.StreamPosition,
) (bool, types.StreamPosition) {
	roomIDs := make([]string, 0, len(req.Rooms))
	for roomID, membership := range req.Rooms {
		if membership == spec.Join {
			roomIDs = append(roomIDs, roomID)
		}
	}

	// Snapshot the typing state and its latest position together. A per-room
	// scan could miss an update to a room it already visited while still seeing
	// a later update in another room; reporting any position from that scan
	// would then skip the missed room. The snapshot's latest position is safe:
	// every update above it landed after the snapshot.
	typing, latest := p.EDUCache.TypingSnapshot(roomIDs, int64(from))
	latestPosition := types.StreamPosition(latest)

	added := false
	for _, roomID := range roomIDs {
		users, ok := typing[roomID]
		if !ok {
			continue
		}

		jr, ok := req.Response.Rooms.Join[roomID]
		if !ok {
			jr = types.NewJoinResponse()
		}

		typingUsers := make([]string, 0, len(users))
		for _, userID := range users {
			// skip ignored user events
			if _, ok := req.IgnoredUsers.List[userID]; !ok {
				typingUsers = append(typingUsers, userID)
			}
		}
		ev := synctypes.ClientEvent{
			Type: spec.MTyping,
		}
		var err error
		ev.Content, err = json.Marshal(map[string]any{
			"user_ids": typingUsers,
		})
		if err != nil {
			req.Log.WithError(err).Error("json.Marshal failed")
			return added, latestPosition
		}

		jr.Ephemeral.Events = append(jr.Ephemeral.Events, ev)
		req.Response.Rooms.Join[roomID] = jr
		added = true
	}
	return added, latestPosition
}
