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
	to := types.StreamPosition(p.EDUCache.GetLatestSyncPosition())
	_, _ = p.addTypingEvents(req, 0)
	return to
}

func (p *TypingStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	if added, _ := p.addTypingEvents(req, from); added {
		return to
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
	if added, oldest := p.addTypingEvents(req, from); added {
		return p.positionAfterEvents(to, from, oldest)
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
			if added, oldest := p.addTypingEvents(req, from); added {
				return p.positionAfterEvents(to, from, oldest)
			}
		}
	}
}

// positionAfterEvents returns the position to report once typing events were
// added. The position read before the events is safe to report: every update
// the response lacks happened after it. It can sit at or below from when an
// update landed between the two reads, and the client would then be handed a
// position that did not advance. In that case use oldest, the lowest room
// position among the events returned. It is above from, and a lower value only
// redelivers an event, which is harmless for ephemeral typing; the highest
// position could skip an update in another room.
func (p *TypingStreamProvider) positionAfterEvents(to, from, oldest types.StreamPosition) types.StreamPosition {
	if to > from {
		return to
	}
	return oldest
}

func (p *TypingStreamProvider) addTypingEvents(
	req *types.SyncRequest,
	from types.StreamPosition,
) (bool, types.StreamPosition) {
	var err error
	added := false
	var oldest types.StreamPosition
	for roomID, membership := range req.Rooms {
		if membership != spec.Join {
			continue
		}

		jr, ok := req.Response.Rooms.Join[roomID]
		if !ok {
			jr = types.NewJoinResponse()
		}

		if users, roomPosition, updated := p.EDUCache.GetTypingUsersAndPositionIfUpdatedAfter(
			roomID, int64(from),
		); updated {
			if pos := types.StreamPosition(roomPosition); oldest == 0 || pos < oldest {
				oldest = pos
			}
			typingUsers := make([]string, 0, len(users))
			for i := range users {
				// skip ignored user events
				if _, ok := req.IgnoredUsers.List[users[i]]; !ok {
					typingUsers = append(typingUsers, users[i])
				}
			}
			ev := synctypes.ClientEvent{
				Type: spec.MTyping,
			}
			ev.Content, err = json.Marshal(map[string]any{
				"user_ids": typingUsers,
			})
			if err != nil {
				req.Log.WithError(err).Error("json.Marshal failed")
				return added, oldest
			}

			jr.Ephemeral.Events = append(jr.Ephemeral.Events, ev)
			req.Response.Rooms.Join[roomID] = jr
			added = true
		}
	}
	return added, oldest
}
