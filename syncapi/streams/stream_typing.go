package streams

import (
	"context"
	"encoding/json"
	"time"

	"github.com/element-hq/dendrite/internal/caching"
	"github.com/element-hq/dendrite/syncapi/storage"
	"github.com/element-hq/dendrite/syncapi/synctypes"
	"github.com/element-hq/dendrite/syncapi/types"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

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
	p.addTypingEvents(req, 0)
	return to
}

func (p *TypingStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	if p.addTypingEvents(req, from) {
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
	if p.addTypingEvents(req, from) {
		return to
	}
	if allowImmediate && req.Timeout <= 0 {
		return to
	}

	wait := 50 * time.Millisecond
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
			if p.addTypingEvents(req, from) {
				return to
			}
		}
	}
}

func (p *TypingStreamProvider) addTypingEvents(
	req *types.SyncRequest,
	from types.StreamPosition,
) bool {
	var err error
	added := false
	for roomID, membership := range req.Rooms {
		if membership != spec.Join {
			continue
		}

		jr, ok := req.Response.Rooms.Join[roomID]
		if !ok {
			jr = types.NewJoinResponse()
		}

		if users, updated := p.EDUCache.GetTypingUsersIfUpdatedAfter(
			roomID, int64(from),
		); updated {
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
			ev.Content, err = json.Marshal(map[string]interface{}{
				"user_ids": typingUsers,
			})
			if err != nil {
				req.Log.WithError(err).Error("json.Marshal failed")
				return added
			}

			jr.Ephemeral.Events = append(jr.Ephemeral.Events, ev)
			req.Response.Rooms.Join[roomID] = jr
			added = true
		}
	}
	return added
}
