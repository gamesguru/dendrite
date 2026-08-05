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
	to := p.LatestPosition(ctx)
	if p.addTypingEvents(req, 0) {
		return to
	}

	timer := time.NewTimer(50 * time.Millisecond)
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
			if p.addTypingEvents(req, 0) {
				return p.LatestPosition(ctx)
			}
		}
	}
}

func (p *TypingStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, to types.StreamPosition,
) types.StreamPosition {
	p.addTypingEvents(req, from)
	return to
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
