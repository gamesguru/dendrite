package streams

import (
	"context"

	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/internal/eventutil"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
)

type NotificationDataStreamProvider struct {
	DefaultStreamProvider
}

func (p *NotificationDataStreamProvider) Setup(
	ctx context.Context, snapshot storage.DatabaseTransaction,
) {
	p.DefaultStreamProvider.Setup(ctx, snapshot)

	p.latestMutex.Lock()
	defer p.latestMutex.Unlock()

	id, err := snapshot.MaxStreamPositionForNotificationData(ctx)
	if err != nil {
		panic(err)
	}
	p.latest = id
}

func (p *NotificationDataStreamProvider) CompleteSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
) types.StreamPosition {
	return p.IncrementalSync(ctx, snapshot, req, 0, p.LatestPosition(ctx))
}

func (p *NotificationDataStreamProvider) IncrementalSync(
	ctx context.Context,
	snapshot storage.DatabaseTransaction,
	req *types.SyncRequest,
	from, _ types.StreamPosition,
) types.StreamPosition {
	// Read the latest committed position from this snapshot: it bounds the
	// changed-room query below and is the cursor handed back, so the token never
	// covers data the response could not see.
	latest, err := snapshot.MaxStreamPositionForNotificationData(ctx)
	if err != nil {
		req.Log.WithError(err).Error("MaxStreamPositionForNotificationData failed")
		return from
	}
	if latest < from {
		latest = from
	}

	// A count can change with no event in the window, for example when its
	// notification row lands after the event it belongs to was already synced,
	// or when a read receipt clears it. Add those joined rooms to the response so
	// the client receives the new count before the cursor moves past it. A
	// complete sync already includes every joined room.
	if from > 0 && latest > from {
		changed, err := snapshot.GetUserNotificationRoomsAfter(ctx, req.Device.UserID, from, latest)
		if err != nil {
			req.Log.WithError(err).Error("GetUserNotificationRoomsAfter failed")
			return from
		}
		if len(changed) > 0 {
			// req.Rooms only holds rooms with a PDU in this window, which is
			// exactly what a count-only change lacks. Check current membership.
			joined, err := snapshot.RoomIDsWithMembership(ctx, req.Device.UserID, spec.Join)
			if err != nil {
				req.Log.WithError(err).Error("RoomIDsWithMembership failed")
				return from
			}
			joinedSet := make(map[string]struct{}, len(joined))
			for _, roomID := range joined {
				joinedSet[roomID] = struct{}{}
			}
			for _, roomID := range changed {
				if _, ok := joinedSet[roomID]; !ok {
					continue
				}
				if _, ok := req.Response.Rooms.Join[roomID]; !ok {
					jr := types.NewJoinResponse()
					jr.UnreadNotificationsChanged = true
					req.Response.Rooms.Join[roomID] = jr
				}
				// The counts lookup below only reads rooms marked joined.
				req.Rooms[roomID] = spec.Join
			}
		}
	}

	// Get the unread notifications for rooms in our join response.
	// This is to ensure clients always have an unread notification section
	// and can display the correct numbers.
	countsByRoom, err := snapshot.GetUserUnreadNotificationCountsForRooms(ctx, req.Device.UserID, req.Rooms)
	if err != nil {
		req.Log.WithError(err).Error("GetUserUnreadNotificationCountsForRooms failed")
		return from
	}

	// With per-thread counts, the room's counts cover only the main timeline.
	byThread := req.Filter.Room.Timeline.UnreadThreadNotifications

	// We're merely decorating existing rooms.
	for roomID, jr := range req.Response.Rooms.Join {
		counts := countsByRoom[roomID]
		if counts == nil {
			counts = &eventutil.NotificationData{}
		}
		jr.UnreadNotifications = &types.UnreadNotifications{
			HighlightCount:    counts.UnreadHighlightCount,
			NotificationCount: counts.UnreadNotificationCount,
		}
		if byThread {
			main, threads := counts.SplitByThread()
			jr.UnreadNotifications = &types.UnreadNotifications{
				HighlightCount:    main.UnreadHighlightCount,
				NotificationCount: main.UnreadNotificationCount,
			}
			jr.UnreadThreadNotifications = make(map[string]*types.UnreadNotifications, len(threads))
			for threadID, thread := range threads {
				jr.UnreadThreadNotifications[threadID] = &types.UnreadNotifications{
					HighlightCount:    thread.UnreadHighlightCount,
					NotificationCount: thread.UnreadNotificationCount,
				}
			}
		}
		req.Response.Rooms.Join[roomID] = jr
	}

	return latest
}
