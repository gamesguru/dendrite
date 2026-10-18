package tables

import (
	"database/sql"
	"encoding/json"

	"codefloe.com/pat-s/zendrite/internal/eventutil"
)

// MarshalThreadCounts encodes a notification count's per-thread breakdown
// for the thread_counts column, or nil when there is none.
func MarshalThreadCounts(data *eventutil.NotificationData) (any, error) {
	if data.Threads == nil {
		return nil, nil
	}
	raw, err := json.Marshal(data.Threads)
	if err != nil {
		return nil, err
	}
	return string(raw), nil
}

// UnmarshalNotificationData rebuilds stored notification counts.
func UnmarshalNotificationData(roomID string, notificationCount, highlightCount int, threads sql.NullString) (*eventutil.NotificationData, error) {
	data := &eventutil.NotificationData{
		RoomID:                  roomID,
		UnreadNotificationCount: notificationCount,
		UnreadHighlightCount:    highlightCount,
	}
	if threads.Valid {
		if err := json.Unmarshal([]byte(threads.String), &data.Threads); err != nil {
			return nil, err
		}
	}
	return data, nil
}
