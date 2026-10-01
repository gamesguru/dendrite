package eventutil

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSplitByThread(t *testing.T) {
	data := NotificationData{
		UnreadNotificationCount: 5, UnreadHighlightCount: 2,
		Threads: map[string]ThreadNotificationCounts{
			MainThread: {UnreadNotificationCount: 2, UnreadHighlightCount: 1},
			"$a":       {UnreadNotificationCount: 3, UnreadHighlightCount: 1},
			"$read":    {},
		},
	}
	main, threads := data.SplitByThread()
	require.Equal(t, ThreadNotificationCounts{UnreadNotificationCount: 2, UnreadHighlightCount: 1}, main)
	require.Equal(t, map[string]ThreadNotificationCounts{"$a": {UnreadNotificationCount: 3, UnreadHighlightCount: 1}}, threads)

	// Totals written without a matching breakdown, as by a build that does not
	// count by thread, attribute everything to the main timeline.
	data.UnreadNotificationCount = 6
	main, threads = data.SplitByThread()
	require.Equal(t, ThreadNotificationCounts{UnreadNotificationCount: 6, UnreadHighlightCount: 2}, main)
	require.Empty(t, threads)
	main, threads = (&NotificationData{UnreadNotificationCount: 1}).SplitByThread()
	require.Equal(t, 1, main.UnreadNotificationCount)
	require.Empty(t, threads)
	main, threads = (&NotificationData{}).SplitByThread()
	require.Zero(t, main)
	require.Empty(t, threads)
}
