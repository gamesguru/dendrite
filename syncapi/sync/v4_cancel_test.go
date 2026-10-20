package sync

import (
	"context"
	"fmt"
	"testing"
	"time"

	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

func TestSlidingSyncInterruptCancellation(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		canceled, expired         bool
		snapshotErr               error
		wantSnapshots, wantErrors int
	}{
		{name: "disconnect", canceled: true},
		{name: "expired deadline", expired: true},
		{name: "snapshot cancellation", snapshotErr: fmt.Errorf("snapshot: %w", context.Canceled), wantSnapshots: 1},
		{name: "snapshot deadline", snapshotErr: context.DeadlineExceeded, wantSnapshots: 1},
		{name: "genuine snapshot failure", snapshotErr: errBrokenDatabase, wantSnapshots: 1, wantErrors: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := logrustest.NewGlobal()
			defer hook.Reset()
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			if tc.canceled {
				cancel(context.Canceled)
			}
			if tc.expired {
				var deadlineCancel context.CancelFunc
				ctx, deadlineCancel = context.WithDeadline(ctx, time.Now().Add(-time.Second))
				defer deadlineCancel()
			}
			db := &snapshotCountingDB{err: tc.snapshotErr}
			rp := &RequestPool{db: db}
			since := &types.SlidingSyncStreamToken{}
			req := &types.SlidingSyncRequest{}
			if tc.canceled || tc.expired {
				req.Lists = map[string]types.SlidingListConfig{"all_rooms": {}}
			}
			res := rp.buildInterruptResponse(ctx, req, &userapi.Device{UserID: "@a:test"}, nil, since, types.StreamingToken{}, 0, "timeout")
			require.Equal(t, since.String(), res.Pos)
			require.Empty(t, res.Rooms)
			require.Equal(t, tc.wantSnapshots, db.snapshotCount())
			require.Equal(t, tc.wantErrors, countErrors(hook, "Failed"))
		})
	}
}
