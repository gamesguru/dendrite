// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"testing"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

// The notifier advances after the consumer commits, so a sliding sync request
// can arrive while it is behind events that are already stored, such as a leave
// the client has just made. The response must read up to the committed events.
func TestCommittedPDUPositionCoversEventsAheadOfNotifier(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		db, err := storage.NewSyncServerDatasource(processCtx.Context(), cm, &cfg.SyncAPI.Database)
		require.NoError(t, err)

		ctx := t.Context()
		alice := test.NewUser(t)
		var last types.StreamPosition
		for _, ev := range test.NewRoom(t, alice).Events() {
			var addState []string
			if ev.StateKey() != nil {
				addState = []string{ev.EventID()}
			}
			last, err = db.WriteEvent(ctx, ev, nil, addState, nil, nil, false, gomatrixserverlib.HistoryVisibilityShared)
			require.NoError(t, err)
		}
		require.Greater(t, last, types.StreamPosition(1))

		rp := &RequestPool{db: db}
		assert.Equal(t, last, rp.committedPDUPosition(ctx, last-1), "a notifier behind the database is moved up to the committed events")
		assert.Equal(t, last+5, rp.committedPDUPosition(ctx, last+5), "a notifier ahead of the database is kept")
	})
}

