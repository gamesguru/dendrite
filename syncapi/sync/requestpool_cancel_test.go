// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package sync

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"

	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/notifier"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/storage/shared"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// snapshotCountingDB only implements the handful of storage.Database methods
// that a /sync request reaches before it asks for its first snapshot. Every
// other method is left to the embedded nil interface and would panic if called,
// which is what we want: the test is about not getting that far.
type snapshotCountingDB struct {
	storage.Database

	mu        sync.Mutex
	snapshots int
	err       error
}

func (db *snapshotCountingDB) NewDatabaseSnapshot(ctx context.Context) (*shared.DatabaseTransaction, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	db.snapshots++
	return nil, db.err
}

func (db *snapshotCountingDB) snapshotCount() int {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.snapshots
}

func (db *snapshotCountingDB) CleanSendToDeviceUpdates(ctx context.Context, userID, deviceID string, before types.StreamPosition) error {
	return nil
}

func newCancellationTestPool(db storage.Database, device *userapi.Device) *RequestPool {
	rp := &RequestPool{
		db: db,
		cfg: &config.SyncAPI{
			Matrix: &config.Global{},
		},
		lastseen: &sync.Map{},
		presence: &sync.Map{},
		Notifier: notifier.NewNotifier(nil),
	}
	// Pretend we have already recorded this device so that OnIncomingSyncRequest
	// does not need a user API to update the last-seen timestamp.
	rp.lastseen.Store(device.UserID+device.ID, struct{}{})
	return rp
}

// nonEmptySinceToken returns a since token that makes /sync take the
// incremental path, where one snapshot is taken per stream provider.
func nonEmptySinceToken() string {
	return types.StreamingToken{
		PDUPosition:              1,
		TypingPosition:           1,
		ReceiptPosition:          1,
		SendToDevicePosition:     1,
		InvitePosition:           1,
		AccountDataPosition:      1,
		DeviceListPosition:       1,
		NotificationDataPosition: 1,
		PresencePosition:         1,
	}.String()
}

var errBrokenDatabase = errors.New("database is on fire")

// countErrors returns how many entries at error level contain substr.
func countErrors(hook *logrustest.Hook, substr string) int {
	n := 0
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.ErrorLevel && strings.Contains(entry.Message, substr) {
			n++
		}
	}
	return n
}

// A canceled /sync request used to ask for one database snapshot per stream
// provider, each of which failed with "context canceled" and was logged as its
// own error. See issue #289.
func TestSyncRequestStopsAtFirstCancellation(t *testing.T) {
	device := &userapi.Device{UserID: "@alice:localhost", ID: "05LqQkIj"}
	since := nonEmptySinceToken()

	for _, tc := range []struct {
		name string
		// cancelBeforeRequest cancels the request context before the sync runs,
		// i.e. the client went away while we were still working.
		cancelBeforeRequest bool
		snapshotErr         error
		wantSnapshots       int
	}{
		{
			name:                "context already canceled",
			cancelBeforeRequest: true,
			snapshotErr:         context.Canceled,
			wantSnapshots:       0,
		},
		{
			name:          "context canceled underneath us",
			snapshotErr:   context.Canceled,
			wantSnapshots: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hook := logrustest.NewGlobal()
			defer hook.Reset()

			db := &snapshotCountingDB{err: tc.snapshotErr}
			rp := newCancellationTestPool(db, device)

			ctx, cancel := context.WithCancelCause(context.Background())
			defer cancel(nil)
			if tc.cancelBeforeRequest {
				cancel(context.Canceled)
			}
			req := httptest.NewRequest("GET", "/_matrix/client/v3/sync?timeout=0&since="+since, nil).WithContext(ctx)

			res := rp.OnIncomingSyncRequest(req, device)
			if res.Code != 200 {
				t.Fatalf("expected 200, got %d", res.Code)
			}
			if got := db.snapshotCount(); got != tc.wantSnapshots {
				t.Errorf("expected %d snapshot attempts, got %d", tc.wantSnapshots, got)
			}
			if got := countErrors(hook, "Failed to acquire database snapshot"); got != 0 {
				t.Errorf("expected cancellation not to be logged as an error, got %d error entries", got)
			}
		})
	}
}

// A snapshot failure that is not a cancellation should still be reported, once
// per stream provider that could not get one.
func TestSyncRequestReportsRealSnapshotFailures(t *testing.T) {
	device := &userapi.Device{UserID: "@alice:localhost", ID: "05LqQkIj"}
	since := nonEmptySinceToken()

	hook := logrustest.NewGlobal()
	defer hook.Reset()

	db := &snapshotCountingDB{err: errBrokenDatabase}
	rp := newCancellationTestPool(db, device)

	req := httptest.NewRequest("GET", "/_matrix/client/v3/sync?timeout=0&since="+since, nil)

	res := rp.OnIncomingSyncRequest(req, device)
	if res.Code != 200 {
		t.Fatalf("expected 200, got %d", res.Code)
	}
	if got := db.snapshotCount(); got == 0 {
		t.Fatal("expected the sync to keep trying after a non-cancellation failure")
	}
	if got := countErrors(hook, "Failed to acquire database snapshot"); got != db.snapshotCount() {
		t.Errorf("expected %d error entries, got %d", db.snapshotCount(), got)
	}
}

// otkUserAPI answers the one-time key count query a finished long poll makes.
type otkUserAPI struct {
	userapi.SyncUserAPI
}

func (otkUserAPI) QueryOneTimeKeys(context.Context, *userapi.QueryOneTimeKeysRequest, *userapi.QueryOneTimeKeysResponse) error {
	return nil
}

// A long-polling sync whose pass cannot read the database, as during a
// PostgreSQL failover, must not hand the client a token past what it read.
// Treating the empty, failed pass like a quiet one moved the client's since
// token to the notifier's current position, and the events at the positions
// in between were never sent (issue #416).
func TestSyncRequestDoesNotSkipPositionsItCouldNotRead(t *testing.T) {
	device := &userapi.Device{UserID: "@alice:localhost", ID: "05LqQkIj"}
	logrustest.NewGlobal()

	db := &snapshotCountingDB{err: errBrokenDatabase}
	rp := newCancellationTestPool(db, device)
	rp.userAPI = otkUserAPI{}
	since, err := types.NewStreamTokenFromString(nonEmptySinceToken())
	if err != nil {
		t.Fatal(err)
	}
	// An event was written at a position after the client's token.
	current := since
	current.PDUPosition = since.PDUPosition + 1
	rp.Notifier.SetCurrentPosition(current)

	req := httptest.NewRequest("GET", "/_matrix/client/v3/sync?timeout=1000&since="+since.String(), nil)
	res := rp.OnIncomingSyncRequest(req, device)
	if res.Code != 200 {
		t.Fatalf("expected 200, got %d", res.Code)
	}
	resp, ok := res.JSON.(*types.Response)
	if !ok {
		t.Fatalf("unexpected response body %T", res.JSON)
	}
	if resp.NextBatch.PDUPosition != since.PDUPosition {
		t.Errorf("next_batch moved the PDU position from %d to %d without reading it", since.PDUPosition, resp.NextBatch.PDUPosition)
	}
	if got := db.snapshotCount(); got < 2 {
		t.Errorf("expected the sync to retry the failed read, got %d snapshot attempts", got)
	}
}
