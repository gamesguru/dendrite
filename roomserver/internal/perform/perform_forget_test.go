// Copyright 2025 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.
package perform

import (
	"context"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/storage"
)

type fakeForgetDB struct {
	storage.Database
	gotSenderID string
	gotForget   bool
}

func (f *fakeForgetDB) ForgetRoom(ctx context.Context, senderID, roomID string, forget bool) error {
	f.gotSenderID = senderID
	f.gotForget = forget
	return nil
}

type fakeForgetRSAPI struct {
	api.RoomserverInternalAPI
	senderID   *spec.SenderID
	senderErr  error
	scheduled  string
	queryCalls int
}

func (f *fakeForgetRSAPI) QuerySenderIDForUser(ctx context.Context, roomID spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	f.queryCalls++
	return f.senderID, f.senderErr
}

func (f *fakeForgetRSAPI) ScheduleAutoPurgeIfEligible(ctx context.Context, roomID string) {
	f.scheduled = roomID
}

func TestPerformForgetUsesSenderID(t *testing.T) {
	const userID = "@user:example.com"
	const roomID = "!room:example.com"

	t.Run("pseudo-ID room forgets the sender-ID-keyed row", func(t *testing.T) {
		pseudo := spec.SenderID("pseudoABC")
		db := &fakeForgetDB{}
		rsAPI := &fakeForgetRSAPI{senderID: &pseudo}
		f := &Forgetter{DB: db, RSAPI: rsAPI}

		if err := f.PerformForget(context.Background(), &api.PerformForgetRequest{RoomID: roomID, UserID: userID}, &api.PerformForgetResponse{}); err != nil {
			t.Fatal(err)
		}
		if db.gotSenderID != "pseudoABC" {
			t.Fatalf("ForgetRoom should be called with resolved sender ID, got %q", db.gotSenderID)
		}
		if !db.gotForget {
			t.Fatal("expected forget=true")
		}
		if rsAPI.scheduled != roomID {
			t.Fatalf("expected auto-purge scheduled for %q, got %q", roomID, rsAPI.scheduled)
		}
	})

	t.Run("non-pseudo room falls back to the user ID", func(t *testing.T) {
		// In non-pseudo rooms the sender ID equals the user ID.
		sid := spec.SenderID(userID)
		db := &fakeForgetDB{}
		f := &Forgetter{DB: db, RSAPI: &fakeForgetRSAPI{senderID: &sid}}

		if err := f.PerformForget(context.Background(), &api.PerformForgetRequest{RoomID: roomID, UserID: userID}, &api.PerformForgetResponse{}); err != nil {
			t.Fatal(err)
		}
		if db.gotSenderID != userID {
			t.Fatalf("ForgetRoom should be called with the user ID, got %q", db.gotSenderID)
		}
	})

	t.Run("falls back to the user ID when sender ID cannot be resolved", func(t *testing.T) {
		db := &fakeForgetDB{}
		f := &Forgetter{DB: db, RSAPI: &fakeForgetRSAPI{senderID: nil}}

		if err := f.PerformForget(context.Background(), &api.PerformForgetRequest{RoomID: roomID, UserID: userID}, &api.PerformForgetResponse{}); err != nil {
			t.Fatal(err)
		}
		if db.gotSenderID != userID {
			t.Fatalf("ForgetRoom should fall back to the user ID, got %q", db.gotSenderID)
		}
	})
}
