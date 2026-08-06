// Copyright 2024 New Vector Ltd.
// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package eventutil

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/fclient"
	"github.com/matrix-org/gomatrixserverlib/spec"

	"github.com/element-hq/dendrite/roomserver/api"
)

// A room that doesn't exist locally has RoomExists=false and an empty
// RoomVersion. BuildEvent must report this as ErrRoomNoExists (which callers
// such as room-join handling use to trigger a federated join fallback), not
// as a room-version parsing error. This guards against an ordering bug where
// gomatrixserverlib.GetRoomVersion ran on the empty RoomVersion before the
// RoomExists check, masking ErrRoomNoExists behind an unsupported-room-version
// error instead.
func TestBuildEventReturnsErrRoomNoExistsForMissingRoom(t *testing.T) {
	proto := &gomatrixserverlib.ProtoEvent{
		Type:     spec.MRoomMember,
		SenderID: "@alice:test",
		RoomID:   "!doesnotexist:test",
	}
	identity := &fclient.SigningIdentity{
		ServerName: "test",
	}
	queryRes := &api.QueryLatestEventsAndStateResponse{
		RoomExists:  false,
		RoomVersion: "",
	}

	_, err := BuildEvent(context.Background(), proto, identity, time.Now(), &gomatrixserverlib.StateNeeded{}, queryRes)
	if err == nil {
		t.Fatal("expected an error for a nonexistent room, got nil")
	}
	if !errors.As(err, &ErrRoomNoExists{}) {
		t.Fatalf("expected ErrRoomNoExists, got %T: %s", err, err)
	}
}
