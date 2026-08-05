package streams

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/element-hq/dendrite/internal/caching"
	"github.com/element-hq/dendrite/syncapi/types"
	userapi "github.com/element-hq/dendrite/userapi/api"
	"github.com/matrix-org/gomatrixserverlib/spec"
)

func TestTypingCompleteSyncWaitsForRecentTypingUpdate(t *testing.T) {
	const roomID = "!room:test"
	const userID = "@alice:test"

	cache := caching.NewTypingCache()
	p := &TypingStreamProvider{EDUCache: cache}
	req := &types.SyncRequest{
		Context:  context.Background(),
		Log:      logrus.NewEntry(logrus.New()),
		Device:   &userapi.Device{UserID: userID},
		Response: types.NewResponse(),
		Rooms: map[string]string{
			roomID: spec.Join,
		},
	}
	req.Response.Rooms.Join[roomID] = types.NewJoinResponse()

	go func() {
		time.Sleep(10 * time.Millisecond)
		cache.AddTypingUser(userID, roomID, nil)
	}()

	p.CompleteSync(context.Background(), nil, req)

	events := req.Response.Rooms.Join[roomID].Ephemeral.Events
	if len(events) != 1 {
		t.Fatalf("expected one typing event, got %d", len(events))
	}
	if events[0].Type != spec.MTyping {
		t.Fatalf("expected %q event, got %q", spec.MTyping, events[0].Type)
	}

	var content struct {
		UserIDs []string `json:"user_ids"`
	}
	if err := json.Unmarshal(events[0].Content, &content); err != nil {
		t.Fatalf("failed to unmarshal typing content: %s", err)
	}
	if len(content.UserIDs) != 1 || content.UserIDs[0] != userID {
		t.Fatalf("unexpected typing users: %+v", content.UserIDs)
	}
}
