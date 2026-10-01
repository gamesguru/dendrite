package consumers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/eventutil"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/synctypes"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/producers"
)

// threadRoomserver serves a two-member room for push rule evaluation.
type threadRoomserver struct {
	rsapi.UserRoomserverAPI
	member      synctypes.ClientEvent
	eventsError error
}

func (r *threadRoomserver) QueryEventsByID(context.Context, *rsapi.QueryEventsByIDRequest, *rsapi.QueryEventsByIDResponse) error {
	return r.eventsError
}

func (r *threadRoomserver) QueryMembershipsForRoom(_ context.Context, _ *rsapi.QueryMembershipsForRoomRequest, res *rsapi.QueryMembershipsForRoomResponse) error {
	res.JoinEvents = []synctypes.ClientEvent{r.member}
	return nil
}

func (r *threadRoomserver) JoinedUserCount(context.Context, string) (int, error) { return 2, nil }

func (r *threadRoomserver) QueryCurrentState(_ context.Context, _ *rsapi.QueryCurrentStateRequest, res *rsapi.QueryCurrentStateResponse) error {
	return nil
}

func (r *threadRoomserver) QueryUserIDForSender(_ context.Context, _ spec.RoomID, sender spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(sender), true)
}

func (r *threadRoomserver) QuerySenderIDForUser(_ context.Context, _ spec.RoomID, user spec.UserID) (*spec.SenderID, error) {
	sender := spec.SenderID(user.String())
	return &sender, nil
}

type capturePublisher struct{ msgs []*nats.Msg }

func (p *capturePublisher) PublishMsg(msg *nats.Msg, _ ...nats.PubOpt) (*nats.PubAck, error) {
	p.msgs = append(p.msgs, msg)
	return &nats.PubAck{}, nil
}

// TestThreadReplyCountedInThread checks that the counts published for a new
// thread reply already attribute it to its thread.
func TestThreadReplyCountedInThread(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, closeDB := mustCreateDatabase(t, dbType)
		defer closeDB()
		alice, bob := test.NewUser(t), test.NewUser(t)
		room := test.NewRoom(t, alice)
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		root := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"msgtype": "m.text", "body": "root"})
		reply := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{
			"msgtype": "m.text", "body": "reply", "m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": root.EventID()},
		})
		stateKey := alice.ID
		member := synctypes.ClientEvent{Type: spec.MRoomMember, StateKey: &stateKey, Content: []byte(`{"membership":"join"}`)}

		publisher := &capturePublisher{}
		cfg := &config.UserAPI{Matrix: &config.Global{ServerName: "test"}}
		consumer := OutputRoomEventConsumer{
			cfg: cfg, db: db, rsAPI: &threadRoomserver{member: member},
			syncProducer: producers.NewSyncAPI(db, publisher, "client", "notifications"),
		}
		for i, event := range []*types.HeaderedEvent{root, reply} {
			require.NoError(t, consumer.processMessage(t.Context(), event, uint64(i+1)))
		}

		var last eventutil.NotificationData
		for _, msg := range publisher.msgs {
			if msg.Subject == "notifications" && msg.Header.Get("user_id") == alice.ID {
				require.NoError(t, json.Unmarshal(msg.Data, &last))
			}
		}
		require.Equal(t, 2, last.UnreadNotificationCount)
		main, threads := last.SplitByThread()
		require.Equal(t, 1, main.UnreadNotificationCount, "the root is in the main timeline")
		require.Equal(t, map[string]eventutil.ThreadNotificationCounts{root.EventID(): {UnreadNotificationCount: 1}}, threads)

		// When the event's thread cannot be resolved, its stored notification
		// is still published, counted in the main timeline.
		consumer.rsAPI = &threadRoomserver{member: member, eventsError: errors.New("roomserver unavailable")}
		reference := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{
			"msgtype": "m.text", "body": "see above", "m.relates_to": map[string]any{"rel_type": "m.reference", "event_id": root.EventID()},
		})
		publisher.msgs = nil
		require.Error(t, consumer.processMessage(t.Context(), reference, 3))
		require.NotEmpty(t, publisher.msgs)
		require.NoError(t, json.Unmarshal(publisher.msgs[len(publisher.msgs)-1].Data, &last))
		require.Equal(t, 3, last.UnreadNotificationCount)
	})
}
