package routing

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/clientapi/producers"
	roomapi "codefloe.com/pat-s/zendrite/roomserver/api"
	roomtypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type receiptEventQuery struct{ events []*roomtypes.HeaderedEvent }

func (q receiptEventQuery) QueryEventsByID(_ context.Context, req *roomapi.QueryEventsByIDRequest, res *roomapi.QueryEventsByIDResponse) error {
	for _, event := range q.events {
		if event.EventID() == req.EventIDs[0] {
			res.Events = append(res.Events, event)
		}
	}
	return nil
}

type receiptPublisher struct {
	nats.JetStreamContext
	messages []*nats.Msg
}

func (p *receiptPublisher) PublishMsg(msg *nats.Msg, _ ...nats.PubOpt) (*nats.PubAck, error) {
	p.messages = append(p.messages, msg)
	return &nats.PubAck{}, nil
}

func TestSetThreadedReceipt(t *testing.T) {
	alice := test.NewUser(t)
	room := test.NewRoom(t, alice)
	root := room.CreateEvent(t, alice, "m.room.message", map[string]any{"body": "root"})
	reply := room.CreateEvent(t, alice, "m.room.message", map[string]any{"m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": root.EventID()}})
	reaction := room.CreateEvent(t, alice, "m.reaction", map[string]any{"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": reply.EventID(), "key": "x"}})
	rootReaction := room.CreateEvent(t, alice, "m.reaction", map[string]any{"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": root.EventID(), "key": "x"}})
	reference := room.CreateEvent(t, alice, "m.room.message", map[string]any{"m.relates_to": map[string]any{"rel_type": "m.reference", "event_id": root.EventID()}})
	referenceReaction := room.CreateEvent(t, alice, "m.reaction", map[string]any{"m.relates_to": map[string]any{"rel_type": "m.annotation", "event_id": reference.EventID(), "key": "x"}})
	query := receiptEventQuery{events: []*roomtypes.HeaderedEvent{root, reply, reaction, rootReaction, reference, referenceReaction}}
	threadBody, err := json.Marshal(map[string]string{"thread_id": root.EventID()})
	require.NoError(t, err)
	for _, tc := range []struct{ name, body, receiptType, eventID, wantThread, wantError string }{
		{name: "absent", body: `{}`, eventID: reply.EventID()},
		{name: "empty body", eventID: root.EventID()},
		{name: "main", body: `{"thread_id":"main"}`, eventID: root.EventID(), wantThread: "main"},
		{name: "thread", body: string(threadBody), eventID: reply.EventID(), wantThread: root.EventID()},
		{name: "private", body: string(threadBody), receiptType: "m.read.private", eventID: reply.EventID(), wantThread: root.EventID()},
		{name: "indirect", body: string(threadBody), eventID: reaction.EventID(), wantThread: root.EventID()},
		// An annotation on a reference to the thread root is related to the
		// thread but does not itself have an m.thread relation (MSC3771).
		{name: "indirect reference", body: string(threadBody), eventID: referenceReaction.EventID(), wantThread: root.EventID()},
		{name: "root relation", body: `{"thread_id":"main"}`, eventID: rootReaction.EventID(), wantThread: "main"},
		{name: "null", body: `{"thread_id":null}`, wantError: "M_INVALID_PARAM"},
		{name: "empty", body: `{"thread_id":""}`, wantError: "M_INVALID_PARAM"},
		{name: "number", body: `{"thread_id":42}`, wantError: "M_INVALID_PARAM"},
		{name: "invalid id", body: `{"thread_id":"no"}`, wantError: "M_INVALID_PARAM"},
		{name: "fully read", body: `{"thread_id":"main"}`, receiptType: "m.fully_read", wantError: "M_INVALID_PARAM"},
		{name: "wrong thread", body: `{"thread_id":"$other"}`, eventID: reply.EventID(), wantError: "M_INVALID_PARAM"},
		{name: "thread as main", body: `{"thread_id":"main"}`, eventID: reply.EventID(), wantError: "M_INVALID_PARAM"},
		// MSC3771: a main-timeline event such as the thread root may carry an explicit thread receipt.
		{name: "root in thread", body: string(threadBody), eventID: root.EventID(), wantThread: root.EventID()},
		{name: "main event in unrelated thread", body: `{"thread_id":"$other"}`, eventID: root.EventID(), wantError: "M_INVALID_PARAM"},
		{name: "unknown event", body: `{"thread_id":"main"}`, eventID: "$unknown", wantError: "M_INVALID_PARAM"},
		{name: "malformed", body: `{`, wantError: "M_BAD_JSON"},
		{name: "array", body: `[]`, wantError: "M_BAD_JSON"},
		{name: "null body", body: `null`, wantError: "M_BAD_JSON"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.receiptType == "" {
				tc.receiptType = "m.read"
			}
			pub := &receiptPublisher{}
			producer := &producers.SyncAPIProducer{JetStream: pub}
			req := httptest.NewRequest("POST", "/receipt", strings.NewReader(tc.body))
			res := SetReceipt(req, query, nil, producer, &userapi.Device{UserID: alice.ID}, room.ID, tc.receiptType, tc.eventID)
			if tc.wantError != "" {
				require.Equal(t, 400, res.Code)
				raw, err := json.Marshal(res.JSON)
				require.NoError(t, err)
				require.Contains(t, string(raw), tc.wantError)
				require.Empty(t, pub.messages)
			} else {
				require.Equal(t, 200, res.Code)
				require.Len(t, pub.messages, 1)
				require.Equal(t, tc.wantThread, pub.messages[0].Header.Get("thread_id"))
				require.Equal(t, tc.receiptType, pub.messages[0].Header.Get("type"))
			}
		})
	}
}
