// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"encoding/json"
	"errors"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/syncapi/producers"
	"codefloe.com/pat-s/zendrite/test"
)

// recordingJetStream records the subject and data of each published message
// when it is published; the producers reuse and re-address one message.
type recordingJetStream struct {
	nats.JetStreamContext

	subjects []string
	data     [][]byte
	err      error
}

func (m *recordingJetStream) PublishMsg(msg *nats.Msg, _ ...nats.PubOpt) (*nats.PubAck, error) {
	if m.err != nil {
		return nil, m.err
	}
	m.subjects = append(m.subjects, msg.Subject)
	m.data = append(m.data, msg.Data)
	return &nats.PubAck{}, nil
}

// TestRedactionReachesAppservices checks that a redaction is passed to
// application services when it arrives, in room versions with the top-level
// redacts key and in those that also carry it in the content.
func TestRedactionReachesAppservices(t *testing.T) {
	alice := test.NewUser(t)
	for _, version := range []gomatrixserverlib.RoomVersion{gomatrixserverlib.RoomVersionV10, gomatrixserverlib.RoomVersionV12} {
		t.Run(string(version), func(t *testing.T) {
			room := test.NewRoom(t, alice, test.RoomVersion(version))
			message := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "hi"})
			redaction := room.CreateEvent(t, alice, spec.MRoomRedaction, map[string]any{"redacts": message.EventID()}, test.WithRedacts(message.EventID()))
			data, err := json.Marshal(api.OutputEvent{Type: api.OutputTypeNewRoomEvent, NewRoomEvent: &api.OutputNewRoomEvent{Event: redaction}})
			require.NoError(t, err)
			newMsg := func() *nats.Msg { return &nats.Msg{Subject: "OutputRoomEvent", Data: data} }

			appservices, push := &recordingJetStream{}, &recordingJetStream{}
			s := &OutputRoomEventConsumer{
				asProducer:   &producers.AppserviceEventProducer{Topic: "OutputAppserviceEvent", JetStream: appservices},
				pushProducer: &producers.PushEventProducer{Topic: "OutputPushEvent", JetStream: push},
			}
			require.True(t, s.onMessage(t.Context(), []*nats.Msg{newMsg()}))
			require.Equal(t, []string{"OutputAppserviceEvent"}, appservices.subjects)
			require.Equal(t, [][]byte{data}, appservices.data)
			require.Equal(t, []string{"OutputPushEvent"}, push.subjects)

			// A failed publish NAKs the message, so the redaction is redelivered
			// instead of being lost.
			appservices.err = errors.New("publish failed")
			push.subjects = nil
			require.False(t, s.onMessage(t.Context(), []*nats.Msg{newMsg()}))
			require.Empty(t, push.subjects)
		})
	}
}
