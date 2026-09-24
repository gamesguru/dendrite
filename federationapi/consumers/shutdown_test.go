// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
)

// cancellingRoomserverAPI fails every lookup the way the roomserver does once
// the process context has been canceled.
type cancellingRoomserverAPI struct {
	api.FederationRoomserverAPI
	err error
}

func (f *cancellingRoomserverAPI) QueryEventsByID(_ context.Context, _ *api.QueryEventsByIDRequest, _ *api.QueryEventsByIDResponse) error {
	return f.err
}

// TestOnMessageInterruptedDoesNotPanic covers shutdown racing an in-flight
// message. The handler panics on a write failure to avoid continuing with an
// inconsistent database, but a canceled context means nothing was written at
// all, so panicking there crashes the process (and, in CI, the whole test
// binary) over a message that simply has to be redelivered.
func TestOnMessageInterruptedDoesNotPanic(t *testing.T) {
	user := test.NewUser(t)
	room := test.NewRoom(t, user)
	ev := room.Events()[len(room.Events())-1]

	for name, cause := range map[string]error{
		"canceled":          context.Canceled,
		"deadline exceeded": context.DeadlineExceeded,
		"wrapped":           fmt.Errorf("s.rsAPI.QueryEventsByID: %w", context.Canceled),
	} {
		t.Run(name, func(t *testing.T) {
			s := &OutputRoomEventConsumer{
				ctx:   context.Background(),
				rsAPI: &cancellingRoomserverAPI{err: cause},
			}

			data, err := json.Marshal(api.OutputEvent{
				Type: api.OutputTypeNewRoomEvent,
				NewRoomEvent: &api.OutputNewRoomEvent{
					Event: ev,
					// An ID other than the event's own, so processMessage has
					// to ask the roomserver for it and hits the cancellation.
					AddsStateEventIDs: []string{"$absent:test"},
				},
			})
			require.NoError(t, err)

			msg := &nats.Msg{Data: data, Header: nats.Header{}}
			msg.Header.Set(jetstream.RoomEventType, string(api.OutputTypeNewRoomEvent))

			var acked bool
			require.NotPanics(t, func() {
				acked = s.onMessage(context.Background(), []*nats.Msg{msg})
			})
			assert.False(t, acked, "an unwritten event must be NAKed so redelivery retries it")
		})
	}
}

func TestInterrupted(t *testing.T) {
	t.Parallel()

	assert.True(t, interrupted(context.Canceled))
	assert.True(t, interrupted(context.DeadlineExceeded))
	assert.True(t, interrupted(fmt.Errorf("wrapped: %w", context.Canceled)),
		"processMessage wraps with %w, so the check has to see through it")
	assert.False(t, interrupted(errors.New("write failed")))
	assert.False(t, interrupted(nil))
}
