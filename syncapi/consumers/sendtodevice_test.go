// Copyright 2026 The Zendrite Authors
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package consumers

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type fakeJetStream struct {
	nats.JetStreamContext

	stream       *nats.StreamInfo
	streamErr    error
	consumerInfo []*nats.ConsumerInfo
	consumerErr  []error
	calls        int
}

func (f *fakeJetStream) StreamInfo(string, ...nats.JSOpt) (*nats.StreamInfo, error) {
	return f.stream, f.streamErr
}

func (f *fakeJetStream) ConsumerInfo(string, string, ...nats.JSOpt) (*nats.ConsumerInfo, error) {
	call := f.calls
	f.calls++
	if len(f.consumerErr) > 0 && call < len(f.consumerErr) && f.consumerErr[call] != nil {
		return nil, f.consumerErr[call]
	}
	if len(f.consumerInfo) == 0 {
		return nil, errors.New("no consumer info")
	}
	if call >= len(f.consumerInfo) {
		call = len(f.consumerInfo) - 1
	}
	return f.consumerInfo[call], nil
}

// A message published after the barrier started must not keep it waiting once
// everything that existed at the start has been acknowledged.
func TestWaitUntilCaughtUpIgnoresLaterPublications(t *testing.T) {
	js := &fakeJetStream{
		stream: &nats.StreamInfo{State: nats.StreamState{LastSeq: 5}},
		consumerInfo: []*nats.ConsumerInfo{
			{NumPending: 2, NumAckPending: 1, AckFloor: nats.SequenceInfo{Stream: 4}},
			// Message 6 was published after the cutoff; message 5 is now acked.
			{NumPending: 1, AckFloor: nats.SequenceInfo{Stream: 5}},
		},
	}
	s := &OutputSendToDeviceEventConsumer{jetstream: js, topic: "stream", pullDurable: "consumer"}

	start := time.Now()
	s.WaitUntilCaughtUp(context.Background(), time.Second)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("waited %s for a later publication, want under 500ms", elapsed)
	}
	if js.calls < 2 {
		t.Fatalf("consumer info called %d times, want at least 2", js.calls)
	}
}

// A lookup failure is retried until the consumer can be read or the deadline
// ends; it must not let the sync race ahead immediately.
func TestWaitUntilCaughtUpRetriesLookupErrors(t *testing.T) {
	js := &fakeJetStream{
		stream:      &nats.StreamInfo{State: nats.StreamState{LastSeq: 0}},
		consumerErr: []error{errors.New("consumer not found")},
		consumerInfo: []*nats.ConsumerInfo{
			{},
		},
	}
	s := &OutputSendToDeviceEventConsumer{jetstream: js, topic: "stream", pullDurable: "consumer"}

	start := time.Now()
	s.WaitUntilCaughtUp(context.Background(), time.Second)
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("waited %s after a successful retry", elapsed)
	}
	if js.calls < 2 {
		t.Fatalf("consumer info called %d times, want at least 2", js.calls)
	}
}

// When every lookup fails, the barrier still gives up at the deadline.
func TestWaitUntilCaughtUpStopsAtDeadline(t *testing.T) {
	js := &fakeJetStream{
		stream:      &nats.StreamInfo{State: nats.StreamState{LastSeq: 3}},
		consumerErr: []error{errors.New("consumer not found"), errors.New("consumer not found"), errors.New("consumer not found")},
	}
	s := &OutputSendToDeviceEventConsumer{jetstream: js, topic: "stream", pullDurable: "consumer"}

	start := time.Now()
	s.WaitUntilCaughtUp(context.Background(), 50*time.Millisecond)
	if elapsed := time.Since(start); elapsed < 40*time.Millisecond {
		t.Fatalf("returned after %s, want to wait for the deadline", elapsed)
	}
	if elapsed := time.Since(start); elapsed > 500*time.Millisecond {
		t.Fatalf("waited %s, want to stop near the deadline", elapsed)
	}
}
