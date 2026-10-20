package input_test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/sirupsen/logrus"
	logrustest "github.com/sirupsen/logrus/hooks/test"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/roomserver/internal/input"
	"codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
)

func TestSingleTransactionOnInput(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, close := testrig.CreateConfig(t, dbType)
		defer close()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)

		natsInstance := &jetstream.NATSInstance{}
		js, jc := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		caches := caching.NewRistrettoCache(8*1024*1024, time.Hour, caching.DisableMetrics)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)

		deadline, _ := t.Deadline()
		if max := time.Now().Add(time.Second * 3); deadline.Before(max) {
			deadline = max
		}
		ctx, cancel := context.WithDeadline(processCtx.Context(), deadline)
		defer cancel()

		event, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV6).NewEventFromTrustedJSON(
			[]byte(`{"auth_events":[],"content":{"creator":"@neilalexander:zendrite.matrix.org","room_version":"6"},"depth":1,"hashes":{"sha256":"jqOqdNEH5r0NiN3xJtj0u5XUVmRqq9YvGbki1wxxuuM"},"origin":"zendrite.matrix.org","origin_server_ts":1644595362726,"prev_events":[],"prev_state":[],"room_id":"!jSZZRknA6GkTBXNP:zendrite.matrix.org","sender":"@neilalexander:zendrite.matrix.org","signatures":{"zendrite.matrix.org":{"ed25519:6jB2aB":"bsQXO1wketf1OSe9xlndDIWe71W9KIundc6rBw4KEZdGPW7x4Tv4zDWWvbxDsG64sS2IPWfIm+J0OOozbrWIDw"}},"state_key":"","type":"m.room.create"}`),
			false,
		)
		if err != nil {
			t.Fatal(err)
		}
		in := api.InputRoomEvent{
			Kind:  api.KindOutlier, // don't panic if we generate an output event
			Event: &types.HeaderedEvent{PDU: event},
		}

		inputter := &input.Inputer{
			JetStream:  js,
			NATSClient: jc,
			Cfg:        &cfg.RoomServer,
		}
		res := &api.InputRoomEventsResponse{}
		inputter.InputRoomEvents(
			ctx,
			&api.InputRoomEventsRequest{
				InputRoomEvents: []api.InputRoomEvent{in},
				Asynchronous:    false,
			},
			res,
		)
		// If we fail here then it's because we've hit the test deadline,
		// so we probably deadlocked
		if err := res.Err(); err != nil {
			t.Fatal(err)
		}
	})
}

// A synchronous caller whose replies never come waits for as long as its
// context allows. It used to do so without a trace, so a hung request could
// not be told apart from a slow one: it now reports, once, which inputs are
// unanswered and where they were queued (#438).
func TestUnansweredSyncInputIsReported(t *testing.T) {
	cfg, processCtx, closeDB := testrig.CreateConfig(t, test.DBTypeSQLite)
	defer closeDB()
	defer input.SetSyncInputReplyOverdue(100 * time.Millisecond)()
	hook := logrustest.NewGlobal()
	defer hook.Reset()

	natsInstance := &jetstream.NATSInstance{}
	js, jc := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	event, err := gomatrixserverlib.MustGetRoomVersion(gomatrixserverlib.RoomVersionV6).NewEventFromTrustedJSON(
		[]byte(`{"auth_events":[],"content":{"creator":"@neilalexander:zendrite.matrix.org","room_version":"6"},"depth":1,"hashes":{"sha256":"jqOqdNEH5r0NiN3xJtj0u5XUVmRqq9YvGbki1wxxuuM"},"origin":"zendrite.matrix.org","origin_server_ts":1644595362726,"prev_events":[],"prev_state":[],"room_id":"!jSZZRknA6GkTBXNP:zendrite.matrix.org","sender":"@neilalexander:zendrite.matrix.org","signatures":{"zendrite.matrix.org":{"ed25519:6jB2aB":"bsQXO1wketf1OSe9xlndDIWe71W9KIundc6rBw4KEZdGPW7x4Tv4zDWWvbxDsG64sS2IPWfIm+J0OOozbrWIDw"}},"state_key":"","type":"m.room.create"}`),
		false,
	)
	require.NoError(t, err)

	// No roomserver is running, so nothing ever answers.
	inputter := &input.Inputer{JetStream: js, NATSClient: jc, Cfg: &cfg.RoomServer}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	res := &api.InputRoomEventsResponse{}
	inputter.InputRoomEvents(ctx, &api.InputRoomEventsRequest{
		InputRoomEvents: []api.InputRoomEvent{{Kind: api.KindOutlier, Event: &types.HeaderedEvent{PDU: event}}},
	}, res)

	require.Error(t, res.Err(), "the caller still waits until its own deadline")
	var warnings []*logrus.Entry
	for _, entry := range hook.AllEntries() {
		if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, "has not answered synchronous input") {
			warnings = append(warnings, entry)
		}
	}
	require.Len(t, warnings, 1)
	assert.Equal(t, 1, warnings[0].Data["missing_replies"])
	assert.Contains(t, fmt.Sprint(warnings[0].Data["unanswered"]), event.EventID()+"@")
	assert.Contains(t, fmt.Sprint(warnings[0].Data["rooms"]), "worker none")
}
