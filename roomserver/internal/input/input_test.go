package input_test

import (
	"context"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"

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
