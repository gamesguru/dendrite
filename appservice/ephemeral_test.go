// Copyright 2026 Patrick Schratz
//
// SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-Element-Commercial
// Please see LICENSE files in the repository root for full details.

package appservice_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/appservice"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
)

// ephemeralReceiver records the ephemeral data an application service
// receives, keyed by the transaction field it arrived in.
type ephemeralReceiver struct {
	mu     sync.Mutex
	fields map[string][]gjson.Result
	events int
}

func (r *ephemeralReceiver) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	var body map[string]json.RawMessage
	_ = json.NewDecoder(req.Body).Decode(&body)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, field := range []string{"ephemeral", "de.sorunome.msc2409.ephemeral"} {
		r.fields[field] = append(r.fields[field], gjson.ParseBytes(body[field]).Array()...)
	}
	r.events += len(gjson.ParseBytes(body["events"]).Array())
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte("{}"))
}

// find returns the first received ephemeral event in field that matches.
func (r *ephemeralReceiver) find(field string, match func(gjson.Result) bool) (gjson.Result, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, ev := range r.fields[field] {
		if match(ev) {
			return ev, true
		}
	}
	return gjson.Result{}, false
}

func (r *ephemeralReceiver) count(field string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.fields[field])
}

// TestEphemeralToAppservices covers sending typing, receipts and presence to
// application services that set receive_ephemeral (MSC2409).
func TestEphemeralToAppservices(t *testing.T) {
	alice := test.NewUser(t)
	bridged := test.NewUser(t)
	stranger := test.NewUser(t)
	room := test.NewRoom(t, alice)
	room.CreateAndInsert(t, bridged, spec.MRoomMember, map[string]any{"membership": "join"}, test.WithStateKey(bridged.ID))
	unrelated := test.NewRoom(t, stranger)

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, closeDB := testrig.CreateConfig(t, dbType)
		defer closeDB()
		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		natsInstance := &jetstream.NATSInstance{}

		stable := &ephemeralReceiver{fields: map[string][]gjson.Result{}}
		unstable := &ephemeralReceiver{fields: map[string][]gjson.Result{}}
		uninterested := &ephemeralReceiver{fields: map[string][]gjson.Result{}}
		var appservices []config.ApplicationService
		for i, recv := range []*ephemeralReceiver{stable, unstable, uninterested} {
			srv := httptest.NewServer(recv)
			defer srv.Close()
			as := config.ApplicationService{
				ID:              "bridge" + strconv.Itoa(i),
				URL:             srv.URL,
				ASToken:         "as_token_" + strconv.Itoa(i),
				SenderLocalpart: "bridgebot" + strconv.Itoa(i),
				NamespaceMap: map[string][]config.ApplicationServiceNamespace{
					"users": {{RegexpObject: regexp.MustCompile(regexp.QuoteMeta(bridged.ID))}},
				},
				ReceiveEphemeral:         recv == stable,
				UnstableReceiveEphemeral: recv == unstable,
			}
			as.CreateHTTPClient(true)
			appservices = append(appservices, as)
		}
		cfg.AppServiceAPI.Derived.ApplicationServices = appservices

		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil)
		usrAPI := userapi.NewInternalAPI(processCtx, cfg, cm, natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		require.NoError(t, rsapi.SendEvents(context.Background(), rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false))
		require.NoError(t, rsapi.SendEvents(context.Background(), rsAPI, rsapi.KindNew, unrelated.Events(), "test", "test", "test", nil, false))
		appservice.NewInternalAPI(processCtx, cfg, natsInstance, usrAPI, rsAPI)

		js, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
		publish := func(topic string, headers map[string]string) {
			t.Helper()
			msg := nats.NewMsg(cfg.Global.JetStream.Prefixed(topic))
			for k, v := range headers {
				msg.Header.Set(k, v)
			}
			_, err := js.PublishMsg(msg)
			require.NoError(t, err)
		}
		now := strconv.FormatInt(int64(spec.AsTimestamp(time.Now())), 10)
		messageID := room.Events()[len(room.Events())-1].EventID()

		publish(jetstream.OutputTypingEvent, map[string]string{
			jetstream.RoomID: room.ID, jetstream.UserID: alice.ID, "typing": "true", "timeout_ms": "300",
		})
		publish(jetstream.OutputTypingEvent, map[string]string{
			jetstream.RoomID: unrelated.ID, jetstream.UserID: stranger.ID, "typing": "true", "timeout_ms": "30000",
		})
		publish(jetstream.OutputReceiptEvent, map[string]string{
			jetstream.RoomID: room.ID, jetstream.UserID: alice.ID, jetstream.EventID: messageID,
			"type": "m.read", "timestamp": now, "thread_id": "main",
		})
		publish(jetstream.OutputReceiptEvent, map[string]string{
			jetstream.RoomID: room.ID, jetstream.UserID: alice.ID, jetstream.EventID: messageID,
			"type": "m.read.private", "timestamp": now,
		})
		publish(jetstream.OutputReceiptEvent, map[string]string{
			jetstream.RoomID: room.ID, jetstream.UserID: bridged.ID, jetstream.EventID: messageID,
			"type": "m.read.private", "timestamp": now,
		})
		publish(jetstream.OutputPresenceEvent, map[string]string{
			jetstream.UserID: alice.ID, "presence": "online", "status_msg": "busy", "last_active_ts": now,
		})
		publish(jetstream.OutputPresenceEvent, map[string]string{
			jetstream.UserID: stranger.ID, "presence": "online", "last_active_ts": now,
		})

		wait := func(recv *ephemeralReceiver, field string, what string, match func(gjson.Result) bool) gjson.Result {
			t.Helper()
			var ev gjson.Result
			require.Eventually(t, func() bool {
				var ok bool
				ev, ok = recv.find(field, match)
				return ok
			}, 10*time.Second, 20*time.Millisecond, "no %s in %s", what, field)
			return ev
		}
		ofType := func(eventType string) func(gjson.Result) bool {
			return func(ev gjson.Result) bool { return ev.Get("type").Str == eventType }
		}

		for field, recv := range map[string]*ephemeralReceiver{"ephemeral": stable, "de.sorunome.msc2409.ephemeral": unstable} {
			typing := wait(recv, field, "typing", func(ev gjson.Result) bool {
				return ev.Get("type").Str == spec.MTyping && len(ev.Get("content.user_ids").Array()) == 1
			})
			require.Equal(t, room.ID, typing.Get("room_id").Str, typing.Raw)
			require.Equal(t, alice.ID, typing.Get("content.user_ids.0").Str, typing.Raw)
			// The typing notification times out.
			wait(recv, field, "typing timeout", func(ev gjson.Result) bool {
				return ev.Get("type").Str == spec.MTyping && ev.Get("content.user_ids").IsArray() && len(ev.Get("content.user_ids").Array()) == 0
			})

			receipt := func(receiptType string, user *test.User) func(gjson.Result) bool {
				return func(ev gjson.Result) bool {
					return ev.Get("type").Str == spec.MReceipt && ev.Get("room_id").Str == room.ID &&
						ev.Get("content").Get(gjson.Escape(messageID)).Get(gjson.Escape(receiptType)).Get(gjson.Escape(user.ID)).Exists()
				}
			}
			read := wait(recv, field, "read receipt", receipt("m.read", alice))
			require.Equal(t, "main", read.Get("content").Get(gjson.Escape(messageID)).Get(`m\.read`).Get(gjson.Escape(alice.ID)).Get("thread_id").Str, read.Raw)
			// Private read receipts only for users in the namespaces.
			wait(recv, field, "private receipt", receipt("m.read.private", bridged))
			_, found := recv.find(field, receipt("m.read.private", alice))
			require.False(t, found)

			presence := wait(recv, field, "presence", ofType(spec.MPresence))
			require.Equal(t, alice.ID, presence.Get("sender").Str, presence.Raw)
			require.Equal(t, "online", presence.Get("content.presence").Str)
			require.Equal(t, "busy", presence.Get("content.status_msg").Str)
			require.True(t, presence.Get("content.currently_active").Bool())

			// Nothing about the stranger's room or presence.
			_, found = recv.find(field, func(ev gjson.Result) bool {
				return ev.Get("room_id").Str == unrelated.ID || ev.Get("sender").Str == stranger.ID
			})
			require.False(t, found)
		}
		require.Zero(t, stable.count("de.sorunome.msc2409.ephemeral"))
		require.Zero(t, unstable.count("ephemeral"))
		require.Zero(t, uninterested.count("ephemeral")+uninterested.count("de.sorunome.msc2409.ephemeral"))
	})
}
