package syncapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	rsapi "codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

// accountDataUserAPI answers sliding sync's account data lookups with nothing.
type accountDataUserAPI struct {
	*syncUserAPI
}

func (accountDataUserAPI) QueryAccountData(context.Context, *userapi.QueryAccountDataRequest, *userapi.QueryAccountDataResponse) error {
	return nil
}

// TestBundledAggregations checks that every endpoint the spec lists serves
// thread, edit and reference aggregations in unsigned.m.relations.
func TestBundledAggregations(t *testing.T) {
	test.WithAllDatabases(t, testBundledAggregations)
}

func testBundledAggregations(t *testing.T, dbType test.DBType) {
	alice, bob := test.NewUser(t), test.NewUser(t)
	aliceDev := userapi.Device{ID: "ALICEID", UserID: alice.ID, AccessToken: "ALICE_BEARER_TOKEN", AccountType: userapi.AccountTypeUser}
	bobDev := userapi.Device{ID: "BOBID", UserID: bob.ID, AccessToken: "BOB_BEARER_TOKEN", AccountType: userapi.AccountTypeUser}

	cfg, processCtx, closeRig := testrig.CreateConfig(t, dbType)
	defer closeRig()
	routers := httputil.NewRouters()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, accountDataUserAPI{&syncUserAPI{accounts: []userapi.Device{aliceDev, bobDev}}}, rsAPI, caches, caching.DisableMetrics, nil, nil)
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)

	room := test.NewRoom(t, alice)
	room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
	relatesTo := func(relType, eventID string) map[string]any {
		return map[string]any{"rel_type": relType, "event_id": eventID}
	}
	root := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"msgtype": "m.text", "body": "root"})
	room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"msgtype": "m.text", "body": "reply 1", "m.relates_to": relatesTo("m.thread", root.EventID())})
	latestReply := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"msgtype": "m.text", "body": "reply 2", "m.relates_to": relatesTo("m.thread", root.EventID())})
	edit := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
		"msgtype": "m.text", "body": "* edited", "m.new_content": map[string]any{"msgtype": "m.text", "body": "edited"}, "m.relates_to": relatesTo("m.replace", root.EventID()),
	})
	// Bob cannot edit Alice's message, however recent his edit is.
	room.CreateAndInsert(t, bob, "m.room.message", map[string]any{
		"msgtype": "m.text", "body": "* hijacked", "m.new_content": map[string]any{"msgtype": "m.text", "body": "hijacked"}, "m.relates_to": relatesTo("m.replace", root.EventID()),
	})
	reference := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"msgtype": "m.text", "body": "see above", "m.relates_to": relatesTo("m.reference", root.EventID())})
	replyEdit := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{
		"msgtype": "m.text", "body": "* reply 2 edited", "m.new_content": map[string]any{"msgtype": "m.text", "body": "reply 2 edited"}, "m.relates_to": relatesTo("m.replace", latestReply.EventID()),
	})
	last := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"msgtype": "m.text", "body": "unrelated"})
	// The root and the seven events after it fill a timeline of eight.
	const rootTimelineLimit = 8

	require.NoError(t, rsapi.SendEvents(context.Background(), rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false))
	syncUntil(t, routers, aliceDev.AccessToken, false, func(syncBody string) bool {
		path := fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id=="%s")`, gjson.Escape(room.ID), last.EventID())
		return gjson.Get(syncBody, path).Exists()
	})

	request := func(t *testing.T, method, path string, query map[string]string, body any) gjson.Result {
		t.Helper()
		params := map[string]string{"access_token": aliceDev.AccessToken}
		for k, v := range query {
			params[k] = v
		}
		opts := []test.HTTPRequestOpt{test.WithQueryParams(params)}
		if body != nil {
			opts = append(opts, test.WithJSONBody(t, body))
		}
		w := httptest.NewRecorder()
		routers.Client.ServeHTTP(w, test.NewRequest(t, method, path, opts...))
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		return gjson.ParseBytes(w.Body.Bytes())
	}
	byID := func(events gjson.Result, eventID string) gjson.Result {
		return events.Get(fmt.Sprintf(`#(event_id=="%s")`, eventID))
	}
	requireRootBundle := func(t *testing.T, event gjson.Result) {
		t.Helper()
		require.True(t, event.Exists(), "root event missing")
		relations := event.Get(`unsigned.m\.relations`)
		require.True(t, relations.Exists(), "no bundled aggregations on %s", event.Raw)
		assert.Equal(t, int64(2), relations.Get(`m\.thread.count`).Int())
		assert.True(t, relations.Get(`m\.thread.current_user_participated`).Bool())
		assert.Equal(t, latestReply.EventID(), relations.Get(`m\.thread.latest_event.event_id`).Str)
		assert.Equal(t, replyEdit.EventID(), relations.Get(`m\.thread.latest_event.unsigned.m\.relations.m\.replace.event_id`).Str)
		assert.Equal(t, edit.EventID(), relations.Get(`m\.replace.event_id`).Str)
		assert.Equal(t, "edited", relations.Get(`m\.replace.content.m\.new_content.body`).Str)
		assert.Equal(t, []any{map[string]any{"event_id": reference.EventID()}}, relations.Get(`m\.reference.chunk`).Value())
		// The original content stays intact, since clients apply the edit themselves.
		assert.Equal(t, "root", event.Get("content.body").Str)
	}
	roomPath := "/_matrix/client/v3/rooms/" + room.ID

	t.Run("event", func(t *testing.T) {
		requireRootBundle(t, request(t, http.MethodGet, roomPath+"/event/"+root.EventID(), nil, nil))
	})
	t.Run("context", func(t *testing.T) {
		requireRootBundle(t, request(t, http.MethodGet, roomPath+"/context/"+root.EventID(), nil, nil).Get("event"))
		after := request(t, http.MethodGet, roomPath+"/context/"+edit.EventID(), map[string]string{"limit": "20"}, nil)
		requireRootBundle(t, byID(after.Get("events_before"), root.EventID()))
	})
	t.Run("messages", func(t *testing.T) {
		chunk := request(t, http.MethodGet, roomPath+"/messages", map[string]string{"dir": "b", "limit": "50"}, nil).Get("chunk")
		requireRootBundle(t, byID(chunk, root.EventID()))
	})
	t.Run("relations", func(t *testing.T) {
		chunk := request(t, http.MethodGet, "/_matrix/client/v1/rooms/"+room.ID+"/relations/"+root.EventID()+"/m.thread", nil, nil).Get("chunk")
		assert.Equal(t, replyEdit.EventID(), byID(chunk, latestReply.EventID()).Get(`unsigned.m\.relations.m\.replace.event_id`).Str)
	})
	syncRoom := func(t *testing.T, timelineLimit int) gjson.Result {
		t.Helper()
		filter, err := json.Marshal(map[string]any{"room": map[string]any{"timeline": map[string]any{"limit": timelineLimit}}})
		require.NoError(t, err)
		return request(t, http.MethodGet, "/_matrix/client/v3/sync", map[string]string{"filter": string(filter)}, nil).Get("rooms.join." + gjson.Escape(room.ID))
	}
	t.Run("limited sync", func(t *testing.T) {
		joined := syncRoom(t, rootTimelineLimit)
		require.True(t, joined.Get("timeline.limited").Bool(), joined.Raw)
		requireRootBundle(t, byID(joined.Get("timeline.events"), root.EventID()))
	})
	t.Run("unlimited sync", func(t *testing.T) {
		// The client receives every child in the timeline, so it aggregates them itself.
		joined := syncRoom(t, 100)
		require.False(t, joined.Get("timeline.limited").Bool(), joined.Raw)
		event := byID(joined.Get("timeline.events"), root.EventID())
		require.True(t, event.Exists())
		assert.False(t, event.Get(`unsigned.m\.relations`).Exists(), event.Raw)
	})
	t.Run("sliding sync", func(t *testing.T) {
		body := map[string]any{"lists": map[string]any{"all": map[string]any{"ranges": [][]int{{0, 20}}, "timeline_limit": rootTimelineLimit}}}
		roomData := request(t, http.MethodPost, "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync", nil, body).Get("rooms." + gjson.Escape(room.ID))
		require.True(t, roomData.Get("limited").Bool(), roomData.Raw)
		requireRootBundle(t, byID(roomData.Get("timeline"), root.EventID()))
	})
}

// TestEditAggregation checks the m.replace rules of the event replacements
// module: validity, ordering, encryption and redactions.
func TestEditAggregation(t *testing.T) {
	test.WithAllDatabases(t, testEditAggregation)
}

func testEditAggregation(t *testing.T, dbType test.DBType) {
	alice, bob := test.NewUser(t), test.NewUser(t)
	aliceDev := userapi.Device{ID: "ALICEID", UserID: alice.ID, AccessToken: "ALICE_BEARER_TOKEN", AccountType: userapi.AccountTypeUser}

	cfg, processCtx, closeRig := testrig.CreateConfig(t, dbType)
	defer closeRig()
	routers := httputil.NewRouters()
	cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
	caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
	natsInstance := jetstream.NATSInstance{}
	rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
	rsAPI.SetFederationAPI(nil, nil)
	AddPublicRoutes(processCtx, routers, cfg, cm, &natsInstance, accountDataUserAPI{&syncUserAPI{accounts: []userapi.Device{aliceDev}}}, rsAPI, caches, caching.DisableMetrics, nil, nil)
	jsctx, _ := natsInstance.Prepare(processCtx, &cfg.Global.JetStream)
	defer jetstream.DeleteAllStreams(jsctx, &cfg.Global.JetStream)

	room := test.NewRoom(t, alice)
	room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
	start := time.Now()
	at := func(seconds int) time.Time { return start.Add(time.Duration(seconds) * time.Second) }
	replace := func(eventID string, newContent map[string]any) map[string]any {
		content := map[string]any{"msgtype": "m.text", "body": "* edit", "m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": eventID}}
		if newContent != nil {
			content["m.new_content"] = newContent
		}
		return content
	}
	text := func(body string) map[string]any { return map[string]any{"msgtype": "m.text", "body": body} }

	root := room.CreateAndInsert(t, alice, "m.room.message", text("original"), test.WithTimestamp(at(0)))
	first := room.CreateAndInsert(t, alice, "m.room.message", replace(root.EventID(), text("first")), test.WithTimestamp(at(1)))
	second := room.CreateAndInsert(t, alice, "m.room.message", replace(root.EventID(), text("second")), test.WithTimestamp(at(2)))
	// Each of these is newer than every valid edit, but invalid.
	room.CreateAndInsert(t, bob, "m.room.message", replace(root.EventID(), text("by bob")), test.WithTimestamp(at(3)))
	room.CreateAndInsert(t, alice, "org.example.other", replace(root.EventID(), text("other type")), test.WithTimestamp(at(4)))
	room.CreateAndInsert(t, alice, "m.room.message", replace(root.EventID(), nil), test.WithTimestamp(at(5)))
	room.CreateAndInsert(t, alice, "m.room.message", replace(root.EventID(), text("state")), test.WithTimestamp(at(6)), test.WithStateKey(""))
	editOfEdit := room.CreateAndInsert(t, alice, "m.room.message", replace(second.EventID(), text("edit of edit")), test.WithTimestamp(at(7)))
	// Encrypted edits are valid without a cleartext m.new_content.
	encrypted := map[string]any{"algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "opaque", "session_id": "s", "device_id": "D", "sender_key": "k"}
	encryptedRoot := room.CreateAndInsert(t, alice, "m.room.encrypted", encrypted, test.WithTimestamp(at(8)))
	encryptedEdit := room.CreateAndInsert(t, alice, "m.room.encrypted", map[string]any{
		"algorithm": "m.megolm.v1.aes-sha2", "ciphertext": "opaque edit", "session_id": "s", "device_id": "D", "sender_key": "k",
		"m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": encryptedRoot.EventID()},
	}, test.WithTimestamp(at(9)))
	last := room.CreateAndInsert(t, alice, "m.room.message", text("last"), test.WithTimestamp(at(10)))

	require.NoError(t, rsapi.SendEvents(context.Background(), rsAPI, rsapi.KindNew, room.Events(), "test", "test", "test", nil, false))
	syncUntil(t, routers, aliceDev.AccessToken, false, func(syncBody string) bool {
		return gjson.Get(syncBody, fmt.Sprintf(`rooms.join.%s.timeline.events.#(event_id=="%s")`, gjson.Escape(room.ID), last.EventID())).Exists()
	})

	getEvent := func(t *testing.T, eventID string) gjson.Result {
		t.Helper()
		w := httptest.NewRecorder()
		routers.Client.ServeHTTP(w, test.NewRequest(t, http.MethodGet, "/_matrix/client/v3/rooms/"+room.ID+"/event/"+eventID, test.WithQueryParams(map[string]string{"access_token": aliceDev.AccessToken})))
		require.Equal(t, http.StatusOK, w.Code, w.Body.String())
		return gjson.ParseBytes(w.Body.Bytes())
	}
	bundledEdit := func(t *testing.T, eventID string) string {
		t.Helper()
		return getEvent(t, eventID).Get(`unsigned.m\.relations.m\.replace.event_id`).Str
	}
	redact := func(t *testing.T, eventID string, seconds int, until func() bool) {
		t.Helper()
		redaction := room.CreateAndInsert(t, alice, spec.MRoomRedaction, map[string]any{}, test.WithRedacts(eventID), test.WithTimestamp(at(seconds)))
		require.NoError(t, rsapi.SendEvents(context.Background(), rsAPI, rsapi.KindNew, []*rstypes.HeaderedEvent{redaction}, "test", "test", "test", nil, false))
		require.Eventually(t, until, 5*time.Second, 50*time.Millisecond)
	}

	assert.Equal(t, second.EventID(), bundledEdit(t, root.EventID()), "the latest valid edit wins")
	assert.Equal(t, "second", getEvent(t, root.EventID()).Get(`unsigned.m\.relations.m\.replace.content.m\.new_content.body`).Str)
	assert.Empty(t, bundledEdit(t, second.EventID()), "an edit cannot itself be edited: %s", editOfEdit.EventID())
	assert.Equal(t, encryptedEdit.EventID(), bundledEdit(t, encryptedRoot.EventID()))

	// Redacting the latest edit reverts to the one before it.
	redact(t, second.EventID(), 11, func() bool { return bundledEdit(t, root.EventID()) == first.EventID() })
	// Redacting the original drops every edit from its bundle.
	redact(t, root.EventID(), 12, func() bool { return !getEvent(t, root.EventID()).Get(`unsigned.m\.relations.m\.replace`).Exists() })
}
