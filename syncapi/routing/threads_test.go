package routing

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type threadRoomserver struct {
	api.SyncRoomserverAPI
	room       string
	membership string
	hidden     map[string]bool
	join       *rstypes.HeaderedEvent
}

func (r *threadRoomserver) QueryUserIDForSender(_ context.Context, _ spec.RoomID, sender spec.SenderID) (*spec.UserID, error) {
	return spec.NewUserID(string(sender), true)
}

func (r *threadRoomserver) QuerySenderIDForUser(_ context.Context, _ spec.RoomID, user spec.UserID) (*spec.SenderID, error) {
	sender := spec.SenderID(user.String())
	return &sender, nil
}

func (r *threadRoomserver) QueryMembershipForUser(_ context.Context, req *api.QueryMembershipForUserRequest, res *api.QueryMembershipForUserResponse) error {
	res.RoomExists = req.RoomID == r.room
	res.Membership = r.membership
	res.HasBeenInRoom = r.membership == spec.Join || r.membership == spec.Leave
	return nil
}

func (r *threadRoomserver) QueryMembershipAtEvent(_ context.Context, _ spec.RoomID, ids []string, _ spec.SenderID) (map[string]*rstypes.HeaderedEvent, error) {
	result := make(map[string]*rstypes.HeaderedEvent)
	for _, id := range ids {
		if !r.hidden[id] {
			result[id] = r.join
		}
	}
	return result, nil
}

func TestThreads(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		ctx := t.Context()
		conn, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		db, err := storage.NewSyncServerDatasource(ctx, sqlutil.NewConnectionManager(nil, config.DatabaseOptions{}), &config.DatabaseOptions{ConnectionString: config.DataSource(conn)})
		require.NoError(t, err)
		alice, bob := test.NewUser(t), test.NewUser(t)
		room := test.NewRoom(t, alice)
		room.CreateAndInsert(t, bob, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(bob.ID))
		join := room.CreateAndInsert(t, alice, spec.MRoomMember, map[string]any{"membership": spec.Join}, test.WithStateKey(alice.ID))
		rs := &threadRoomserver{room: room.ID, membership: spec.Join, join: join, hidden: map[string]bool{}}
		historyVisibility := gomatrixserverlib.HistoryVisibilityJoined
		write := func(event *rstypes.HeaderedEvent) {
			var state []*rstypes.HeaderedEvent
			var ids []string
			if event.StateKey() != nil {
				state = append(state, event)
				ids = append(ids, event.EventID())
			}
			_, err := db.WriteEvent(ctx, event, state, ids, nil, nil, false, historyVisibility)
			require.NoError(t, err)
			require.NoError(t, db.UpdateRelations(ctx, event))
		}
		for _, event := range room.Events() {
			write(event)
		}
		request := func(query string, status int) *ThreadsResponse {
			res := Threads(httptest.NewRequest(http.MethodGet, "/?"+query, nil), &userapi.Device{UserID: alice.ID}, db, rs, room.ID)
			require.Equal(t, status, res.Code, "%+v", res.JSON)
			if status != http.StatusOK {
				return nil
			}
			body, ok := res.JSON.(*ThreadsResponse)
			require.True(t, ok)
			return body
		}
		require.Empty(t, request("", http.StatusOK).Chunk)
		for _, query := range []string{"limit=0", "limit=-1", "limit=no", "limit=", "include=no", "from=garbage", "from="} {
			t.Run(query, func(t *testing.T) { request(query, http.StatusBadRequest) })
		}
		rootA := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "root A"})
		rootB := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "root B"})
		rootC := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "root C"})
		for _, root := range []*rstypes.HeaderedEvent{rootA, rootB, rootC} {
			write(root)
		}
		reply := func(user *test.User, root *rstypes.HeaderedEvent) *rstypes.HeaderedEvent {
			event := room.CreateAndInsert(t, user, "m.room.message", map[string]any{"body": "reply", "m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": root.EventID()}})
			write(event)
			return event
		}
		reply(bob, rootA)
		reply(bob, rootC)
		latestB := reply(alice, rootB)
		// The last A reply arrives late with an older topology position than B.
		oldA := room.CreateEvent(t, bob, "m.room.message", map[string]any{"body": "late", "m.relates_to": map[string]any{"rel_type": "m.thread", "event_id": rootA.EventID()}})
		room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "advance depth"})
		newestB := reply(alice, rootB)
		write(oldA)
		// Bundle edits on both the root and the nested latest reply, plus references.
		edit := room.CreateAndInsert(t, bob, "m.room.message", map[string]any{"body": "* edited", "m.new_content": map[string]any{"body": "edited"}, "m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": rootB.EventID()}})
		write(edit)
		replyEdit := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "* edited reply", "m.new_content": map[string]any{"body": "edited reply"}, "m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": newestB.EventID()}})
		write(replyEdit)
		reference := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"m.relates_to": map[string]any{"rel_type": "m.reference", "event_id": rootB.EventID()}})
		write(reference)
		all := request("", http.StatusOK)
		require.Len(t, all.Chunk, 3)
		require.Equal(t, rootB.EventID(), all.Chunk[0].EventID)
		require.Equal(t, edit.EventID(), gjson.GetBytes(all.Chunk[0].Unsigned, `m\.relations.m\.replace.event_id`).Str)
		require.Equal(t, replyEdit.EventID(), gjson.GetBytes(all.Chunk[0].Unsigned, `m\.relations.m\.thread.latest_event.unsigned.m\.relations.m\.replace.event_id`).Str)
		require.Equal(t, reference.EventID(), gjson.GetBytes(all.Chunk[0].Unsigned, `m\.relations.m\.reference.chunk.0.event_id`).Str)
		rs.hidden[edit.EventID()] = true
		rs.hidden[reference.EventID()] = true
		hiddenBundles := request("", http.StatusOK)
		require.False(t, gjson.GetBytes(hiddenBundles.Chunk[0].Unsigned, `m\.relations.m\.replace`).Exists())
		require.False(t, gjson.GetBytes(hiddenBundles.Chunk[0].Unsigned, `m\.relations.m\.reference`).Exists())
		delete(rs.hidden, edit.EventID())
		delete(rs.hidden, reference.EventID())

		require.Equal(t, rootA.EventID(), all.Chunk[1].EventID)
		require.Equal(t, rootC.EventID(), all.Chunk[2].EventID)
		require.Equal(t, int64(2), gjson.GetBytes(all.Chunk[0].Unsigned, `m\.relations.m\.thread.count`).Int())
		require.True(t, gjson.GetBytes(all.Chunk[0].Unsigned, `m\.relations.m\.thread.current_user_participated`).Bool())
		require.Len(t, request("include=participated", http.StatusOK).Chunk, 2)
		first := request("limit=1", http.StatusOK)
		require.NotEmpty(t, first.NextBatch)
		// New replies must not move an unvisited root ahead of the saved cursor.
		reply(alice, rootC)
		second := request("limit=1&from="+url.QueryEscape(first.NextBatch), http.StatusOK)
		require.Equal(t, rootA.EventID(), second.Chunk[0].EventID)
		third := request("limit=1&from="+url.QueryEscape(second.NextBatch), http.StatusOK)
		require.Equal(t, rootC.EventID(), third.Chunk[0].EventID)
		require.Empty(t, third.NextBatch)
		// More than one internal batch of replies must still produce one root.
		for range threadPageSize + 1 {
			reply(alice, rootB)
		}
		require.Len(t, request("", http.StatusOK).Chunk, 3)
		require.NoError(t, db.UpdateIgnoresForUser(ctx, alice.ID, &types.IgnoredUsers{List: map[string]any{bob.ID: map[string]any{}}}))
		ignored := request("", http.StatusOK)
		require.Len(t, ignored.Chunk, 2)
		require.False(t, gjson.GetBytes(ignored.Chunk[0].Content, "body").Exists())
		ignoredReply := reply(bob, rootB)
		hiddenReply := reply(alice, rootB)
		rs.hidden[hiddenReply.EventID()] = true
		ignored = request("", http.StatusOK)
		latest := gjson.GetBytes(ignored.Chunk[0].Unsigned, `m\.relations.m\.thread.latest_event.event_id`).String()
		require.NotEqual(t, ignoredReply.EventID(), latest)
		require.NotEqual(t, hiddenReply.EventID(), latest)
		// A hidden root is never returned, even if its replies are visible.
		rs.hidden[rootB.EventID()] = true
		require.Len(t, request("", http.StatusOK).Chunk, 1)
		delete(rs.hidden, rootB.EventID())
		require.NoError(t, db.RedactRelations(ctx, room.ID, latestB.EventID()))
		require.NoError(t, db.UpdateIgnoresForUser(ctx, alice.ID, &types.IgnoredUsers{List: map[string]any{}}))
		redactedReplyRoot := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "only redacted reply"})
		write(redactedReplyRoot)
		onlyReply := reply(alice, redactedReplyRoot)
		require.Len(t, request("", http.StatusOK).Chunk, 4)
		require.NoError(t, db.RedactRelations(ctx, room.ID, onlyReply.EventID()))
		require.Len(t, request("", http.StatusOK).Chunk, 3)
		// Relations cannot turn an event from another room into a local thread root.
		otherRoom := test.NewRoom(t, alice)
		foreignRoot := otherRoom.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "foreign"})
		write(foreignRoot)
		reply(alice, foreignRoot)
		require.Len(t, request("", http.StatusOK).Chunk, 3)
		// The registered stable path requires authentication and serves authenticated requests.
		router := httputil.NewRouter("/_matrix/client")
		Setup(router, nil, db, &threadUserAPI{device: userapi.Device{UserID: alice.ID}}, rs, &config.SyncAPI{}, nil, nil, httputil.NewRateLimits(&config.RateLimiting{}), nil)
		for _, token := range []string{"", "valid"} {
			req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v1/rooms/"+room.ID+"/threads", nil)
			if token != "" {
				req.Header.Set("Authorization", "Bearer "+token)
			}
			rec := httptest.NewRecorder()
			router.ServeHTTP(rec, req)
			if token == "" {
				require.Equal(t, http.StatusUnauthorized, rec.Code)
			} else {
				require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
			}
		}

		for _, membership := range []string{spec.Ban, spec.Invite, ""} {
			rs.membership = membership
			request("", http.StatusForbidden)
		}
		rs.membership = spec.Join
		unknown := Threads(httptest.NewRequest(http.MethodGet, "/", nil), &userapi.Device{UserID: alice.ID}, db, rs, "!unknown:test")
		require.Equal(t, http.StatusForbidden, unknown.Code)
		rs.membership = spec.Leave
		request("", http.StatusOK)
		// World-readable rooms permit peeking without prior membership.
		world := room.CreateAndInsert(t, alice, spec.MRoomHistoryVisibility, map[string]any{"history_visibility": spec.WorldReadable}, test.WithStateKey(""))
		write(world)
		rs.membership = ""
		historyVisibility = gomatrixserverlib.HistoryVisibilityWorldReadable
		publicRoot := room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "public"})
		write(publicRoot)
		reply(alice, publicRoot)
		// The roomserver reports no historic membership for the peeking user.
		rs.join = nil
		peek := request("", http.StatusOK)
		require.Len(t, peek.Chunk, 1)
		require.Equal(t, publicRoot.EventID(), peek.Chunk[0].EventID)
		rs.membership = spec.Ban
		request("", http.StatusForbidden)
	})
}

type threadUserAPI struct {
	userapi.SyncUserAPI
	device userapi.Device
}

func (u *threadUserAPI) QueryAccessToken(_ context.Context, _ *userapi.QueryAccessTokenRequest, res *userapi.QueryAccessTokenResponse) error {
	res.Device = &u.device
	return nil
}
