package routing

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"
	"github.com/tidwall/gjson"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/syncapi/types"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

func TestRelations(t *testing.T) {
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
		write := func(event *rstypes.HeaderedEvent) *rstypes.HeaderedEvent {
			var state []*rstypes.HeaderedEvent
			var ids []string
			if event.StateKey() != nil {
				state = append(state, event)
				ids = append(ids, event.EventID())
			}
			_, err := db.WriteEvent(ctx, event, state, ids, nil, nil, false, gomatrixserverlib.HistoryVisibilityJoined)
			require.NoError(t, err)
			require.NoError(t, db.UpdateRelations(ctx, event))
			return event
		}
		for _, event := range room.Events() {
			write(event)
		}
		relate := func(user *test.User, eventType, relType string, parent *rstypes.HeaderedEvent) *rstypes.HeaderedEvent {
			return write(room.CreateAndInsert(t, user, eventType, map[string]any{"body": relType, "m.relates_to": map[string]any{"rel_type": relType, "event_id": parent.EventID()}}))
		}
		request := func(t *testing.T, parent, relType, eventType, query string, status int) (*RelationsResponse, []string) {
			t.Helper()
			res := Relations(httptest.NewRequest(http.MethodGet, "/?"+query, nil), &userapi.Device{UserID: alice.ID}, db, rs, room.ID, parent, relType, eventType)
			require.Equal(t, status, res.Code, "%+v", res.JSON)
			if status != http.StatusOK {
				return nil, nil
			}
			body, ok := res.JSON.(*RelationsResponse)
			require.True(t, ok)
			ids := []string{}
			for _, event := range body.Chunk {
				ids = append(ids, event.EventID)
			}
			return body, ids
		}

		root := write(room.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "root"}))
		reply := relate(bob, "m.room.message", "m.thread", root)
		edit := relate(alice, "m.room.message", "m.replace", root)
		reaction := relate(bob, "m.reaction", "m.annotation", reply)
		reference := relate(alice, "m.room.message", "m.reference", reaction)
		deepest := relate(alice, "m.room.message", "m.reference", reference)
		lastReply := relate(alice, "m.room.message", "m.thread", root)

		t.Run("missing or hidden parents are not found", func(t *testing.T) {
			request(t, "$missing", "", "", "", http.StatusNotFound)
			otherRoom := test.NewRoom(t, alice)
			foreign := write(otherRoom.CreateAndInsert(t, alice, "m.room.message", map[string]any{"body": "foreign"}))
			request(t, foreign.EventID(), "", "", "", http.StatusNotFound)
			rs.hidden[root.EventID()] = true
			defer delete(rs.hidden, root.EventID())
			request(t, root.EventID(), "", "", "", http.StatusNotFound)
		})
		t.Run("invalid parameters", func(t *testing.T) {
			for _, query := range []string{"dir=x", "limit=-1", "limit=no", "recurse=maybe", "from=garbage", "to=garbage"} {
				request(t, root.EventID(), "", "", query, http.StatusBadRequest)
			}
		})
		t.Run("topological order in both directions", func(t *testing.T) {
			res, ids := request(t, root.EventID(), "", "", "", http.StatusOK)
			require.Equal(t, []string{lastReply.EventID(), edit.EventID(), reply.EventID()}, ids)
			require.Empty(t, res.NextBatch)
			require.Empty(t, res.PrevBatch)
			require.Nil(t, res.RecursionDepth, "recursion_depth must be absent without recurse")
			_, ids = request(t, root.EventID(), "", "", "dir=f", http.StatusOK)
			require.Equal(t, []string{reply.EventID(), edit.EventID(), lastReply.EventID()}, ids)
		})
		t.Run("pagination", func(t *testing.T) {
			for _, dir := range []string{"b", "f"} {
				var all []string
				query := "limit=1&dir=" + dir
				for {
					res, ids := request(t, root.EventID(), "", "", query, http.StatusOK)
					all = append(all, ids...)
					if res.NextBatch == "" {
						break
					}
					query = "limit=1&dir=" + dir + "&from=" + url.QueryEscape(res.NextBatch)
				}
				want := []string{lastReply.EventID(), edit.EventID(), reply.EventID()}
				if dir == "f" {
					want = []string{reply.EventID(), edit.EventID(), lastReply.EventID()}
				}
				require.Equal(t, want, all, dir)
			}
		})
		t.Run("tokens from messages and sync", func(t *testing.T) {
			// A /messages token names the cursor after an event.
			snapshot, err := db.NewDatabaseSnapshot(ctx)
			require.NoError(t, err)
			position, err := snapshot.EventPositionInTopology(ctx, edit.EventID())
			require.NoError(t, err)
			require.NoError(t, snapshot.Rollback())
			res, ids := request(t, root.EventID(), "", "", "from="+url.QueryEscape(position.String()), http.StatusOK)
			require.Equal(t, []string{edit.EventID(), reply.EventID()}, ids)
			require.Equal(t, position.String(), res.PrevBatch, "prev_batch echoes the from token")
			_, ids = request(t, root.EventID(), "", "", "dir=f&from="+url.QueryEscape(position.String()), http.StatusOK)
			require.Equal(t, []string{lastReply.EventID()}, ids)
			_, ids = request(t, root.EventID(), "", "", "to="+url.QueryEscape(position.String()), http.StatusOK)
			require.Equal(t, []string{lastReply.EventID()}, ids)
			// A /sync token covers every event up to its stream position.
			sync := types.StreamingToken{PDUPosition: position.PDUPosition}
			_, ids = request(t, root.EventID(), "", "", "from="+url.QueryEscape(sync.String()), http.StatusOK)
			require.Equal(t, []string{edit.EventID(), reply.EventID()}, ids)
		})
		t.Run("filters", func(t *testing.T) {
			_, ids := request(t, root.EventID(), "m.thread", "", "", http.StatusOK)
			require.Equal(t, []string{lastReply.EventID(), reply.EventID()}, ids)
			_, ids = request(t, root.EventID(), "m.thread", "m.reaction", "", http.StatusOK)
			require.Empty(t, ids)
			_, ids = request(t, reply.EventID(), "m.annotation", "m.reaction", "", http.StatusOK)
			require.Equal(t, []string{reaction.EventID()}, ids)
		})
		t.Run("recurse", func(t *testing.T) {
			res, ids := request(t, root.EventID(), "", "", "recurse=true", http.StatusOK)
			require.Equal(t, 3, *res.RecursionDepth)
			// The deepest reference is four relations away from the root.
			require.Equal(t, []string{lastReply.EventID(), reference.EventID(), reaction.EventID(), edit.EventID(), reply.EventID()}, ids, deepest.EventID())
			res, ids = request(t, root.EventID(), "", "", "recurse=false", http.StatusOK)
			require.Equal(t, 1, *res.RecursionDepth)
			require.Len(t, ids, 3)
			_, ids = request(t, root.EventID(), "m.reference", "", "org.matrix.msc3981.recurse=true", http.StatusOK)
			require.Equal(t, []string{reference.EventID()}, ids)
		})
		t.Run("hidden and ignored children", func(t *testing.T) {
			rs.hidden[edit.EventID()] = true
			defer delete(rs.hidden, edit.EventID())
			require.NoError(t, db.UpdateIgnoresForUser(ctx, alice.ID, &types.IgnoredUsers{List: map[string]any{bob.ID: map[string]any{}}}))
			defer func() {
				require.NoError(t, db.UpdateIgnoresForUser(ctx, alice.ID, &types.IgnoredUsers{List: map[string]any{}}))
			}()
			_, ids := request(t, root.EventID(), "", "", "", http.StatusOK)
			require.Equal(t, []string{lastReply.EventID()}, ids)
		})
		t.Run("children carry their own bundles", func(t *testing.T) {
			replyEdit := write(room.CreateAndInsert(t, bob, "m.room.message", map[string]any{
				"body": "* edited", "m.new_content": map[string]any{"body": "edited"},
				"m.relates_to": map[string]any{"rel_type": "m.replace", "event_id": reply.EventID()},
			}))
			res, ids := request(t, root.EventID(), "m.thread", "", "", http.StatusOK)
			require.Equal(t, reply.EventID(), ids[1])
			require.Equal(t, replyEdit.EventID(), gjson.GetBytes(res.Chunk[1].Unsigned, `m\.relations.m\.replace.event_id`).Str)
		})
	})
}
