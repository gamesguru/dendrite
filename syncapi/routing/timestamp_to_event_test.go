package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"codefloe.com/pat-s/gomatrixserverlib"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/httputil"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver/api"
	rstypes "codefloe.com/pat-s/zendrite/roomserver/types"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/syncapi/storage"
	"codefloe.com/pat-s/zendrite/test"
	userapi "codefloe.com/pat-s/zendrite/userapi/api"
)

type timestampRoomserver struct {
	api.SyncRoomserverAPI
	userID      string
	memberships map[string]*rstypes.HeaderedEvent
}

func (s *timestampRoomserver) QuerySenderIDForUser(_ context.Context, _ spec.RoomID, userID spec.UserID) (*spec.SenderID, error) {
	if userID.String() != s.userID {
		return nil, nil
	}
	sender := spec.SenderID(userID.String())
	return &sender, nil
}

func (s *timestampRoomserver) QueryMembershipAtEvent(_ context.Context, _ spec.RoomID, eventIDs []string, _ spec.SenderID) (map[string]*rstypes.HeaderedEvent, error) {
	result := make(map[string]*rstypes.HeaderedEvent)
	for _, id := range eventIDs {
		result[id] = s.memberships[id]
	}
	return result, nil
}

type timestampUserAPI struct {
	userapi.SyncUserAPI
	device userapi.Device
}

func (s *timestampUserAPI) QueryAccessToken(_ context.Context, req *userapi.QueryAccessTokenRequest, res *userapi.QueryAccessTokenResponse) error {
	if req.AccessToken == "valid" {
		res.Device = &s.device
	}
	return nil
}

func TestTimestampToEvent(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		connStr, closeDB := test.PrepareDBConnectionString(t, dbType)
		defer closeDB()
		options := config.DatabaseOptions{ConnectionString: config.DataSource(connStr)}
		cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
		db, err := storage.NewSyncServerDatasource(t.Context(), cm, &options)
		require.NoError(t, err)
		sqlDB, _, err := cm.Connection(&options)
		require.NoError(t, err)
		defer sqlDB.Close()

		alice := test.NewUser(t)
		bob := test.NewUser(t)
		room := test.NewRoom(t, alice)
		rs := &timestampRoomserver{userID: alice.ID, memberships: make(map[string]*rstypes.HeaderedEvent)}
		membership := room.CreateEvent(t, alice, spec.MRoomMember, map[string]string{"membership": spec.Join}, test.WithStateKey(alice.ID))
		write := func(room *test.Room, ts int64, visible, backfill bool, visibility gomatrixserverlib.HistoryVisibility) *rstypes.HeaderedEvent {
			event := room.CreateAndInsert(t, alice, "m.room.message", map[string]string{"body": fmt.Sprint(ts)}, test.WithTimestamp(time.UnixMilli(ts)))
			_, err := db.WriteEvent(t.Context(), event, nil, nil, nil, nil, backfill, visibility)
			require.NoError(t, err)
			if visible {
				rs.memberships[event.EventID()] = membership
			}
			return event
		}
		// Timestamp order deliberately differs from insertion and topological order.
		later := write(room, 3000, true, false, gomatrixserverlib.HistoryVisibilityJoined)
		earlier := write(room, 1000, true, true, gomatrixserverlib.HistoryVisibilityJoined)
		write(room, 2000, false, false, gomatrixserverlib.HistoryVisibilityJoined)
		publicRoom := test.NewRoom(t, alice)
		public := write(publicRoom, 2000, false, false, gomatrixserverlib.HistoryVisibilityWorldReadable)

		// Simulate upgrading a database whose event rows predate the timestamp index.
		_, err = sqlDB.ExecContext(t.Context(), "DROP INDEX syncapi_output_room_events_timestamp_idx")
		require.NoError(t, err)
		db, err = storage.NewSyncServerDatasource(t.Context(), cm, &options)
		require.NoError(t, err)

		device := userapi.Device{UserID: alice.ID, ID: "device"}
		userAPI := &timestampUserAPI{device: device}
		router := httputil.NewRouter("/_matrix/client")
		Setup(router, nil, db, userAPI, rs, &config.SyncAPI{}, nil, nil, httputil.NewRateLimits(&config.RateLimiting{}))

		type lookupCase struct {
			name, query, roomID, userID string
			want                        *rstypes.HeaderedEvent
			status                      int
		}
		cases := []lookupCase{
			{name: "forward", query: "ts=1500&dir=f", want: later},
			{name: "backward backfilled", query: "ts=2500&dir=b", want: earlier},
			{name: "exact forward", query: "ts=1000&dir=f", want: earlier},
			{name: "exact backward", query: "ts=3000&dir=b", want: later},
			{name: "invisible exact forward", query: "ts=2000&dir=f", want: later},
			{name: "invisible exact backward", query: "ts=2000&dir=b", want: earlier},
			{name: "epoch", query: "ts=0&dir=f", want: earlier},
			{name: "negative", query: "ts=-1&dir=f", want: earlier},
			{name: "no forward result", query: "ts=3001&dir=f", status: http.StatusNotFound},
			{name: "no backward result", query: "ts=999&dir=b", status: http.StatusNotFound},
			{name: "unknown room", query: "ts=0&dir=f", roomID: "!unknown:example.com", status: http.StatusNotFound},
			{name: "inaccessible", query: "ts=0&dir=f", userID: bob.ID, status: http.StatusNotFound},
			{name: "world readable", query: "ts=0&dir=f", roomID: publicRoom.ID, userID: bob.ID, want: public},
			{name: "invalid room", query: "ts=0&dir=f", roomID: "invalid", status: http.StatusBadRequest},
		}
		for _, query := range []string{"dir=f", "ts=1000", "ts=&dir=f", "ts=x&dir=f", "ts=1.5&dir=f", "ts=9223372036854775808&dir=f", "ts=0&dir=", "ts=0&dir=x", "ts=0&dir=forward", "ts=0&ts=1&dir=f", "ts=0&dir=b&dir=f"} {
			cases = append(cases, lookupCase{name: query, query: query, status: http.StatusBadRequest})
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				roomID := tc.roomID
				if roomID == "" {
					roomID = room.ID
				}
				userAPI.device.UserID = tc.userID
				if tc.userID == "" {
					userAPI.device.UserID = alice.ID
				}
				req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v1/rooms/"+url.PathEscape(roomID)+"/timestamp_to_event?"+tc.query, nil)
				req.Header.Set("Authorization", "Bearer valid")
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				status := tc.status
				if status == 0 {
					status = http.StatusOK
				}
				require.Equal(t, status, rec.Code, rec.Body.String())
				if tc.want != nil {
					var result timestampToEventResponse
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
					require.Equal(t, tc.want.EventID(), result.EventID)
					require.Equal(t, tc.want.OriginServerTS(), result.OriginServerTS)
				} else if status == http.StatusNotFound {
					var result spec.MatrixError
					require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &result))
					require.Equal(t, spec.ErrorNotFound, result.ErrCode)
				}
			})
		}
		t.Run("authentication", func(t *testing.T) {
			for _, token := range []string{"", "invalid"} {
				req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v1/rooms/"+url.PathEscape(room.ID)+"/timestamp_to_event?ts=0&dir=f", nil)
				if token != "" {
					req.Header.Set("Authorization", "Bearer "+token)
				}
				rec := httptest.NewRecorder()
				router.ServeHTTP(rec, req)
				require.Equal(t, http.StatusUnauthorized, rec.Code)
			}
		})
		t.Run("rate limiting", func(t *testing.T) {
			limitedRouter := httputil.NewRouter("/_matrix/client")
			limits := httputil.NewRateLimits(&config.RateLimiting{Enabled: true, Threshold: 0, CooloffMS: 1000})
			Setup(limitedRouter, nil, db, userAPI, rs, &config.SyncAPI{}, nil, nil, limits)
			req := httptest.NewRequest(http.MethodGet, "/_matrix/client/v1/rooms/"+url.PathEscape(room.ID)+"/timestamp_to_event?ts=0&dir=f", nil)
			req.Header.Set("Authorization", "Bearer valid")
			rec := httptest.NewRecorder()
			limitedRouter.ServeHTTP(rec, req)
			require.Equal(t, http.StatusTooManyRequests, rec.Code)
		})
		t.Run("visibility across timestamp ties and batches", func(t *testing.T) {
			backward := write(room, 5000, true, false, gomatrixserverlib.HistoryVisibilityJoined)
			for range timestampLookupBatchSize + 1 {
				write(room, 5000, false, false, gomatrixserverlib.HistoryVisibilityJoined)
			}
			forward := write(room, 5000, true, false, gomatrixserverlib.HistoryVisibilityJoined)
			for _, backwards := range []bool{false, true} {
				// Hide the first candidate so the other visible event is beyond a full batch.
				hidden, want, dir := backward, forward, "f"
				if backwards {
					hidden, want, dir = forward, backward, "b"
				}
				delete(rs.memberships, hidden.EventID())
				rs.memberships[want.EventID()] = membership
				req := httptest.NewRequest(http.MethodGet, "/?ts=5000&dir="+dir, nil)
				result := TimestampToEvent(req, &device, room.ID, db, rs)
				require.Equal(t, http.StatusOK, result.Code)
				body, ok := result.JSON.(timestampToEventResponse)
				require.True(t, ok)
				require.Equal(t, want.EventID(), body.EventID)
			}
		})
	})
}
