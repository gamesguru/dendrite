package routing

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/element-hq/dendrite/federationapi/statistics"
	"github.com/element-hq/dendrite/internal/caching"
	"github.com/element-hq/dendrite/internal/sqlutil"
	"github.com/element-hq/dendrite/setup/jetstream"
	"github.com/matrix-org/gomatrixserverlib"
	"github.com/matrix-org/gomatrixserverlib/spec"

	"github.com/element-hq/dendrite/appservice"
	"github.com/element-hq/dendrite/roomserver"
	"github.com/element-hq/dendrite/test"
	"github.com/element-hq/dendrite/test/testrig"
	"github.com/element-hq/dendrite/userapi"
	uapi "github.com/element-hq/dendrite/userapi/api"
	"golang.org/x/sync/singleflight"
)

var testIsBlacklistedOrBackingOff = func(s spec.ServerName) (*statistics.ServerStatistics, error) {
	return &statistics.ServerStatistics{}, nil
}

func TestJoinRoomByIDOrAlias(t *testing.T) {
	alice := test.NewUser(t)
	bob := test.NewUser(t)
	charlie := test.NewUser(t, test.WithAccountType(uapi.AccountTypeGuest))

	ctx := context.Background()
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		cfg, processCtx, close := testrig.CreateConfig(t, dbType)
		defer close()

		cm := sqlutil.NewConnectionManager(processCtx, cfg.Global.DatabaseOptions)
		caches := caching.NewRistrettoCache(128*1024*1024, time.Hour, caching.DisableMetrics)
		natsInstance := jetstream.NATSInstance{}
		rsAPI := roomserver.NewInternalAPI(processCtx, cfg, cm, &natsInstance, caches, caching.DisableMetrics)
		rsAPI.SetFederationAPI(nil, nil) // creates the rs.Inputer etc
		userAPI := userapi.NewInternalAPI(processCtx, cfg, cm, &natsInstance, rsAPI, nil, caching.DisableMetrics, testIsBlacklistedOrBackingOff)
		asAPI := appservice.NewInternalAPI(processCtx, cfg, &natsInstance, userAPI, rsAPI)

		// Create the users in the userapi
		for _, u := range []*test.User{alice, bob, charlie} {
			localpart, serverName, _ := gomatrixserverlib.SplitID('@', u.ID)
			userRes := &uapi.PerformAccountCreationResponse{}
			if err := userAPI.PerformAccountCreation(ctx, &uapi.PerformAccountCreationRequest{
				AccountType: u.AccountType,
				Localpart:   localpart,
				ServerName:  serverName,
				Password:    "someRandomPassword",
			}, userRes); err != nil {
				t.Errorf("failed to create account: %s", err)
			}

		}

		aliceDev := &uapi.Device{UserID: alice.ID}
		bobDev := &uapi.Device{UserID: bob.ID}
		charlieDev := &uapi.Device{UserID: charlie.ID, AccountType: uapi.AccountTypeGuest}

		// create a room with disabled guest access and invite Bob
		resp := createRoom(ctx, createRoomRequest{
			Name:          "testing",
			IsDirect:      true,
			Topic:         "testing",
			Visibility:    "public",
			Preset:        spec.PresetPublicChat,
			RoomAliasName: "alias",
			Invite:        []string{bob.ID},
		}, aliceDev, &cfg.ClientAPI, userAPI, rsAPI, asAPI, time.Now())
		crResp, ok := resp.JSON.(createRoomResponse)
		if !ok {
			t.Fatalf("response is not a createRoomResponse: %+v", resp)
		}

		// create a room with guest access enabled and invite Charlie
		resp = createRoom(ctx, createRoomRequest{
			Name:       "testing",
			IsDirect:   true,
			Topic:      "testing",
			Visibility: "public",
			Preset:     spec.PresetPublicChat,
			Invite:     []string{charlie.ID},
		}, aliceDev, &cfg.ClientAPI, userAPI, rsAPI, asAPI, time.Now())
		crRespWithGuestAccess, ok := resp.JSON.(createRoomResponse)
		if !ok {
			t.Fatalf("response is not a createRoomResponse: %+v", resp)
		}

		testCases := []struct {
			name        string
			device      *uapi.Device
			roomID      string
			wantHTTP200 bool
		}{
			{
				name:        "User can join successfully by alias",
				device:      bobDev,
				roomID:      crResp.RoomAlias,
				wantHTTP200: true,
			},
			{
				name:        "User can join successfully by roomID",
				device:      bobDev,
				roomID:      crResp.RoomID,
				wantHTTP200: true,
			},
			{
				name:   "join is forbidden if user is guest",
				device: charlieDev,
				roomID: crResp.RoomID,
			},
			{
				name:   "room does not exist",
				device: aliceDev,
				roomID: "!doesnotexist:test",
			},
			{
				name:   "user from different server",
				device: &uapi.Device{UserID: "@wrong:server"},
				roomID: crResp.RoomAlias,
			},
			{
				name:   "user doesn't exist locally",
				device: &uapi.Device{UserID: "@doesnotexist:test"},
				roomID: crResp.RoomAlias,
			},
			{
				name:   "invalid room ID",
				device: aliceDev,
				roomID: "invalidRoomID",
			},
			{
				name:   "roomAlias does not exist",
				device: aliceDev,
				roomID: "#doesnotexist:test",
			},
			{
				name:   "room with guest_access event",
				device: charlieDev,
				roomID: crRespWithGuestAccess.RoomID,
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				req, err := http.NewRequest(http.MethodPost, "/?server_name=test", &bytes.Buffer{})
				if err != nil {
					t.Fatal(err)
				}
				content, serverNames, resErr := joinRequestContentAndServers(req)
				if resErr != nil {
					t.Fatalf("joinRequestContentAndServers returned error: %+v", resErr)
				}
				joinResp := JoinRoomByIDOrAlias(context.Background(), tc.device, rsAPI, userAPI, tc.roomID, content, serverNames)
				if tc.wantHTTP200 && !joinResp.Is2xx() {
					t.Fatalf("expected join room to succeed, but didn't: %+v", joinResp)
				}
			})
		}
	})
}

func TestJoinRoomMalformedJSONDoesNotStartJoin(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "/?server_name=test", strings.NewReader("{"))
	if err != nil {
		t.Fatal(err)
	}
	resp := joinRoomByIDOrAliasWithTimeout(
		req,
		&uapi.Device{UserID: "@alice:test"},
		&singleflight.Group{},
		nil,
		nil,
		"!room:test",
	)
	if resp.Code != http.StatusBadRequest {
		t.Fatalf("expected malformed JSON to return HTTP 400, got %+v", resp)
	}
}

func TestJoinRoomSlowBodyReturnsAsyncResponse(t *testing.T) {
	req, err := http.NewRequest(http.MethodPost, "/?server_name=test", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Body = newBlockingReadCloser()

	start := time.Now()
	resp := joinRoomByIDOrAliasWithTimeoutDuration(
		req,
		&uapi.Device{UserID: "@alice:test"},
		&singleflight.Group{},
		nil,
		nil,
		"!room:test",
		10*time.Millisecond,
	)
	if resp.Code != http.StatusAccepted {
		t.Fatalf("expected slow body to return HTTP 202, got %+v", resp)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("slow body timeout took too long: %s", elapsed)
	}
}

type blockingReadCloser struct {
	closed chan struct{}
	once   sync.Once
}

func newBlockingReadCloser() *blockingReadCloser {
	return &blockingReadCloser{closed: make(chan struct{})}
}

func (b *blockingReadCloser) Read(_ []byte) (int, error) {
	<-b.closed
	return 0, io.ErrClosedPipe
}

func (b *blockingReadCloser) Close() error {
	b.once.Do(func() {
		close(b.closed)
	})
	return nil
}
