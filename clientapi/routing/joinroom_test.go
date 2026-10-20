package routing

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gamesguru/gomatrixserverlib"
	"github.com/gamesguru/gomatrixserverlib/spec"

	"codefloe.com/pat-s/zendrite/appservice"
	"codefloe.com/pat-s/zendrite/federationapi/statistics"
	"codefloe.com/pat-s/zendrite/internal/caching"
	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/roomserver"
	"codefloe.com/pat-s/zendrite/setup/jetstream"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/test/testrig"
	"codefloe.com/pat-s/zendrite/userapi"
	uapi "codefloe.com/pat-s/zendrite/userapi/api"
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
		asAPI := appservice.NewInternalAPI(processCtx, cfg, &natsInstance, userAPI, rsAPI) //nolint:contextcheck

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

		// create an invite-only room that nobody else is invited to
		resp = createRoom(ctx, createRoomRequest{
			Name:       "private",
			Topic:      "private",
			Visibility: "private",
			Preset:     spec.PresetPrivateChat,
		}, aliceDev, &cfg.ClientAPI, userAPI, rsAPI, asAPI, time.Now())
		crRespPrivate, ok := resp.JSON.(createRoomResponse)
		if !ok {
			t.Fatalf("response is not a createRoomResponse: %+v", resp)
		}

		// Dummy request
		body := &bytes.Buffer{}
		req, err := http.NewRequest(http.MethodPost, "/?server_name=test", body)
		if err != nil {
			t.Fatal(err)
		}

		testCases := []struct {
			name        string
			device      *uapi.Device
			roomID      string
			wantHTTP200 bool
			// wantCode, when non-zero, asserts the exact status code returned.
			wantCode int
			// wantErrContains, when set, asserts the error shown to the client
			// mentions why the join was refused.
			wantErrContains string
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
			{
				// A rejected join must tell the client why it was refused,
				// not just that something was rejected.
				name:            "join is forbidden if the room is invite only",
				device:          bobDev,
				roomID:          crRespPrivate.RoomID,
				wantCode:        http.StatusForbidden,
				wantErrContains: "join rule",
			},
		}

		for _, tc := range testCases {
			t.Run(tc.name, func(t *testing.T) {
				joinResp := JoinRoomByIDOrAlias(req, tc.device, rsAPI, userAPI, tc.roomID)
				if tc.wantHTTP200 && !joinResp.Is2xx() {
					t.Fatalf("expected join room to succeed, but didn't: %+v", joinResp)
				}
				if tc.wantCode != 0 && joinResp.Code != tc.wantCode {
					t.Fatalf("expected status %d, got %d: %+v", tc.wantCode, joinResp.Code, joinResp)
				}
				if tc.wantErrContains != "" {
					body, err := json.Marshal(joinResp.JSON)
					if err != nil {
						t.Fatalf("failed to marshal response: %s", err)
					}
					if !strings.Contains(string(body), tc.wantErrContains) {
						t.Fatalf("expected error to contain %q, got %s", tc.wantErrContains, body)
					}
				}
			})
		}
	})
}
