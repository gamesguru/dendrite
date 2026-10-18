package internal_test

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/internal/sqlutil"
	"codefloe.com/pat-s/zendrite/setup/config"
	"codefloe.com/pat-s/zendrite/test"
	"codefloe.com/pat-s/zendrite/userapi/api"
	"codefloe.com/pat-s/zendrite/userapi/internal"
	"codefloe.com/pat-s/zendrite/userapi/storage"
)

func mustCreateDatabase(t *testing.T, dbType test.DBType) (storage.KeyDatabase, func()) {
	t.Helper()
	connStr, close := test.PrepareDBConnectionString(t, dbType)
	cm := sqlutil.NewConnectionManager(nil, config.DatabaseOptions{})
	db, err := storage.NewKeyDatabase(cm, &config.DatabaseOptions{
		ConnectionString: config.DataSource(connStr),
	})
	if err != nil {
		t.Fatalf("failed to create new user db: %v", err)
	}
	return db, func() {
		close()
	}
}

func Test_QueryDeviceMessages(t *testing.T) {
	ctx := context.Background()

	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		// Create test data inside the callback to avoid data races
		alice := test.NewUser(t)

		type args struct {
			req *api.QueryDeviceMessagesRequest
			res *api.QueryDeviceMessagesResponse
		}
		tests := []struct {
			name    string
			args    args
			wantErr bool
			want    *api.QueryDeviceMessagesResponse
		}{
			{
				name: "no existing keys",
				args: args{
					req: &api.QueryDeviceMessagesRequest{
						UserID: "@doesNotExist:localhost",
					},
					res: &api.QueryDeviceMessagesResponse{},
				},
				want: &api.QueryDeviceMessagesResponse{},
			},
			{
				name: "existing user returns devices",
				args: args{
					req: &api.QueryDeviceMessagesRequest{
						UserID: alice.ID,
					},
					res: &api.QueryDeviceMessagesResponse{},
				},
				want: &api.QueryDeviceMessagesResponse{
					StreamID: 6,
					Devices: []api.DeviceMessage{
						{
							Type: api.TypeDeviceKeyUpdate, StreamID: 5, DeviceKeys: &api.DeviceKeys{
								DeviceID:    "myDevice",
								DisplayName: "first device",
								UserID:      alice.ID,
								KeyJSON:     []byte("ghi"),
							},
						},
						{
							Type: api.TypeDeviceKeyUpdate, StreamID: 6, DeviceKeys: &api.DeviceKeys{
								DeviceID:    "mySecondDevice",
								DisplayName: "second device",
								UserID:      alice.ID,
								KeyJSON:     []byte("jkl"),
							}, // streamID 6
						},
					},
				},
			},
		}

		deviceMessages := []api.DeviceMessage{
			{ // not the user we're looking for
				Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
					UserID: "@doesNotExist:localhost",
				},
				// streamID 1 for this user
			},
			{ // empty keyJSON will be ignored
				Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
					DeviceID: "myDevice",
					UserID:   alice.ID,
				}, // streamID 1
			},
			{
				Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
					DeviceID: "myDevice",
					UserID:   alice.ID,
					KeyJSON:  []byte("abc"),
				}, // streamID 2
			},
			{
				Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
					DeviceID: "myDevice",
					UserID:   alice.ID,
					KeyJSON:  []byte("def"),
				}, // streamID 3
			},
			{
				Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
					DeviceID: "myDevice",
					UserID:   alice.ID,
					KeyJSON:  []byte(""),
				}, // streamID 4
			},
			{
				Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
					DeviceID:    "myDevice",
					DisplayName: "first device",
					UserID:      alice.ID,
					KeyJSON:     []byte("ghi"),
				}, // streamID 5
			},
			{
				Type: api.TypeDeviceKeyUpdate, DeviceKeys: &api.DeviceKeys{
					DeviceID:    "mySecondDevice",
					UserID:      alice.ID,
					KeyJSON:     []byte("jkl"),
					DisplayName: "second device",
				}, // streamID 6
			},
		}
		db, closeDB := mustCreateDatabase(t, dbType) //nolint:contextcheck
		defer closeDB()
		if err := db.StoreLocalDeviceKeys(ctx, deviceMessages); err != nil {
			t.Fatalf("failed to store local devicesKeys")
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				a := &internal.UserInternalAPI{
					KeyDatabase: db,
				}
				if err := a.QueryDeviceMessages(ctx, tt.args.req, tt.args.res); (err != nil) != tt.wantErr {
					t.Errorf("QueryDeviceMessages() error = %v, wantErr %v", err, tt.wantErr)
				}
				got := tt.args.res
				if !reflect.DeepEqual(got, tt.want) {
					t.Errorf("QueryDeviceMessages(): got:\n%+v, want:\n%+v", got, tt.want)
				}
			})
		}
	})
}

func TestFallbackKeyUploadAndClaim(t *testing.T) {
	test.WithAllDatabases(t, func(t *testing.T, dbType test.DBType) {
		db, closeDB := mustCreateDatabase(t, dbType)
		defer closeDB()
		ctx := t.Context()
		cfg := &config.UserAPI{}
		cfg.Matrix = &config.Global{ServerName: "localhost"}
		a := &internal.UserInternalAPI{KeyDatabase: db, Config: cfg}
		userID, deviceID := "@alice:localhost", "DEVICE"
		upload := func(keys map[string]json.RawMessage) []string {
			t.Helper()
			res := api.PerformUploadKeysResponse{}
			req := api.PerformUploadKeysRequest{UserID: userID, DeviceID: deviceID}
			if keys != nil {
				req.FallbackKeys = []api.FallbackKeys{{UserID: userID, DeviceID: deviceID, KeyJSON: keys}}
			}
			require.NoError(t, a.PerformUploadKeys(ctx, &req, &res))
			require.Nil(t, res.Error)
			require.Empty(t, res.KeyErrors)
			return res.FallbackKeysUnusedAlgorithms
		}
		claim := func(algorithm, keyID string) {
			t.Helper()
			res := api.PerformClaimKeysResponse{}
			a.PerformClaimKeys(ctx, &api.PerformClaimKeysRequest{OneTimeKeys: map[string]map[string]string{userID: {deviceID: algorithm}}}, &res)
			require.Nil(t, res.Error)
			require.Contains(t, res.OneTimeKeys[userID][deviceID], algorithm+":"+keyID)
		}
		first := map[string]json.RawMessage{"signed_curve25519:first": json.RawMessage(`{"key":"first","fallback":true}`)}
		require.Equal(t, []string{"signed_curve25519"}, upload(first))
		require.Equal(t, []string{"signed_curve25519"}, upload(nil))
		// One-time keys must be claimed before the reusable fallback key.
		var oneTimeRes api.PerformUploadKeysResponse
		require.NoError(t, a.PerformUploadKeys(ctx, &api.PerformUploadKeysRequest{UserID: userID, DeviceID: deviceID, OneTimeKeys: []api.OneTimeKeys{{UserID: userID, DeviceID: deviceID, KeyJSON: map[string]json.RawMessage{"signed_curve25519:one_time": json.RawMessage(`{"key":"one-time"}`)}}}}, &oneTimeRes))
		require.Nil(t, oneTimeRes.Error)
		require.Empty(t, oneTimeRes.KeyErrors)
		claim("signed_curve25519", "one_time")
		require.Equal(t, []string{"signed_curve25519"}, upload(nil))
		claim("signed_curve25519", "first")
		require.Empty(t, upload(nil))
		claim("signed_curve25519", "first")
		// Retrying the same upload must not mark a used key as unused again.
		require.Empty(t, upload(first))
		require.Equal(t, []string{"signed_curve25519"}, upload(map[string]json.RawMessage{"signed_curve25519:second": json.RawMessage(`{"key":"second","fallback":true}`)}))
		// Replacing one algorithm preserves the fallback key for other algorithms.
		require.ElementsMatch(t, []string{"curve25519", "signed_curve25519"}, upload(map[string]json.RawMessage{"curve25519:unsigned": json.RawMessage(`"unsigned"`)}))
		claim("signed_curve25519", "second")
		require.Equal(t, []string{"curve25519"}, upload(nil))
		claim("curve25519", "unsigned")
		require.Empty(t, upload(nil))
	})
}
