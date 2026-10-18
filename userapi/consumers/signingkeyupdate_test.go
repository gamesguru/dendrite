package consumers

import (
	"context"
	"encoding/json"
	"testing"

	"codefloe.com/pat-s/gomatrixserverlib/fclient"
	"codefloe.com/pat-s/gomatrixserverlib/spec"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"codefloe.com/pat-s/zendrite/userapi/api"
)

type uploadResult struct{ err *api.KeyError }

func (u uploadResult) PerformUploadDeviceKeys(_ context.Context, _ *api.PerformUploadDeviceKeysRequest, res *api.PerformUploadDeviceKeysResponse) {
	res.Error = u.err
}

// TestSigningKeyUpdateAcknowledgement checks that a stored update is
// acknowledged, so it is not uploaded again on every redelivery, and that a
// failed upload is refused so it is retried.
func TestSigningKeyUpdateAcknowledgement(t *testing.T) {
	data, err := json.Marshal(api.CrossSigningKeyUpdate{
		UserID:    "@bob:remote",
		MasterKey: &fclient.CrossSigningKey{UserID: "@bob:remote"},
	})
	require.NoError(t, err)
	for name, tc := range map[string]struct {
		err  *api.KeyError
		want bool
	}{
		"stored": {want: true},
		"failed": {err: &api.KeyError{Err: "database unavailable"}, want: false},
	} {
		t.Run(name, func(t *testing.T) {
			consumer := &SigningKeyUpdateConsumer{
				userAPI:           uploadResult{err: tc.err},
				isLocalServerName: func(name spec.ServerName) bool { return name == "localhost" },
			}
			message := &nats.Msg{Header: nats.Header{}, Data: data}
			message.Header.Set("origin", "remote")
			require.Equal(t, tc.want, consumer.onMessage(t.Context(), []*nats.Msg{message}))
		})
	}
}
